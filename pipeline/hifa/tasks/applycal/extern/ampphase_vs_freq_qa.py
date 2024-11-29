import collections
import functools
import operator
import warnings

import numpy as np
import scipy.optimize

MEMORY_CHUNK_SIZE = 8  # Size of the memory chunk when loading the MS (in GB)


# AntennaFit is used to associate ant/pol metadata with the amp/phase best fits
AntennaFit = collections.namedtuple(
    'AntennaFit',
    ['spw', 'scan', 'ant', 'pol', 'amp', 'phase']
)

# LinearFitParameters is a struct to hold best fit parameters for a linear model
LinearFitParameters = collections.namedtuple(
    'LinearFitParameters',
    ['slope', 'intercept']
)

# ValueAndUncertainty is a simple 2-tuple to hold a value and the uncertainty in that value
ValueAndUncertainty = collections.namedtuple(
    'ValueAndUncertainty',
    ['value', 'unc']
)


# nsigma thresholds for marking deviating fits as an outlier
AMPLITUDE_SLOPE_THRESHOLD = 25.0
AMPLITUDE_INTERCEPT_THRESHOLD = 53.0
PHASE_SLOPE_THRESHOLD = 40.0
PHASE_INTERCEPT_THRESHOLD = 60.5

#Threholds for physical deviations
AMPLITUDE_SLOPE_PHYSICAL_THRESHOLD = 0.05     # in %/GHz (0.05=5%/GHz)
AMPLITUDE_INTERCEPT_PHYSICAL_THRESHOLD = 0.1  # in % (0.1=10%)
PHASE_SLOPE_PHYSICAL_THRESHOLD = 3.0          # in deg/GHz (3.0=3deg/GHz)
PHASE_INTERCEPT_PHYSICAL_THRESHOLD = 6.0      # in deg (6.0=6deg)

#Dictionary to reference the thresholds above
delta_physical_limit = {"amp_slope": AMPLITUDE_SLOPE_PHYSICAL_THRESHOLD,
                        "amp_intercept": AMPLITUDE_INTERCEPT_PHYSICAL_THRESHOLD,
                        "phase_slope": PHASE_SLOPE_PHYSICAL_THRESHOLD,
                        "phase_intercept": PHASE_INTERCEPT_PHYSICAL_THRESHOLD}

def get_best_fits_per_ant(wrapper,frequencies):
    """
    Calculate and return the best amp/phase vs freq fits for data in the input
    MSWrapper.

    This function calculates an independent best fit per polarisation per
    antenna, returning a list of AntennaFit objects that characterise the fit
    parameters and fit uncertainties per fit.

    :param wrapper: MSWrapper to process
    :return: a list of AntennaFit objects
    """
    data = wrapper.V

    t_avg = data['t_avg']
    t_sigma = data['t_sigma']

    num_antennas, _, num_chans = t_avg.shape
    # Filter cross-pol data
    pol_indices = tuple(np.where((wrapper.corr_axis=='XX') | (wrapper.corr_axis=='YY'))[0])

    all_fits = []

    for ant in range(num_antennas):
        bandwidth = np.ma.max(frequencies) - np.ma.min(frequencies)
        band_midpoint = (np.ma.max(frequencies) + np.ma.min(frequencies)) / 2.0
        frequency_scale = 1.0 / bandwidth

        amp_model_fn = get_linear_function(band_midpoint, frequency_scale)
        ang_model_fn = get_angular_linear_function(band_midpoint, frequency_scale)

        for pol in pol_indices:
            visibilities = t_avg[ant, pol, :]
            ta_sigma = t_sigma[ant, pol, :]

            if visibilities.count() == 0:
                print('Could not fit ant {} pol {}: data is completely flagged'.format(ant, pol))
                continue
            median_sn = np.ma.median(np.ma.abs(visibilities).real / np.ma.abs(ta_sigma).real)
            if median_sn > 3:  # PIPE-401: Check S/N and either fit or use average
                # Fit the amplitude
                try:
                    amp_fit, amp_err = get_amp_fit(amp_model_fn, frequencies, visibilities, ta_sigma)
                    amplitude_fit = to_linear_fit_parameters(amp_fit, amp_err)
                except TypeError:
                    # Antenna probably flagged..
                    print('Could not fit phase vs frequency for ant {} pol {} (high S/N; amp. vs frequency)'.format(
                        ant, pol))
                    continue
                # Fit the phase
                try:
                    phase_fit, phase_err = get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, ta_sigma)
                    phase_fit = to_linear_fit_parameters(phase_fit, phase_err)
                except TypeError:
                    # Antenna probably flagged..
                    print('Could not fit phase vs frequency for ant {} pol {} (high S/N; phase vs frequency)'.format(
                        ant, pol))
                    continue
            else:
                print('Low S/N for ant {} pol {}'.format(ant, pol))
                # 'Fit' the amplitude
                try:  # nOTE: PIPE-401 This try block may not be necessary
                    amp_vis = np.ma.abs(visibilities)
                    n_channels_unmasked = np.sum(~amp_vis.mask)
                    if n_channels_unmasked != 0:
                        amplitude_fit = LinearFitParameters(
                            slope=ValueAndUncertainty(value=0., unc=1.0e06),
                            intercept=ValueAndUncertainty(
                                value=np.ma.median(amp_vis),
                                unc=np.ma.std(amp_vis)/np.sqrt(n_channels_unmasked))
                        )
                    else:
                        print('Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    print('Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(ant, pol))
                    continue
                # 'Fit' the phase
                try:  # nOTE: PIPE-401 This try block may not be necessary
                    phase_vis = np.ma.angle(visibilities)
                    n_channels_unmasked = np.sum(~phase_vis.mask)
                    if n_channels_unmasked != 0:
                        phase_fit = LinearFitParameters(
                            slope=ValueAndUncertainty(value=0., unc=1.0e06),
                            intercept=ValueAndUncertainty(
                                value=np.ma.median(phase_vis),
                                unc=np.ma.std(phase_vis)/np.sqrt(n_channels_unmasked)
                            )
                        )
                    else:
                        print('Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    print('Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(ant, pol))
                    continue

            fit_obj = AntennaFit(spw=wrapper.spw, scan=stdListStr(wrapper.scan), ant=ant, pol=pol, amp=amplitude_fit, phase=phase_fit)
            all_fits.append(fit_obj)

    return all_fits


def score_all(all_fits, outlier_fn, unitdicts, flag_all: bool = False):
    """
    Compare and score the calculated best fits based on how they deviate from
    a reference value.

    For all amplitude or slope vs frequency fits, score the slope or intercept
    of the fit against the slope or intercept of the median best fit or a value
    of 0, marking fits that deviate by sigma_threshold from the median dispersion
    as outliers. Identified outliers are returned as a list of Outlier object
    returned by the outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    Setting the test argument flag_all to True sets all fits as outliers. This
    is useful for testing the QA score roll-up and summary functions in the QA
    plugin.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param flag_all: True if all fits should be classed as outliers
    :return: list of Outlier objects
    """
    # Dictionary for the different cases to consider. Each is defined by a tuple
    #  with: attr, ref_value_fn, sigma_threshold
    score_definitions = {
        "amp_slope": ('amp.slope', get_median_fit, AMPLITUDE_SLOPE_THRESHOLD),
        "amp_intercept": ('amp.intercept', get_median_fit, AMPLITUDE_INTERCEPT_THRESHOLD),
        "phase_slope": ('phase.slope', PHASE_REF_FN, PHASE_SLOPE_THRESHOLD),
        "phase_intercept": ('phase.intercept', PHASE_REF_FN, PHASE_INTERCEPT_THRESHOLD),
    }

    outliers = []

    for k, v in score_definitions.items():
        threshold = 0.0 if flag_all else v[2]
        scores = score_X_vs_freq_fits(all_fits, v[0], v[1], outlier_fn, threshold, unitdicts)
        outliers.extend(scores)

    return outliers


def get_median_fit(all_fits, accessor):
    """
    Get the median best fit from a list of best fits.

    The accessor argument should be a function that, when given the list of
    all fits, returns fits of the desired type (amp.slope, phase.intercept,
    etc.)

    :param all_fits: mixed list of best fits
    :type: list of AntennaFit instances
    :param accessor: function to filter best fits
    :return: median value and uncertainty of fits of selected type
    """
    pol_slopes = [accessor(f) for f in all_fits]
    values = [f.value for f in pol_slopes]
    median, median_sigma = robust_stats(values)
    return ValueAndUncertainty(value=median, unc=median_sigma)


def score_X_vs_freq_fits(all_fits, attr, ref_value_fn, outlier_fn, sigma_threshold, unitdicts):
    """
    Score a set of best fits, comparing the fits identified by the 'attr'
    attribute against a reference value calculated by the ref_value_fn,
    marking outliers that deviate by more than sigma_threshold from this
    reference value as outliers, to be returned as Outlier objects created by
    the outlier_fn.

    :param all_fits: a list of fit parameters
    :param attr: identifier of the fits to consider, e.g., 'amp.slope'
    :param ref_value_fn: a function that takes a list of fits and returns a
        value to be used as a reference value in fit comparisons
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    # convert linear fit metadata to a reason that identifies this fit as
    # originating from this metric in a wider context, e.g., from 'amp.slope'
    # to 'amp_vs_freq.slope'
    y_axis, fit_parameter = attr.split('.')
    reason = f'{y_axis}_vs_freq.{fit_parameter}'
    outlier_fn_wreason = functools.partial(outlier_fn, reason={reason, })

    accessor = operator.attrgetter(attr)
    outliers = score_fits(all_fits, ref_value_fn, accessor, outlier_fn_wreason, sigma_threshold, unitdicts)

    # Check for >90deg phase offsets which should have extra QA messages
    if (y_axis == 'phase') and (fit_parameter == 'intercept'):
        for i in range(len(outliers)):
            if outliers[i].delta_physical > 90.0:
                outliers[i] = outlier_fn(ant=outliers[i].ant, pol=outliers[i].pol,
                                         num_sigma=outliers[i].num_sigma,
                                         delta_physical=outliers[i].delta_physical,
                                         amp_freq_sym_off=outliers[i].amp_freq_sym_off,
                                         reason={f'gt90deg_offset_{y_axis}_vs_freq.{fit_parameter}', })

    return outliers


def score_fits(all_fits, reference_value_fn, accessor, outlier_fn, sigma_threshold, unitdicts):
    """
    Score a list of best fit parameters against a reference value, identifying
    outliers as fits that deviate by more than sigma_threshold * std dev from
    the reference value.

    :param all_fits: list of AntennaFits
    :param reference_value_fn: function that returns a reference
        ValueAndUncertainty from a list of these objects
    :param accessor: function that returns one LinearFitParameters from an
        AntennaFit
    :param outlier_fn: function that returns an Outlier instance
    :param sigma_threshold: threshold nsigma deviation for comparisons
    :return: list of Outliers, dictionary of values for summary
    """
    median_cor_factor = np.sqrt(np.pi / 2.)  # PIPE-401: factor due to using the median instead of the mean
    (unitfactor, unitstr) = unitdicts
    pols = {f.pol for f in all_fits}
    spws = {f.spw for f in all_fits}
    ants = {f.ant for f in all_fits}
    if len(spws) == 1:
        thisspw = spws.pop()
    elif len(all_fits) == 0:
        print('No fits to evaluate here!')
        return []
    elif len(spws) > 1:
        print('Error: Not all antenna fits object belong to same SPW!!')
        print('***all_fits***')
        print(str(all_fits))
        print('spws: '+str(spws))
        return []
    else:
        print('Unknown error with antenna fits list!')
        return []

    #Get accessor metric name
    thismetric = ((accessor.__str__()).split("'")[1]).replace('.', '_')
    normfactor = {}
    #Get factors to convert deviation into requested physical units
    #If this metric is amp_slope/intercept), get median flux to calculate % or %/GHz
    if (thismetric == 'amp_slope') or (thismetric == 'amp_intercept'):
        medampaccessor = operator.attrgetter('amp.intercept')
        for pol in pols:
            pol_fits = [f for f in all_fits if f.pol == pol]
            normfactor[pol] = unitfactor[thisspw][thismetric]/(get_median_fit(pol_fits, medampaccessor).value)
    #If this metric is phase_slope/intercept, just include the factor in the units dictionary
    #this leaves the "physical values" in deg/GHz and deg, respectively.
    elif (thismetric == 'phase_slope') or (thismetric == 'phase_intercept'):
        for pol in pols:
            normfactor[pol] = unitfactor[thisspw][thismetric]
    else:
        print('Unknown metric: '+str(thismetric)+' !!! Setting units normalization to 1.0')
        for pol in pols:
            normfactor[pol] = 1.0

    #Gather information for assessing outliers
    outliers = []
    databuffer = {}
    for pol in pols:
        pol_fits = [f for f in all_fits if f.pol == pol]
        n_antennas = len(pol_fits)

        # get reference val. Usually median, could be zero for phase
        reference_val, sigma_sample = reference_value_fn(pol_fits, accessor)
        reference_sigma = median_cor_factor * sigma_sample / np.sqrt(n_antennas)

        databuffer[pol] = {}
        #Examine each antenna fit
        for fit in pol_fits:
            ant = fit.ant
            unc = accessor(fit).unc
            value = accessor(fit).value
            this_sigma = np.sqrt(reference_sigma ** 2 + unc ** 2)
            raw_sigma = (value - reference_val) / this_sigma
            num_sigma = np.abs(raw_sigma)
            delta_physical = np.abs(value - reference_val)*normfactor[pol]
            databuffer[pol][ant] = {'value': value, 'num_sigma': raw_sigma, 'refval': reference_val, 'this_sigma': this_sigma, 'delta_physical': delta_physical, 'masked': False}
    #Mark any antenna that doesn exist in the list of fits as flagged
    for pol in pols:
        for ant in ants:
            if not ((pol in databuffer.keys()) and (ant in databuffer[pol].keys())):
                databuffer[pol][ant] = {'value': np.nan, 'num_sigma': np.nan, 'refval': np.nan, 'this_sigma': np.nan, 'delta_physical': np.nan, 'masked': True}

    #If this are Amplitude intercepts, determine value for combined polarization XX+YY
    #if not relevant for this metric, just fill in NANs and a masked value
    databufferI = {}
    for ant in ants:
        antvalues = []
        antsigma = []
        antref = []
        for pol in pols:
            if (pol in databuffer.keys()) and (ant in databuffer[pol].keys()):
                antvalues.append(databuffer[pol][ant]['value'])
                antsigma.append(databuffer[pol][ant]['this_sigma'])
                antref.append(databuffer[pol][ant]['refval'])
        if (thismetric == 'amp_intercept') and (len(antvalues) == len(pols)):
            antIvalue = np.mean(antvalues)
            antIref = np.mean(antref)
            antIsigma = 1.0/np.sqrt(np.sum(1.0/np.square(antsigma)))
            raw_I_sigma = (antIvalue - antIref)/antIsigma
            num_I_sigma = np.abs(raw_I_sigma)
            delta_I_physical = np.abs(antIvalue - antIref)*normfactor[pol]
            databufferI[ant] = {'value': antIvalue, 'num_sigma': raw_I_sigma, 'refval': antIref, 'this_sigma': antIsigma, 'delta_physical': delta_I_physical, 'masked': False}
        else:
            databufferI[ant] = {'value': np.nan, 'num_sigma': np.nan, 'refval': np.nan, 'this_sigma': np.nan, 'delta_physical': np.nan, 'masked': True}
            

    #Create list of outliers evaluating both individual offset per polarization (for all metrics)
    #and both XX,YY and I in the case of Amplitude 
    for pol in pols:
        for ant in ants:
            #First evaluate if this is an outlier in the individual polarizations
            #including both relative and absolute deviation criteria
            isoutlier = (not databuffer[pol][ant]['masked']) and \
                        (np.abs(databuffer[pol][ant]['num_sigma']) > sigma_threshold) and \
                        (np.abs(databuffer[pol][ant]['delta_physical']) > delta_physical_limit[thismetric])
            #Additionally if for this antenna this is an Amp Offset outlier.
            #If not an Amp intercept metric, just leave that boolean as True
            isAmpIoutlier = (thismetric == 'amp_intercept') and \
                            (not databufferI[ant]['masked']) and \
                            (np.abs(databufferI[ant]['num_sigma']) > sigma_threshold) and \
                            (np.abs(databufferI[ant]['delta_physical']) > delta_physical_limit[thismetric])
            if isoutlier and isAmpIoutlier:
                outlier = outlier_fn(ant={ant, }, pol={pol, }, num_sigma=databuffer[pol][ant]['num_sigma'], delta_physical=databuffer[pol][ant]['delta_physical'], amp_freq_sym_off=False)
                outliers.append(outlier)
            elif isoutlier and (not isAmpIoutlier):
                outlier = outlier_fn(ant={ant, }, pol={pol, }, num_sigma=databuffer[pol][ant]['num_sigma'], delta_physical=databuffer[pol][ant]['delta_physical'], amp_freq_sym_off=True)
                outliers.append(outlier)

    return outliers


def to_linear_fit_parameters(fit, err):
    """
    Convert tuples from the best fit evaluation into a LinearFitParameters
    namedtuple.

    :param fit: 2-tuple of (slope, intercept) best-fit parameters
    :param err: 2-tuple of uncertainty in (slope, intercept) parameters
    :return:
    """
    return LinearFitParameters(slope=ValueAndUncertainty(value=fit[0], unc=err[0]),
                               intercept=ValueAndUncertainty(value=fit[1], unc=err[1]))


def get_amp_fit(amp_model_fn, frequencies, visibilities, sigma):
    """
    Fit a linear amplitude vs frequency model to a set of time-averaged
    visibilities.

    :param amp_model_fn: the amplitude linear model to optimise
    :param frequencies: numpy array of channel frequencies
    :param visibilities: numpy array of time-averaged visibilities
    :param sigma: numpy array of uncertainies in time-averaged visibilities
    :return: tuple of best fit params, uncertainty tuple
    """
    # calculate amplitude and phase from visibility, inc. std. deviations for each
    amp = np.ma.abs(visibilities)
    # angle of complex argument, in radians
    sigma_amp = np.ma.sqrt((visibilities.real * sigma.real) ** 2 + (visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = np.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (visibilities.real * sigma.imag) ** 2) / (
            amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = np.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_amp = amp.data[~mask]
    trimmed_sigma_amp = sigma_amp.data[~mask]

    Cinit = np.ma.median(trimmed_amp)

    amp_fit, amp_cov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, trimmed_amp,
                                                p0=[0.0, Cinit], sigma=trimmed_sigma_amp, absolute_sigma=True)

    amp_err = np.sqrt(np.diag(amp_cov))

    return amp_fit, amp_err


def get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, sigma):
    """
    Fit a linear model for phase vs frequency to a set of time-averaged
    visibilities.

    :param amp_model_fn: model function for amplitude
    :param ang_model_fn: model function for phase angle
    :param frequencies: numpy array of channel frequencies
    :param visibilities: numpy array of time-averaged visibilities
    :param sigma: numpy array of uncertainies in time-averaged visibilities
    :return: tuple of best fit params, uncertainty tuple
    """
    # calculate amplitude and phase from visibility, inc. std. deviations for each
    amp = np.ma.abs(visibilities)
    phase = np.ma.angle(visibilities)

    zeroamp = (amp.data <= 0.0)
    amp.mask[zeroamp] = True
    phase.mask[zeroamp] = True

    sigma_amp = np.ma.sqrt((visibilities.real * sigma.real) ** 2 + (
            visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = np.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (
            visibilities.real * sigma.imag) ** 2) / (amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = np.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_phase = phase.data[~mask]
    trimmed_sigma_phase = sigma_phase.data[~mask]

    phi_init = np.ma.median(trimmed_phase)

    # normalise visibilities by amplitude to fit linear angular model
    normalised_visibilities = np.ma.divide(visibilities, amp)
    normalised_sigma = np.ma.divide(sigma, amp)

    ang_fit_res = fit_angular_model(ang_model_fn, frequencies, normalised_visibilities, normalised_sigma)

    # Detrend phases using fit
    detrend_model = ang_model_fn(frequencies, -ang_fit_res['x'][0], -ang_fit_res['x'][1])
    detrend_data = normalised_visibilities * detrend_model
    detrend_phase = np.ma.angle(detrend_data)[~mask]

    # Refit phases to obtain errors from the same curve_fit method
    zerophasefit, phasecov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, detrend_phase,
                                                      p0=[0.0, phi_init - ang_fit_res['x'][1]],
                                                      sigma=trimmed_sigma_phase, absolute_sigma=True)
    # Final result is detrending model + new fit (close to zero)
    phase_fit = ang_fit_res['x'] + zerophasefit

    phase_err = np.sqrt(np.diag(phasecov))

    return phase_fit, phase_err


def get_linear_function(midpoint, x_scale):
    """
    Return a scaled linear function (a function of slope and intercept).

    :param midpoint:
    :param x_scale:
    :return:
    """

    def f(x, slope, intercept):
        return np.ma.multiply(
            np.ma.multiply(slope, np.ma.subtract(x, midpoint)),
            x_scale
        ) + intercept

    return f


def get_angular_linear_function(midpoint, x_scale):
    """
    Angular linear model to fit phases only.

    :param midpoint:
    :param x_scale:
    :return:
    """
    linear_fn = get_linear_function(midpoint, x_scale)

    def f(x, slope, intercept):
        return np.ma.exp(1j * linear_fn(x, slope, intercept))

    return f


def get_chi2_ang_model(angular_model, nu, omega, phi, angdata, angsigma):
    m = angular_model(nu, omega, phi)
    diff = angdata - m
    aux = (np.square(diff.real / angsigma.real) + np.square(diff.imag / angsigma.imag))[~angdata.mask]
    return float(np.sum(aux.real))


def fit_angular_model(angular_model, nu, angdata, angsigma):
    f_aux = lambda omega_phi: get_chi2_ang_model(angular_model, nu, omega_phi[0], omega_phi[1], angdata, angsigma)
    angle = np.ma.angle(angdata[~angdata.mask])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', np.ComplexWarning)
        phi_init = np.ma.median(angle)
    fitres = scipy.optimize.minimize(f_aux, np.array([0.0, phi_init]), method='L-BFGS-B')
    return fitres

def convert_to_amp_phase(visibilities, sigma):
    '''Calculate amplitude and phase from visibility's real and imaginary
    components, inc. std. deviations for each
    '''

    amp = np.ma.abs(visibilities)
    phase = np.ma.angle(visibilities)

    zeroamp = (amp.data <= 0.0)
    amp.mask[zeroamp] = True
    phase.mask[zeroamp] = True

    sigma_amp = np.ma.sqrt((visibilities.real * sigma.real) ** 2 + (
            visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = np.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (
            visibilities.real * sigma.imag) ** 2) / (amp ** 2)

    return (amp, sigma_amp, phase, sigma_phase)

def robust_stats(a):
    """
    Return median and estimate standard deviation
    of numpy array A using median statistics

    :param a:
    :return:
    """
    # correction factor for obtaining sigma from MAD
    madfactor = 1.482602218505602

    n = len(a)
    # Fitting parameters for sample size correction factor b(n)
    if n % 2 == 0:
        alpha = 1.32
        beta = -1.5
    else:
        alpha = 1.32
        beta = -0.9
    bn = 1.0 - 1.0 / (alpha * n + beta)
    mu = np.median(a)
    sigma = (1.0 / bn) * madfactor * np.median(np.abs(a - mu))
    return mu, sigma

def stdListStr(thislist):
    if (type(thislist) == list) or (type(thislist) == np.ndarray):
        return str(sorted(thislist)).replace(' ','').replace(',','_').replace('[','').replace(']','')
    else:
        return str(thislist).replace(' ','').replace(',','_').replace('[','').replace(']','')

# The function used to create the reference value for phase vs frequency best fits
# Select this to compare fits against the median
# PHASE_REF_FN = get_median_fit
# Select this to compare fits against zero
PHASE_REF_FN = lambda _, __: ValueAndUncertainty(0, 0)
