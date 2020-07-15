import collections
import copy
import functools
import operator
import warnings
from typing import List

import numpy
import scipy.optimize

import pipeline.infrastructure.logging as logging
from . import mswrapper

LOG = logging.get_logger(__name__)

# used to associate ant/pol metadata with the amp/phase best fits
AntennaFit = collections.namedtuple('AntennaFit', 'ant pol amp phase')
# struct to hold best fit parameters for a linear model
LinearFitParameters = collections.namedtuple('LinearFitParameters', 'slope intercept')
# describes an outlier data selection with why it's an outlier, and by how much
Outlier = collections.namedtuple('Outlier', 'vis intent scan spw ant pol num_sigma reason')
# simple 2-tuple to hold a value and the uncertainty in that value
ValueAndUncertainty = collections.namedtuple('FitAndError', 'value unc')

# nsigma thresholds for marking deviating fits as an outlier
AMPLITUDE_SLOPE_THRESHOLD = 10
AMPLITUDE_INTERCEPT_THRESHOLD = 10
PHASE_SLOPE_THRESHOLD = 6.5
PHASE_INTERCEPT_THRESHOLD = 8.4


def score_all_scans(ms, intent: str, flag_all: bool = False) -> List[Outlier]:
    """
    Calculate best fits for amplitude vs frequency and phase vs frequency
    for time-averaged visibilities, score each fit by comparison against a
    reference value, and return outliers.

    Outliers are returned as a list of Outlier objects.

    By default, outliers are measured against a PWG-defined threshold for
    each fit type. Set flag_all to True to make classify all fits as outliers.
    This is useful when testing QA plugin score roll-up functionality.

    :param ms: MeasurementSet to process
    :param intent: data intent to process
    :param flag_all: (optional) True if all fits should be classified as
        outliers
    :return: outliers that deviate from a reference fit
    """
    outliers = []
    for scan in sorted(ms.get_scans(scan_intent=intent), key=operator.attrgetter('id')):
        spws = sorted([spw for spw in scan.spws if spw.type in ('FDM', 'TDM')],
                      key=operator.attrgetter('id'))
        for spw in spws:
            LOG.info('Applycal QA analysis: processing {} scan {} spw {}'.format(ms.basename, scan.id, spw.id))
            wrapper = mswrapper.MSWrapper.create_from_ms(ms.name, scan=scan.id, spw=spw.id)
            fits = get_best_fits_per_ant(wrapper)

            outlier_fn = functools.partial(Outlier,
                                           vis={ms.basename, },
                                           intent={intent, },
                                           spw={spw.id, },
                                           scan={scan.id, })

            outliers.extend(score_all(fits, outlier_fn, flag_all))

    return outliers


def calc_vk(wrapper):
    """
    Return a NumPy array containing time-averaged visibilities for each
    baseline in the input MSWrapper.

    :param wrapper: MSWrapper to process
    :return:
    """
    # find indices of all antennas
    antenna1 = set(wrapper['antenna1'])
    antenna2 = set(wrapper['antenna2'])
    all_antennas = antenna1.union(antenna2)

    # Sigma is a function of sqrt(num_antennas - 1). Calculate and cache this value now.
    root_num_antennas = numpy.sqrt(len(all_antennas) - 1)

    # columns in this list are omitted from V_k
    excluded_columns = ['antenna1', 'antenna2', 'corrected_phase', 'flag']

    # create a new dtype that adds 'antenna' and 'sigma' columns, filtering out columns we want to omit
    column_names = [c for c in wrapper.data.dtype.names if c not in excluded_columns]
    result_dtype = [mswrapper.get_dtype(wrapper.data, c) for c in column_names]
    result_dtype.insert(0, ('antenna', numpy.int32))
    result_dtype.append(('sigma', wrapper['corrected_data'].dtype, wrapper['corrected_data'].shape[1:]))

    # get 1D array of channel frequencies and include its definition in the dtype
    chan_freq = wrapper.freq_axis['chan_freq']
    chan_freq = chan_freq.swapaxes(0, 1)[0]
    result_dtype.append(('chan_freq', chan_freq.dtype, chan_freq.shape))

    # get 1D array of channel widths and include the column in the dtype
    resolution = wrapper.freq_axis['resolution']
    resolution = resolution.swapaxes(0, 1)[0]
    result_dtype.append(('resolution', resolution.dtype, resolution.shape))

    # new numpy array to hold visibilities V
    V = numpy.ma.empty((0,), dtype=result_dtype)

    for k in all_antennas:
        # create new row to hold all data for this antenna
        V_k = numpy.ma.empty((1,), dtype=V.data.dtype)

        # add antenna and channel frequencies to the row for this antenna
        V_k['antenna'] = k
        V_k['chan_freq'] = chan_freq
        V_k['resolution'] = resolution

        # Equation 2: sigma_{k}(nu_{i}) = std(V_{jk}(nu_{i}))_{j} / sqrt(n_{ant})
        # select all visibilities created using this antenna.
        V_jk = wrapper.xor_filter(antenna1=k, antenna2=k)
        sigma_k_real = V_jk['corrected_data'].real.std(axis=0) / root_num_antennas
        sigma_k_imag = V_jk['corrected_data'].imag.std(axis=0) / root_num_antennas
        V_k['sigma'] = sigma_k_real + 1j * sigma_k_imag

        # add the remaining columns
        for col in column_names:
            V_k[col] = V_jk[col].mean(axis=0)

        V = numpy.ma.concatenate((V, V_k), axis=0)

    return V


def get_best_fits_per_ant(wrapper):
    """
    Calculate and return the best amp/phase vs freq fits for data in the input
    MSWrapper.

    This function calculates an independent best fit per polarisation per
    antenna, returning a list of AntennaFit objects that characterise the fit
    parameters and fit uncertainties per fit.

    :param wrapper: MSWrapper to process
    :return: a list of AntennaFit objects
    """
    V_k = calc_vk(wrapper)
    corrected_data = V_k['corrected_data']
    sigma = V_k['sigma']

    num_antennas, num_pols, num_chans = corrected_data.shape

    all_fits = []

    for ant in range(num_antennas):
        frequencies = V_k['chan_freq'][ant]

        bandwidth = numpy.ma.max(frequencies) - numpy.ma.min(frequencies)
        band_midpoint = (numpy.ma.max(frequencies) + numpy.ma.min(frequencies)) / 2.0
        frequency_scale = 1.0 / bandwidth
        amp_model_fn = get_linear_function(band_midpoint, frequency_scale)
        ang_model_fn = get_angular_linear_function(band_midpoint, frequency_scale)

        for pol in range(num_pols):
            visibilities = corrected_data[ant, pol, :]
            ta_sigma = sigma[ant, pol, :]

            if visibilities.count() == 0:
                LOG.info('Could not fit ant {} pol {}: data is completely flagged'.format(ant, pol))
                continue

            try:
                (amp_fit, amp_err) = get_amp_fit(amp_model_fn, frequencies, visibilities, ta_sigma)
                amplitude_fit = to_linear_fit_parameters(amp_fit, amp_err)
            except TypeError:
                # Antenna probably flagged..
                LOG.info('Could not fit amplitude vs frequency for ant {} pol {}'.format(ant, pol))
                continue

            try:
                (phase_fit, phase_err) = get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, ta_sigma)
                phase_fit = to_linear_fit_parameters(phase_fit, phase_err)
            except TypeError:
                # Antenna probably flagged..
                LOG.info('Could not fit phase vs frequency for ant {} pol {}'.format(ant, pol))
                continue

            fit_obj = AntennaFit(ant=ant, pol=pol, amp=amplitude_fit, phase=phase_fit)
            all_fits.append(fit_obj)

    return all_fits


def score_all(all_fits, outlier_fn, flag_all: bool = False):
    """
    Compare and score the calculated best fits based on how they deviate from
    a reference value.

    Setting the test argument flag_all to True sets all fits as outliers. This
    is useful for testing the QA score roll-up and summary functions in the QA
    plugin.

    :param all_fits:
    :param outlier_fn:
    :param flag_all: True if all fits should be classed as outliers
    :return:
    """
    amp_slope_threshold = 0.0 if flag_all else AMPLITUDE_SLOPE_THRESHOLD
    amp_intercept_threshold = 0.0 if flag_all else AMPLITUDE_INTERCEPT_THRESHOLD
    phase_slope_threshold = 0.0 if flag_all else PHASE_SLOPE_THRESHOLD
    phase_intercept_threshold = 0.0 if flag_all else PHASE_INTERCEPT_THRESHOLD

    outliers = []
    outliers.extend(score_amp_slope(all_fits, outlier_fn, amp_slope_threshold))
    outliers.extend(score_amp_intercept(all_fits, outlier_fn, amp_intercept_threshold))
    outliers.extend(score_phase_slope(all_fits, outlier_fn, phase_slope_threshold))
    outliers.extend(score_phase_intercept(all_fits, outlier_fn, phase_intercept_threshold))

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


def score_amp_slope(all_fits, outlier_fn, sigma_threshold):
    """
    For all amplitude vs frequency fits, score the slope of the fit against
    the slope of the median best fit, marking fits that deviate by
    sigma_threshold from the median dispersion as outliers. Identified
    outliers are returned as a list of Outlier object returned by the
    outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    return score_X_vs_freq_fits(all_fits, 'amp.slope', get_median_fit, outlier_fn, sigma_threshold)


def score_amp_intercept(all_fits, outlier_fn, sigma_threshold):
    """
    Score the intercept of the best fit against the intercept of the median
    best fit, marking fits that deviate by sigma_threshold from the median
    dispersion as outliers. Identified outliers are returned as a list of
    Outlier object returned by the outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    return score_X_vs_freq_fits(all_fits, 'amp.intercept', get_median_fit, outlier_fn, sigma_threshold)


def score_phase_slope(all_fits, outlier_fn, sigma_threshold):
    """
    For all phase vs frequency fits, score the slope of the fit against the
    slope of the median best fit, marking fits that deviate by sigma_threshold
    from the median dispersion as outliers. Identified outliers are returned
    as a list of Outlier object returned by the outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    # phase score is calculated as deviation from zero
    return score_X_vs_freq_fits(all_fits, 'phase.slope', PHASE_REF_FN, outlier_fn, sigma_threshold)


def score_phase_intercept(all_fits, outlier_fn, sigma_threshold):
    """
    For all phase vs frequency fits, score the intercept of the fit against
    the intercept of the median best fit, marking fits that deviate by
    sigma_threshold from the median dispersion as outliers. Identified
    outliers are returned as a list of Outlier object returned by the
    outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    # phase score is calculated as deviation from zero
    return score_X_vs_freq_fits(all_fits, 'phase.intercept', PHASE_REF_FN, outlier_fn, sigma_threshold)


def score_X_vs_freq_fits(all_fits, attr, ref_value_fn, outlier_fn, sigma_threshold):
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
    outlier_fn = functools.partial(outlier_fn, reason={reason, })

    accessor = operator.attrgetter(attr)
    return score_fits(all_fits, ref_value_fn, accessor, outlier_fn, sigma_threshold)


def score_fits(all_fits, reference_value_fn, accessor, outlier_fn, sigma_threshold):
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
    :return: list of Outliers
    """
    outliers = []

    pols = {f.pol for f in all_fits}
    for pol in pols:
        pol_fits = [f for f in all_fits if f.pol == pol]

        # get reference val. Usually median, could be zero for phase
        reference_val, reference_sigma = reference_value_fn(pol_fits, accessor)

        for fit in pol_fits:
            ant = fit.ant
            unc = accessor(fit).unc
            value = accessor(fit).value
            this_sigma = numpy.sqrt(reference_sigma ** 2 + unc ** 2)
            num_sigma = numpy.abs((value - reference_val) / this_sigma)

            if num_sigma > sigma_threshold:
                outlier = outlier_fn(ant={ant, }, pol={pol, }, num_sigma=num_sigma)
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
    amp = numpy.ma.abs(visibilities)
    # angle of complex argument, in radians
    sigma_amp = numpy.ma.sqrt((visibilities.real * sigma.real) ** 2 + (visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = numpy.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (visibilities.real * sigma.imag) ** 2) / (
            amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = numpy.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_amp = amp.data[~mask]
    trimmed_sigma_amp = sigma_amp.data[~mask]

    Cinit = numpy.ma.median(trimmed_amp)

    amp_fit, amp_cov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, trimmed_amp,
                                                p0=[0.0, Cinit], sigma=trimmed_sigma_amp, absolute_sigma=True)

    amp_err = numpy.sqrt(numpy.diag(amp_cov))

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
    amp = numpy.ma.abs(visibilities)
    phase = numpy.ma.angle(visibilities)

    zeroamp = (amp.data <= 0.0)
    amp.mask[zeroamp] = True
    phase.mask[zeroamp] = True

    sigma_amp = numpy.ma.sqrt((visibilities.real * sigma.real) ** 2 + (
            visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = numpy.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (
            visibilities.real * sigma.imag) ** 2) / (amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = numpy.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_phase = phase.data[~mask]
    trimmed_sigma_phase = sigma_phase.data[~mask]

    phi_init = numpy.ma.median(trimmed_phase)

    # normalise visibilities by amplitude to fit linear angular model
    normalised_visibilities = numpy.ma.divide(visibilities, amp)
    normalised_sigma = numpy.ma.divide(sigma, amp)

    ang_fit_res = fit_angular_model(ang_model_fn, frequencies, normalised_visibilities, normalised_sigma)

    # Detrend phases using fit
    detrend_model = ang_model_fn(frequencies, -ang_fit_res['x'][0], -ang_fit_res['x'][1])
    detrend_data = normalised_visibilities * detrend_model
    detrend_phase = numpy.ma.angle(detrend_data)[~mask]

    # Refit phases to obtain errors from the same curve_fit method
    zerophasefit, phasecov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, detrend_phase,
                                                      p0=[0.0, phi_init - ang_fit_res['x'][1]],
                                                      sigma=trimmed_sigma_phase, absolute_sigma=True)
    # Final result is detrending model + new fit (close to zero)
    phase_fit = ang_fit_res['x'] + zerophasefit

    phase_err = numpy.sqrt(numpy.diag(phasecov))

    return phase_fit, phase_err


def get_linear_function(midpoint, x_scale):
    """
    Return a scaled linear function (a function of slope and intercept).

    :param midpoint:
    :param x_scale:
    :return:
    """

    def f(x, slope, intercept):
        return numpy.ma.multiply(
            numpy.ma.multiply(slope, numpy.ma.subtract(x, midpoint)),
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
        return numpy.ma.exp(1j * linear_fn(x, slope, intercept))

    return f


def get_chi2_ang_model(angular_model, nu, omega, phi, angdata, angsigma):
    m = angular_model(nu, omega, phi)
    diff = angdata - m
    aux = (numpy.square(diff.real / angsigma.real) + numpy.square(diff.imag / angsigma.imag))[~angdata.mask]
    return float(numpy.sum(aux.real))


def fit_angular_model(angular_model, nu, angdata, angsigma):
    f_aux = lambda omega_phi: get_chi2_ang_model(angular_model, nu, omega_phi[0], omega_phi[1], angdata, angsigma)
    angle = numpy.ma.angle(angdata[~angdata.mask])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', numpy.ComplexWarning)
        phi_init = numpy.ma.median(angle)
    fitres = scipy.optimize.minimize(f_aux, numpy.array([0.0, phi_init]), method='L-BFGS-B')
    return fitres


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
    mu = numpy.median(a)
    sigma = (1.0 / bn) * madfactor * numpy.median(numpy.abs(a - mu))
    return mu, sigma


def consolidate_data_selections(outliers):
    """
    Consolidate a list of Outliers into a smaller set of equivalent Outliers
    by consolidating their data selection arguments.

    This function works by merging Outliers that have the same list of
    reasons.

    :param outliers: an iterable of Outliers
    :return: an equivalent consolidated list of Outliers
    """
    # dict mapping an reason hash to the reason itself:
    hash_to_reason = {}
    # dict mapping from object hash to corresponding list of Outliers
    reason_hash_to_outliers = collections.defaultdict(list)

    # create our maps of hashes, which we need to test for overlapping data
    # selections
    for outlier in outliers:
        # create a tuple, as lists are not hashable
        reason_hash = tuple([hash(reason) for reason in outlier.reason])
        reason_hash_to_outliers[reason_hash].append(outlier)

        if reason_hash not in hash_to_reason:
            hash_to_reason[reason_hash] = outlier.reason

    # dict that maps holds accepted data selections and their reasons
    accepted = {}
    for reason_hash, outliers in reason_hash_to_outliers.items():
        # assemble the other outliers which we will compare for conflicts
        other_outliers = []
        for v in [v for k, v in reason_hash_to_outliers.items() if k != reason_hash]:
            other_outliers.extend(v)

        for outlier_to_merge in outliers:
            if reason_hash not in accepted:
                # first time round for this outlier, therefore it can always
                # be added as there will be nothing to merge
                accepted[reason_hash] = [copy.deepcopy(outlier_to_merge)]
                continue

            for existing_outlier in accepted[reason_hash]:
                proposed_outlier = copy.deepcopy(existing_outlier)

                proposed_outlier.vis.update(outlier_to_merge.vis)
                proposed_outlier.intent.update(outlier_to_merge.intent)
                proposed_outlier.spw.update(outlier_to_merge.spw)
                proposed_outlier.scan.update(outlier_to_merge.scan)
                proposed_outlier.ant.update(outlier_to_merge.ant)
                proposed_outlier.pol.update(outlier_to_merge.pol)

                # if the merged outlier does not conflict with any of the
                # explicitly registered outliers that require a different
                # reason, then it is safe to add the merged outlier and
                # discard the unmerged data selection
                if not any((data_selection_contains(proposed_outlier, other) for other in other_outliers)):
                    LOG.trace('No conflicting outlier detected')
                    LOG.trace('Accepting merged outlier: {!s}'.format(proposed_outlier))
                    LOG.trace('Discarding unmerged outlier: {!s}'.format(outlier_to_merge))
                    accepted[reason_hash] = [proposed_outlier]
                    break

            else:
                # we get here if all of the proposed outliers conflict with
                # the outlier in hand. In this case, it should be added as it
                # stands, completely unaltered.
                LOG.trace('Merged outlier conflicts with other registrations')
                LOG.trace('Abandoning proposed outlier: {!s}'.format(proposed_outlier))
                LOG.trace('Appending new unmerged outlier: {!s}'.format(outlier_to_merge))
                unmergeable = outlier_to_merge
                accepted[reason_hash].append(unmergeable)

    # dict values are lists, which we need to flatten into a single list
    result = []
    for l in accepted.values():
        result.extend(l)
    return result


def data_selection_contains(proposed, ds_args):
    """
    Return True if one data selection is contained within another.

    :param proposed: data selection 1
    :param ds_args: data selection 2
    :return: True if data selection 2 is contained within data selection 1
    """
    return all([not proposed.vis.isdisjoint(ds_args.vis),
                not proposed.intent.isdisjoint(ds_args.intent),
                not proposed.scan.isdisjoint(ds_args.scan),
                not proposed.spw.isdisjoint(ds_args.spw),
                not proposed.ant.isdisjoint(ds_args.ant),
                not proposed.pol.isdisjoint(ds_args.pol)])


# The function used to create the reference value for phase vs frequency best fits
# Select this to compare fits against the median
PHASE_REF_FN = get_median_fit
# Select this to compare fits against zero
# PHASE_REF_FN = lambda _, __: ValueAndUncertainty(0, 0)
