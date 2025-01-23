import collections
import functools
import operator
import warnings
from typing import Callable

import numpy as np
import scipy.optimize

import pipeline.infrastructure.logging as logging
from ..ampphase_vs_freq_qa import get_median_fit, to_linear_fit_parameters, get_amp_fit, get_phase_fit, PHASE_REF_FN, \
    get_chi2_ang_model, get_linear_function, get_angular_linear_function, LinearFitParameters, ValueAndUncertainty, \
    AMPLITUDE_SLOPE_THRESHOLD, AMPLITUDE_INTERCEPT_THRESHOLD, PHASE_SLOPE_THRESHOLD, PHASE_INTERCEPT_THRESHOLD

LOG = logging.get_logger(__name__)

# TODO TBC: is 1Gb in original
MEMORY_CHUNK_SIZE = 8  # Size of the memory chunk when loading the MS (in GB)

# AntennaFit is used to associate ant/pol metadata with the amp/phase best fits
AntennaFit = collections.namedtuple(
    'AntennaFit',
    ['spw', 'scan', 'ant', 'pol', 'amp', 'phase']
)

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
                LOG.info('Could not fit ant {} pol {}: data is completely flagged'.format(ant, pol))
                continue
            median_sn = np.ma.median(np.ma.abs(visibilities).real / np.ma.abs(ta_sigma).real)
            if median_sn > 3:  # PIPE-401: Check S/N and either fit or use average
                # Fit the amplitude
                try:
                    amp_fit, amp_err = get_amp_fit(amp_model_fn, frequencies, visibilities, ta_sigma)
                    amplitude_fit = to_linear_fit_parameters(amp_fit, amp_err)
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (high S/N; amp. vs frequency)'.format(
                        ant, pol))
                    continue
                # Fit the phase
                try:
                    phase_fit, phase_err = get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, ta_sigma)
                    phase_fit = to_linear_fit_parameters(phase_fit, phase_err)
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (high S/N; phase vs frequency)'.format(
                        ant, pol))
                    continue
            else:
                LOG.debug('Low S/N for ant {} pol {}'.format(ant, pol))
                # 'Fit' the amplitude
                try:  # NOTE: PIPE-401 This try block may not be necessary
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
                        LOG.info(
                            'Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(
                                ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info(
                        'Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(
                            ant, pol))
                    continue
                # 'Fit' the phase
                try:  # NOTE: PIPE-401 This try block may not be necessary
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
                        LOG.info(
                            'Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(
                                ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(
                        ant, pol))
                    continue

            fit_obj = AntennaFit(spw=wrapper.spw, scan=stdListStr(wrapper.scan), ant=ant, pol=pol, amp=amplitude_fit, phase=phase_fit)
            all_fits.append(fit_obj)

    return all_fits


def score_all(all_fits, outlier_fn, unitfactor, flag_all: bool = False):
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
        # TODO only difference with original is passing of unitdicts in the following call
        scores = score_X_vs_freq_fits(all_fits, v[0], v[1], outlier_fn, threshold, unitfactor)
        outliers.extend(scores)

    return outliers


def score_X_vs_freq_fits(all_fits, attr, ref_value_fn, outlier_fn, sigma_threshold, unitfactor):
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
    outliers = score_fits(all_fits, ref_value_fn, accessor, outlier_fn_wreason, sigma_threshold, unitfactor)

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


def score_fits(all_fits, reference_value_fn, accessor, outlier_fn, sigma_threshold, unitfactor):
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
    spws = {f.spw for f in all_fits}
    if len(spws) > 1:
        raise ValueError(f"Multiple SPWs detected: {spws}. All fits must belong to the same SPW.")
    if not all_fits:
        LOG.debug('No fits to evaluate')
        return []

    this_spw = spws.pop()
    pols = {f.pol for f in all_fits}
    ants = {f.ant for f in all_fits}

    # Get accessor metric name
    this_metric = _get_metric_name_from_accessor(accessor)

    # Calculate normalization factors
    normfactor = _calculate_normalisation_factors(
        all_fits, pols, this_spw, this_metric, unitfactor
    )

    # Calculate metric fits
    data_buffer = _create_data_buffer(all_fits, pols, ants, accessor, reference_value_fn, normfactor)

    # Calculate combined polarization fits (I)
    last_pol = sorted(pols).pop() if pols else None
    data_buffer_i = _calculate_combined_polarization_data(
        pols, ants, data_buffer, this_metric, normfactor, last_pol
    )

    # Detect outliers
    return _detect_outliers(
        pols, ants, data_buffer, data_buffer_i,
        sigma_threshold, delta_physical_limit,
        this_metric, outlier_fn
    )


def _create_data_buffer(all_fits, pols, ants, accessor, reference_value_fn, normfactor):
    """Creates structured data buffer with fit information."""
    median_cor_factor = np.sqrt(np.pi / 2)
    buffer = collections.defaultdict(dict)

    for pol in pols:
        pol_fits = [f for f in all_fits if f.pol == pol]
        n_antennas = len(pol_fits)

        # get reference val. Usually median, could be zero for phase
        reference_val, sigma_sample = reference_value_fn(pol_fits, accessor)
        ref_sigma = median_cor_factor * sigma_sample / np.sqrt(n_antennas)

        for fit in pol_fits:
            ant = fit.ant
            fit_param = accessor(fit)
            value = fit_param.value
            unc = fit_param.unc

            this_sigma = np.sqrt(ref_sigma ** 2 + unc ** 2)
            raw_sigma = (value - reference_val) / this_sigma
            delta_physical = np.abs(value - reference_val) * normfactor[pol]

            buffer[pol][ant] = {
                'value': value,
                'num_sigma': raw_sigma,
                'refval': reference_val,
                'this_sigma': this_sigma,
                'delta_physical': delta_physical,
                'masked': False
            }

        # Flag any antenna that doesn't exist in the list of fits
        for ant in ants - buffer[pol].keys():
            buffer[pol][ant] = _create_masked_entry()

    return buffer


def _calculate_combined_polarization_data(pols, ants, data_buffer, metric, normfactor, last_pol):
    """Calculates combined polarization data (I) where applicable."""
    buffer_i = {}

    for ant in ants:
        values, sigmas, refs = [], [], []

        for pol in pols:
            if ant in data_buffer[pol]:
                entry = data_buffer[pol][ant]
                values.append(entry['value'])
                sigmas.append(entry['this_sigma'])
                refs.append(entry['refval'])

        # If these are Amplitude intercepts, determine value for combined
        # polarization XX+YY
        if metric == 'amp_intercept' and len(values) == len(pols):
            avg_value = np.mean(values)
            avg_ref = np.mean(refs)
            combined_sigma = 1 / np.sqrt(sum(1 / np.square(sigmas)))
            raw_sigma = (avg_value - avg_ref) / combined_sigma
            delta_physical = abs(avg_value - avg_ref) * normfactor[last_pol]

            buffer_i[ant] = {
                'value': avg_value,
                'num_sigma': raw_sigma,
                'refval': avg_ref,
                'this_sigma': combined_sigma,
                'delta_physical': delta_physical,
                'masked': False
            }

        # if not relevant for this metric, just fill in NANs and a masked
        # value
        else:
            buffer_i[ant] = _create_masked_entry()

    return buffer_i


def _calculate_normalisation_factors(all_fits, pols, spw, metric, unitfactor) -> dict:
    """Calculates normalisation factors for different metrics."""
    factors = {}

    # For amp slope and amp intercept metric, get median flux to calculate %
    # or %/GHz
    if metric in {'amp_slope', 'amp_intercept'}:
        med_amp_accessor = operator.attrgetter('amp.intercept')
        for pol in pols:
            pol_fits = [f for f in all_fits if f.pol == pol]
            median_flux = get_median_fit(pol_fits, med_amp_accessor).value
            factors[pol] = unitfactor[spw][metric] / median_flux

    # For phase slope and phase intercept metrics, just include the factor in
    # the units dictionary. This leaves the "physical values" in deg/GHz and
    # deg, respectively.
    elif metric in {'phase_slope', 'phase_intercept'}:
        for pol in pols:
            factors[pol] = unitfactor[spw][metric]

    else:
        print(f'Unknown metric: {metric}! Using unit factor 1.0')
        for pol in pols:
            factors[pol] = 1.0

    return factors


def _get_metric_name_from_accessor(accessor: Callable) -> str:
    """Extracts metric name from accessor function."""
    return ((accessor.__str__()).split("'")[1]).replace('.', '_')


def _detect_outliers(pols, ants, data_buffer, data_buffer_i, sigma_thresh, delta_lim, metric, outlier_fn):
    """Identifies outliers based on statistical thresholds."""
    outliers = []

    # Create list of outliers evaluating per polarization for all metrics,
    # plus I in the case of amplitude
    for ant in ants:
        for pol in pols:
            entry = data_buffer[pol][ant]
            i_entry = data_buffer_i[ant]

            if entry['masked']:
                continue

            # First evaluate if this is an outlier in the individual
            # polarizations, including both relative and absolute deviation
            # criteria
            sigma_condition = abs(entry['num_sigma']) > sigma_thresh
            delta_condition = entry['delta_physical'] > delta_lim[metric]
            is_outlier = sigma_condition and delta_condition

            # Additionally, if this antenna has an amp offset outlier, see if
            # this is an outlier in the combined polarization XX+YY too
            is_i_outlier = False
            if metric == 'amp_intercept' and not i_entry['masked']:
                i_sigma_cond = abs(i_entry['num_sigma']) > sigma_thresh
                i_delta_cond = i_entry['delta_physical'] > delta_lim[metric]
                is_i_outlier = i_sigma_cond and i_delta_cond

            if is_outlier:
                outlier = outlier_fn(
                    ant={ant},
                    pol={pol},
                    num_sigma=entry['num_sigma'],
                    delta_physical=entry['delta_physical'],
                    amp_freq_sym_off=is_i_outlier
                )
                outliers.append(outlier)

    return outliers


def _create_masked_entry():
    """Helper to create a masked data entry."""
    return {
        'value': np.nan,
        'num_sigma': np.nan,
        'refval': np.nan,
        'this_sigma': np.nan,
        'delta_physical': np.nan,
        'masked': True
    }

def fit_angular_model(angular_model, nu, angdata, angsigma):
    f_aux = lambda omega_phi: get_chi2_ang_model(angular_model, nu, omega_phi[0], omega_phi[1], angdata, angsigma)
    angle = np.ma.angle(angdata[~angdata.mask])
    # TODO only difference with original is wrapping in catch_warnings and simplefilter
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', np.exceptions.ComplexWarning)
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


def stdListStr(thislist):
    if (type(thislist) == list) or (type(thislist) == np.ndarray):
        return str(sorted(thislist)).replace(' ','').replace(',','_').replace('[','').replace(']','')
    else:
        return str(thislist).replace(' ','').replace(',','_').replace('[','').replace(']','')

