import pipeline.infrastructure.logging as logging
from ..ampphase_vs_freq_qa import get_median_fit, PHASE_REF_FN, \
    AMPLITUDE_SLOPE_THRESHOLD, AMPLITUDE_INTERCEPT_THRESHOLD, PHASE_SLOPE_THRESHOLD, PHASE_INTERCEPT_THRESHOLD, \
    score_X_vs_freq_fits

LOG = logging.get_logger(__name__)


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
