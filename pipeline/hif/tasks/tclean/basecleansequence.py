from __future__ import absolute_import

import pipeline.infrastructure as infrastructure
from pipeline.hif.heuristics import cleanbox as cbheuristic
from .resultobjects import BoxResult

LOG = infrastructure.get_logger(__name__)


class BaseCleanSequence:

    def __init__(self, multiterm=None, gridder='', threshold='0.0mJy', sensitivity=0.0, niter=0):
        """Constructor.
        """
        self.iter = None
        self.result = BoxResult()

        self.psf = None
        self.flux = None

        self.iters = []
        self.residuals = []
        self.cleanmasks = []
        self.model_sums = []
        self.residual_maxs = []
        self.residual_mins = []
        self.residual_non_cleanmask_rms_list = []
        self.image_non_cleanmask_rms_list = []
        self.image_non_cleanmask_rms_min_list = []
        self.image_non_cleanmask_rms_max_list = []
        self.thresholds = []
        self.multiterm = multiterm
        self.gridder = gridder
        self.threshold = threshold
        self.sensitivity = sensitivity
        self.dr_corrected_sensitivity = sensitivity
        self.niter = niter

    def iteration_result(self, iter, psf, model, restored, residual,
                         flux, cleanmask, threshold=None, pblimit_image=0.2, pblimit_cleanmask=0.3,
                         cont_freq_ranges=None):
        """This method sets the iteration counter and returns statistics for
        that iteration.
        """
        self.iter = iter

        self.psf = psf
        self.flux = flux

        model_sum, \
        residual_cleanmask_rms, \
        residual_non_cleanmask_rms, \
        residual_min, \
        residual_max, \
        nonpbcor_image_non_cleanmask_rms_min, \
        nonpbcor_image_non_cleanmask_rms_max, \
        nonpbcor_image_non_cleanmask_rms, \
        pbcor_image_min, pbcor_image_max, \
        residual_robust_rms = cbheuristic.analyse_clean_result(self.multiterm, model, restored,
                                                               residual, flux, cleanmask,
                                                               pblimit_image, pblimit_cleanmask,
                                                               cont_freq_ranges)

        peak_over_rms = residual_max/residual_robust_rms
        LOG.info('Residual peak: %s', residual_max)
        LOG.info('Residual scaled MAD: %s', residual_robust_rms)
        LOG.info('Residual peak / scaled MAD: %s', peak_over_rms)

        # Append the statistics.
        self.iters.append(iter)
        self.residuals.append(residual)
        self.cleanmasks.append(cleanmask)
        self.thresholds.append(threshold)
        self.model_sums.append(model_sum)
        self.residual_maxs.append(residual_max)
        self.residual_mins.append(residual_min)
        self.residual_non_cleanmask_rms_list.append(residual_non_cleanmask_rms)
        self.image_non_cleanmask_rms_list.append(nonpbcor_image_non_cleanmask_rms)
        self.image_non_cleanmask_rms_min_list.append(nonpbcor_image_non_cleanmask_rms_min)
        self.image_non_cleanmask_rms_max_list.append(nonpbcor_image_non_cleanmask_rms_max)

        return model_sum, \
               residual_cleanmask_rms, \
               residual_non_cleanmask_rms, \
               residual_min, \
               residual_max, \
               nonpbcor_image_non_cleanmask_rms_min, \
               nonpbcor_image_non_cleanmask_rms_max, \
               nonpbcor_image_non_cleanmask_rms, \
               pbcor_image_min, pbcor_image_max, \
               residual_robust_rms

    def iteration(self, new_cleanmask):
        """The base boxworker allows only one iteration.
        """

        return self.result
