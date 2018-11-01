from __future__ import absolute_import

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class ManualMaskThresholdSequence(BaseCleanSequence):

    def __init__(self, multiterm=None, gridder='', threshold='0.0mJy', sensitivity=0.0, niter=0, mask='',
                 channel_rms_factor=1.0):
        """Constructor.
        """
        BaseCleanSequence.__init__(self, multiterm, gridder, threshold, sensitivity, niter)
        self.mask = mask
        self.channel_rms_factor = channel_rms_factor

    def iteration(self, new_cleanmask, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None, keep_iterating=None):

        if self.iter is None:
            raise Exception('no data for iteration')

        elif self.iter == 0:
            tbTool = casatools.table
            tbTool.open(self.mask)
            tbTool.copy(new_cleanmask)
            tbTool.done()
            LOG.info('Copyied {} to {}'.format(self.mask, new_cleanmask))

            self.result.cleanmask = new_cleanmask
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
            self.result.iterating = True

        else:
            self.result.cleanmask = ''
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
            self.result.iterating = False

        return self.result
