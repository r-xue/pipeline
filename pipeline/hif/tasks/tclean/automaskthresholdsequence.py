from __future__ import absolute_import

import os

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure
from .basecleansequence import BaseCleanSequence
from .resultobjects import BoxResult

LOG = infrastructure.get_logger(__name__)


class AutoMaskThresholdSequence(BaseCleanSequence):

    def __init__(self, gridder, threshold='0.0mJy', sensitivity=0.0, niter=5000):
        """Constructor.
        """
        BaseCleanSequence.__init__(self)
        self.gridder = gridder
        self.threshold = threshold
        self.sensitivity = sensitivity
        self.dr_corrected_sensitivity = sensitivity
        self.niter = niter
        self.iter = None
        self.result = BoxResult()
        self.sidelobe_ratio = -1

    def iteration(self, new_cleanmask, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None):

        if self.iter is None:
            raise Exception, 'no data for iteration'

        elif self.iter == 0:
            self.result.cleanmask = new_cleanmask
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter =  self.niter
            self.result.iterating = True
        else:
            self.result.cleanmask = ''
            self.result.threshold = '0.0mJy'
            self.result.sensitivity = 0.0
            self.result.niter =  0
            self.result.iterating = False

        return self.result
