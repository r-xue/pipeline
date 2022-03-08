import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from .basecleansequence import BaseCleanSequence

LOG = infrastructure.get_logger(__name__)


class VlassMaskThresholdSequence(BaseCleanSequence):

    def __init__(self, multiterm=None, gridder='', threshold='0.0mJy', sensitivity=0.0, niter=0, mask='',
                 channel_rms_factor=1.0):
        """Constructor.
        """
        BaseCleanSequence.__init__(self, multiterm, gridder, threshold, sensitivity, niter)
        self.mask = mask
        self.channel_rms_factor = channel_rms_factor

    def iteration(self, new_cleanmask=None, pblimit_image=-1, pblimit_cleanmask=-1, spw=None, frequency_selection=None,
                  iteration=None):

        if iteration is None:
            raise Exception('no data for iteration')

        if iteration in [1, 2] and new_cleanmask not in ['pb', '']:

            # VLASS-SE-CONT tier-2 masking stage results in two element list for self.mask
            iter_mask = self.mask[iteration-1] if type(self.mask) is list else self.mask
            LOG.info('Copying {} to {}'.format(iter_mask, new_cleanmask))
            tbTool = casa_tools.table
            tbTool.open(iter_mask)
            tbTool.copy(new_cleanmask)
            tbTool.done()

            self.result.cleanmask = new_cleanmask
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter
        else:
            # Special case cleaning without mask if new_cleanmask is 'pb'
            self.result.cleanmask = 'pb' if new_cleanmask == 'pb' else ''
            self.result.threshold = self.threshold
            self.result.sensitivity = self.sensitivity
            self.result.niter = self.niter

        return self.result
