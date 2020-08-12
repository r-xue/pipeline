import pipeline.infrastructure as infrastructure
from .imageparams_alma import ImageParamsHeuristicsALMA

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsALMASRDP(ImageParamsHeuristicsALMA):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        super().__init__(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'ALMA-SRDP'

    def uvtaper(self, beam_natural=None, protect_long=3):
        """Adjustment of uvtaper parameter based on desired resolution or representative baseline length."""

        # Re-enable imaging_params value for PIPE-708 (set only in hifas_imageprecheck)
        if 'uvtaper' in self.imaging_params:
            uvtaper = self.imaging_params['uvtaper']
            LOG.info('ALMA uvtaper heuristics: Using imageprecheck value of uvtaper=%s' % (str(uvtaper)))
            return uvtaper

        # Disabled heuristic for ALMA Cycle 6
        return []

