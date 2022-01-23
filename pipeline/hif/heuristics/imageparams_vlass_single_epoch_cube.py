import re
from typing import Union, Tuple, Optional

import numpy

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from .imageparams_base import ImageParamsHeuristics
from .imageparams_vlass_single_epoch_continuum import ImageParamsHeuristicsVlassSeContMosaic

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVlassSeCube(ImageParamsHeuristicsVlassSeContMosaic):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None, linesfile=None, imaging_params={}):
        super().__init__(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CUBE'
        self.vlass_stage = 3

    def reffreq(self) -> Optional[str]:
        """Tclean reffreq parameter heuristics.
        
        Default to None for CoarseCube/tclean (automatically calculated as the middle of the selected frequency range)
        """
        return None

    def mask(self, hm_masking=None, rootname=None, iteration=None, mask=None,
             results_list: Union[list, None] = None, clean_no_mask=None) -> Union[str, list]:
        """Tier-1 mask name to be used for computing Tier-1 and Tier-2 combined mask.

            Obtain the mask name from the latest MakeImagesResult object in context.results.
            If not found, then set empty string (as base heuristics)."""
        mask_list = ''
        if results_list and type(results_list) is list:
            for result in results_list:
                result_meta = result.read()
                if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                        'hifv_restorepims'):
                    mask_list = [r.mask_list for r in result_meta][0]

        # Add 'pb' string as a placeholder for cleaning without mask (pbmask only, see PIPE-977). This should
        # always stand at the last place in the mask list.
        # On request for first imaging stage (selfcal image) and automatically for the final imaging stage.
        if (clean_no_mask and self.vlass_stage == 1) or self.vlass_stage == 3:
            if clean_no_mask:
                LOG.info('Cleaning without user mask is performed in pre-self calibration imaging stage '
                         '(clean_no_mask_selfcal_image=True)')
            if type(mask_list) is list:
                mask_list.append('pb')
            elif mask_list != '':  # mask is non-empty string
                mask_list = [mask_list, 'pb']
            else:
                mask_list = 'pb'

        # In case hif_makeimages result was not found or results_list was not provided
        return mask_list

    def nterms(self, spwspec) -> Union[int, None]:
        """Tclean nterms parameter heuristics."""
        return 1

    def stokes(self) -> str:
        """Tclean stokes parameter heuristics."""
        return 'IQUV'
