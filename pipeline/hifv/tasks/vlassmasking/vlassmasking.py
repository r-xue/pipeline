import os
import re
import shutil


import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

from pipeline.hifv.heuristics.vip_helper_functions import mask_from_catalog

LOG = infrastructure.get_logger(__name__)


class VlassmaskingResults(basetask.Results):
    def __init__(self):
        super(VlassmaskingResults, self).__init__()
        self.pipeline_casa_task = 'Vlassmasking'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'VlassmaskingResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'VlassmaskingResults:'


class VlassmaskingInputs(vdp.StandardInputs):
    phasecenter = vdp.VisDependentProperty(default='')
    vlass_ql_database = vdp.VisDependentProperty(default='/home/vlass/packages/VLASS1Q.fits')
    maskingmode = vdp.VisDependentProperty(default='vlass-se-tier-1')

    def __init__(self, context, vis=None, phasecenter=None, vlass_ql_database=None, maskingmode=None):
        self.context = context
        self.vis = vis
        self.phasecenter = phasecenter
        self.vlass_ql_database = vlass_ql_database
        self.maskingmode = maskingmode


@task_registry.set_equivalent_casa_task('hifv_vlassmasking')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Vlassmasking(basetask.StandardTaskTemplate):
    Inputs = VlassmaskingInputs

    def prepare(self):

        LOG.debug("This Vlassmasking class is running.")

        inext = 'iter0.psf.tt0'
        # catalog_fits_file = '/home/vlass/packages/VLASS1Q.fits'

        # Catalog masking function executed here.
        # mask_from_catalog(inext=inext, outext="QLcatmask.mask", catalog_search_size=1.5,
        #                   catalog_fits_file=self.inputs.vlass_ql_database)

        return VlassmaskingResults()

    def analyse(self, results):
        return results
