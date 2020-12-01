import os
import re
import shutil

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
import pipeline.infrastructure.casatools as casatools

from pipeline.hifv.heuristics.vip_helper_functions import mask_from_catalog

LOG = infrastructure.get_logger(__name__)


class VlassmaskingResults(basetask.Results):
    def __init__(self, inext=None, outext=None, catalog_fits_file=None,
                 catalog_search_size=None, outfile=None,
                 combinedmask=None):
        super(VlassmaskingResults, self).__init__()

        self.inext = inext
        self.outext = outext
        self.catalog_fits_file = catalog_fits_file
        self.catalog_search_size = catalog_search_size
        self.outfile = outfile
        self.combinedmask = combinedmask

        self.pipeline_casa_task = 'Vlassmasking'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        # return 'VlassmaskingResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'VlassmaskingResults:'


class VlassmaskingInputs(vdp.StandardInputs):
    vlass_ql_database = vdp.VisDependentProperty(default='/home/vlass/packages/VLASS1Q.fits')
    maskingmode = vdp.VisDependentProperty(default='vlass-se-tier-1')
    catalog_search_size = vdp.VisDependentProperty(default=1.5)

    def __init__(self, context, vis=None, vlass_ql_database=None, maskingmode=None,
                 catalog_search_size=None):
        self.context = context
        self.vis = vis
        self.vlass_ql_database = vlass_ql_database
        self.maskingmode = maskingmode
        self.catalog_search_size = catalog_search_size


@task_registry.set_equivalent_casa_task('hifv_vlassmasking')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Vlassmasking(basetask.StandardTaskTemplate):
    Inputs = VlassmaskingInputs

    def prepare(self):

        LOG.debug("This Vlassmasking class is running.")

        # Note that these parameters are just here for reference of creating an image
        inext = 'iter0.psf.tt0'
        # Location of catalog file at the AOC
        # catalog_fits_file = '/home/vlass/packages/VLASS1Q.fits'

        imagename_base = 'VIP_'

        shapelist = self.inputs.context.clean_list_pending[0]['imsize']
        mask_shape = np.array([shapelist[0], shapelist[1], 1, 1])
        phasecenter = self.inputs.context.clean_list_pending[0]['phasecenter']
        cell = self.inputs.context.clean_list_pending[0]['cell']
        frequency = self.inputs.context.clean_list_pending[0]['reffreq']
        QLmask = 'QLcatmask.mask'

        # Test parameters for reference
        # imsize = shapelist[0]
        # wprojplanes = 32
        # cfcache = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(wprojplanes) + '_conjT.cf'
        # cfcache_nowb = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(
        #     wprojplanes) + '_conjT_psf_wbawp_False.cf'
        # phasecenter = 'J2000 13:33:35.814 +16.44.04.255'
        # mask_shape = np.array([imsize, imsize, 1, 1])
        # frequency = '3.0GHz'

        LOG.info("Executing with masking mode: {!s}".format(self.inputs.maskingmode))

        if self.inputs.maskingmode == 'vlass-se-tier-1':
            if not os.path.exists(self.inputs.vlass_ql_database) and self.inputs.maskingmode == 'vlass-se-tier-1':
                LOG.error("VLASS Quicklook database file {!s} does not exist.".format(self.inputs.vlass_ql_database))

            LOG.debug("Executing mask_from_catalog masking mode = {!s}".format(self.inputs.maskingmode))

            outext = QLmask
            catalog_fits_file = self.inputs.vlass_ql_database

            mask_from_catalog(inext=inext, outext=outext,
                              catalog_fits_file=catalog_fits_file,
                              catalog_search_size=self.inputs.catalog_search_size, mask_shape=mask_shape,
                              frequency=frequency, cell=cell, phasecenter=phasecenter, mask_name=imagename_base+outext)

            outfile = ''
            combinedmask = imagename_base + outext

        elif self.inputs.maskingmode == 'vlass-se-tier-2':
            LOG.debug("Executing mask_from_catalog masking mode = {!s}".format(self.inputs.maskingmode))

            outext = "secondmask.mask"
            catalog_fits_file = imagename_base + 'iter1b.image.smooth5.cat.edited.fits'
            if not os.path.exists(catalog_fits_file):
                LOG.error("Catalog file {!s} does not exist.".format(catalog_fits_file))

            mask_from_catalog(inext=inext, outext=outext,
                              catalog_fits_file=catalog_fits_file,
                              catalog_search_size=self.inputs.catalog_search_size, mask_shape=mask_shape,
                              frequency=frequency, cell=cell, phasecenter=phasecenter, mask_name=imagename_base+outext)

            # combine first and 2nd order masks
            outfile = imagename_base + 'sum_of_masks.mask'
            task = casa_tasks.immath(imagename=[imagename_base + 'secondmask.mask', imagename_base + QLmask],
                                     expr='IM0+IM1', outfile=outfile)

            runtask = self._executor.execute(task)

            myim = casatools.imager
            LOG.info("Executing imager.mask()...")
            combinedmask = imagename_base + 'combined.mask'
            myim.mask(image=outfile, mask=imagename_base + 'combined.mask', threshold=0.5)
            myim.close()
        else:
            LOG.error("Invalid maskingmode input.")

        return VlassmaskingResults(inext=inext, outext=outext, catalog_fits_file=catalog_fits_file,
                                   catalog_search_size=1.5, outfile=outfile,
                                   combinedmask=combinedmask)

    def analyse(self, results):
        return results
