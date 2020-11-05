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
    phasecenter = vdp.VisDependentProperty(default='')
    vlass_ql_database = vdp.VisDependentProperty(default='/home/vlass/packages/VLASS1Q.fits')
    maskingmode = vdp.VisDependentProperty(default='vlass-se-tier-1')
    catalog_search_size = vdp.VisDependentProperty(default=1.5)
    mask_shape = vdp.VisDependentProperty(default=[1024, 1024,    1,    1])

    def __init__(self, context, vis=None, phasecenter=None, vlass_ql_database=None, maskingmode=None,
                 catalog_search_size=None, mask_shape=None):
        self.context = context
        self.vis = vis
        self.phasecenter = phasecenter
        self.vlass_ql_database = vlass_ql_database
        self.maskingmode = maskingmode
        self.catalog_search_size = catalog_search_size
        self.mask_shape = mask_shape


@task_registry.set_equivalent_casa_task('hifv_vlassmasking')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Vlassmasking(basetask.StandardTaskTemplate):
    Inputs = VlassmaskingInputs

    def prepare(self):

        LOG.debug("This Vlassmasking class is running.")

        # TODO:
        # There needs to be a mechanism to have the shape and csys reference inputs for mask_from_catalog
        # The task currently assumes images are on disk to retrieve that information.
        # mask_from_catalog globs() the iter0.psf.tt0 files and gets the appropriate information

        # Note that these parameters are just here for my benefit and reference of creating an image
        inext = 'iter0.psf.tt0'
        # Location of catalog file at the AOC
        # catalog_fits_file = '/home/vlass/packages/VLASS1Q.fits'

        imagename_base = 'VIP_'
        imsize = 1024
        cell = '2.0arcsec'
        wprojplanes = 32

        cfcache = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(wprojplanes) + '_conjT.cf'
        cfcache_nowb = './cfcache_imsize' + str(imsize) + '_cell' + cell + '_w' + str(
            wprojplanes) + '_conjT_psf_wbawp_False.cf'

        # Catalog masking function executed here.

        LOG.info("Executing with masking mode: {!s}".format(self.inputs.maskingmode))

        if self.inputs.maskingmode == 'vlass-se-tier-1':
            if not os.path.exists(self.inputs.vlass_ql_database) and self.inputs.maskingmode == 'vlass-se-tier-1':
                LOG.error("VLASS Quicklook database file {!s} does not exist.".format(self.inputs.vlass_ql_database))

            # This command here is just for the sake of creating an image if need be
            tclean_result = self.run_tclean('iter0', cfcache=cfcache_nowb, robust=-2.0, uvtaper='3arcsec',
                                            calcres=False, parallel=False, wbawp=False)

            LOG.debug("Executing mask_from_catalog")

            outext = "QLcatmask.mask"
            catalog_fits_file = self.inputs.vlass_ql_database

            mask_from_catalog(inext=inext, outext=outext,
                              catalog_fits_file=catalog_fits_file,
                              catalog_search_size=self.inputs.catalog_search_size)

            outfile = ''
            combinedmask = ''

        elif self.inputs.maskingmode == 'vlass-se-tier-2':
            # hifv_vlassmasking(known_mask='tier-1', uniform_image='image_name', maskingmode='vlass-se-tier-2')
            LOG.debug("Executing mask_from_catalog")

            outext = "secondmask.mask"
            catalog_fits_file = imagename_base + 'iter1b.image.smooth5.cat.edited.fits'
            if not os.path.exists(catalog_fits_file):
                LOG.error("Catalog file {!s} does not exist.".format(catalog_fits_file))

            mask_from_catalog(inext=inext, outext=outext,
                              catalog_fits_file=catalog_fits_file,
                              catalog_search_size=self.inputs.catalog_search_size)

            # combine first and 2nd order masks
            outfile = imagename_base + 'sum_of_masks.mask'
            task = casa_tasks.immath(imagename=[imagename_base + 'secondmask.mask', imagename_base + 'QLcatmask.mask'],
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

    def run_tclean(self, image_iter, datacolumn='data', cfcache='', scales=[0], robust=1.0, uvtaper='', niter=0,
                   gain=0.1, nsigma=2.0, \
                   cycleniter=5000, cyclefactor=3, mask='', savemodel="none", calcres=True, calcpsf=True,
                   parallel=True, wbawp=True):
        """
        This class method is simply here for convenience and reference to run tclean and obtain *iter0.psf.tt0 images
        """

        field = ''
        spw = ''
        antenna = ''
        scan = ''

        imagename_base = 'VIP_'
        imsize = 1024
        cell = '2.0arcsec'
        reffreq = '3.0GHz'
        uvrange = '<12km'
        intent = 'OBSERVE_TARGET#UNSPECIFIED'
        wprojplanes = 32
        usepointing = True

        if mask:
            mask = imagename_base + mask
        task = casa_tasks.tclean(vis=self.inputs.vis, field=field, spw=spw, uvrange=uvrange, datacolumn=datacolumn,
                                 imagename=imagename_base + image_iter, imsize=imsize,
                                 antenna=antenna, scan=scan, intent=intent, pointingoffsetsigdev=[300, 30],
                                 cell=cell, phasecenter=self.inputs.phasecenter, reffreq=reffreq, gridder="awproject",
                                 wprojplanes=wprojplanes, cfcache=cfcache, conjbeams=True,
                                 usepointing=usepointing, rotatepastep=5.0, pblimit=0.02, deconvolver="mtmfs",
                                 scales=scales, nterms=2, smallscalebias=0.4,
                                 weighting="briggs", robust=robust, uvtaper=uvtaper, niter=niter, gain=gain,
                                 threshold=0.0, nsigma=nsigma, cycleniter=cycleniter,
                                 cyclefactor=cyclefactor, usemask="user", mask=mask, restart=True, savemodel=savemodel,
                                 calcres=calcres, calcpsf=calcpsf, parallel=parallel, wbawp=wbawp)

        return self._executor.execute(task)

