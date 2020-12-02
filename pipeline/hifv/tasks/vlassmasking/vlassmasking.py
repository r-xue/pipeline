import os
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
import pipeline.infrastructure.casatools as casatools

from pipeline.hifv.heuristics.vip_helper_functions import mask_from_catalog

LOG = infrastructure.get_logger(__name__)


class VlassmaskingResults(basetask.Results):
    """Results class for the hifv_vlassmasking pipeline smoothing task.  Used on VLASS measurement sets.

    The class inherits from basetask.Results

    """
    def __init__(self, inext=None, outext=None, catalog_fits_file=None,
                 catalog_search_size=None, outfile=None,
                 combinedmask=None):
        """
        Args:
            final(list): final list of tables (not used in this task)
            pool(list): pool list (not used in this task)
            preceding(list): preceding list (not used in this task)
            catalog_fits_files(str): Path to a PyBDSF sky catalog in FITS format.
                                     Default at AOC is '/home/vlass/packages/VLASS1Q.fits'
            outfile(str): Output file - sum of masks
            combinedmask(str):  Final combined mask

        """
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
        Args:
            context(:obj:): Pipeline context object
        """
        return

    def __repr__(self):
        # return 'VlassmaskingResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'VlassmaskingResults:'


class VlassmaskingInputs(vdp.StandardInputs):
    """Inputs class for the hifv_vlassmasking pipeline task.  Used in conjunction with VLASS measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    vlass_ql_database = vdp.VisDependentProperty(default='/home/vlass/packages/VLASS1Q.fits')
    maskingmode = vdp.VisDependentProperty(default='vlass-se-tier-1')
    catalog_search_size = vdp.VisDependentProperty(default=1.5)

    def __init__(self, context, vis=None, vlass_ql_database=None, maskingmode=None,
                 catalog_search_size=None):
        """
            Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set
            vlass_ql_database(str): Path to a PyBDSF sky catalog in FITS format.
                                    Default at AOC is '/home/vlass/packages/VLASS1Q.fits'
            maskingmode(str): Two modes are: vlass-se-tier-1 (QL mask) and vlass-se-tier-2 (combined mask)
            catalog_search_size (float): The half-width (in degrees) of the catalog search centered on the
                                        image's reference pixel.
        """

        self.context = context
        self.vis = vis
        self.vlass_ql_database = vlass_ql_database
        self.maskingmode = maskingmode
        self.catalog_search_size = catalog_search_size


@task_registry.set_equivalent_casa_task('hifv_vlassmasking')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Vlassmasking(basetask.StandardTaskTemplate):
    """Class for the hifv_vlassmasking pipeline task.  Used on VLASS measurement sets.

        The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = VlassmaskingInputs

    def prepare(self):
        """Method where the VLASS Masking operation is executed.

        The mask creation steps are critical pieces of the VIP workflow. Mask creation uses islands
        identified using pyBDSF and the process is described in detail in VLASS Memo #15. If the
        image to be produced is in a region of the sky where no components were identified, this step
        and subsequent steps will not succeed. Thus, several metrics are needed to evaluate the efficacy
        of mask creation in this step.

        Return:
            VlassmaskingResults() type object, with output mask and metadata information
        """

        LOG.debug("This Vlassmasking class is running.")

        # Note that these parameters are just here for reference of creating an image
        inext = 'iter0.psf.tt0'

        imagename_base = 'VIP_'

        shapelist = self.inputs.context.clean_list_pending[0]['imsize']
        mask_shape = np.array([shapelist[0], shapelist[1], 1, 1])
        phasecenter = self.inputs.context.clean_list_pending[0]['phasecenter']
        cell = self.inputs.context.clean_list_pending[0]['cell']
        frequency = self.inputs.context.clean_list_pending[0]['reffreq']
        QLmask = 'QLcatmask.mask'

        # Test parameters for reference
        # Location of catalog file at the AOC
        # catalog_fits_file = '/home/vlass/packages/VLASS1Q.fits'
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
