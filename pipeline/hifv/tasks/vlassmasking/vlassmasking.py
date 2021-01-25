import os
import numpy as np

from casatasks import imsmooth

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure import casa_tools

from pipeline.hifv.heuristics.vip_helper_functions import mask_from_catalog, edit_pybdsf_islands, run_bdsf

LOG = infrastructure.get_logger(__name__)


class VlassmaskingResults(basetask.Results):
    """Results class for the hifv_vlassmasking pipeline smoothing task.  Used on VLASS measurement sets.

    The class inherits from basetask.Results

    """
    def __init__(self, catalog_fits_file=None,
                 catalog_search_size=None, outfile=None,
                 combinedmask=None, number_islands_found=None,
                 number_islands_found_onedeg=None, pixelfraction=None, maskingmode=None):
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

        self.catalog_fits_file = catalog_fits_file
        self.catalog_search_size = catalog_search_size
        self.outfile = outfile
        self.combinedmask = combinedmask
        self.number_islands_found = number_islands_found
        self.number_islands_found_onedeg = number_islands_found_onedeg
        self.pixelfraction = pixelfraction
        self.maskingmode = maskingmode

        self.pipeline_casa_task = 'Vlassmasking'

    def merge_with_context(self, context):
        """
        Args:
            context(:obj:): Pipeline context object
        """
        # Update the mask name in clean list pending.
        if len(context.clean_list_pending[0]) == 0:
            LOG.error('Clean list pending is empty. Mask name was not set for imaging target.')
            return
        elif 'mask' in context.clean_list_pending[0].keys() and context.clean_list_pending[0]['mask']:
            LOG.warning('Updating existing clean list pending mask selection with {}.'.format(self.combinedmask))
        else:
            LOG.info('Setting clean list pending mask selection to {}.'.format(self.combinedmask))

        # In tier-2 mode cleaning is done first with tier-1 mask (iter1 in tclean.py), then with combined mask (iter2)
        context.clean_list_pending[0]['mask'] = [context.clean_list_pending[0]['mask'], self.combinedmask] if self.maskingmode == 'vlass-se-tier-2' else self.combinedmask
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
            VlassmaskingResults() type object, with final output mask name and metadata information
        """

        LOG.debug("This Vlassmasking class is running.")

        maskname_base = self._get_maskname_base()

        shapelist = self.inputs.context.clean_list_pending[0]['imsize']
        mask_shape = np.array([shapelist[0], shapelist[1], 1, 1])
        phasecenter = self.inputs.context.clean_list_pending[0]['phasecenter']
        cell = self.inputs.context.clean_list_pending[0]['cell']
        frequency = self.inputs.context.clean_list_pending[0]['reffreq']
        QLmask = '.QLcatmask-tier1.mask'
        catalog_fits_file = ''
        outfile = ''
        combinedmask = ''
        number_islands_found = 0
        number_islands_found_onedeg = 0

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

            catalog_fits_file = self.inputs.vlass_ql_database

            number_islands_found, \
            number_islands_found_onedeg = mask_from_catalog(catalog_fits_file=catalog_fits_file,
                                                            catalog_search_size=self.inputs.catalog_search_size,
                                                            mask_shape=mask_shape, frequency=frequency, cell=cell,
                                                            phasecenter=phasecenter,
                                                            mask_name=maskname_base + QLmask)

            combinedmask = maskname_base + QLmask

        elif self.inputs.maskingmode == 'vlass-se-tier-2':
            LOG.debug("Executing mask_from_catalog masking mode = {!s}".format(self.inputs.maskingmode))

            # Obrain Tier 1 mask name
            tier1_mask = maskname_base + QLmask if not self.inputs.context.clean_list_pending[0]['mask'] else self.inputs.context.clean_list_pending[0]['mask']

            # Obtain image name
            imagename_base = self._get_bdsf_imagename(maskname_base, iter=1)

            initial_catalog_fits_file = self.bdsfcompute(imagename_base)

            suffix = ".secondmask.mask"

            catalog_fits_file = edit_pybdsf_islands(catalog_fits_file=initial_catalog_fits_file)

            if not os.path.exists(catalog_fits_file):
                LOG.error("Catalog file {!s} does not exist.".format(catalog_fits_file))

            number_islands_found, \
            number_islands_found_onedeg = mask_from_catalog(catalog_fits_file=catalog_fits_file,
                                                            catalog_search_size=self.inputs.catalog_search_size,
                                                            mask_shape=mask_shape, frequency=frequency, cell=cell,
                                                            phasecenter=phasecenter,
                                                            mask_name=maskname_base + suffix)

            # combine first and second order masks
            outfile = maskname_base + '.sum_of_masks.mask'
            task = casa_tasks.immath(imagename=[maskname_base + suffix, tier1_mask], expr='IM0+IM1', outfile=outfile)

            runtask = self._executor.execute(task)

            myim = casa_tools.imager
            LOG.info("Executing imager.mask()...")
            combinedmask = maskname_base + '.combined-tier2.mask'
            myim.mask(image=outfile, mask=combinedmask, threshold=0.5)
            myim.close()
        else:
            LOG.error("Invalid maskingmode input.")

        # Compute fraction of pixels enclosed in the mask
        with casa_tools.ImageReader(combinedmask) as myia:
            computechunk = myia.getchunk()
            pixelfraction = computechunk.sum() / computechunk.size

        # Compute fraction of pixels enclosed in the inner square degree
        # TODO

        return VlassmaskingResults(catalog_fits_file=catalog_fits_file,
                                   catalog_search_size=1.5, outfile=outfile,
                                   combinedmask=combinedmask, number_islands_found=number_islands_found,
                                   number_islands_found_onedeg=number_islands_found_onedeg,
                                   pixelfraction=pixelfraction, maskingmode=self.inputs.maskingmode)

    def analyse(self, results):
        return results

    def bdsfcompute(self, imagename_base):

        imsmooth(imagename=imagename_base + ".tt0", major='5arcsec', minor='5arcsec', pa='0deg',
                 outfile=imagename_base + ".smooth5.tt0")

        fitsimage = imagename_base + ".smooth5.fits"
        export_task = casa_tasks.exportfits(imagename=imagename_base + ".smooth5.tt0",
                                            fitsimage=fitsimage)
        runtask = self._executor.execute(export_task)

        # subprocess.call(['/users/jmarvil/scripts/run_bdsf.py',
        #                  imagename_base+'iter1b.image.smooth5.fits'],env={'PYTHONPATH':''})
        bdsf_result = run_bdsf(infile=fitsimage)

        # Return the catalogue fits file name
        return fitsimage.replace('.fits', '.cat.fits')

    def _get_maskname_base(self):
        """
        Returns base name for the mask.

        If context.clean_list_pending is populated, then it will take 'imagename' parameter from the first imlist entry.
        The 'STAGENUMBER' substring is replaced by the current stage number.
        """
        # TODO: context.stage for some reason returns x_1 instead of x_0. Until understood, force the latter.
        if 'imagename' in self.inputs.context.clean_list_pending[0].keys():
            return self.inputs.context.clean_list_pending[0]['imagename'].replace('STAGENUMBER',
                                                                                  '{}_0'.format(
                                                                                      self.inputs.context.stage.split(
                                                                                          '_')[0]))
        else:
            return 'VIP_'

    def _get_bdsf_imagename(self, imagename_base, iter=1):
        """
        Image file name to be used as input to BDSF blob finder.

        Obtain the image name from the latest MakeImagesResult object in context.results. The iter parameter controlls
        which iteration image name should be returned. In the current VLASS-SE-CONT workflow, iter=1.

        If hif_makimages result is not found, then construct image name from imagename_base argument. This replicates 
        the image name used in the VLASS Memo 15 VIP script.
        """
        for result in self.inputs.context.results[::-1]:
            result_meta = result.read()
            if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith('hif_makeimages'):
                return result_meta.results[0].iterations[iter]['image']

        # In case hif_makeimages result was not found.
        return imagename_base + 'iter1b.image'
