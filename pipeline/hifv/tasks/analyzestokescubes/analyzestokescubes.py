import glob
import collections

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry, casa_tools

import copy

LOG = infrastructure.get_logger(__name__)


class AnalyzestokescubesResults(basetask.Results):
    def __init__(self, stats=None):
        super().__init__()
        self.pipeline_casa_task = 'Analyzestokescubes'
        self.stats = stats

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'AnalyzestokescubesResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'AnalyzestokescubesResults:'


class AnalyzestokescubesInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_analyzestokescubes')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Analyzestokescubes(basetask.StandardTaskTemplate):
    Inputs = AnalyzestokescubesInputs

    def prepare(self):

        LOG.info("Analyzestokescubes is running.")

        imlist = self.inputs.context.subimlist.get_imlist()

        # initialize the stats container
        stats = collections.OrderedDict()
        roi_stats_default = {'stokesi': [], 'stokesq': [], 'stokesu': [], 'stokesv': [],
                             'spw': [], 'rms': [], 'reffreq': [], 'beamarea': [],
                             'xy': None, 'world': None}
        roi_list = ['peak_stokesi', 'peak_linpolint']

        # add the roi location
        img_stats = self._get_imstat(imlist)
        for idx, roi_name in enumerate(roi_list):
            stats.setdefault(roi_name, copy.deepcopy(roi_stats_default))
            stats[roi_name]['xy'] = (img_stats[idx]['maxpos'][0], img_stats[idx]['maxpos'][1])
            stats[roi_name]['world'] = img_stats[idx]['maxposf']

        # get the roi property and append them into the roi properties
        for imageitem in imlist:

            img_name = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.subim'))[0]
            rms_name = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.rms.subim'))[0]
            LOG.info(f'Getting properties from {img_name} and {rms_name}')

            with casa_tools.ImagepolReader(img_name) as imagepol:
                with casa_tools.ImageReader(rms_name) as image:

                    rms_stats = image.statistics(robust=True, axes=[0, 1])['median']

                    img_stokesi = imagepol.stokesi()
                    img_stokesq = imagepol.stokesq()
                    img_stokesu = imagepol.stokesu()
                    cs = img_stokesi.coordsys()
                    bm = img_stokesi.restoringbeam(polarization=0)
                    beamarea = bm['major']['value']*bm['minor']['value']

                    for idx, roi_name in enumerate(roi_list):

                        try:
                            npix_halfwidth = 1
                            blc = [stats[roi_name]['xy'][0]-npix_halfwidth, stats[roi_name]['xy'][1]-npix_halfwidth]
                            trc = [stats[roi_name]['xy'][0]+npix_halfwidth, stats[roi_name]['xy'][1]+npix_halfwidth]
                            rg = casa_tools.regionmanager.box(blc=blc, trc=trc)
                            stokesi_mean = img_stokesi.statistics(robust=False, region=rg)['mean'][0]
                            stokesq_mean = img_stokesq.statistics(robust=False, region=rg)['mean'][0]
                            stokesu_mean = img_stokesu.statistics(robust=False, region=rg)['mean'][0]

                            stats[roi_name]['spw'].append(imageitem['spwlist'])
                            stats[roi_name]['stokesi'].append(stokesi_mean)
                            stats[roi_name]['stokesq'].append(stokesq_mean)
                            stats[roi_name]['stokesu'].append(stokesu_mean)
                            stats[roi_name]['rms'].append(rms_stats)
                            stats[roi_name]['beamarea'].append(beamarea)
                            stats[roi_name]['reffreq'].append(cs.referencevalue(format='n')['numeric'][3])

                        except Exception as e:
                            LOG.warning(
                                'Failed to derive the Stokes brightness at the region-of-interest (ROI): {} / spw = {!r}.'.format(roi_name, imageitem['spwlist']))

        return AnalyzestokescubesResults(stats=stats)

    def analyse(self, results):
        return results

    def _get_imstat(self, imlist):
        """Identify the image with lowest ref. frequency and measure its 'maxpos'.
        
        See the requirement in PIPE-1356.
        """

        frequency_list = []
        imagename_list = []
        for imageitem in imlist:
            img_name = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.subim'))[0]
            imagename_list.append(img_name)
            with casa_tools.ImageReader(img_name) as image:
                frequency_list.append(image.coordsys().referencevalue(
                    format='q', type='spectral')['quantity']['*1']['value'])

        idx_freqlow = frequency_list.index(min(frequency_list))
        idx_freqhigh = frequency_list.index(max(frequency_list))

        LOG.info(
            f'{imagename_list[idx_freqlow]} has the lowest reference frequency at {frequency_list[idx_freqlow]} Hz.')
        LOG.info(
            f'{imagename_list[idx_freqhigh]} has the highest reference frequency at {frequency_list[idx_freqhigh]} Hz.')

        # option 1: use pb>0.4 to restrict the search region.

        pbname_freqhigh = imagename_list[idx_freqhigh].replace('.image.pbcor.tt0.subim', '.pb.tt0.subim')
        pbname_freqhigh_flatten = pbname_freqhigh+'.flattened'
        LOG.info(f'Generating a flattend PB subimage at the highest frequency: {pbname_freqhigh_flatten}')
        with casa_tools.ImageReader(pbname_freqhigh) as image:
            collapsed_image = image.collapse(
                function='max', axes=[2, 3], outfile=pbname_freqhigh_flatten, overwrite=True)
            collapsed_image.close()
            subim_cs = image.coordsys()
            subim_shape = image.shape()

            pblimit = 0.4  # only search peak inside above this pb level.
            mask_lel = f'"{pbname_freqhigh_flatten}">{pblimit}'

        # option 2: use .mask from tclean to restrict the search region.
        # Note that .mask is generated from tclean(pbmask=0.4,mask='pb',...) for vlass-se-cube iter3.
        # Therefore, it's equivalent to option 1.

        tclean_mask = imagename_list[idx_freqhigh].replace('.image.pbcor.tt0.subim', '.mask')
        tclean_mask_flatten = tclean_mask+'.subim'
        LOG.info(f'Generating a flattend tclean mask subimage at the highest frequency: {tclean_mask_flatten}')
        with casa_tools.ImageReader(tclean_mask) as image:
            rgTool = casa_tools.regionmanager
            region = rgTool.frombcs(csys=subim_cs.torecord(), shape=subim_shape,
                                    stokes='I', stokescontrol='a')
            image.subimage(outfile=tclean_mask_flatten, region=region, overwrite=True)
            mask_lel = f'"{tclean_mask_flatten}">0.0'

        # do the peak search in the pbcor subimage at the lowest frequency

        imagename_freqlow = imagename_list[idx_freqlow]
        with casa_tools.ImagepolReader(imagename_freqlow) as imagepol:
            img_stokesi = imagepol.stokesi()
            stokesi_stats = img_stokesi.statistics(robust=False, mask=mask_lel, stretch=True)
            LOG.info('Found the Stokes-I peak intensity at {maxpos} / {maxposf}'.format(**stokesi_stats))
            # stokesi_stats['maxradec_str'] = img_stokesi.coordsys().toworld([maxposx, maxposy], format='s')['string']
            img_linpolint = imagepol.linpolint()
            linpolint_stats = img_linpolint.statistics(robust=False, mask=mask_lel, stretch=True)
            LOG.info('Found the linearly polarized intensity peak at {maxpos} / {maxposf}'.format(**linpolint_stats))

        return (stokesi_stats, linpolint_stats)
