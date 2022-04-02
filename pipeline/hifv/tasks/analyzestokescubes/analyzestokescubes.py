import glob

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure import casa_tools

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

        stats = {'spw': [], 'peak_q': [], 'peak_u': [], 'rms': [], 'reffreq': [], 'beamarea': []}

        for imageitem in imlist:
            img_name = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.subim'))[0]
            rms_name = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.rms.subim'))[0]
            LOG.info(f'Getting properties from {img_name} and {rms_name}')
            with casa_tools.ImagepolReader(img_name) as imagepol:
                img_stokesi = imagepol.stokesi()
                img_stokesq = imagepol.stokesq()
                img_stokesu = imagepol.stokesu()
                stokesi_stats = img_stokesi.statistics(robust=False)
                maxposx = stokesi_stats['maxpos'][0]
                maxposy = stokesi_stats['maxpos'][1]
                maxposf = stokesi_stats['maxposf']
                rg = casa_tools.regionmanager.box(blc=[maxposx-1, maxposy-1], trc=[maxposx+1, maxposy+1])
                stokesq_stats = img_stokesq.statistics(robust=False, region=rg)
                stokesu_stats = img_stokesu.statistics(robust=False, region=rg)
                stats['spw'].append(imageitem['spwlist'])
                cs = img_stokesi.coordsys()
                stats['reffreq'].append(cs.referencevalue(format='n')['numeric'][3])
                bm = img_stokesi.restoringbeam(polarization=0)
                stats['beamarea'].append(bm['major']['value']*bm['minor']['value'])
                stats['peak_q'].append(stokesq_stats['mean'])
                stats['peak_u'].append(stokesu_stats['mean'])
            with casa_tools.ImageReader(rms_name) as image:
                rms_stats = image.statistics(robust=True, axes=[0, 1, 3])
                stats['rms'].append(rms_stats['median'])

        return AnalyzestokescubesResults(stats=stats)

    def analyse(self, results):
        return results
