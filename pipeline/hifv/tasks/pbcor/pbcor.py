import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PbcorResults(basetask.Results):
    def __init__(self, pbcorimagenames=[]):
        super(PbcorResults, self).__init__()
        self.pbcorimagenames = pbcorimagenames[:]

    def __repr__(self):
        # return 'PbcorResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'PbcorResults:'


class PbcorInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        super(PbcorInputs, self).__init__()
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_pbcor')
class Pbcor(basetask.StandardTaskTemplate):
    Inputs = PbcorInputs

    def prepare(self):

        imlist = self.inputs.context.sciimlist.get_imlist()
        pbcor_list = []
        for image in imlist:

            imgname = image['imagename']
            pbname = imgname[:imgname.rfind('.image')] + '.pb'
            resname = imgname[:imgname.rfind('.image')] + '.residual'
            term_ext = '.tt0' if image['multiterm'] else ''

            pbcor_list.append(pbname+term_ext)
            for basename in [imgname, resname]:
                task = casa_tasks.impbcor(imagename=basename+term_ext, pbimage=pbname+term_ext,
                                          outfile=basename+'.pbcor'+term_ext, mode='divide', cutoff=-1.0, stretch=False)
                self._executor.execute(task)
                pbcor_list.append(basename+'.pbcor'+term_ext)

            LOG.info("PBCOR image names: " + ','.join(pbcor_list))

        return PbcorResults(pbcorimagenames=pbcor_list)

    def analyse(self, results):
        return results



