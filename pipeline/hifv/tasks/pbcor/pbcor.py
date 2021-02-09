import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PbcorResults(basetask.Results):
    def __init__(self, pbcorimagenames={}):
        super(PbcorResults, self).__init__()
        self.pbcorimagenames = pbcorimagenames

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

        sci_imlist = self.inputs.context.sciimlist.get_imlist()
        pbcor_dict = {}
        for sci_im in sci_imlist:

            imgname = sci_im['imagename']
            basename = imgname[:imgname.rfind('.image')]
            pbname = basename + '.pb'
            resname = basename + '.residual'
            term_ext = '.tt0' if sci_im['multiterm'] else ''

            pbcor_images = []
            for inpname in [imgname, resname]:
                task = casa_tasks.impbcor(imagename=inpname+term_ext, pbimage=pbname+term_ext,
                                          outfile=inpname+'.pbcor'+term_ext, mode='divide', cutoff=-1.0, stretch=False)
                self._executor.execute(task)
                pbcor_images.append(inpname+'.pbcor'+term_ext)
            pbcor_images.append(pbname+term_ext)

            LOG.info("PBCOR image names: " + ','.join(pbcor_images))
            pbcor_dict[basename] = pbcor_images

        return PbcorResults(pbcorimagenames=pbcor_dict)

    def analyse(self, results):
        return results



