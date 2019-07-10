from __future__ import absolute_import

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
import pipeline.infrastructure.casatools as casatools

LOG = infrastructure.get_logger(__name__)


class AnalyzealphaResults(basetask.Results):
    def __init__(self):
        super(AnalyzealphaResults, self).__init__()
        self.pipeline_casa_task = 'Analyzealpha'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'AnalyzealphaResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'AnalyzealphaResults:'


class AnalyzealphaInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None, image=None, alphafile=None, alphaerrorfile=None):
        self.context = context
        self.vis = vis
        self.image = image
        self.alphafile = alphafile
        self.alphaerrorfile = alphaerrorfile


@task_registry.set_equivalent_casa_task('hif_analyzealpha')
@task_registry.set_casa_commands_comment('Diagnostics of spectral index image.')
class Analyzealpha(basetask.StandardTaskTemplate):
    Inputs = AnalyzealphaInputs

    def prepare(self):
        inputs = self.inputs

        LOG.info("This Analyzealpha class is running.")
        #
        # The following is example code to extract the value from the .alpha and .alpha.error
        # images (for wideband continuum MTMFS with nterms>1)
        #
        # Run imstat on the restored tt0 I subimage
        with casatools.ImageReader(inputs.image) as image:
            stats = image.statistics(robust=False)

        # Extract the position of the maximum from imstat return dictionary
        maxposx = stats['maxpos'][0]
        maxposy = stats['maxpos'][1]
        maxposf = stats['maxposf']
        statstring = '|* Restored max at %s (%i,%i)' % (maxposf, maxposx, maxposy)
        print(statstring)

        # Set up a box string for that max pixel
        mybox = '%i,%i,%i,%i' % (maxposx, maxposy, maxposx, maxposy)

        # Extract the value of that pixel from the alpha subimage
        try:
            alpha_val = self._do_imval(imagename=inputs.alphafile, box=mybox)
        except:
            alpha_val = -999.
        alpha_at_max = alpha_val['data'][0]
        alpha_string = '{.3f}'.format(alpha_at_max)

        # Extract the value of that pixel from the alphaerror subimage
        try:
            alphaerror_val = self._do_imval(imagename=inputs.alphaerrorfile, box=mybox)
        except:
            alphaerror_val = -999.
        alphaerror_at_max = alphaerror_val['data'][0]
        alphaerror_string = '{.3f}'.format(alphaerror_at_max)

        statstring = '|* Alpha at restored max %s +/- %s' % (alpha_string, alphaerror_string)
        LOG.info(statstring)
        return AnalyzealphaResults()

    def analyse(self, results):
        return results

    def _do_imval(self, **kwargs):
        task = casa_tasks.imval(kwargs)

        return self._executor.execute(task)

