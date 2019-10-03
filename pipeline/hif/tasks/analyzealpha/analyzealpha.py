import glob

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class AnalyzealphaResults(basetask.Results):
    def __init__(self, max_location=None, alpha_and_error=None):
        super(AnalyzealphaResults, self).__init__()
        self.pipeline_casa_task = 'Analyzealpha'
        self.max_location = max_location
        self.alpha_and_error = alpha_and_error

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

        LOG.info("Analyzealpha is running.")

        imlist = self.inputs.context.subimlist.get_imlist()

        subimagefile = inputs.image
        alphafile = inputs.alphafile
        alphaerrorfile = inputs.alphaerrorfile

        # there should only be one subimage used in this task.  what if there are others in the directory?
        for imageitem in imlist:

            if not subimagefile:
                if imageitem['multiterm']:
                    subimagefile = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.tt0.subim'))[0]
                else:
                    subimagefile = glob.glob(imageitem['imagename'].replace('.subim', '.pbcor.subim'))[0]

            if not alphafile:
                alphafile = glob.glob(imlist[0]['imagename'].replace('.image.subim', '.alpha'))[0]

            if not alphaerrorfile:
                alphaerrorfile = glob.glob(imlist[0]['imagename'].replace('.image.subim', '.alpha.error'))[0]

            #
            # The following is example code to extract the value from the .alpha and .alpha.error
            # images (for wideband continuum MTMFS with nterms>1)
            #
            # Run imstat on the restored tt0 I subimage
            with casatools.ImageReader(subimagefile) as image:
                stats = image.statistics(robust=False)

            # Extract the position of the maximum from imstat return dictionary
            maxposx = stats['maxpos'][0]
            maxposy = stats['maxpos'][1]
            maxposf = stats['maxposf']
            max_location = '%s  (%i, %i)' % (maxposf, maxposx, maxposy)
            LOG.info('|* Restored max at {}'.format(max_location))

            # Set up a box string for that max pixel
            mybox = '%i,%i,%i,%i' % (maxposx, maxposy, maxposx, maxposy)

            # Extract the value of that pixel from the alpha subimage
            try:
                task = casa_tasks.imval(imagename=alphafile, box=mybox)
                alpha_val = self._executor.execute(task)
            except:
                alpha_val = -999.

            alpha_at_max = alpha_val['data'][0]
            alpha_string = '{:.3f}'.format(alpha_at_max)

            # Extract the value of that pixel from the alphaerror subimage
            try:
                task = casa_tasks.imval(imagename=alphaerrorfile, box=mybox)
                alphaerror_val = self._executor.execute(task)
            except:
                alphaerror_val = -999.
            alphaerror_at_max = alphaerror_val['data'][0]
            alphaerror_string = '{:.3f}'.format(alphaerror_at_max)

            alpha_and_error = '%s +/- %s' % (alpha_string, alphaerror_string)
            LOG.info('|* Alpha at restored max {}'.format(alpha_and_error))

        return AnalyzealphaResults(max_location=max_location, alpha_and_error=alpha_and_error)

    def analyse(self, results):
        return results

