import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class FlagtargetsdataResults(basetask.Results):
    def __init__(self):
        super(FlagtargetsdataResults, self).__init__()
        self.pipeline_casa_task = 'Flagtargetsdata'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'FlagtargetsdataResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'FlagtargetsdataResults:'


class FlagtargetsdataInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hifv_flagtargetsdata')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Flagtargetsdata(basetask.StandardTaskTemplate):
    Inputs = FlagtargetsdataInputs

    def prepare(self):

        LOG.info("This Flagtargetsdata class is running.")

        return FlagtargetsdataResults()

    def analyse(self, results):
        return results

    def _do_somethingflagtargetsdata(self):

        task = casa_tasks.flagdata(vis=self.inputs.vis, caltable='tempcal.flagtargetsdata')

        return self._executor.execute(task)

