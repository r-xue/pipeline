import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self):
        super(RenormResults, self).__init__()
        self.pipeline_casa_task = 'Renorm'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'RenormResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'RenormResults:'


class RenormInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Renorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):

        LOG.info("This Renorm class is running.")

        return RenormResults()

    def analyse(self, results):
        return results

    def _do_somethingrenorm(self):

        task = casa_tasks.renormcal(vis=self.inputs.vis, caltable='tempcal.renorm')

        return self._executor.execute(task)

