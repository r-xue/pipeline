import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class AnalyzestokescubesResults(basetask.Results):
    def __init__(self):
        super(AnalyzestokescubesResults, self).__init__()
        self.pipeline_casa_task = 'Analyzestokescubes'

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

        LOG.info("This Analyzestokescubes class is running.")

        return AnalyzestokescubesResults()

    def analyse(self, results):
        return results

    def _do_somethinganalyzestokescubes(self):

        task = casa_tasks.analyzestokescubescal(vis=self.inputs.vis, caltable='tempcal.analyzestokescubes')

        return self._executor.execute(task)

