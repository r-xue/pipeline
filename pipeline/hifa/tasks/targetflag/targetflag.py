import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class TargetFlagResults(basetask.Results):
    def __init__(self):
        super(TargetFlagResults, self).__init__()

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'TargetFlagResults:'


class TargetFlagInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hifa_targetflag')
@task_registry.set_casa_commands_comment('Flag target source outliers.')
class TargetFlag(basetask.StandardTaskTemplate):
    Inputs = TargetFlagInputs

    def prepare(self):

        return TargetFlagResults()

    def analyse(self, results):
        return results
