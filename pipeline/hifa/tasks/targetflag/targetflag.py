import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag

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

        inputs = self.inputs

        # Initialize results.
        result = TargetFlagResults()

        # Find amplitude outliers and flag data
        LOG.info('Running correctedampflag to identify outliers to flag.')
        cafinputs = correctedampflag.Correctedampflag.Inputs(
            context=inputs.context, vis=inputs.vis, intent='TARGET')
        caftask = correctedampflag.Correctedampflag(cafinputs)
        cafresult = self._executor.execute(caftask)

        return result

    def analyse(self, results):
        return results
