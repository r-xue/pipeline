import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, apply, threhold):
        super(RenormResults, self).__init__()
        self.pipeline_casa_task = 'Renorm'
        self.apply = apply
        self.threshold = threhold

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return (f'RenormResults:\n'
                '\tapply={self.apply}\n'
                '\tthreshold={self.threshold}')

class RenormInputs(vdp.StandardInputs):

    def __init__(self, context, vis=None, apply=None, threhold=None):
        self.context = context
        self.vis = vis
        self.apply = apply
        self.threshold = threhold

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Renorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):

        LOG.info("This Renorm class is running.")
        # call the renorm script

        result = RenormResults(self.inputs.apply, self.inputs.threshold)

        return result

    def analyse(self, results):
        return results
