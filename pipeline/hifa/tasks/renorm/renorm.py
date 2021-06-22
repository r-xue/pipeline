import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.extern.almarenorm import ACreNorm

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, vis, apply, threshold, stats):
        super(RenormResults, self).__init__()
        self.pipeline_casa_task = 'Renorm'
        self.vis = vis
        self.apply = apply
        self.threshold = threshold
        self.stats = stats

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return (f'RenormResults:\n'
                f'\tvis={self.vis}\n'
                f'\tapply={self.apply}\n'
                f'\tthreshold={self.threshold}\n'
                f'\tstats={self.stats}')

class RenormInputs(vdp.StandardInputs):
    apply = vdp.VisDependentProperty(default=False)
    threshold = vdp.VisDependentProperty(default=0.0)

    def __init__(self, context, vis=None, apply=None, threshold=None):
        super(RenormInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.apply = apply
        if 0.0 == threshold:
            self.threshold = None
        else:
            self.threshold = threshold

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Renorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):
        inp = self.inputs

        LOG.info("This Renorm class is running.")

        # call the renorm code
        rn = ACreNorm(inp.vis)
        rn.renormalize(docorr=inp.apply, docorrThresh=inp.threshold, correctATM=False)
        # get stats (dictionary) indexed by source, spw
        rn.plotSpectra()
        stats = rn.rnpipestats
        rn.close()

        result = RenormResults(inp. vis, inp.apply, inp.threshold, stats)

        return result

    def analyse(self, results):
        return results
