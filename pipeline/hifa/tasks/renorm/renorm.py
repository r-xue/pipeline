import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.extern.almarenorm import ACreNorm

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, vis, apply, threshold, correctATM, diagspectra, corrApplied, corrColExists, stats, rnstats, alltdm, exception=None):
        super(RenormResults, self).__init__()
        self.pipeline_casa_task = 'Renorm'
        self.vis = vis
        self.apply = apply
        self.threshold = threshold
        self.correctATM = correctATM
        self.diagspectra = diagspectra
        self.corrApplied = corrApplied
        self.corrColExists = corrColExists
        self.stats = stats
        self.rnstats = rnstats
        self.alltdm = alltdm
        self.exception = exception

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
                f'\tcorrectATM={self.correctATM}\n'
                f'\tdiagspectra={self.diagspectra}\n'
                f'\talltdm={self.alltdm}\n'
                f'\tstats={self.stats}')

class RenormInputs(vdp.StandardInputs):
    apply = vdp.VisDependentProperty(default=False)
    threshold = vdp.VisDependentProperty(default=1.02)
    correctATM = vdp.VisDependentProperty(default=False)
    diagspectra = vdp.VisDependentProperty(default=True)

    def __init__(self, context, vis=None, apply=None, threshold=None, correctATM=None, diagspectra=None):
        super(RenormInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.apply = apply
        self.threshold = threshold
        self.correctATM = correctATM
        self.diagspectra = diagspectra

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Renorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):
        inp = self.inputs
        alltdm = True  # assume no FDM present

        LOG.info("This Renorm class is running.")

        # Issue warning if band 9 and 10 data is found
        bands = [s.band for sub in [m.get_spectral_windows() for m in inp.context.observing_run.measurement_sets] for s in sub]
        if 'ALMA Band 9' in bands:
            LOG.warning('Running hifa_renorm on ALMA Band 9 (DSB) data')
        if 'ALMA Band 10' in bands:
            LOG.warning('Running hifa_renorm on ALMA Band 10 (DSB) data')

        # call the renorm code
        try:
            rn = ACreNorm(inp.vis)
            # Check if the correction has already been applied
            corrApplied = rn.checkApply()
            corrColExists = rn.correxists

            if not rn.tdm_only:
                rn.renormalize(docorr=inp.apply, docorrThresh=inp.threshold, correctATM=inp.correctATM,
                               diagspectra=inp.diagspectra)
                rn.plotSpectra()
                alltdm = False

            if corrColExists and not corrApplied:
                # get stats (dictionary) indexed by source, spw
                stats = rn.rnpipestats
                # get all factors for QA
                rnstats = rn.stats()
            else:
                stats = {}
                rnstats = {}
            rn.close()

            result = RenormResults(inp.vis, inp.apply, inp.threshold, inp.correctATM, inp.diagspectra, corrApplied, corrColExists, stats, rnstats, alltdm)
        except Exception as e:
            LOG.error('Failure in running renormalization heuristic: {}'.format(e))
            result = RenormResults(inp.vis, inp.apply, inp.threshold, inp.correctATM, inp.diagspectra, False, False, {}, {}, alltdm, e)

        return result

    def analyse(self, results):
        return results
