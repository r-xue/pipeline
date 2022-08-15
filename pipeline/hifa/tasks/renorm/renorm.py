import ast

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from pipeline.extern.almarenorm import ACreNorm

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, renorm_applied, vis, apply, threshold, correctATM, spw, excludechan, corrApplied, corrColExists, stats, rnstats, alltdm, atmAutoExclude, atmWarning, atmExcludeCmd, exception=None):
        super(RenormResults, self).__init__()
        self.renorm_applied = renorm_applied
        self.vis = vis
        self.apply = apply
        self.threshold = threshold
        self.correctATM = correctATM
        self.spw = spw
        self.excludechan = excludechan
        self.corrApplied = corrApplied
        self.corrColExists = corrColExists
        self.stats = stats
        self.rnstats = rnstats
        self.alltdm = alltdm
        self.exception = exception
        self.atmAutoExclude = atmAutoExclude
        self.atmWarning = atmWarning
        self.atmExcludeCmd = atmExcludeCmd

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return (f'RenormResults:\n'
                f'\trenorm_applied={self.renorm_applied}\n'
                f'\tvis={self.vis}\n'
                f'\tapply={self.apply}\n'
                f'\tthreshold={self.threshold}\n'
                f'\tcorrectATM={self.correctATM}\n'
                f'\tspw={self.spw}\n'
                f'\texcludechan={self.excludechan}\n'
                f'\talltdm={self.alltdm}\n'
                f'\tstats={self.stats}\n'
                f'\atmAutoExclude={self.atmAutoExclude}')

class RenormInputs(vdp.StandardInputs):
    apply = vdp.VisDependentProperty(default=False)
    threshold = vdp.VisDependentProperty(default=1.02)
    correctATM = vdp.VisDependentProperty(default=False)
    spw = vdp.VisDependentProperty(default='')
    excludechan = vdp.VisDependentProperty(default={})
    atm_auto_exclude = vdp.VisDependentProperty(default=False)

    @spw.convert
    def spw(self, value):
        # turn comma seperated string into a list of integers
        return [int(x) for x in value.split(',')]

    @excludechan.convert
    def excludechan(self, value):
        pyobj = ast.literal_eval(value)
        if isinstance(pyobj, dict):
            return pyobj

    def __init__(self, context, vis=None, apply=None, threshold=None, correctATM=None, spw=None, excludechan=None, atm_auto_exclude=False):
        super(RenormInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.apply = apply
        self.threshold = threshold
        self.correctATM = correctATM
        self.spw = spw
        self.excludechan = excludechan
        self.atm_auto_exclude = atm_auto_exclude

@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Renormalize data affected by strong line emission.')
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

        renorm_applied = False

        # call the renorm code
        try:
            rn = ACreNorm(inp.vis)

            # Store "all TDM" information before trying the renormalization so
            # that the weblog is rendered properly.
            alltdm = rn.tdm_only

            # Check if the correction has already been applied
            corrApplied = rn.checkApply()
            corrColExists = rn.correxists

            stats = {}
            rnstats = {}
            atmWarning = {}
            atmExcludeCmd = {}

            if not alltdm:

                rn.renormalize(docorr=inp.apply, docorrThresh=inp.threshold, correctATM=inp.correctATM,
                               spws=inp.spw, excludechan=inp.excludechan, atmAutoExclude=inp.atm_auto_exclude)
                rn.plotSpectra(includeSummary=False)

                # if we tried to renormalize, and it was done, store info in the results
                #   so that it can be passed to the manifest and used during restore
                if inp.apply and rn.checkApply():
                    renorm_applied = True

                if corrColExists and not corrApplied:
                    # get stats (dictionary) indexed by source, spw
                    stats = rn.rnpipestats
                    # get all factors for QA
                    rnstats = rn.stats()
                    # get information related to detecting false positives caused by atmospheric features, also needed for QA
                    atmWarning = rn.atmWarning
                    atmExcludeCmd = rn.atmExcludeCmd

            rn.close()

            result = RenormResults(renorm_applied, inp.vis, inp.apply, inp.threshold, inp.correctATM, inp.spw, inp.excludechan, corrApplied, corrColExists, stats, rnstats, alltdm, inp.atm_auto_exclude, atmWarning, atmExcludeCmd)
        except Exception as e:
            LOG.error('Failure in running renormalization heuristic: {}'.format(e))
            result = RenormResults(renorm_applied, inp.vis, inp.apply, inp.threshold, inp.correctATM, inp.spw, inp.excludechan, False, False, {}, {}, alltdm, inp.atm_auto_exclude, {}, {}, e)

        return result

    def analyse(self, results):
        return results
