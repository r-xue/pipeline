import copy
import os
import traceback

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.extern.almarenorm import alma_renorm
from pipeline.infrastructure import task_registry
from pipeline.infrastructure import casa_tools
from pipeline.h.heuristics import caltable as caltable_heuristic

LOG = infrastructure.get_logger(__name__)


class RenormResults(basetask.Results):
    def __init__(self, renorm_applied, vis, createcaltable, threshold, correctATM, spw, excludechan, corrApplied, corrColExists,
                 stats, rnstats, alltdm, atmAutoExclude, atmWarning, atmExcludeCmd, bwthreshspw, caltable, calapps, exception=None):
        super().__init__()
        self.renorm_applied = renorm_applied
        self.vis = vis
        self.createcaltable = createcaltable
        self.threshold = threshold
        self.correctATM = correctATM
        self.spw = spw
        self.excludechan = excludechan
        self.corrApplied = corrApplied
        self.corrColExists = corrColExists
        self.stats = stats
        self.rnstats = rnstats
        self.alltdm = alltdm
        self.atmAutoExclude = atmAutoExclude
        self.atmWarning = atmWarning
        self.atmExcludeCmd = atmExcludeCmd
        self.bwthreshspw = bwthreshspw
        self.caltable = caltable
        self.calapps = calapps
        self.exception = exception

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        if not self.calapps:
            LOG.error('No results to merge')
            return
        else:
            for calapp in self.calapps:
                LOG.debug('Adding calibration to callibrary:\n'
                          '%s\n%s' % (calapp.calto, calapp.calfrom))
                context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        return (f'RenormResults:\n'
                f'\trenorm_applied={self.renorm_applied}\n'
                f'\tvis={self.vis}\n'
                f'\tcreatecaltable={self.createcaltable}\n'
                f'\tthreshold={self.threshold}\n'
                f'\tcorrectATM={self.correctATM}\n'
                f'\tspw={self.spw}\n'
                f'\texcludechan={self.excludechan}\n'
                f'\talltdm={self.alltdm}\n'
                f'\tstats={self.stats}\n'
                f'\tatmAutoExclude={self.atmAutoExclude}\n'
                f'\tbwthreshspw={self.bwthreshspw}\n'
                f'\tcaltable={self.caltable}\n')


class RenormInputs(vdp.StandardInputs):
    createcaltable = vdp.VisDependentProperty(default=False)
    threshold = vdp.VisDependentProperty(default=1.02)
    correctATM = vdp.VisDependentProperty(default=False)
    spw = vdp.VisDependentProperty(default='')
    excludechan = vdp.VisDependentProperty(default={})
    atm_auto_exclude = vdp.VisDependentProperty(default=False)
    bwthreshspw = vdp.VisDependentProperty(default={})

    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, vis=None, createcaltable=None, threshold=None, correctATM=None, spw=None,
                 excludechan=None, atm_auto_exclude=None, bwthreshspw=None, caltable=None, parallel=None):
        super().__init__()
        self.context = context
        self.vis = vis
        self.createcaltable = createcaltable
        self.threshold = threshold
        self.correctATM = correctATM
        self.spw = spw
        self.excludechan = excludechan
        self.atm_auto_exclude = atm_auto_exclude
        self.bwthreshspw = bwthreshspw
        self.caltable = caltable
        self.parallel = parallel

    @vdp.VisDependentProperty
    def caltable(self):
        """
        Get the caltable argument for these inputs.

        If set to a table-naming heuristic, this should give a sensible name
        considering the current CASA task arguments.
        """
        namer = caltable_heuristic.AmpCaltable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)


class SerialRenorm(basetask.StandardTaskTemplate):
    Inputs = RenormInputs

    def prepare(self):
        inp = self.inputs

        # FIXME: Remove? almarenorm.py could do this check if necessary.
        if not isinstance(inp.excludechan, dict):
            msg = "excludechan parameter requires dictionary input. {0} with type {1} is not valid input." \
                  "".format(inp.excludechan, type(inp.excludechan).__name__)
            LOG.error(msg)
            raise TypeError(msg)

        # FIXME: this is evaluating the presence of band 9/10 for all MSes in the observing run, even though the task
        #  is expected to operate only on the current MS.
        # Issue warning if band 9 and 10 data is found
        bands = [s.band
                 for sub in [m.get_spectral_windows() for m in inp.context.observing_run.measurement_sets]
                 for s in sub]
        if 'ALMA Band 9' in bands:
            LOG.warning('Running hifa_renorm on ALMA Band 9 (DSB) data')
        if 'ALMA Band 10' in bands:
            LOG.warning('Running hifa_renorm on ALMA Band 10 (DSB) data')

        # PIPE-2150: safely create the "RN_plots" directory before calling the renormalization external code to prevent
        # potential race condition when checking/examining the directory existence in the tier0 setup.
        # This workaround might be removed after the changes from PIPE-2151
        os.makedirs('RN_plots', exist_ok=True)

        # Create inputs for the call to the ALMA renorm function.
        alma_renorm_inputs = {
            'vis': inp.vis,
            'spw': [int(x) for x in inp.spw.split(',') if x],  # alma_renorm expects SpWs as list of integers
            'apply': inp.createcaltable,
            'threshold': inp.threshold,
            'excludechan': copy.deepcopy(inp.excludechan),  # create copy, PIPE-1612.
            'correct_atm': inp.correctATM,
            'atm_auto_exclude': inp.atm_auto_exclude,
            'bwthreshspw': inp.bwthreshspw,
            'caltable': inp.caltable
        }

        # Call the ALMA renormalization function and collect its output in task
        # result.
        try:
            LOG.info("Calling the renormalization heuristic function.")
            alltdm, atmExcludeCmd, atmWarning, corrApplied, corrColExists, renorm_applied, rnstats, stats = \
                alma_renorm(**alma_renorm_inputs)

            calapps = []
            if inp.createcaltable:
                msObj = inp.context.observing_run.get_ms(inp.vis)

                origin = callibrary.CalAppOrigin(task=SerialRenorm, inputs=inp.to_casa_args())

                tbTool = casa_tools.table
                tbTool.open(inp.caltable)
                field_ids = tbTool.getcol('FIELD_ID')
                spw_ids = tbTool.getcol('SPECTRAL_WINDOW_ID')
                tbTool.done()

                # Get unique field/spw combinations
                field_spw = dict()
                for f_id, s_id in zip(field_ids, spw_ids):
                    field_name = msObj.fields[f_id].name
                    if field_name not in field_spw:
                        field_spw[field_name] = set()
                    field_spw[field_name].add(str(s_id))

                for field_name in field_spw:
                    spw = ','.join(field_spw[field_name])

                    calto_args = {'vis': inp.vis,
                                  'intent': 'TARGET',
                                  'field': field_name,
                                  'spw': spw}
                    calto = callibrary.CalTo(**calto_args)

                    calfrom_args = {'gaintable': inp.caltable,
                                    'interp': 'nearest'}
                    calfrom = callibrary.CalFrom(**calfrom_args)

                    calapps.append(callibrary.CalApplication(calto, calfrom, origin))

            result = RenormResults(renorm_applied, inp.vis, inp.createcaltable, inp.threshold, inp.correctATM, inp.spw,
                                   inp.excludechan, corrApplied, corrColExists, stats, rnstats, alltdm,
                                   inp.atm_auto_exclude, atmWarning, atmExcludeCmd, inp.bwthreshspw, inp.caltable, calapps)
        except Exception as e:
            LOG.error('Failure in running renormalization heuristic: {}'.format(e))
            LOG.error(traceback.format_exc())
            result = RenormResults(False, inp.vis, inp.createcaltable, inp.threshold, inp.correctATM, inp.spw,
                                   inp.excludechan, False, False, {}, {}, True, inp.atm_auto_exclude, {}, {}, {}, inp.caltable, [], e)

        return result

    def analyse(self, results):
        return results


@task_registry.set_equivalent_casa_task('hifa_renorm')
@task_registry.set_casa_commands_comment('Renormalize data affected by strong line emission.')
class Renorm(sessionutils.ParallelTemplate):
    Inputs = RenormInputs
    Task = SerialRenorm
