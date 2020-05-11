import os

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
import pipeline.infrastructure.casatools as casatools
from pipeline.hifv.heuristics import set_add_model_column_parameters

LOG = infrastructure.get_logger(__name__)


class SelfcalResults(basetask.Results):
    def __init__(self, caltable=None):
        super(SelfcalResults, self).__init__()
        self.pipeline_casa_task = 'Selfcal'

        self.caltable = caltable

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'SelfcalResults: Executed on caltable {!s}'.format(self.caltable)


class SelfcalInputs(vdp.StandardInputs):
    refantignore = vdp.VisDependentProperty(default='')
    combine = vdp.VisDependentProperty(default='spw,field')
    selfcalmode = vdp.VisDependentProperty(default='VLASS')
    overwrite_modelcol = vdp.VisDependentProperty(default=False)
    refantmode = 'strict'

    def __init__(self, context, vis=None, refantignore=None, combine=None, selfcalmode=None, refantmode=None,
                 overwrite_modelcol=None):
        self.context = context
        self.vis = vis
        self.refantignore = refantignore
        self.combine = combine
        self.selfcalmode = selfcalmode
        self.refantmode = refantmode
        self.overwrite_modelcol = overwrite_modelcol


@task_registry.set_equivalent_casa_task('hifv_selfcal')
@task_registry.set_casa_commands_comment('hifv_selfcal task')
class Selfcal(basetask.StandardTaskTemplate):
    Inputs = SelfcalInputs

    def prepare(self):

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        # context = self.inputs.context
        # m = self.inputs.context.observing_run.measurement_sets[0]
        # mses = context.evla['msinfo'].keys()
        # refantfield = context.evla['msinfo'][mses[0]].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field='',
                                                geometry=True, flagging=True, intent='',
                                                spw='', refantignore=self.inputs.refantignore)

        self.RefAntOutput = refantobj.calculate()

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_selfcal.s'

        self.caltable = tableprefix + str(stage_number) + '_1.' + 'phase-self-cal.tbl'

        LOG.info('Checking for model column')
        self._check_for_modelcolumn()
        self._do_gaincal()
        self._do_applycal()

        return SelfcalResults(caltable=self.caltable)

    def analyse(self, results):
        return results

    def _check_for_modelcolumn(self):
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casatools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames() or self.inputs.overwrite_modelcol:
                LOG.info('Writing model data to {}'.format(ms.basename))
                imaging_parameters = set_add_model_column_parameters(self.inputs.context)
                job = casa_tasks.tclean(**imaging_parameters)
                tclean_result = self._executor.execute(job)
            else:
                LOG.info('Using existing MODEL_DATA column found in {}'.format(ms.basename))

    def _do_gaincal(self):
        """Run CASA task gaincal"""

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spwsobjlist = m.get_spectral_windows(science_windows_only=True)
        spws = ','.join([str(spw.id) for spw in spwsobjlist])

        casa_task_args = {'vis': self.inputs.vis,
                          'caltable': self.caltable,
                          'spw': spws,
                          'solint': 'inf',
                          'combine': self.inputs.combine,
                          'refant': ','.join(self.RefAntOutput),
                          'refantmode': self.inputs.refantmode,
                          'minblperant': 4,
                          'minsnr': 1.0,
                          'gaintype': 'G',
                          'calmode': 'p',
                          'parang': False,
                          'append': False}

        job = casa_tasks.gaincal(**casa_task_args)

        return self._executor.execute(job)

    def _do_applycal(self):
        """Run CASA task applycal"""

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spwsobjlist = m.get_spectral_windows(science_windows_only=True)
        spws = [int(spw.id) for spw in spwsobjlist]
        numspws = len(m.get_spectral_windows(science_windows_only=False))
        lowestscispwid = str(min(spws))  # PIPE-101

        applycal_task_args = {'vis': self.inputs.vis,
                              'gaintable': self.caltable,
                              'interp': ['nearestPD'],
                              'spwmap': [numspws*[lowestscispwid]],
                              'parang': False,
                              'applymode': 'calonly'}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)
