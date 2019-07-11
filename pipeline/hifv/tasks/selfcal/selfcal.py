from __future__ import absolute_import
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry

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
    refant = vdp.VisDependentProperty(default='0')
    combine = vdp.VisDependentProperty(default='spw,field')
    selfcalmode = vdp.VisDependentProperty(default='VLASS')

    def __init__(self, context, vis=None, refant=None, combine=None, selfcalmode=None):
        self.context = context
        self.vis = vis
        self.refant = refant
        self.combine = combine
        self.selfcalmode = selfcalmode


@task_registry.set_equivalent_casa_task('hifv_selfcal')

@task_registry.set_casa_commands_comment('hifv_selfcal task')
class Selfcal(basetask.StandardTaskTemplate):
    Inputs = SelfcalInputs

    def prepare(self):

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_selfcal.s'

        self.caltable = tableprefix + str(stage_number) + '_1.' + 'phase-self-cal.tbl'

        self._do_gaincal()
        self._do_applycal()

        return SelfcalResults(caltable=self.caltable)

    def analyse(self, results):
        return results

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
                          'refant': self.inputs.refant,
                          'refantmode': 'strict',
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
        spws = [str(spw.id) for spw in spwsobjlist]
        numspws = len(spws)
        lowestscispwid = min(spws)  # PIPE-101

        applycal_task_args = {'vis': self.inputs.vis,
                              'gaintable': self.caltable,
                              'interp': ['nearestPD'],
                              'spwmap': [numspws*[lowestscispwid]],
                              'parang': False,
                              'applymode': 'calonly'}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)

