from __future__ import absolute_import
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from recipes import tec_maps

LOG = infrastructure.get_logger(__name__)


class AnalyzealphaResults(basetask.Results):
    def __init__(self):
        super(AnalyzealphaResults, self).__init__()
        self.pipeline_casa_task = 'Analyzealpha'

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'AnalyzealphaResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'AnalyzealphaResults:'


class AnalyzealphaInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hif_analyzealpha')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Analyzealpha(basetask.StandardTaskTemplate):
    Inputs = AnalyzealphaInputs

    def prepare(self):

        LOG.info("This Analyzealpha class is running.")

        return AnalyzealphaResults()

    def analyse(self, results):
        return results

    def _do_somethinganalyzealpha(self):

        task = casa_tasks.analyzealphacal(vis=self.inputs.vis, caltable='tempcal.analyzealpha')

        return self._executor.execute(task)

    def _do_tec_maps(self):

        tec_maps.create(vis=self.vis, doplot=True, imname='iono')
        # gencal_job = casa_tasks.gencal(**gencal_args)
        gencal_job = casa_tasks.gencal(vis=self.vis, caltable='tec.cal', caltype='tecim', infile='iono.IGS_TEC.im')
        self._executor.execute(gencal_job)

