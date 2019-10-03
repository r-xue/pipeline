import pipeline.h.tasks.applycal as h_applycal
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PlotSummaryInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_plotsummary')
class PlotSummary(basetask.StandardTaskTemplate):
    Inputs = PlotSummaryInputs

    def prepare(self):
        # get the applied calibration state from callibrary.active. This holds
        # the CalApplications for everything applied by the pipeline in this run.
        applied = self.inputs.context.callibrary.applied.merged()

        calapps = [callibrary.CalApplication(calto, calfroms) for calto, calfroms in applied.items()]

        result = h_applycal.ApplycalResults(applied=calapps)

        return result

    def analyse(self, results):
        return results
