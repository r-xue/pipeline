import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class PolcalResults(basetask.Results):
    def __init__(self, vis=None):
        super().__init__()
        self.vis = vis

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'PolcalResults'


class PolcalInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_polcal')
@task_registry.set_casa_commands_comment('Compute the polarisation calibration.')
class Polcal(basetask.StandardTaskTemplate):
    Inputs = PolcalInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results.
        result = PolcalResults(vis=inputs.vis)

        return result

    def analyse(self, results):
        return results
