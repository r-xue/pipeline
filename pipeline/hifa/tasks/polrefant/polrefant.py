import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from .resultobjects import PolRefAntResults

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'PolRefAntInputs',
    'PolRefAntResults',
    'PolRefAnt'
]


class PolRefAntInputs(vdp.StandardInputs):
    """
    PolRefAntInputs defines the inputs for the PolRefAnt pipeline task.
    """
    def __init__(self, context, vis=None, output_dir=None):
        self.context = context
        self.vis = vis
        self.output_dir = output_dir


@task_registry.set_equivalent_casa_task('hifa_polrefant')
@task_registry.set_casa_commands_comment(
    'Reference antenna lists from all measurement sets within current session\n'
    'are evaluated and combined into a single common ranked reference antenna\n'
    'list for the session, that is to be used in any subsequent pipeline\n'
    'stages.'
)
class PolRefAnt(basetask.StandardTaskTemplate):
    Inputs = PolRefAntInputs

    # This is a multi-vis task that handles all MSes in a session at once.
    is_multi_vis_task = True

    def __init__(self, inputs):
        super(PolRefAnt, self).__init__(inputs)

    def prepare(self, **parameters):
        # Initialize results.
        result = PolRefAntResults()

        return result

    def analyse(self, result):
        return result
