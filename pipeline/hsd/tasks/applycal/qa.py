"""QA score module for applycal task."""

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.qa.scorecalculator as scorecal
from . import applycal
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.hsd.tasks.applycal.applycal import SDApplycalResults

LOG = logging.get_logger(__name__)


class SDApplyCalQAHandler(pqa.QAPlugin):
    """Class to handle QA score for skycal result."""

    result_cls = applycal.SDApplycalResults
    child_cls = None

    def handle(self, context: 'Context', result: 'SDApplycalResults') -> None:
        """Evaluate QA score for applycal result.

        Args:
            context: Pipeline context.
            result: SDApplycalResults instance.
        """
        scores = []
        scores = scorecal.score_sdapplycal_flagged(context, result)

        result.qa.pool.extend(scores)
