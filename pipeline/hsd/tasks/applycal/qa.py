"""QA score module for applycal task."""

from __future__ import annotations
import pipeline.infrastructure.basetask as basetask
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

    def handle(self, context: Context, result: SDApplycalResults) -> None:
        """Evaluate QA score for applycal result.

        Args:
            context: Pipeline context.
            result: SDApplycalResults instance.
        """
        scores = []
        scores = scorecal.score_amp_vs_time_plots(context, result)

        result.qa.pool.extend(scores)


class SDApplycalQAListHandler(pqa.QAPlugin):
    """Class to handle QA score for a list of applycal results."""

    result_cls = basetask.ResultsList
    child_cls = applycal.SDApplycalResults

    def handle(self, context: Context, result: SDApplycalResults) -> None:
        """Evaluate QA score for a list of applycal results.

        Args:
            context: Pipeline context (not used).
            result: List of SDApplycalResults instances.
        """
        pass
