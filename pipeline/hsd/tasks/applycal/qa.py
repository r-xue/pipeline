"""QA score module for applycal task."""
from __future__ import annotations

import collections
from typing import TYPE_CHECKING

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.h.tasks.exportdata.aqua as aqua
import pipeline.qa.scorecalculator as qacalc
from .applycal import SerialSDApplycal, SDApplycal
from . import applycal

if TYPE_CHECKING:
    from pipeline.hsd.tasks.applycal.applycal import SDApplycalResults
    from pipeline.infrastructure.basetask import ResultsList
    from pipeline.infrastructure.launcher import Context

LOG = logging.get_logger(__name__)


class SDApplyCalQAHandler(pqa.QAPlugin):
    """Class to handle QA score for skycal result."""

    result_cls = applycal.SDApplycalResults
    child_cls = None
    generating_task = SerialSDApplycal

    def handle(self, context: Context, result: SDApplycalResults) -> None:
        """Evaluate QA score for applycal result.

        Args:
            context: Pipeline context.
            result: SDApplycalResults instance.
        """
        scores = []
        scores = qacalc.score_amp_vs_time_plots(context, result)
        result.qa.pool.extend(scores)


class SDApplycalListQAHandler(pqa.QAPlugin):
    """Class to handle QA score for a list of applycal results."""

    result_cls = collections.abc.Iterable
    child_cls = applycal.SDApplycalResults
    generating_task = SDApplycal

    def handle(self, context: Context, result: ResultsList) -> None:
        """Evaluate QA score for a list of applycal results.

        Args:
            context: Pipeline context (not used).
            result: List of SDApplycalResults instances.
        """
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool.extend(collated)


aqua_exporter = aqua.xml_generator_for_metric('%ApplycalFlags', '{:0.3%}')
aqua.register_aqua_metric(aqua_exporter)
