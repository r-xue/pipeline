import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import restoredata

LOG = logging.get_logger(__name__)

class RestoreQAHandler(pqa.QAPlugin):
    result_cls = restoredata.RestoreResults
    child_cls = None
    generating_task = restoredata.Restore

    def handle(self, context, result):
        # Check to see if renorm was applied
        score1 = qacalc.score_renorm(result)
        scores = [score1]

        result.qa.pool.extend(scores)

class RestoreListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing RestoreResults.
    """

    def handle(self, context, result):
        pass