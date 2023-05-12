import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils

from . import polcal

LOG = logging.get_logger(__name__)


class PolcalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained PolcalResults.
    """
    result_cls = polcal.PolcalResults
    child_cls = None

    def handle(self, context, result):
        pass


class PolcalflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing PolcalResults.
    """
    result_cls = basetask.ResultsList
    child_cls = polcal.PolcalResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
