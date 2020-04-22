import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import resultobjects

LOG = logging.get_logger(__name__)


class BandpassflagQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained BandpassflagResults.
    """
    result_cls = resultobjects.BandpassflagResults
    child_cls = None

    def handle(self, context, result):
        # Run correctedampflag QA on correctedampflag result.
        pqa.qa_registry.do_qa(context, result.cafresult)

        # Store flagging score into result.
        result.qa.pool[:] = result.cafresult.qa.pool


class BandpassflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing BandpassflagResults.
    """
    result_cls = basetask.ResultsList
    child_cls = resultobjects.BandpassflagResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
