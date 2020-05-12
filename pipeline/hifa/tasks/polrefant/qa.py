import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import resultobjects

LOG = logging.get_logger(__name__)


class PolRefAntQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained PolRefAntResults.
    """
    result_cls = resultobjects.PolRefAntResults
    child_cls = None

    def handle(self, context, result):
        # TODO:
        #  implement QA scoring:
        #  * was new refant list found?
        #  * any QA scoring heuristics for judging new refant list?
        pass


# TODO:
#  Since PolRefAnt is a session task, is a ListQAHandler necessary?
class PolRefAntListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing PolRefAntResults.
    """
    result_cls = basetask.ResultsList
    child_cls = resultobjects.PolRefAntResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
