import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.tsysflag.qa import TsysflagQAHandler
from pipeline.h.tasks.tsysflag.resultobjects import TsysflagResults
from .tsysflagcontamination import TsysFlagContamination

LOG = logging.get_logger(__name__)


class TsysflagContaminationQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained TsysflagResult generated by TsysflagContamination.
    """

    result_cls = TsysflagResults
    child_cls = None
    generating_task = TsysFlagContamination

    def handle(self, context, result):
        # we must instantiate rather than extend TsysflagQAHandler as plugin
        # registration only works on classes that directly extend QAPlugin
        delegate = TsysflagQAHandler()
        delegate.handle(context, result)
        result.qa.pool = [
            score
            for score in result.qa.pool
            if score.longmsg != "Task ended prematurely"
        ]


class TsysflagContaminationListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing TsysflagResults generated by TsysflagContamination.
    """

    result_cls = collections.abc.Iterable
    child_cls = TsysflagResults
    generating_task = TsysFlagContamination

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        caltables = [r.inputs["caltable"] for r in result]
        longmsg = "No extra data was flagged in %s".format(
            utils.commafy(caltables, quotes=False, conjunction="or")
        )
        result.qa.all_unity_longmsg = longmsg
