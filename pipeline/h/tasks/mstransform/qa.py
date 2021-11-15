import os

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import mssplit

LOG = logging.get_logger(__name__)


class MsSplitQAHandler(pqa.QAPlugin):
    result_cls = mssplit.MsSplitResults
    child_cls = None
    generating_task = mssplit.MsSplit

    def handle(self, context, result):

        # Check for existance of the the continuum MS.
        score1 = self._contms_exists(os.path.dirname(result.outputvis), os.path.basename(result.outputvis))
        scores = [score1]

        result.qa.pool.extend(scores)

    def _contms_exists(self, output_dir, cont_ms):
        """
        Check for the existence of the cont MS
        """
        return qacalc.score_path_exists(output_dir, cont_ms, 'science continuum ms')


class MsSplitListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing MsSplitResults.
    """
    result_cls = basetask.ResultsList
    child_cls = mssplit.MsSplitResults
    generating_task = mssplit.MsSplit

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing continuum MS(s) for %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg
