import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import mstransform

LOG = logging.get_logger(__name__)


class MstransformQAHandler(pqa.QAPlugin):
    result_cls = mstransform.MstransformResults
    child_cls = None
    generating_task = mstransform.Mstransform

    def handle(self, context, result):

        # Check for existance of the the continuum MS.
        score1 = self._contms_exists(os.path.dirname(result.outputvis), os.path.basename(result.outputvis))
        scores = [score1]

        result.qa.pool.extend(scores)

    def _contms_exists(self, output_dir, target_ms):
        """
        Check for the existence of the continuum MS
        """
        return qacalc.score_path_exists(output_dir, target_ms, 'science target continuum ms')


class MstransformListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing MstransformResults.
    """
    result_cls = collections.Iterable
    child_cls = mstransform.MstransformResults
    generating_task = mstransform.Mstransform

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing target continuum MS(s) for %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg
