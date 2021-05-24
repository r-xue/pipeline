import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import atmcor

LOG = logging.get_logger(__name__)


class SDATMCorrectionQAHandler(pqa.QAPlugin):
    result_cls = atmcor.SDATMCorrectionResults
    child_cls = None

    def handle(self, context, result):
        atmcor_ms_name = result.outcome
        is_outfile_exists = os.path.exists(atmcor_ms_name)
        task_exec_status = result.success
        is_successful = (task_exec_status is True) and (is_outfile_exists is True)

        vis = os.path.basename(result.inputs['vis'])

        if is_successful:
            shortmsg = 'Execution of sdatmcor was successful'
            longmsg = f'Execution of sdatmcor for {vis} was successful'
            score = 1.0
        else:
            shortmsg = 'Execution of sdatmcor failed'
            longmsg = f'Execution of sdatmcor for {vis} failed. output MS may be created but will be corrupted.'
            score = 0.0
        scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]

        result.qa.pool.extend(scores)


class SDATMCorrectionListQAHandler(pqa.QAPlugin):
    result_cls = collections.Iterable
    child_cls = atmcor.SDATMCorrectionResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
