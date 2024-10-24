import collections

import pipeline.h.tasks.exportdata.aqua as aqua
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from pipeline.infrastructure.refantflag import format_fully_flagged_antenna_notification
from . import resultobjects

LOG = logging.get_logger(__name__)


class LowgainflagQAHandler(pqa.QAPlugin):
    result_cls = resultobjects.LowgainflagResults
    child_cls = None

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # Calculate QA score from presence of flagging views and from the
        # flagging summary in the result, adopting the minimum score as the
        # representative score for this task.

        score1 = qacalc.score_fraction_newly_flagged(ms.basename, result.summaries, ms.basename)
        new_origin = pqa.QAOrigin(metric_name='%HighLowGainFlags',
                                  metric_score=score1.origin.metric_score,
                                  metric_units='Percentage of high or low gain flag data newly flagged')
        score1.origin = new_origin

        score2 = qacalc.score_flagging_view_exists(ms.basename, result)
        new_origin = pqa.QAOrigin(metric_name='ValidFlaggingView',
                                  metric_score=score2.origin.metric_score,
                                  metric_units='Valid flagging view')
        score2.origin = new_origin

        scores = [score1, score2]

        try:
            for notification in result.fully_flagged_antenna_notifications:
                score = pqa.QAScore(
                    0.8,
                    longmsg=format_fully_flagged_antenna_notification(result.inputs['vis'], notification),
                    shortmsg='Fully flagged antennas',
                    vis=result.inputs['vis'])
                scores.append(score)
        except AttributeError:
            LOG.error('Unable to find the list of fully flagged antennas')

        result.qa.pool[:] = scores


class LowgainflagListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = resultobjects.LowgainflagResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated


aqua_exporter = aqua.xml_generator_for_metric('%HighLowGainFlags', '{:0.3%}')
aqua.register_aqua_metric(aqua_exporter)
