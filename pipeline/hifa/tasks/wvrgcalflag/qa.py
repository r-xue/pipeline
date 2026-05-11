import collections.abc
import os

import pipeline.h.tasks.exportdata.aqua as aqua
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import resultobjects

LOG = infrastructure.logging.get_logger(__name__)


class WvrgcalflagQAHandler(pqa.QAPlugin):    
    """
    QA handler for an uncontained WvrgcalflagResults.
    """
    result_cls = resultobjects.WvrgcalflagResults
    child_cls = None

    def handle(self, context, result):
        ms_name = os.path.basename(result.inputs['vis'])

        # If too few unflagged antennas were left over after flagging,
        # then return a fixed low score. PIPE-1868 change score from 0.1 to 0.34
        # and add ms name to the longmsg string. If the Bandpass phase RMS
        # without-WVR is good (<1 radian) elevate score to 0.67, with updated
        # message according to if BP and PH phase RMS are good
        if result.too_few_wvr_post_flagging:
            score_too_few = 0.67 if result.flaggerresult.dataresult.BPgood else 0.34
            longmsg_too_few = 'Not enough unflagged WVR available for %s. %s' % \
                (ms_name,'Bandpass '+(str('and Phase ') if result.flaggerresult.dataresult.PHgood else '')\
                  +'calibrator atmospheric phase stability appears to be good'\
                 if result.flaggerresult.dataresult.BPgood else '')
            score_object = pqa.QAScore(
                score_too_few, longmsg=longmsg_too_few,
                shortmsg='Not enough unflagged WVR', vis=ms_name)
            new_origin = pqa.QAOrigin(
                metric_name='PhaseRmsRatio',
                metric_score=score_object.origin.metric_score,
                metric_units='Phase RMS improvement after applying WVR correction')
            score_object.origin = new_origin
            result.qa.pool[:] = [score_object]
        else:
            # Try to retrieve WVR QA score from result.
            try:
                wvr_score = result.flaggerresult.dataresult.qa_wvr.overall_score
                # If a WVR QA score was available, then adopt this as the
                # final QA score for the task.
                if wvr_score:
                    score_object = qacalc.score_wvrgcal(ms_name, result.flaggerresult.dataresult)
                    new_origin = pqa.QAOrigin(
                        metric_name='PhaseRmsRatio',
                        metric_score=score_object.origin.metric_score,
                        metric_units='Phase RMS improvement after applying WVR correction')
                    score_object.origin = new_origin
                    result.qa.pool[:] = [score_object]
                else:
                    # If wvr_score was not available, check if this is caused
                    # by too few antennas with WVR (set by threshold). If so,
                    # then no QA score is necessary; if not, then set task QA
                    # score to 0.
                    if not result.too_few_wvr:
                        score_object = pqa.QAScore(
                            0.0, longmsg='No WVR scores available',
                            shortmsg='No WVR', vis=ms_name)
                        new_origin = pqa.QAOrigin(
                            metric_name='PhaseRmsRatio',
                            metric_score=score_object.origin.metric_score,
                            metric_units='Phase RMS improvement after applying WVR correction')
                        score_object.origin = new_origin
                        result.qa.pool[:] = [score_object]
            except AttributeError:
                pass


class WvrgcalflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing WvrgcalflagResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = resultobjects.WvrgcalflagResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated


aqua_exporter = aqua.xml_generator_for_metric('PhaseRmsRatio', '{:0.3f}')
aqua.register_aqua_metric(aqua_exporter)
