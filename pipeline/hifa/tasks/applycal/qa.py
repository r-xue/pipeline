from __future__ import absolute_import

import operator

import pipeline.h.tasks.applycal.applycal as h_applycal
import pipeline.hif.tasks.applycal.ifapplycal as hif_applycal
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import ampphase_vs_freq_qa

LOG = logging.get_logger(__name__)


class ALMAApplycalQAHandler(pqa.QAPlugin):
    result_cls = h_applycal.ApplycalResults
    child_cls = None
    generating_task = hif_applycal.IFApplycal

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # skip if this is a VLA data set
        if 'ALMA' not in ms.antenna_array.name:
            return

        qa_scores = get_qa_scores(ms)
        result.qa.pool.extend(qa_scores)


def get_qa_scores(ms):
    intents = ['AMPLITUDE', 'BANDPASS', 'PHASE', 'CHECK']
    all_scores = []
    for intent in intents:
        outliers = ampphase_vs_freq_qa.score_all_scans(ms, intent)
        consolidated = ampphase_vs_freq_qa.consolidate_data_selections(outliers)
        scores_for_intent = outliers_to_qa_scores(ms, consolidated)
        all_scores.extend(scores_for_intent)
    return all_scores


class QAMessage(object):
    """
    QAMessage constructs a user-friendly QA message for an Outlier.

    The QAMessage instance has two attributes, full_message and short_message,
    that are of interest. full_message holds the text to be used when the
    message is the first to be printed. short_message holds the text to be
    used when this message is to be appended to the text of other QAMessages.
    Naturally, this assumes the the calling code only concatenates messages
    that originate from the same reason.
    """

    def __init__(self, ms, outlier):
        metric_axes, outlier_description = REASONS_TO_TEXT[outlier.reason]

        # convert ant=1,3,5 to ant=DV03,CM05,CM08 etc.
        ant_names = sorted([ant.name for ant_id in outlier.ant for ant in ms.antennas if ant.id == ant_id])
        ant_msg = ','.join(ant_names)

        # convert pol=0,1 to pol=XX,YY
        # corr axis should be the same for all windows so just pick the first
        spw_id = list(outlier.spw)[0]
        corr_axes = dict(enumerate(ms.get_data_description(spw=spw_id).corr_axis))
        corrs = sorted([corr_axes[c] for c in outlier.pol])
        corr_msg = ','.join(corrs)

        msg_args = dict(
            vis=utils.commafy(sorted(outlier.vis), quotes=False),
            intent=utils.commafy(sorted(outlier.intent), quotes=False),
            scan=utils.find_ranges(outlier.scan),
            spw=utils.find_ranges(outlier.spw),
            ant=ant_msg,
            corr=corr_msg,
            metric_axes=metric_axes,
            outlier_description=outlier_description
        )

        self.short_message = '{ant} spw {spw} {corr} scan {scan}'.format(**msg_args)
        self.full_message = ('{metric_axes} for {vis}, {intent} calibrator: {outlier_description}='
                             '{short_message}'.format(short_message=self.short_message, **msg_args))

def outliers_to_qa_scores(ms, outliers):
    """
    Convert a list of consolidated Outliers into a list of equivalent
    QAScores.

    :param ms: MeasurementSet domain object for the DataSelections
    :param outliers: list of Outliers
    :return:
    """
    hashable = []
    for outlier in outliers:
        # convert ['amp.slope','amp.intercept'] into 'amp.slope,amp.intercept', etc.
        hashable.append(ampphase_vs_freq_qa.Outlier(vis=outlier.vis,
                                                    intent=outlier.intent,
                                                    scan=outlier.scan,
                                                    spw=outlier.spw,
                                                    ant=outlier.ant,
                                                    pol=outlier.pol,
                                                    num_sigma=outlier.num_sigma,
                                                    reason=','.join(sorted(outlier.reason))))
    reasons = {outlier.reason for outlier in hashable}

    # recursive function to concatenate QAMessages, taking the long QA message
    # for the first QAMessage, and the short message for subsequent messages.
    def combine(messages):
        if len(messages) == 1:
            return messages[0].full_message
        return '{}; {}'.format(combine(messages[1:]), messages[0].short_message)


    qa_scores = []
    for reason in reasons:
        outliers_for_reason = [outlier for outlier in hashable if outlier.reason == reason]
        if not outliers_for_reason:
            continue

        msgs = [QAMessage(ms, outlier) for outlier in outliers_for_reason]
        long_msg = combine(msgs)

        worst_outlier = max(outliers_for_reason, key=operator.attrgetter('num_sigma'))
        vis = ','.join(worst_outlier.vis)
        intent = ','.join(worst_outlier.intent)

        metric_axes, outlier_description = REASONS_TO_TEXT[outlier.reason]
        short_msg = '{} {} for {} {} calibrator'.format(metric_axes, outlier_description, vis, intent)

        score = pqa.QAScore(0.5, longmsg=long_msg, shortmsg=short_msg)
        score.origin = pqa.QAOrigin(metric_name='sigma_deviation',
                                    metric_score=worst_outlier.num_sigma,
                                    metric_units='Sigma deviation from reference fit')
        qa_scores.append(score)

    return qa_scores


REASONS_TO_TEXT = {
    'amp.intercept,amp.slope': ('Amp-freq', 'zero point and slope outliers'),
    'amp.intercept': ('Amp-freq', 'zero point outliers'),
    'amp.slope': ('Amp-freq', 'slope outliers'),
    'phase.intercept,phase.slope': ('Phase-freq', 'zero point and slope outliers'),
    'phase.intercept': ('Phase-freq', 'zero point outliers'),
    'phase.slope': ('Phase-freq', 'slope outliers')
}
