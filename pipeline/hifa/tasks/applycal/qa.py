import collections
import operator
import os

import pipeline.h.tasks.applycal.applycal as h_applycal
import pipeline.hif.tasks.applycal.ifapplycal as hif_applycal
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import ampphase_vs_freq_qa

LOG = logging.get_logger(__name__)

REASONS_TO_TEXT = {
    'amp.intercept,amp.slope': ('Amp-freq', 'zero point and slope outliers'),
    'amp.intercept': ('Amp-freq', 'zero point outliers'),
    'amp.slope': ('Amp-freq', 'slope outliers'),
    'phase.intercept,phase.slope': ('Phase-freq', 'zero point and slope outliers'),
    'phase.intercept': ('Phase-freq', 'zero point outliers'),
    'phase.slope': ('Phase-freq', 'slope outliers')
}

PIPE356Switches = collections.namedtuple(
    'PIPE356Switches', 'calculate_metrics export_outliers export_warnings include_scores outlier_score')

PIPE356_MODES = {
    'ON': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_warnings=False, include_scores=True,
                          outlier_score=0.9),
    'DEBUG': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_warnings=True, include_scores=False,
                             outlier_score=0.5),
    'OFF': PIPE356Switches(calculate_metrics=False, export_outliers=False, export_warnings=False, include_scores=False,
                           outlier_score=0.5)
}


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

        pipe356_mode = os.environ.get('PIPE356_QA_MODE', 'ON').upper()
        mode_switches = PIPE356_MODES[pipe356_mode]

        if mode_switches.calculate_metrics:
            qa_scores = get_qa_scores(ms, mode_switches.export_outliers, mode_switches.outlier_score)
        else:
            qa_scores = []

        if mode_switches.export_warnings:
            with open('PIPE356_outliers.txt', 'a') as export_file:
                for qa_score in qa_scores:
                    export_file.write('{}\n'.format(qa_score.longmsg))

        if mode_switches.include_scores:
            result.qa.pool.extend(qa_scores)


def get_qa_scores(ms, export_outliers, outlier_score):
    intents = ['AMPLITUDE', 'BANDPASS', 'PHASE', 'CHECK']

    # holds the metrics that generated an outlier
    outlier_metrics = set()

    all_scores = []
    for intent in intents:
        outliers = ampphase_vs_freq_qa.score_all_scans(ms, intent, export_outliers)
        consolidated = ampphase_vs_freq_qa.consolidate_data_selections(outliers)
        scores_for_intent = outliers_to_qa_scores(ms, consolidated, outlier_score)
        all_scores.extend(scores_for_intent)

        outlier_metrics.update({metric for outlier in outliers for metric in outlier.reason})

    # add a 1.0 score for metrics that generated no outlier. This cannot be
    # done in lower level stages as those stages operate on a per-spw/per-scan
    # basis, which would give us a 1.0 entry per spw/scan.
    all_metrics = {'amp.intercept', 'amp.slope', 'phase.intercept', 'phase.slope'}
    metrics_with_no_outliers = all_metrics - outlier_metrics
    for metric in metrics_with_no_outliers:
        metric_axes, outlier_description = REASONS_TO_TEXT[metric]
        short_msg = 'No {} outliers'.format(metric_axes)
        long_msg = 'No {} {} detected for {}'.format(metric_axes, outlier_description, ms.basename)
        score = pqa.QAScore(1.0, longmsg=long_msg, shortmsg=short_msg)
        score.origin = pqa.QAOrigin(metric_name=metric,
                                    metric_score=0,
                                    metric_units='number of outliers')
        all_scores.append(score)

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


def outliers_to_qa_scores(ms, outliers, outlier_score):
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

        # for a 'joint' outlier, e.g., an outlier with reason=amp.phase,amp.slope, this identifies
        # the worst outlier as the one with maximum deviation for *any* of its reasons. That is, an
        # outlier may have a small amp.phase deviation and a huge amp.slope deviation, and it
        # would be identified as the worst outlier by virtue of the amp.slope deviation. AQUA
        # doesn't currently make use of these metrics, but if it ever does then it needs to
        # understand this or we modify what's reported as the origin.
        worst_outlier = max(outliers_for_reason, key=lambda outlier: max([s for s in outlier.num_sigma]))

        metric_axes, outlier_description = REASONS_TO_TEXT[reason]
        short_msg = '{} outliers'.format(metric_axes)

        score = pqa.QAScore(outlier_score, longmsg=long_msg, shortmsg=short_msg)
        score.origin = pqa.QAOrigin(metric_name=reason,
                                    metric_score=max(worst_outlier.num_sigma),
                                    metric_units='sigma deviation from reference fit')
        qa_scores.append(score)

    return qa_scores


# def all_ok_qascore(ms, all_scores):
#     qa_scores = []
#     all_metrics = {'amp.intercept', 'amp.slope', 'phase.intercept', 'phase.slope'}
#
#     outlier_metrics = {metric for metric in qascore.}
#
#     outlier_metrics = {metric for outlier in outliers for metric in outlier.reason}
#     metrics_with_no_outliers = all_metrics - outlier_metrics
#     for metric in metrics_with_no_outliers:
#         short_metric, long_metric = REASONS_TO_TEXT[metric]
#         short_msg = 'No {} outliers'.format(short_metric)
#         long_msg = 'No {} detected for {}'.format(long_metric, ms.basename)
#         score = pqa.QAScore(1.0, longmsg=long_msg, shortmsg=short_msg)
#         score.origin = pqa.QAOrigin(metric_name=metric,
#                                     metric_score=0,
#                                     metric_units='number of outliers')
#         qa_scores.append(score)
#
#     return qa_scores
