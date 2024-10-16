import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import checkflag
from . import targetflag
from . import flagdetervla

LOG = logging.get_logger(__name__)


class TargetflagQAHandler(pqa.QAPlugin):
    result_cls = targetflag.TargetflagResults
    child_cls = None
    generating_task = targetflag.Targetflag

    def handle(self, context, result):
        # get a QA score for flagging
        # < 5%   of data flagged  --> 1
        # 5%-60% of data flagged  --> 1 to 0
        # > 60%  of data flagged  --> 0

        if result.summarydict:
            score1 = qacalc.score_total_data_flagged_vla(os.path.basename(result.inputs['vis']),
                                                         result.summarydict)
            scores = [score1]
        else:
            LOG.error('No flag summary statistics.')
            scores = [pqa.QAScore(0.0, longmsg='No flag summary statistics',
                                  shortmsg='Flag Summary off')]

        result.qa.pool[:] = scores


class TargetflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing TargetflagResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = targetflag.TargetflagResults
    generating_task = targetflag.Targetflag

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool[:] for r in result])
        result.qa.pool[:] = collated


class FlagdataQAHandler(pqa.QAPlugin):
    result_cls = flagdetervla.FlagDeterVLAResults
    child_cls = None
    generating_task = flagdetervla.FlagDeterVLA

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        score = qacalc.score_vla_agents(ms, result.summaries)
        result.qa.pool[:] = [score]


class FlagdataListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing TargetflagResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = targetflag.TargetflagResults
    generating_task = targetflag.Targetflag

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool[:] for r in result])
        result.qa.pool[:] = collated


class CheckflagQAHandler(pqa.QAPlugin):
    result_cls = checkflag.CheckflagResults
    child_cls = None
    generating_task = checkflag.Checkflag

    def handle(self, context, result):
        # get a QA score for flagging
        # < 5%   of data flagged  --> 1
        # 5%-60% of data flagged  --> 1 to 0
        # > 60%  of data flagged  --> 0

        if result.summaries:
            score1 = qacalc.score_total_data_flagged_vla(os.path.basename(result.inputs['vis']),
                                                         result.summaries)
            scores = [score1]
        else:
            LOG.error('No flag summary statistics.')
            scores = [pqa.QAScore(0.0, longmsg='No flag summary statistics',
                                  shortmsg='Flag Summary off')]

        result.qa.pool[:] = scores


class CheckflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing CheckflagResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = checkflag.CheckflagResults
    generating_task = checkflag.Checkflag

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool[:] for r in result])
        result.qa.pool[:] = collated

