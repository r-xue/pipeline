import collections
import numpy as np

from pipeline.infrastructure import casa_tools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.renderer import rendererutils
import pipeline.qa.scorecalculator as qacalc

from . import statwt

LOG = logging.get_logger(__name__)


class StatwtQAHandler(pqa.QAPlugin):
    result_cls = statwt.StatwtResults
    child_cls = None
    generating_task = statwt.Statwt

    def get_max_plot_weight(self, tbl: str) -> float:
        query_str = "ntrue(FLAG)==0"
        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query_str)
            weights = stb.getcol('CPARAM').ravel()
            stb.done()
        max_value = np.max(weights.real)
        return max_value

    def handle(self, context, result):
        vis = result.inputs['vis']
        
        mean = result.jobs[0]['mean']
        variance = result.jobs[0]['variance']
      
        # (1) Gigantic weights
        mean_origin = pqa.QAOrigin(metric_name='%StatwtMean',
                                   metric_score=mean,
                                   metric_units='')

        score = np.max([1 - (np.log10(mean)/6.0)**3.5, 0.0])

        if score <= 0.9:
            shortmsg = "Elevated weights"
            longmsg = "Elevated weights."
        elif score < 0.75:
            shortmsg = "High weights"
            longmsg = "High weights."
        elif score < 0.5:
            shortmsg = "Very high weights"
            longmsg = "Very high weights."
        else:
            shortmsg = "Mean wight OK"
            longmsg = "Mean weight is within normal range."

        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=mean_origin))

        # (2) High weights
        # Condition: weights in stats plots > 150
        max_plot_weight = self.get_max_plot_weight(result.wtables['after'])
        plot_weights_origin = pqa.QAOrigin(metric_name='%StatwtPlotWeight',
                                           metric_score=max_plot_weight,
                                           metric_units='')
        if max_plot_weight > 150:
            score = rendererutils.SCORE_THRESHOLD_SUBOPTIMAL
            shortmsg = "Very high weights"
            longmsg = "Very high weights."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=plot_weights_origin))
        else:
            score = 1.0
            shortmsg = "Weights from plots OK"
            longmsg = "Weights from plots are within normal bounds."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=plot_weights_origin))

        # (3) High variance
        variance_origin = pqa.QAOrigin(metric_name='%StatwtVariance',
                                       metric_score=variance,
                                       metric_units='')

        if variance > mean**2:
            score = rendererutils.SCORE_THRESHOLD_SUBOPTIMAL
            shortmsg = "Very high variance"
            longmsg = "Very high variance."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=variance_origin))
        else:
            score = 1.0
            shortmsg = "Variance OK"
            longmsg = "Variance of the weights is within normal range."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=variance_origin))

        # (4) Flagging increase
        # Condition: flagging increase in statwt > 2% QA score < 0.5
        flagging_increase = qacalc.calc_frac_newly_flagged(result.summaries, agents=["statwt"])

        flagging_origin = pqa.QAOrigin(metric_name='%StatwtFlagging',
                                       metric_score=flagging_increase,
                                       metric_units='')
        if flagging_increase > 0.02:
            score = 0.4
            shortmsg = "High flagging increase"
            longmsg = "High flagging increase."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=flagging_origin))
        else:
            score = 1.0
            shortmsg = "Flagging increase OK"
            longmsg = "Flagging increase is within normal range."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=flagging_origin))


class StatwtListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing StatwtResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = statwt.StatwtResults
    generating_task = statwt.Statwt

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
