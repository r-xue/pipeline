import collections.abc

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import diffgaincal

LOG = logging.get_logger(__name__)


class DiffgaincalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained DiffgaincalResults.
    """
    result_cls = diffgaincal.DiffGaincalResults
    child_cls = None
    generating_task = diffgaincal.DiffGaincal

    def handle(self, context, result):
        ms = context.observing_run.get_ms(result.inputs['vis'])
        # Only create QA scores for BandToBand measurement sets:
        if ms.is_band_to_band:
            result.qa.pool.extend(self._score_gaincal_result(ms, result.ref_phase_result,
                                                             caltable_type='diffgaincal reference caltable',
                                                             phaseup_type='low frequency reference'))
            result.qa.pool.extend(self._score_gaincal_result(ms, result,
                                                             caltable_type='diffgaincal B2B offset caltable',
                                                             phaseup_type='offset'))
            result.qa.pool.extend(self._score_gaincal_result(ms, result.residual_phase_result,
                                                             caltable_type='diffgaincal residual caltable',
                                                             phaseup_type='residual diagnostics'))

    @staticmethod
    def _score_gaincal_result(ms, result, caltable_type, phaseup_type) -> list[pqa.QAScore]:
        """Score the band-to-band result and caltable."""
        scores = []
        if result is None:
            return scores

        if result.final:
            gaintable = result.final[0].gaintable
            # Retrieve combine parameter for gaintable result and create score
            # for whether spw combination was used.
            combine = utils.get_origin_input_arg(result.pool[0], 'combine')
            scores.append(qacalc.score_diffgaincal_combine(ms.name, combine, result.qa_message, phaseup_type))
        elif result.error:
            gaintable = list(result.error)[0].gaintable
        else:
            gaintable = None

        # Create score for whether caltable exists.
        scores.append(qacalc.score_path_exists(ms.name, gaintable, caltable_type))

        return scores


class DiffgaincalListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing DiffgaincalResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = diffgaincal.DiffGaincalResults
    generating_task = diffgaincal.DiffGaincal

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        mses = [r.inputs['vis'] for r in result]
        longmsg = f"Diffgain phase caltables created for {utils.commafy(mses, quotes=False, conjunction='and')}"
        result.qa.all_unity_longmsg = longmsg
