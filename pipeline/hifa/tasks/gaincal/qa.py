import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
import pipeline.qa.gpcal as gpcal
from pipeline.hif.tasks.gaincal import common

import numpy as np

LOG = logging.get_logger(__name__)


class TimegaincalQAPool(pqa.QAScorePool):
    score_types = {
        'PHASE_SCORE_XY': ('XY_TOTAL', 'X-Y phase deviation'),
        'PHASE_SCORE_X2X1': ('X2X1_TOTAL', 'X2-X1 phase deviation')
    }

    short_msg = {
        'PHASE_SCORE_XY': 'X-Y deviation',
        'PHASE_SCORE_X2X1': 'X2-X1 deviation'
    }

    def __init__(self, phase_qa_results_dict, phase_offsets_qa_results_dict):
        super(TimegaincalQAPool, self).__init__()
        self.phase_qa_results_dict = phase_qa_results_dict
        self.phase_offsets_qa_results_dict = phase_offsets_qa_results_dict

    def update_scores(self, ms):

        # X-Y / X2-X1 scores
        try:
            #
            # PIPE-365: Revise hifa_timegaincal QA scores for Cy7
            #
            # Remy: Remove consideration of X-Y, X2-X1 phase vs. time metrics
            # from QA scoring (keep evaluation for now - I will open a CASR to
            # revise these evaluations)
            #
            # phase_field_ids = [field.id for field in ms.get_fields(intent='PHASE')]
            # self.pool.extend([self._get_xy_x2x1_qascore(ms, phase_field_ids, t) for t in self.score_types])
            pass
        except Exception as e:
            LOG.error('X-Y / X2-X1 score calculation failed: %s' % (e))
        else:
            # We need to
            long_msg = 'QA metric calculation successful for {}'.format(ms.basename)
            short_msg = 'QA measured'
            origin = pqa.QAOrigin(metric_name='timegaincal_qa_calculated',
                                  metric_score=1,
                                  metric_units='Timegaincal QA metrics calculated')
            ms_qa_score = pqa.QAScore(score=1.0, longmsg=long_msg, shortmsg=short_msg, vis=ms.basename, origin=origin)
            self.pool.append(ms_qa_score)

        # Phase offsets scores
        try:
            tbTool = casa_tools.table

            phase_scan_ids = [s.id for s in ms.get_scans(scan_intent='PHASE')]
            for gaintable in self.phase_offsets_qa_results_dict:
                tbTool.open(gaintable)
                # Get phase offsets in degrees, obeying any flags.
                phase_offsets = np.rad2deg(np.ma.angle(np.ma.array(tbTool.getcol('CPARAM'), mask=tbTool.getcol('FLAG'))))
                scan_numbers = tbTool.getcol('SCAN_NUMBER')
                spw_ids = tbTool.getcol('SPECTRAL_WINDOW_ID')
                ant_ids = tbTool.getcol('ANTENNA1')
                tbTool.done()
                for spw_id in sorted(list(set(spw_ids))):
                    for ant_id in sorted(list(set(ant_ids))):
                        for pol_id in range(phase_offsets.shape[0]):
                            score = 1.0
                            longmsg = f'Phase offset QA for EB: {ms.basename} Spw: {spw_id} Antenna: {ant_id} Pol: {pol_id}'
                            shortmsg = 'Phase offset QA'
                            origin = pqa.QAOrigin(metric_name='timegaincal_qa_calculated',
                                  metric_score=1,
                                  metric_units='Timegaincal QA metrics calculated')
                            data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                            phase_offset_score = pqa.QAScore(score=score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=data_selection)
                            self.pool.append(phase_offset_score)
        except Exception as e:
            LOG.error('Phase offsets score calculation failed: %s' % (e))

    def _get_xy_x2x1_qascore(self, ms, phase_field_ids, score_type):
        (total_score, table_name, field_name, ant_name, spw_name) = self._get_xy_x2x1_total(phase_field_ids,
                                                                                            self.score_types[score_type][0])
        longmsg = 'Total score for %s is %0.2f (%s field %s %s spw %s)' % (
            self.score_types[score_type][1], total_score, ms.basename, field_name, ant_name, spw_name)
        shortmsg = self.short_msg[score_type]

        origin = pqa.QAOrigin(metric_name='TimegaincalQAPool',
                              metric_score=total_score,
                              metric_units='Total gpcal QA score')

        return pqa.QAScore(total_score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)

    def _get_xy_x2x1_total(self, phase_field_ids, score_key):
        # attrs to hold score and QA identifiers
        total_score = 1.0
        total_table_name = None
        total_field_name = 'N/A'
        total_ant_name = 'N/A'
        total_spw_name = 'N/A'

        for table_name in self.phase_qa_results_dict:
            qa_result = self.phase_qa_results_dict[table_name]
            for field_id in phase_field_ids:
                qa_context_score = qa_result['QASCORES']['SCORES'][field_id][score_key]
                if qa_context_score['SCORE'] != 'C/C':
                    if qa_context_score['SCORE'] < total_score:
                        total_score = qa_context_score['SCORE']
                        total_field_name = qa_result['QASCORES']['FIELDS'][qa_context_score['FIELD']]
                        total_ant_name = qa_result['QASCORES']['ANTENNAS'][qa_context_score['ANTENNA']]
                        total_spw_name = qa_result['QASCORES']['SPWS'][qa_context_score['SPW']]
                        total_table_name = table_name

        return total_score, total_table_name, total_field_name, total_ant_name, total_spw_name

    def _M(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute mean phase offset for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets: numpy masked array with phase offset from calibration table
        :param phase_offsets: numpy masked array with phase offset from calibration table
        """

        return np.ma.mean(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))])

    def _S(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute phase offset standard deviation for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.
        """

        return np.ma.std(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))], ddof=1)

    def _MaxOff(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute maximum phase offset for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.
        """

        return np.ma.max(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))])

    def _Stot(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, spw_id: int) -> float:

        """
        Compute maximum phase offset for given spw selection.
        Uses index arrays to parse the table structure.
        """

        data_selection = phase_offsets[:, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id))]
        return 1.4826 * np.ma.median(np.ma.abs(data_selection - np.ma.median(data_selection)))


class TimegaincalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained TimegaincalResult.
    """
    result_cls = common.GaincalResults
    child_cls = None

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        qa_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number, 'qa')

        if not os.path.exists(qa_dir):
            os.makedirs(qa_dir)

        phase_qa_results_dict = {}
        phase_offsets_qa_results_dict = {}

        try:
            # Get phase cal tables
            for calapp in result.final:
                solint = utils.get_origin_input_arg(calapp, 'solint')
                calmode = utils.get_origin_input_arg(calapp, 'calmode')
                if solint == 'int' and calmode == 'p':
                    phase_qa_results_dict[calapp.gaintable] = gpcal.gpcal(calapp.gaintable)

            # Get phase offsets tables
            for calapp in result.phaseoffsetresult.final:
                phase_offsets_qa_results_dict[calapp.gaintable] = True

            result.qa = TimegaincalQAPool(phase_qa_results_dict, phase_offsets_qa_results_dict)
            result.qa.update_scores(ms)
        except Exception as e:
            LOG.error('Problem occurred running QA analysis. QA results will not be available for this task')
            LOG.exception(e)


class TimegaincalListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing TimegaincalResults.
    """
    result_cls = collections.Iterable
    child_cls = common.GaincalResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
