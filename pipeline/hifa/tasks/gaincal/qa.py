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

            # Define thresholds (in degrees) a factors for QA tests
            NoiseThresh = 15.0

            QA1_Thresh1 = 5.0
            QA1_Thresh2 = 30.0
            QA1_Factor = 6.0

            QA2_Thresh1 = 15.0
            QA2_Thresh2 = 30.0
            QA2_Factor1 = 2.0
            QA2_Factor2 = 4.0

            QA3_Thresh1 = 15.0
            QA3_Thresh2 = 30.0
            QA3_Factor = 6.0

            phase_scan_ids = [s.id for s in ms.get_scans(scan_intent='PHASE')]

            subscores = {}
            for gaintable in self.phase_offsets_qa_results_dict:
                tbTool.open(gaintable)
                # Get phase offsets in degrees, obeying any flags.
                phase_offsets = np.rad2deg(np.ma.angle(np.ma.array(tbTool.getcol('CPARAM'), mask=tbTool.getcol('FLAG'))))
                scan_numbers = tbTool.getcol('SCAN_NUMBER')
                spw_ids = tbTool.getcol('SPECTRAL_WINDOW_ID')
                ant_ids = tbTool.getcol('ANTENNA1')
                tbTool.done()
                subscores[gaintable] = {}
                noisy_spw_ids = []
                for spw_id in sorted(list(set(spw_ids))):
                    subscores[gaintable][spw_id] = {}
                    Stot = self._Stot(phase_offsets, scan_numbers, phase_scan_ids, spw_ids, spw_id)
                    if Stot > NoiseThresh:
                        # Noise too high for further evaluation
                        noisy_spw_ids.append(spw_id)
                        for ant_id in sorted(list(set(ant_ids))):
                            subscores[gaintable][spw_id][ant_id] = {}
                            for pol_id in range(phase_offsets.shape[0]):
                                subscores[gaintable][spw_id][ant_id][pol_id] = {}
                                for scorekey in ['QA1', 'QA2', 'QA3']:
                                    score = 0.82
                                    longmsg = f'Phase offsets too noisy to usefully evaluate {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id}'
                                    shortmsg = 'Phase offsets too noisy'
                                    origin = pqa.QAOrigin(metric_name='Stot(EB, spw)',
                                                          metric_score=Stot,
                                                          metric_units='degrees')
                                    data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                                    weblog_location = pqa.WebLogLocation.HIDDEN
                                    subscores[gaintable][spw_id][ant_id][pol_id][scorekey] = pqa.QAScore(score=score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=data_selection, weblog_location=weblog_location)
                    else:
                        for ant_id in sorted(list(set(ant_ids))):
                            subscores[gaintable][spw_id][ant_id] = {}
                            for pol_id in range(phase_offsets.shape[0]):
                                subscores[gaintable][spw_id][ant_id][pol_id] = {}
                                # Number of solutions
                                N = self._N(phase_offsets, scan_numbers, phase_scan_ids, spw_ids, ant_ids, spw_id, ant_id, pol_id)
                                if N < 4:
                                    # Poor statistics
                                    for scorekey, score in [('QA1', 1.0), ('QA2', 0.85), ('QA3', 1.0)]:
                                        longmsg = f'Phase offsets with poor statistics for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id}'
                                        shortmsg = 'Poor phase offsets statistics'
                                        origin = pqa.QAOrigin(metric_name='Number of solutions',
                                                              metric_score=N,
                                                              metric_units='N/A')
                                        data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                                        weblog_location = pqa.WebLogLocation.HIDDEN
                                        subscores[gaintable][spw_id][ant_id][pol_id][scorekey] = pqa.QAScore(score=score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=data_selection, weblog_location=weblog_location)
                                else:
                                    M = self._M(phase_offsets, scan_numbers, phase_scan_ids, spw_ids, ant_ids, spw_id, ant_id, pol_id)
                                    S = self._S(phase_offsets, scan_numbers, phase_scan_ids, spw_ids, ant_ids, spw_id, ant_id, pol_id)
                                    MaxOff = self._MaxOff(phase_offsets, scan_numbers, phase_scan_ids, spw_ids, ant_ids, spw_id, ant_id, pol_id)

                                    # Mean test
                                    QA1_score = 1.0
                                    QA1_longmsg = f'Phase offsets mean for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} within range'
                                    if np.abs(M) > max(QA1_Thresh1, QA1_Factor * Stot / np.sqrt(N)):
                                        QA1_score = 0.8
                                        QA1_longmsg = f'Phase offsets mean for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding first threshold'
                                        QA1_shortmsg = 'Phase offsets mean too large'
                                    if np.abs(M) > max(QA1_Thresh2, QA1_Factor * Stot / np.sqrt(N)):
                                        QA1_score = 0.5
                                        QA1_longmsg = f'Phase offsets mean for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding second threshold'
                                        QA1_shortmsg = 'Phase offsets mean too large'
                                    QA1_origin = pqa.QAOrigin(metric_name='phase offsets mean',
                                                              metric_score=M,
                                                              metric_units='degrees')
                                    QA1_data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                                    QA1_weblog_location = pqa.WebLogLocation.HIDDEN
                                    subscores[gaintable][spw_id][ant_id][pol_id]['QA1'] = pqa.QAScore(score=QA1_score, longmsg=QA1_longmsg, shortmsg=QA1_shortmsg, vis=ms.basename, origin=QA1_origin, applies_to=QA1_data_selection, weblog_location=QA1_weblog_location)

                                    # Standard deviation test
                                    QA2_score = 1.0
                                    QA2_longmsg = f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} within range'
                                    if S > max(QA2_Thresh1, QA2_Factor1 * S):
                                        QA2_score = 0.8
                                        QA2_longmsg = f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding first threshold'
                                        QA2_shortmsg = 'Phase offsets standard deviation too large'
                                    if S > max(QA2_Thresh2, QA2_Factor2 * S):
                                        QA2_score = 0.5
                                        QA2_longmsg = f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding second threshold'
                                        QA2_shortmsg = 'Phase offsets standard deviation too large'
                                    QA2_origin = pqa.QAOrigin(metric_name='phase offsets standard deviation',
                                                              metric_score=S,
                                                              metric_units='degrees')
                                    QA2_data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                                    QA2_weblog_location = pqa.WebLogLocation.HIDDEN
                                    subscores[gaintable][spw_id][ant_id][pol_id]['QA2'] = pqa.QAScore(score=QA2_score, longmsg=QA2_longmsg, shortmsg=QA2_shortmsg, vis=ms.basename, origin=QA2_origin, applies_to=QA2_data_selection, weblog_location=QA2_weblog_location)

                                    # Maximum test
                                    QA3_score = 1.0
                                    QA3_longmsg = f'Phase offsets maximum for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} within range'
                                    if np.abs(MaxOff) > max(QA3_Thresh1, QA3_Factor * S):
                                        QA3_score = 0.8
                                        QA3_longmsg = f'Phase offsets maximum for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding first threshold'
                                        QA3_shortmsg = 'Phase offsets maximum too large'
                                    if np.abs(MaxOff) > max(QA3_Thresh2, QA3_Factor * S):
                                        QA3_score = 0.5
                                        QA3_longmsg = f'Phase offsets maximum for {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id} exceeding second threshold'
                                        QA3_shortmsg = 'Phase offsets maximum too large'
                                    QA3_origin = pqa.QAOrigin(metric_name='phase offsets maximum',
                                                              metric_score=MaxOff,
                                                              metric_units='degrees')
                                    QA3_data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={str(pol_id)})
                                    QA3_weblog_location = pqa.WebLogLocation.HIDDEN
                                    subscores[gaintable][spw_id][ant_id][pol_id]['QA3'] = pqa.QAScore(score=QA3_score, longmsg=QA3_longmsg, shortmsg=QA3_shortmsg, vis=ms.basename, origin=QA3_origin, applies_to=QA3_data_selection, weblog_location=QA3_weblog_location)

                    if noisy_spw_ids != []:
                        # Add aggregated score for noisy spws
                        score = 0.82
                        longmsg = f'Phase offsets too noisy to usefully evaluate {ms.basename} SPW {spw_id} Antenna {ant_id} Polarization {pol_id}'
                        shortmsg = 'Phase offsets too noisy'
                        origin = pqa.QAOrigin(metric_name='Stot(EB, spw)',
                                              metric_score=-1,
                                              metric_units='degrees')
                        data_selection = pqa.TargetDataSelection(vis={ms.basename}, spw=set(noisy_spw_ids), intent={'PHASE'})
                        weblog_location = pqa.WebLogLocation.ACCORDION
                        self.pool.append(pqa.QAScore(score=score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=data_selection, weblog_location=weblog_location))
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

    def _N(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute number of phase solutions for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets:  numpy masked array with phase offset from calibration table
        :param scan_numbers:   numpy array with scan numbers from calibration table
        :param phase_scan_ids: numpy array with phase calibrator scan numbers
        :param spw_ids:        numpy array with spw ids from calibration table
        :param ant_ids:        numpy array with antenna ids from calibration table
        :param spw_id:         selected spw id
        :param ant_id:         selected antenna id
        :param pol_id:         selected polarization id
        """

        return np.ma.sum(np.ma.where(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))], 1, 0))

    def _M(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute mean phase offset for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets:  numpy masked array with phase offset from calibration table
        :param scan_numbers:   numpy array with scan numbers from calibration table
        :param phase_scan_ids: numpy array with phase calibrator scan numbers
        :param spw_ids:        numpy array with spw ids from calibration table
        :param ant_ids:        numpy array with antenna ids from calibration table
        :param spw_id:         selected spw id
        :param ant_id:         selected antenna id
        :param pol_id:         selected polarization id
        """

        return np.ma.mean(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))])

    def _S(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute phase offset standard deviation for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets:  numpy masked array with phase offset from calibration table
        :param scan_numbers:   numpy array with scan numbers from calibration table
        :param phase_scan_ids: numpy array with phase calibrator scan numbers
        :param spw_ids:        numpy array with spw ids from calibration table
        :param ant_ids:        numpy array with antenna ids from calibration table
        :param spw_id:         selected spw id
        :param ant_id:         selected antenna id
        :param pol_id:         selected polarization id
        """

        return np.ma.std(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))], ddof=1)

    def _MaxOff(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, ant_ids: np.ndarray, spw_id: int, ant_id: int, pol_id: int) -> float:

        """
        Compute maximum phase offset for given spw, ant, pol selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets:  numpy masked array with phase offset from calibration table
        :param scan_numbers:   numpy array with scan numbers from calibration table
        :param phase_scan_ids: numpy array with phase calibrator scan numbers
        :param spw_ids:        numpy array with spw ids from calibration table
        :param ant_ids:        numpy array with antenna ids from calibration table
        :param spw_id:         selected spw id
        :param ant_id:         selected antenna id
        :param pol_id:         selected polarization id
        """

        return np.ma.max(phase_offsets[pol_id, 0, np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids==spw_id) & (ant_ids==ant_id))])

    def _Stot(self, phase_offsets: np.ndarray, scan_numbers: np.ndarray, phase_scan_ids: np.ndarray, spw_ids: np.ndarray, spw_id: int) -> float:

        """
        Compute maximum phase offset for given spw selection.
        Uses index arrays to parse the table structure.

        :param phase_offsets:  numpy masked array with phase offset from calibration table
        :param scan_numbers:   numpy array with scan numbers from calibration table
        :param phase_scan_ids: numpy array with phase calibrator scan numbers
        :param spw_ids:        numpy array with spw ids from calibration table
        :param spw_id:         selected spw id
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
