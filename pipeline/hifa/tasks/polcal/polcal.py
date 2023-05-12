import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import sessionutils
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'Polcal',
    'PolcalInputs',
    'PolcalResults',
]


class PolcalResults(basetask.Results):
    def __init__(self, vis=None):
        super().__init__()
        self.vis = vis
        self.session = {}

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'PolcalResults'


class PolcalInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_polcal')
@task_registry.set_casa_commands_comment('Compute the polarisation calibration.')
class Polcal(basetask.StandardTaskTemplate):
    Inputs = PolcalInputs

    # This is a multi-vis task that handles all MSes for one or more sessions
    # at once.
    is_multi_vis_task = True

    def prepare(self):
        # Initialize results.
        result = PolcalResults(vis=self.inputs.vis)

        # Inspect the vis list to identify sessions and corresponding MSes.
        vislist_for_session = sessionutils.group_vislist_into_sessions(self.inputs.context, self.inputs.vis)

        # Run polarisation calibration for each session.
        for session_name, vislist in vislist_for_session:
            result.session[session_name] = self._polcal_for_session(session_name, vislist)

        return result

    def analyse(self, result):
        return result

    def _polcal_for_session(self, session_name, vislist):
        LOG.info(f"Deriving polarisation calibration for session {session_name} with measurement set(s):"
                 f" {utils.commafy(vislist, quotes=False)}.")

        # Run applycal to apply the registered total intensity caltables to the
        # polarisation calibrator.
        self._run_applycal(vislist)

        # Extract polarisation data and concatenate in session MS.
        session_msname = self._create_session_ms(vislist)

        # Compute duration of polarisation scans.
        scan_duration = self._compute_session_scan_duration(session_msname)

        # Initial gain calibration for polarisation calibrator.
        gcal_result = self._initial_gaincal(session_msname)

        # Compute (uncalibrated) estimate of polarisation of the polarisation
        # calibrator.
        uncal_polfromgain_result = self._compute_polfromgain(session_msname, gcal_result)

        # Identify scan with highest X-Y signal.
        best_scan = self._identify_scan_highest_xy(gcal_result)

        # Compute X-Y delay.
        kcross_result = self._compute_xy_delay(session_msname, gcal_result, best_scan)

        # Calibrate X-Y phase.
        polcal_phase_result = self._calibrate_xy_phase(session_msname, gcal_result, uncal_polfromgain_result,
                                                       kcross_result, scan_duration)

        # Final gain calibration for polarisation calibrator, using the actual
        # polarisation model.
        final_gcal_result = self._final_gaincal(session_msname, polcal_phase_result)

        # Recompute polarisation of the polarisation calibrator after
        # calibration.
        polfromgain_result = self._compute_polfromgain(session_msname, final_gcal_result)

        # Compute leakage terms.
        leak_polcal_result = self._compute_leakage_terms(session_msname, final_gcal_result, kcross_result,
                                                         polcal_phase_result, scan_duration)

        # Compute X-Y ratio.
        xyratio_gcal_result = self._compute_xy_ratio(session_msname, kcross_result, polcal_phase_result,
                                                     leak_polcal_result)

        # Set flux density for polarisation calibrator.
        # TODO: what is this operating on?
        self._setjy_for_polcal()

        # Apply the polarisation calibration to the polarisation calibrator.
        self._apply_polcal()

        # TODO: add visstat step to derive stats for comparison later on.

        # Image the polarisation calibrator in session MS.
        self._image_polcal()

        result = None
        return result

    def _run_applycal(self, vislist):
        pass

    def _create_session_ms(self, vislist):
        session_msname = ''
        return session_msname

    def _compute_session_scan_duration(self, msname):
        scan_duration = 0
        return scan_duration

    def _initial_gaincal(self, msname):
        gcal_result = None
        return gcal_result

    def _compute_polfromgain(self, msname, gcal_result):
        polfromgain_result = None
        return polfromgain_result

    def _identify_scan_highest_xy(self, gcal_result):
        best_scan = None
        return best_scan

    def _compute_xy_delay(self, msname, gcal_result, best_scan):
        kcross_result = None
        return kcross_result

    def _calibrate_xy_phase(self, msname, gcal_result, uncal_polfromgain_result, kcross_result, scan_duration):
        polcal_result = None
        return polcal_result

    def _final_gaincal(self, msname, polcal_phase_result):
        final_gcal_result = None
        return final_gcal_result

    def _compute_leakage_terms(self, msname, final_gcal_result, kcross_result, polcal_phase_result, scan_duration):
        leak_polcal_result = None
        return leak_polcal_result

    def _compute_xy_ratio(self, msname, kcross_result, polcal_phase_result, leak_polcal_result):
        xyratio_gcal_result = None
        return xyratio_gcal_result

    def _setjy_for_polcal(self):
        pass

    def _apply_polcal(self, msname, final_gcal_result, kcross_result, polcal_phase_result, leak_polcal_result):
        pass

    def _image_polcal(self):
        pass
