import os
from typing import List

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks import applycal
from pipeline.infrastructure import casa_tasks
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

    polintent = vdp.VisDependentProperty(default='POLARIZATION,POLANGLE,POLLEAKAGE')

    def __init__(self, context, vis=None, polintent=None):
        self.context = context
        self.vis = vis
        self.polintent = polintent


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

    def _polcal_for_session(self, session_name: str, vislist: List[str]):
        LOG.info(f"Deriving polarisation calibration for session {session_name} with measurement set(s):"
                 f" {utils.commafy(vislist, quotes=False)}.")

        # Check that each MS in session shares the same polarisation calibrator
        # by field name.
        self._check_matching_pol_field(session_name, vislist)

        # Run applycal to apply the registered total intensity caltables to the
        # polarisation calibrator.
        self._run_applycal(vislist)

        # Extract polarisation data and concatenate in session MS.
        session_msname = self._create_session_ms(session_name, vislist)

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

    def _check_matching_pol_field(self, session_name: str, vislist: List[str]):
        # Retrieve polarisation calibrator fields for each MS in session.
        pol_fields = {}
        for vis in vislist:
            ms = self.inputs.context.observing_run.get_ms(vis)
            pol_fields['vis'] = ms.get_fields(intent=self.inputs.polintent)

        # Check if each MS has same number of polarisation fields.
        if len({len(f) for f in pol_fields.values()}) != 1:
            LOG.warning(f"For session {session_name}, the measurement sets do not have equal number of polarisation"
                        f" calibrator fields:")
            for vis, fields in pol_fields:
                LOG.warning(f" {vis}: {utils.commafy([f.name for f in fields])}")
        # If the MSes have matching number of polarisation fields, check if
        # the fields are matching by name.
        else:
            if len({sorted(f.name) for f in pol_fields.values()}) != 1:
                LOG.warning(f"For session {session_name}, the measurement sets do not have the same polarisation"
                            f" calibrator fields, by name:")
            for vis, fields in pol_fields:
                LOG.warning(f" {vis}: {utils.commafy(sorted(f.name for f in fields))}")

    def _run_applycal(self, vislist: List[str]):
        inputs = self.inputs

        # Run applycal for each vis.
        for vis in vislist:
            LOG.info(f'Applying pre-existing caltables to polarisation calibrator intent for MS {vis}.')
            acinputs = applycal.IFApplycalInputs(context=inputs.context, vis=vis, intent=inputs.intent,
                                                 flagsum=False, flagbackup=False, flagdetailedsum=False)
            actask = applycal.IFApplycal(acinputs)
            self._executor.execute(actask)

    def _create_session_ms(self, session_name: str, vislist: List[str]) -> str:
        LOG.info(f"Creating polarisation data MS for session {session_name}.")

        # Extract polarisation data for each vis, and capture name of new MS.
        pol_vislist = []
        for vis in vislist:
            LOG.info(f"Extracting corrected polarisation data for MS {vis}")
            # Set name of output vis.
            outputvis = os.path.splitext(vis)[0] + '.polcalib.ms'

            # Retrieve science SpW(s) for current MS.
            ms = self.inputs.context.observing_run.get_ms(vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Run mstransform to create new polarisation MS.
            mstransform_job = casa_tasks.mstransform(vis=vis, outputvis=outputvis, spw=sci_spws,
                                                     intent=self.inputs.intent, datacolumn='corrected')
            self._executor.execute(mstransform_job)

            pol_vislist.append(outputvis)

        # Concatenate the new polarisation MSes into a single one.
        session_msname = session_name + '_concat_polcalib.ms'
        LOG.info(f"Creating concatenated polarisation data MS {session_msname} from input measurement set(s):"
                 f" {utils.commafy(pol_vislist, quotes=False)}.")
        concat_job = casa_tasks.concat(vis=pol_vislist, concatvis=session_msname)
        self._executor.execute(concat_job)

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
