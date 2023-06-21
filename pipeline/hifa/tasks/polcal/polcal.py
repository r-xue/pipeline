import os
from typing import List, Tuple

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import gaincal
from pipeline.hif.tasks import polcal
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
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
        # Register all CalApplications from each session.
        for sresults in self.session.values():
            for calapp in sresults['calapps']:
                LOG.debug(f'Adding calibration to callibrary:\n{calapp.calto}\n'
                          f'{calapp.calfrom}')
                context.callibrary.add(calapp.calto, calapp.calfrom)
        return

    def __repr__(self):
        return 'PolcalResults'


class PolcalInputs(vdp.StandardInputs):

    intent = vdp.VisDependentProperty(default='POLARIZATION,POLANGLE,POLLEAKAGE')

    def __init__(self, context, vis=None, intent=None):
        self.context = context
        self.vis = vis
        self.intent = intent


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
        for session_name, vislist in vislist_for_session.items():
            result.session[session_name] = self._polcal_for_session(session_name, vislist)

        return result

    def analyse(self, result):
        return result

    def _polcal_for_session(self, session_name: str, vislist: List[str]) -> dict:
        LOG.info(f"Deriving polarisation calibration for session '{session_name}' with measurement set(s):"
                 f" {utils.commafy(vislist, quotes=False)}.")

        # Check that each MS in session shares the same one polarisation
        # calibrator by field name; stop processing session if not.
        polcal_field_name = self._check_matching_pol_field(session_name, vislist)
        if not polcal_field_name:
            return {}

        # Retrieve reference antenna for this session.
        refant = self._get_refant(session_name, vislist)

        # Run applycal to apply the registered total intensity caltables to the
        # polarisation calibrator.
        for vis in vislist:
            LOG.info(f'Session {session_name}: apply pre-existing caltables to polarisation calibrator for MS {vis}.')
            self._run_applycal(vis)

        # Extract polarisation data and concatenate in session MS.
        LOG.info(f"Creating polarisation data MS for session '{session_name}'.")
        session_msname, spwmaps = self._create_session_ms(session_name, vislist)

        # Compute duration of polarisation scans.
        scan_duration = self._compute_pol_scan_duration(session_msname)

        # Compute initial gain calibration for polarisation calibrator, and
        # merge into local context.
        LOG.info(f"{session_msname}: compute initial gain calibration for polarisation calibrator.")
        init_gcal_result = self._initial_gaincal(session_msname, refant)
        self._register_calapps_from_results([init_gcal_result])

        # Compute (uncalibrated) estimate of polarisation of the polarisation
        # calibrator.
        LOG.info(f"{session_msname}: compute estimate of polarisation.")
        uncal_pfg_result = self._compute_polfromgain(session_msname, init_gcal_result)

        # Retrieve fractional Stokes results for averaged SpW for the
        # polarisation calibrator field.
        smodel = list(uncal_pfg_result.values())[0]['SpwAve']

        # Identify scan with highest X-Y signal.
        best_scan_id = self._identify_scan_highest_xy(session_name, init_gcal_result)

        # Compute X-Y delay.
        LOG.info(f"{session_msname}: compute X-Y delay (Kcross) for polarisation calibrator.")
        kcross_result, kcross_calapps = self._compute_xy_delay(session_msname, vislist, refant, best_scan_id, spwmaps)
        self._register_calapps_from_results([kcross_result])

        # Calibrate X-Y phase.
        LOG.info(f"{session_msname}: compute X-Y phase for polarisation calibrator.")
        polcal_phase_result, pol_phase_calapps = self._calibrate_xy_phase(session_msname, vislist, smodel,
                                                                          scan_duration, spwmaps)

        # Retrieve fractional Stokes results for averaged SpW for the
        # polarisation calibrator field.
        smodel = list(polcal_phase_result.polcal_returns[0].values())[0]['SpwAve']

        # Unregister caltables that have been created for the session MS so
        # far, prior to re-computing the gain calibration for polarisation
        # calibrator.
        self._unregister_caltables(session_msname)

        # Final gain calibration for polarisation calibrator, using the actual
        # polarisation model.
        LOG.info(f"{session_msname}: compute final gain calibration for polarisation calibrator.")
        final_gcal_result, final_gcal_calapps = self._final_gaincal(session_msname, vislist, refant, smodel, spwmaps)

        # Recompute polarisation of the polarisation calibrator after
        # calibration.
        LOG.info(f"{session_msname}: recompute polarisation of polarisation calibrator after calibration.")
        cal_pfg_result = self._compute_polfromgain(session_msname, final_gcal_result)

        # (Re-)register the final gain, X-Y delay, and X-Y phase caltables.
        self._register_calapps_from_results([final_gcal_result, kcross_result, polcal_phase_result])

        # Compute leakage terms.
        LOG.info(f"{session_msname}: estimate leakage terms for polarisation calibrator.")
        leak_polcal_result, leak_pcal_calapps = self._compute_leakage_terms(session_msname, vislist, smodel,
                                                                            scan_duration, spwmaps)

        # Unregister caltables created so far, and re-register just the X-Y
        # delay, X-Y phase, and leakage term caltables, prior to computing the
        # X-Y ratio.
        self._unregister_caltables(session_msname)
        self._register_calapps_from_results([kcross_result, polcal_phase_result, leak_polcal_result])

        # Compute X-Y ratio.
        LOG.info(f"{session_msname}: compute X-Y ratio for polarisation calibrator.")
        xyratio_gcal_result, xyratio_calapps = self._compute_xy_ratio(session_msname, vislist, refant, smodel, spwmaps)

        # Prior to applycal, re-register the final gain caltable.
        self._register_calapps_from_results([final_gcal_result])

        # Apply the polarisation calibration to the polarisation calibrator.
        LOG.info(f'{session_msname}: apply polarisation calibrations to the polarisation calibrator.")')
        self._run_applycal(session_msname, parang=True)

        # Run visstat on session MS, once per obsid.
        LOG.info(f'{session_msname}: run visstat for session MS.")')
        session_vs_result = {}
        for obsid in range(len(vislist)):
            session_vs_result[obsid] = self._run_visstat(session_msname, obsid=str(obsid))

        # Register the relevant CalApps for polarisation calibrator in
        # each MS MSes in this session.
        self._register_calapps(final_gcal_calapps + kcross_calapps + pol_phase_calapps + leak_pcal_calapps)

        # Run applycal to apply the newly derived polarisation caltables to the
        # polarisation calibrator in each MS in this session.
        for vis in vislist:
            LOG.info(f'Session {session_name}: apply polarisation caltables to polarisation calibrator for MS {vis}.')
            self._run_applycal(vis)

        # Run visstat on each MS in this session.
        vis_vs_results = {}
        for vis in vislist:
            LOG.info(f'{session_msname}: run visstat for MS {vis}.")')
            vis_vs_results[vis] = self._run_visstat(vis)

        # Compare results from visstat.
        LOG.info(f'{session_msname}: comparison of visstat results.")')
        self._compare_visstat_results(session_vs_result, vis_vs_results, spwmaps)

        # Set flux density for polarisation calibrator in each MS in this
        # session.
        for vis in vislist:
            LOG.info(f'{session_msname}: run setjy for MS {vis}.")')
            self._setjy_for_polcal(vis, smodel)

        # Compute amplitude calibration for polarisation calibrator.
        polcal_amp_result, amp_calapps = self._compute_ampcal_for_polcal(session_msname, vislist, refant, spwmaps)

        # Collect results.
        final_calapps = final_gcal_calapps + kcross_calapps + pol_phase_calapps + leak_pcal_calapps + \
            xyratio_calapps + amp_calapps
        result = {
            'session_vis': session_msname,
            'vislist': vislist,
            'polcal_field_name': polcal_field_name,
            'refant': refant,
            'calapps': final_calapps,
            'init_gcal_result': init_gcal_result,
            'uncal_pfg_result': uncal_pfg_result,
            'kcross_result': kcross_result,
            'polcal_phase_result': polcal_phase_result,
            'final_gcal_result': final_gcal_result,
            'cal_pfg_result': cal_pfg_result,
            'leak_polcal_result': leak_polcal_result,
            'xyratio_gcal_result': xyratio_gcal_result,
            'session_vs_result': session_vs_result,
            'vis_vs_results': vis_vs_results,
            'polcal_amp_results': polcal_amp_results,
        }

        return result

    def _get_refant(self, session_name: str, vislist: List[str]) -> str:
        # In the polarisation recipes, the best reference antenna should have
        # been determined for the entire session by hifa_session_refant, and
        # stored in each MS of the session. Retrieve this refant from the first
        # MS in the MS list.
        ms = self.inputs.context.observing_run.get_ms(name=vislist[0])
        LOG.info(f"Session '{session_name}' is using reference antenna: {ms.reference_antenna}.")
        return ms.reference_antenna

    def _check_matching_pol_field(self, session_name: str, vislist: List[str]) -> str:
        # Retrieve polarisation calibrator field name for each MS in session.
        pol_fields = {}
        for vis in vislist:
            ms = self.inputs.context.observing_run.get_ms(name=vis)
            pol_fields[vis] = [field.name for field in ms.get_fields(intent=self.inputs.intent)]

        # Check that each MS has one and only one polarisation calibrator.
        pol_field_name = ''
        if not all(len(f) == 1 for f in pol_fields.values()):
            msg = f"Cannot process session '{session_name}': one or more measurement sets do not have exactly 1" \
                  f" polarisation calibrator field."
            for vis, fields in pol_fields.items():
                msg += f"\n  {vis} has polarisation calibrator field(s): {utils.commafy(fields)}"
            LOG.warning(msg)
        # Check that the polarisation field for each MS matches by name.
        elif len({field for visfields in pol_fields.values() for field in visfields}) != 1:
            msg = f"Cannot process session '{session_name}': the measurement sets do not have the same polarisation" \
                  f" calibrator (fields do not match by name)."
            for vis, fields in pol_fields.items():
                msg += f"\n  {vis} has polarisation calibrator field: {fields}"
            LOG.warning(msg)
        # If no mismatch was found, then return the name of the single
        # polarisation calibrator field.
        else:
            pol_field_name = pol_fields[vislist[0]][0]

        return pol_field_name

    def _run_applycal(self, vis: str, parang: bool = False):
        acinputs = applycal.IFApplycalInputs(context=self.inputs.context, vis=vis, intent=self.inputs.intent,
                                             parang=parang, flagsum=False, flagbackup=False, flagdetailedsum=False)
        actask = applycal.IFApplycal(acinputs)
        self._executor.execute(actask)

    def _create_session_ms(self, session_name: str, vislist: List[str]) -> Tuple[str, dict]:
        """This method uses mstransform to create a new MS that contains only
        the polarisation calibrator data."""
        # Extract polarisation data for each vis, and capture name of new MS.
        pol_vislist = []
        for vis in vislist:
            LOG.info(f"Extracting corrected polarisation data for MS {vis}")
            # Set name of output vis.
            outputvis = os.path.splitext(vis)[0] + '.polcalib.ms'

            # Retrieve science SpW(s) for current MS.
            ms = self.inputs.context.observing_run.get_ms(name=vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and execute mstransform job to split off the corrected
            # column for polarisation intent.
            task_args = {
                'vis': vis,
                'intent': utils.to_CASA_intent(ms, self.inputs.intent),
                'outputvis': outputvis,
                'spw': sci_spws,
                'datacolumn': 'corrected',
                'reindex': False,
            }
            mstransform_job = casa_tasks.mstransform(**task_args)
            self._executor.execute(mstransform_job)

            pol_vislist.append(outputvis)

        # Concatenate the new polarisation MSes into a single session MS.
        session_msname = session_name + '_concat_polcalib.ms'
        LOG.info(f"Creating polarisation session measurement set '{session_msname}' from input measurement set(s):"
                 f" {utils.commafy(pol_vislist, quotes=False)}.")
        concat_job = casa_tasks.concat(vis=pol_vislist, concatvis=session_msname)
        self._executor.execute(concat_job)

        # Initialize MS object and set its reference antenna to locked, to
        # enforce strict refantmode.
        session_ms = tablereader.MeasurementSetReader.get_measurement_set(session_msname)
        session_ms.reference_antenna_locked = True

        # Add session MS to local context.
        self.inputs.context.observing_run.add_measurement_set(session_ms)

        # Create SpW mapping from session MS to original input MSes.
        spwmaps = {}
        for vis in vislist:
            target_ms = self.inputs.context.observing_run.get_ms(name=vis)
            mapped = sessionutils.get_spwmap(session_ms, target_ms)
            spwmaps[vis] = list(range(max(mapped.values())+1))
            for k, v in mapped.items():
                spwmaps[vis][v] = k

        return session_msname, spwmaps

    def _compute_pol_scan_duration(self, vis: str) -> int:
        # Get polarisation scans for session MS.
        ms = self.inputs.context.observing_run.get_ms(name=vis)
        pol_scans = ms.get_scans(scan_intent=self.inputs.intent)

        # Compute median duration of polarisation scans.
        scan_duration = int(np.median([scan.time_on_source.total_seconds() for scan in pol_scans]))
        LOG.info(f"Session MS {vis}: median scan duration = {scan_duration} seconds.")
        return scan_duration

    def _initial_gaincal(self, vis: str, refant: str) -> gaincal.common.GaincalResults:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'solint': 'int',
            'refant': refant,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=self.inputs.intent, interp='linear')
        result.final = [new_calapp]

        return result

    def _register_calapps_from_results(self, results: List):
        """This method will register any "final" CalApplication present in any
        of the input Results to the callibrary in the local context (stored in
        inputs).

        Note: this is typically done through accepting the result into the
        relevant context. However, the framework does not allow merging a
        Results object into the same context multiple times. In this
        hifa_polcal task, the workflow requires unregistering / re-registering
        certain caltables, hence we use this worker method to do so.
        """
        # Collect CalApps to merge.
        calapps_to_merge = [calapp for result in results for calapp in result.final]
        self._register_calapps(calapps_to_merge)

    def _register_calapps(self, calapps: List):
        """This method will register a list of CalApplications to the
        callibrary in the local context (stored in inputs)."""
        for calapp in calapps:
            LOG.debug(f'Adding calibration to callibrary in task-specific context:\n{calapp.calto}\n{calapp.calfrom}')
            self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

    def _compute_polfromgain(self, vis: str, gcal_result: gaincal.common.GaincalResults) -> dict:
        # Get caltable to analyse, and set name of output caltable.
        intable = gcal_result.final[0].gaintable
        caltable = os.path.splitext(intable)[0] + '_polfromgain.tbl'

        # Create and run polfromgain CASA task.
        pfg_job = casa_tasks.polfromgain(vis=vis, tablein=intable, caltable=caltable)
        pfg_result = self._executor.execute(pfg_job)

        return pfg_result

    @staticmethod
    def _identify_scan_highest_xy(session_name: str, gcal_result: gaincal.common.GaincalResults) -> int:
        # Get caltable to analyse.
        caltable = gcal_result.final[0].gaintable

        # Retrieve scan nr. and gains from initial polarisation caltable.
        with casa_tools.TableReader(caltable) as table:
            scan_ids = table.getcol('SCAN_NUMBER')
            gains = np.squeeze(table.getcol('CPARAM'))

        # For each scan, derive the average X-Y signal ratio.
        uniq_scan_ids = sorted(set(scan_ids))
        ratios = []
        for scan_id in uniq_scan_ids:
            scan_idx = scan_ids == scan_id
            ratios.append(
                np.sqrt(np.average(np.power(np.abs(gains[0, scan_idx]) / np.abs(gains[1, scan_idx]) - 1.0, 2.))))

        # Identify the scan with the best X-Y signal ratio.
        best_ratio_idx = np.argmin(ratios)
        best_scan_id = uniq_scan_ids[best_ratio_idx]
        LOG.info(f"Session {session_name} - scan with highest expected X-Y signal: {best_scan_id}.")

        return best_scan_id

    def _compute_xy_delay(self, vis: str, vislist: List[str], refant: str, best_scan: int, spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'scan': str(best_scan),
            'selectdata': True,  # needed when selecting on scan.
            'solint': 'inf,5MHz',
            'smodel': [1, 0, 1, 0],
            'gaintype': 'KCROSS',
            'refant': refant,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _calibrate_xy_phase(self, vis: str, vislist: List[str], smodel: List[float], scan_duration: int,
                            spwmaps: dict) -> Tuple[polcal.polcalworker.PolcalResults, List]:
        inputs = self.inputs

        # Initialize polcal task inputs.
        task_args = {
            'vis': vis,
            'intent': inputs.intent,
            'solint': 'inf,5MHz',
            'smodel': smodel,
            'combine': 'obs,scan',
            'poltype': 'Xfparang+QU',
            'preavg': scan_duration,
        }
        task_inputs = polcal.PolcalWorker.Inputs(inputs.context, **task_args)

        # Initialize and execute polcal task.
        task = polcal.PolcalWorker(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _unregister_caltables(self, vis: str):
        """
        This method will unregister from the callibrary in the local context
        (stored in inputs) any CalApplication that is registered for caltable
        produced so far during this hifa_polcal task.
        """
        # Define predicate function that matches the kind of caltable that
        # needs to be removed from the CalLibrary.
        def hifa_polcal_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            calto_vis = {os.path.basename(v) for v in calto.vis}
            do_delete = 'hifa_polcal' in calfrom.gaintable and vis in calto_vis
            if do_delete:
                LOG.debug(f'Unregistering caltable {calfrom.gaintable} from task-specific context.')
            return do_delete

        self.inputs.context.callibrary.unregister_calibrations(hifa_polcal_matcher)

    def _final_gaincal(self, vis: str, vislist: List[str], refant: str, smodel: List[float], spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'ap',
            'intent': inputs.intent,
            'solint': 'int',
            'smodel': smodel,
            'refant': refant,
            'parang': True,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='linear')
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws,
                                                                intent=inputs.intent, interp='nearest',
                                                                spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _compute_leakage_terms(self, vis: str, vislist: List[str], smodel: List[float], scan_duration: int,
                               spwmaps: dict) -> Tuple[polcal.polcalworker.PolcalResults, List]:
        inputs = self.inputs

        # Initialize polcal task inputs.
        task_args = {
            'vis': vis,
            'intent': inputs.intent,
            'solint': 'inf,5MHz',
            'smodel': smodel,
            'combine': 'obs,scan',
            'poltype': 'Dflls',
            'preavg': scan_duration,
            'refant': '',
        }
        task_inputs = polcal.PolcalWorker.Inputs(inputs.context, **task_args)

        # Initialize and execute polcal task.
        task = polcal.PolcalWorker(task_inputs)
        result = self._executor.execute(task)

        # Replace the CalApp in the result with a modified CalApplication to
        # register this caltable against the session MS.
        new_calapp = callibrary.copy_calapplication(result.final[0], intent=inputs.intent, interp='nearest',
                                                    calwt=False)
        result.final = [new_calapp]

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against it.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws, intent='',
                                                                interp='nearest', calwt=False, spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _compute_xy_ratio(self, vis: str, vislist: List[str], refant: str, smodel: List[float], spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'a',
            'intent': inputs.intent,
            'solint': 'inf',
            'smodel': smodel,
            'combine': 'obs,scan',
            'refant': refant,
            'solnorm': True,
            'parang': True,
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against the non-polarisation intents.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws,
                                                                intent='AMPLITUDE,BANDPASS,CHECK,PHASE,TARGET',
                                                                interp='nearest', spwmap=spwmaps[inp_vis]))

        return result, final_calapps

    def _run_visstat(self, vis: str, obsid=None) -> dict:
        if obsid is None:
            obsid = ''

        # Retrieve science SpW(s) for vis.
        ms = self.inputs.context.observing_run.get_ms(name=vis)
        sci_spws = [str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True)]

        # Collect visstat results for each SpW in MS.
        vs_result = {}
        for sci_spw in sci_spws:
            # Create and run CASA visstat job.
            task_args = {
                'vis': vis,
                'intent': utils.to_CASA_intent(ms, self.inputs.intent),
                'spw': sci_spw,
                'datacolumn': 'corrected',
                'observation': obsid,
            }
            visstat_job = casa_tasks.visstat(**task_args)
            vs_result[sci_spw] = self._executor.execute(visstat_job)

        return vs_result

    def _compare_visstat_results(self, session_vs_result: dict, vis_vs_results: dict, spwmaps: dict):
        pass

    def _setjy_for_polcal(self, vis: str, smodel: List[float]):
        pass

    def _compute_ampcal_for_polcal(self, vis: str, vislist: List[str], refant: str, spwmaps: dict) \
            -> Tuple[gaincal.common.GaincalResults, List]:
        inputs = self.inputs

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': vis,
            'calmode': 'a',
            'intent': inputs.intent,
            'solint': 'inf',
            'gaintype': 'T',
            'refant': refant,  # TODO: confirm if this needs to be set.
            # 'parang': True,  # TODO: confirm if this needs to be set.
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # For each MS in this session, create a modified CalApplication to
        # register this caltable against the polarisation intents.
        final_calapps = []
        for inp_vis in vislist:
            # Retrieve science SpW(s) for vis.
            ms = inputs.context.observing_run.get_ms(name=inp_vis)
            sci_spws = ','.join(str(spw.id) for spw in ms.get_spectral_windows(science_windows_only=True))

            # Create and append modified CalApplication.
            final_calapps.append(callibrary.copy_calapplication(result.final[0], vis=inp_vis, spw=sci_spws,
                                                                intent=inputs.intent, interp='nearest',
                                                                spwmap=spwmaps[inp_vis]))

        return result, final_calapps
