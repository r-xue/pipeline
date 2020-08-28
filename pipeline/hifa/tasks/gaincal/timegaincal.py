import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks.gaincal import common
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hifa.heuristics import exptimes as gexptimes
from pipeline.hifa.heuristics.phasespwmap import get_spspec_to_spwid_map
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'TimeGaincalInputs',
    'TimeGaincal',
]


class TimeGaincalInputs(gtypegaincal.GTypeGaincalInputs):

    calamptable = vdp.VisDependentProperty(default=None)
    calphasetable = vdp.VisDependentProperty(default=None)
    targetphasetable = vdp.VisDependentProperty(default=None)
    offsetstable = vdp.VisDependentProperty(default=None)
    amptable = vdp.VisDependentProperty(default=None)
    calsolint = vdp.VisDependentProperty(default='int')
    calminsnr = vdp.VisDependentProperty(default=2.0)
    targetsolint = vdp.VisDependentProperty(default='inf')
    targetminsnr = vdp.VisDependentProperty(default=3.0)

    def __init__(self, context, vis=None, output_dir=None, calamptable=None, calphasetable=None, offsetstable=None,
                 amptable=None, targetphasetable=None, calsolint=None, targetsolint=None, calminsnr=None,
                 targetminsnr=None, **parameters):
        super(TimeGaincalInputs, self).__init__(context, vis=vis, output_dir=output_dir,  **parameters)
        self.calamptable = calamptable
        self.calphasetable = calphasetable
        self.targetphasetable = targetphasetable
        self.offsetstable = offsetstable
        self.amptable = amptable
        self.calsolint = calsolint
        self.calminsnr = calminsnr
        self.targetsolint = targetsolint
        self.targetminsnr = targetminsnr

    # Override default base class intents for ALMA.
    @vdp.VisDependentProperty
    def intent(self):
        return 'PHASE,AMPLITUDE,BANDPASS,POLARIZATION,POLANGLE,POLLEAKAGE'


@task_registry.set_equivalent_casa_task('hifa_timegaincal')
@task_registry.set_casa_commands_comment('Time dependent gain calibrations are computed.')
class TimeGaincal(gtypegaincal.GTypeGaincal):
    Inputs = TimeGaincalInputs

    def prepare(self, **parameters):
        inputs = self.inputs

        # Create a results object.
        result = common.GaincalResults()

        # Based on the SpW mapping mode declared in the MeasurementSet, set
        # default values for key parameters for phase calibrations (solint,
        # gaintype, combine).
        if inputs.ms.combine_spwmap:
            phase_gaintype = 'T'
            phase_combine = 'spw'
            # CAS-9264: when combine='spw', set the default solint to a quarter of the
            # shortest scan time for PHASE scans.
            spw_ids = [spw.id for spw in inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=True)]
            field_names = [field.name for field in inputs.ms.get_fields(task_arg=inputs.field, intent='PHASE')]
            exptimes = gexptimes.get_scan_exptimes(inputs.ms, field_names, 'PHASE', spw_ids)
            phase_solint = '%0.3fs' % (min([exptime[1] for exptime in exptimes]) / 4.0)
        else:
            phase_gaintype = 'G'
            phase_combine = inputs.combine
            phase_solint = inputs.calsolint

        # Compute the science target phase solution. This solution will be applied to the target, check source, and
        # phase calibrator when the result for this task is accepted.
        LOG.info('Computing phase gain table for target, check source, and phase calibrator.')
        target_phasecal_calapp = self._do_spectralspec_target_phasecal(solint=inputs.targetsolint,
                                                                       gaintype=phase_gaintype,
                                                                       combine=phase_combine)
        # Add the solutions to final task result, for adopting into context
        # callibrary, but do not merge them into local task context, so they
        # are not used in pre-apply of subsequent gaincal calls.
        result.pool.extend(target_phasecal_calapp)
        result.final.extend(target_phasecal_calapp)

        # Compute the phase solutions for all calibrators in inputs.intents.
        # While the caltable may include solutions for the PHASE calibrator
        # (if inputs.intent includes 'PHASE'), report the table as only
        # applicable for bandpass, flux, and polarization, as that is how the
        # caltable will be later added to the final task result.
        LOG.info('Computing phase gain table for bandpass, flux and polarization calibrator.')
        cal_phase_results = self._do_spectralspec_calibrator_phasecal(solint=phase_solint, gaintype=phase_gaintype,
                                                                      combine=phase_combine)

        # Merge the phase solutions for the calibrators into the local task
        # context so that it is marked for inclusion in pre-apply (gaintable)
        # in subsequent gaincal calls.
        for cpres in cal_phase_results:
            cpres.accept(inputs.context)

        # Compute a second phase solutions caltable. Assuming that
        # inputs.intent included 'PHASE' (true by default, but can be
        # overridden by user), then the merger of the previous phase
        # solutions result into the local task context will mean that an
        # initial phase correction will be included in pre-apply during this
        # gaincal, and thus this new caltable will represent the residual phase
        # offsets.
        LOG.info('Computing offset phase gain table.')
        phase_residuals_result = self._do_offsets_phasecal(solint='inf', gaintype=phase_gaintype, combine='')
        result.phaseoffsetresult = phase_residuals_result

        # Create a new CalApplication to mark the initial phase solutions for
        # calibrators as only-to-be-applied-to AMPLITUDE, BANDPASS, and POL*
        # calibrators. Add this new CalApp to the final task result, to be
        # merged into the context / callibrary.
        for cpres in cal_phase_results:
            cp_calapp = cpres.final[0]
            new_cp_calapp = callibrary.copy_calapplication(
                cp_calapp, intent='AMPLITUDE,BANDPASS,POLARIZATION,POLANGLE,POLLEAKAGE')
            result.final.append(new_cp_calapp)
            result.pool.append(new_cp_calapp)

        # Produce the diagnostic table for displaying amplitude vs time plots.
        # Use the same SpW-mapping-mode-based solint derived for phase
        # calibrations, which is used earlier for the phase solutions for
        # calibrators (used for diagnostic phase vs. time plots), and the
        # "phase offset" caltable (used for the diagnostic "phase offsets vs.
        # time" plots).
        #     This table is not applied to the data
        #     No special mapping required here.
        LOG.info('Computing amplitude gain table for displaying amplitude vs time plots')
        amp_diagnostic_result = self._do_caltarget_ampcal(solint=phase_solint)
        result.calampresult = amp_diagnostic_result

        # Compute the amplitude calibration
        LOG.info('Computing the final amplitude gain table.')
        amplitude_calapps = self._do_target_ampcal()

        # Accept the amplitude results
        result.pool.extend(amplitude_calapps)
        result.final.extend(amplitude_calapps)

        return result

    def analyse(self, result):
        # With no best caltable to find, our task is simply to set the one
        # caltable as the best result

        # double-check that the caltable was actually generated
        on_disk = [table for table in result.pool if table.exists() or self._executor._dry_run]
        result.final[:] = on_disk

        missing = [table for table in result.pool if table not in on_disk and not self._executor._dry_run]
        result.error.clear()
        result.error.update(missing)

        return result

    def _group_by_spectralspec(self, spw_sel):
        """
        Group selected SpWs by SpectralSpec
        Parameters
            spw_sel : A comma separated string of SpW IDs to analyze       

        Returns a dictionary of reference Spw ID (key) and SpW IDs associated
        to the same SpectralSpec (value). Each value element is a comma
        separated string of list of SpW IDs mapped to a same SpW ID,
        e.g., {0: '0,2', 3: '3,5'}
            SpWs (0, 2) and (3, 5) are associated with a same SpectralSpec and
            reference Spw IDs are SpW 0 and 3, respectively.
        """
        ms = self.inputs.ms
        spw_map = ms.combine_spwmap
        if len(spw_map) == 0:  # No SpW combination
            return None
        grouped_spw = {}
        request_spws = set(ms.get_spectral_windows(task_arg=spw_sel))
        for spws in get_spspec_to_spwid_map(request_spws).values():
            ref_spw = {spw_map[i] for i in spws}
            assert len(ref_spw) == 1, 'A SpectralSpec is mapped to more than one SpWs'
            grouped_spw[ref_spw.pop()] = str(',').join([str(i) for i in sorted(spws)])
        LOG.debug('SpectralSpec grouping: {}'.format(grouped_spw))
        return grouped_spw

    def _do_spectralspec_target_phasecal(self, solint=None, gaintype=None, combine=None):
        inputs = self.inputs
        ms = inputs.ms

        # PIPE-390: if not combining across spw, then no need to deal with
        # SpectralSpec: proceed with target phasecal without restrictions
        # on SpW.
        if 'spw' not in combine:
            return self._do_target_phasecal(solint, gaintype, combine)

        # PIPE-390: in case of combined SpW solution: need to solve per
        # SpectralSpec.

        # Group the input SpWs by SpectralSpec.
        spw_groups = self._group_by_spectralspec(inputs.spw)
        if spw_groups is None:
            raise ValueError('Invalid SpW grouping input.')

        # Create separate solutions for each SpectralSpec grouping of SpWs.
        calapp_list = []
        for spw_sel in list(spw_groups.values()):
            LOG.info('Processing spectral spec with spws {}'.format(spw_sel))
            selected_scans = ms.get_scans(scan_intent=inputs.intent, spw=spw_sel)
            if len(selected_scans) == 0:
                LOG.info('Skipping table generation for empty selection: spw={}, intent={}'
                         ''.format(spw_sel, inputs.intent))
                continue
            calapps = self._do_target_phasecal(solint, gaintype, combine, spw_sel)
            calapp_list.extend(calapps)
        return calapp_list
        
    def _do_target_phasecal(self, solint=None, gaintype=None, combine=None, spw=None):
        inputs = self.inputs
        spw_sel = str(spw) if spw is not None else inputs.spw

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.targetphasetable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': spw_sel,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': inputs.targetminsnr,
            'combine': combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        result_calapp = result.final[0]
        # calculate required values for interp/spwmap depending on state of ms.combine_spwmap and phaseup_spwmap
        overrides = get_phaseup_overrides(inputs.ms)

        # Spec for gainfield application from CAS-12214:
        cal_calapp = callibrary.copy_calapplication(result_calapp, intent='PHASE', gainfield='nearest', **overrides)
        target_calapp = callibrary.copy_calapplication(result_calapp, intent='TARGET,CHECK', gainfield='', **overrides)

        return cal_calapp, target_calapp

    # Used to calibrate "selfcaled" targets
    def _do_spectralspec_calibrator_phasecal(self, solint, gaintype, combine):
        inputs = self.inputs

        # Initialize output list of phase gaincal results.
        phasecal_results = []

        # PIPE-390: if not combining across spw, then no need to deal with
        # SpectralSpec, so create a gaincal solution for all SpWs, using
        # provided solint, gaintype, and combine.
        if 'spw' not in combine:
            phasecal_results.extend(self._do_calibrator_phasecal_for_spw_sel(solint, gaintype, combine, inputs.spw))
            return phasecal_results

        # Otherwise, a combined SpW solution is expected, and we need to solve
        # per SpectralSpec.
        #
        # First group the input SpWs by SpectralSpec.
        spw_groups = self._group_by_spectralspec(inputs.spw)
        if spw_groups is None:
            raise ValueError('Invalid SpW grouping input.')

        # Create separate solutions for each SpectralSpec grouping of SpWs.
        for ref_spw, spw_sel in spw_groups.items():
            LOG.info('Processing spectral spec with spws {}'.format(spw_sel))
            phasecal_results.extend(self._do_calibrator_phasecal_for_spw_sel(solint, gaintype, combine, spw_sel,
                                                                             ref_spw=ref_spw))

        return phasecal_results

    # Used to calibrate calibrator targets for selection of SpWs.
    def _do_calibrator_phasecal_for_spw_sel(self, solint, gaintype, combine, spw_sel, ref_spw=None):
        inputs = self.inputs
        ms = inputs.ms

        # Initialize output list of phase gaincal results.
        phasecal_results = []

        # The phase calibration of the calibrators require different values
        # for certain parameters, depending on intent. To handle this case,
        # split the input intents here into two separate lists.
        all_intents = inputs.intent.split(',')
        amp_bp_pol_intents = []
        other_intents = []
        for intent in all_intents:
            if intent in ['AMPLITUDE', 'BANDPASS', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE']:
                amp_bp_pol_intents.append(intent)
            else:
                other_intents.append(intent)

        # Set default values for the two different groups of intents.
        # PIPE-645: for bandpass, amplitude, and polarisation intents, override
        # minsnr to set it to 3; for other intents (typically PHASE), use
        # inputs.calminsnr.
        amp_bp_pol_minsnr = 3.0
        other_minsnr = inputs.calminsnr

        # By default, use the provided solint for both groups of intent (which
        # is normally based on SpW mapping mode), e.g. applicable to the
        # non-SpectralSpec case.
        amp_bp_pol_solint = solint
        other_solint = solint

        # If a reference SpW is provided, then we are handling a SpectralSpec:
        if ref_spw:
            # When handling a SpectralSpec, then for BANDPASS, AMPLITUDE and
            # POL* intents, override the provided solint (which is normally
            # based on SpW mapping mode) to instead use inputs.calsolint.
            amp_bp_pol_solint = inputs.calsolint

            # PIPE-163: low/high SNR heuristic choice for the other
            # calibrators, typically PHASE:
            #
            # Check if the reference SpW appears on the list of low SNR SpWs
            # in the MS:
            #  * if so, then as per low-SNR heuristics request, keep using the
            #  provided solint (which is normally based on SpW mapping mode).
            #  * if not, then override the provided solint to instead use
            #  inputs.calsolint, just like for the "amp_bp_sol" intents.
            if ref_spw not in ms.low_combined_phasesnr_spws:
                other_solint = inputs.calsolint

        # Run phase calibrations for BANDPASS, AMPLITUDE, and POL* calibrators.
        if amp_bp_pol_intents:
            intent = str(',').join(amp_bp_pol_intents)
            phasecal_result = self._do_calibrator_phasecal(amp_bp_pol_solint, gaintype, combine, spw_sel, intent,
                                                           amp_bp_pol_minsnr)
            phasecal_results.append(phasecal_result)

            # PIPE-435: for plotting purposes, force appending the phase
            # solution for the other intents (typically PHASE calibrator)
            # to the caltable produced for bandpass, amplitude, and pol*.
            append_caltable = phasecal_result.final[0].gaintable
        else:
            append_caltable = None

        # Run phase calibrations for other intents (i.e. other calibrators,
        # typically this will be PHASE). These solutions are intended to be
        # used as a temporary pre-apply when generating the phase offsets
        # caltable.
        if other_intents:
            intent = str(',').join(other_intents)
            snr_result = self._do_calibrator_phasecal(other_solint, gaintype, combine, spw_sel, intent, other_minsnr,
                                                      append_caltable=append_caltable)
            # If no solutions were produced for the bandpass, amplitude, and
            # pol* calibrators (presumably because they were not in
            # inputs.intent), then these phase solutions for other intents
            # will not have been appended to the previous caltable, but instead
            # have been saved to a new caltable. In this case, append
            # snr_result to the results instead.
            if not phasecal_results:
                phasecal_results.append(snr_result)

        return phasecal_results

    # Used to calibrate "selfcaled" targets
    def _do_calibrator_phasecal(self, solint, gaintype, combine, spw, intent, minsnr, append_caltable=None):
        inputs = self.inputs

        # Set output caltable name to "calphasetable" if this was explicitly
        # provided to top-level task inputs. Otherwise, set the caltable name
        # to "append_caltable", for in case this needs to be appended to. If
        # through either option, caltable still remains set to None, then the
        # Gtype-gaincal task will, as usual, generate a name for the caltable
        # output based on its input parameters.
        caltable = inputs.calphasetable if inputs.calphasetable is not None else append_caltable
        force_append = append_caltable is not None

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'field': inputs.field,
            'intent': intent,
            'spw': spw,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': minsnr,
            'combine': combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm,
            'append': force_append
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        result_calapp = result.final[0]
        overrides = get_phaseup_overrides(inputs.ms)
        calapp = callibrary.copy_calapplication(result_calapp, **overrides)

        result.final = [calapp]
        result.pool = [calapp]

        return result

    # Used to compute spw mapping diagnostic table
    def _do_offsets_phasecal(self, solint=None, gaintype=None, combine=None):
        # Compute an spw mapping diagnostic table which preapplies the
        # previous table phase table if any spw mapping was done
        # Compute it for maps including the default map
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.offsetstable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': inputs.calminsnr,
            'combine': combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }

        # Create a unique table name. The component filenames depend on task arguments (solint, calmode, gaintype,
        # etc.), hence we create a task to calculate the arguments, then modify the resesulting filename
        task_inputs = gtypegaincal.GTypeGaincalInputs(inputs.context, **task_args)
        root, ext = os.path.splitext(task_inputs.caltable)
        task_args['caltable'] = '{}.{}{}'.format(root, 'offsets', ext)

        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        return result

    # Used for diagnostics not calibration
    def _do_caltarget_ampcal(self, solint):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.calamptable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': solint,
            'gaintype': 'T',
            'calmode': 'a',
            'minsnr': inputs.calminsnr,
            'combine': inputs.combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        return result

    def _do_target_ampcal(self):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.amptable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': 'inf',
            'gaintype': 'T',
            'calmode': 'a',
            'minsnr': inputs.targetminsnr,
            'combine': inputs.combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        # Spec for gainfield application from CAS-12214:
        #
        # 1) When applying phase and/or amp gain tables to calibrators,
        #   continue to use gainfield='nearest'. This will ensure that time
        #   interpolations from one group's complex gain calibrator's
        #   solutions to the next group's complex gain calibrator solutions
        #   will never influence how either calibrator gets calibrated.
        #
        # 2) When applying phase and/or amp gain tables to science targets,
        #    leave gainfield blank (instead of 'nearest'), so that time
        #    interpolation across all gain calibrators will be used to
        #    calibrate science targets. This will automagically take care of
        #    the multiple science groups case. Specifically, since it is
        #    assumed that all science target groups are bracketed on both
        #    sides by "their" complex gain calibrator, there will never be a
        #    case where the interpolated solution between two complex gain
        #    calibrators needs to be used.
        result_calapp = result.final[0]
        cal_calapp = callibrary.copy_calapplication(result_calapp,
                                                    intent='AMPLITUDE,BANDPASS,PHASE,POLARIZATION,POLANGLE,POLLEAKAGE',
                                                    gainfield='nearest', interp='nearest,linear')
        target_calapp = callibrary.copy_calapplication(result_calapp, intent='TARGET,CHECK', gainfield='')

        return [cal_calapp, target_calapp]


def do_gtype_gaincal(context, executor, task_args):
    task_inputs = gtypegaincal.GTypeGaincalInputs(context, **task_args)
    task = gtypegaincal.GTypeGaincal(task_inputs)
    result = executor.execute(task)

    # sanity checks in case gaincal starts returning additional caltable applications
    assert len(result.final) == 1, '>1 caltable application registered by gaincal'

    return result


def get_phaseup_overrides(ms):
    # calwt is always False for the initial phaseups
    overrides = dict(calwt=False)
    if ms.combine_spwmap:
        overrides['interp'] = 'linearPD,linear'
        overrides['spwmap'] = ms.combine_spwmap
    else:
        overrides['interp'] = 'linear,linear'
        overrides['spwmap'] = ms.phaseup_spwmap
    return overrides
