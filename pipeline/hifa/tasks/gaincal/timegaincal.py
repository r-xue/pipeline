from __future__ import absolute_import

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

# PIPE-163: SNR based solint for FLUX and BP cals.
SNR_SOLINT_INTENTS = ['AMPLITUDE', 'BANDPASS']

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


@task_registry.set_equivalent_casa_task('hifa_timegaincal')
@task_registry.set_casa_commands_comment('Time dependent gain calibrations are computed.')
class TimeGaincal(gtypegaincal.GTypeGaincal):
    Inputs = TimeGaincalInputs

    def prepare(self, **parameters):
        # Simplify
        inputs = self.inputs

        # Create a results object.
        result = common.GaincalResults()

        # Get the spw mapping mode
        if inputs.ms.combine_spwmap:
            spw_ids = [spw.id for spw in inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=True)]
            field_names = [field.name for field in inputs.ms.get_fields(task_arg=inputs.field, intent='PHASE')]
            exptimes = gexptimes.get_scan_exptimes(inputs.ms, field_names, 'PHASE', spw_ids)
            # Always, increase phase gaincal solint for PHASE when combine='spw'
            phase_calsolint = '%0.3fs' % (min([exptime[1] for exptime in exptimes]) / 4.0)
            phase_gaintype = 'T'
            phase_combine = 'spw'
        else:
            phase_calsolint = inputs.calsolint
            phase_gaintype = 'G'
            phase_combine = inputs.combine

        amp_calsolint = phase_calsolint

        # Produce the diagnostic table for displaying amplitude vs time plots. 
        #     This table is not applied to the data
        #     No special mapping required here.
        LOG.info('Computing amplitude gain table for displaying amplitude vs time plots')
        amp_diagnostic_result = self._do_caltarget_ampcal(solint=amp_calsolint)
        result.calampresult = amp_diagnostic_result

        # Compute the science target phase solution. This solution will be applied to the target, check source, and
        # phase calibrator when the result for this task is accepted.
        LOG.info('Computing phase gain table for target, check source, and phase calibrator.')
        target_phasecal_calapp = self._do_spectralspec_target_phasecal(solint=inputs.targetsolint,
                                                                       gaintype=phase_gaintype,
                                                                       combine=phase_combine)
        # Adopt the solutions, but don't apply them yet.
        result.pool.extend(target_phasecal_calapp)
        result.final.extend(target_phasecal_calapp)

        # Now compute the calibrator phase solution. This solution will temporarily to the PHASE calibrator 
        # within this task so we can calculate the residual phase offsets.
        # The solution is also eventually be applied to the AMPLITUDE and BANDPASS calibrators, 
        LOG.info('Computing phase gain table for bandpass and flux calibrator.')
        (cal_phase_result, temp_phase_result) = \
            self._do_spectralspec_calibrator_phasecal(solint=phase_calsolint, gaintype=phase_gaintype,
                                                      combine=phase_combine)
        #self._do_calibrator_phasecal(solint=phase_calsolint, gaintype=phase_gaintype, combine=phase_combine)

        # Do a local merge of this result, thus applying the phase solution to the PHASE calibrator but only in the
        # scope of this task. Then, calculate the residuals by calculating another phase solution on the 'corrected'
        # data.
        for calphres in cal_phase_result:
            calphres.accept(inputs.context)
        for calphres in temp_phase_result:
            calphres.accept(inputs.context)
        LOG.info('Computing offset phase gain table.')
        phase_residuals_result = self._do_offsets_phasecal(solint='inf', gaintype=phase_gaintype, combine='')
        result.phaseoffsetresult = phase_residuals_result

        # Direct the initial calibrator phase solution for application to the AMPLTIUDE and BANDPASS calibrators
        for cpres in cal_phase_result:
            calphaseresult_calapp = cpres.final[0]
            calphase_calapp = callibrary.copy_calapplication(calphaseresult_calapp, intent='AMPLITUDE,BANDPASS')
            result.final.append(calphase_calapp)
            result.pool.append(calphase_calapp)

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
        if len(spw_map) == 0: #No SpW combination
            return None
        grouped_spw = {}
        request_spws = set( ms.get_spectral_windows(task_arg=spw_sel) )
        for spws in get_spspec_to_spwid_map(request_spws).values():
            ref_spw = { spw_map[i] for i in spws }
            assert len(ref_spw) == 1, 'A SpectralSpec is mapped to more than one SpWs'
            grouped_spw[ref_spw.pop()] = str(',').join([str(i) for i in sorted(spws)])
        LOG.debug('SpectralSpec grouping: %s' % grouped_spw)
        return grouped_spw

    def _do_spectralspec_target_phasecal(self, solint=None, gaintype=None, combine=None):
        if 'spw' not in combine:
            return self._do_target_phasecal(solint, gaintype, combine)
        # Combined SpW solution. Need to solve per SpectralSpec
        inputs = self.inputs
        ms = inputs.ms
#         spw_groups = self._group_spw_by_spwmap(ms.combine_spwmap, inputs.spw)
        spw_groups = self._group_by_spectralspec(inputs.spw)
        if spw_groups is None:
            raise ValueError('Invalid SpW grouping input.')
        # Need to solve per SpectralSpec
        calapp_list = []
        for spw_sel in spw_groups.values():
            LOG.info('Processing spectral spec with spws {}'.format(spw_sel))
            selected_scans = ms.get_scans(scan_intent=inputs.intent, spw=spw_sel)
            if len(selected_scans) == 0:
                LOG.info('Skipping table generation for empty selection: spw=%s, intent=%s' % (spw_sel, inputs.intent))
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

        return (cal_calapp, target_calapp)

    # Used to calibrate "selfcaled" targets
    def _do_spectralspec_calibrator_phasecal(self, solint=None, gaintype=None, combine=None):
        if 'spw' not in combine:
            apply_list = [ self._do_calibrator_phasecal(solint, gaintype, combine) ]
            temporal_list = []
            return (apply_list, temporal_list)
        # Combined SpW solution. Need to solve per SpectralSpec
        inputs = self.inputs
        ms = inputs.ms
#         spw_groups = self._group_spw_by_spwmap(ms.combine_spwmap, inputs.spw)
        spw_groups = self._group_by_spectralspec(inputs.spw)
        low_combined_snr_spw_list = ms.low_combined_phasesnr_spws
        if spw_groups is None:
            raise ValueError('Invalid SpW grouping input.')
        # Need to solve per SpectralSpec when combine = 'spw'
        all_intents = self.inputs.intent.split(',')
        snr_intents = []
        other_intents = []
        for intent in all_intents:
            if intent in SNR_SOLINT_INTENTS:
                snr_intents.append(intent)
            else:
                other_intents.append(intent)
        apply_list = []
        temporal_list = []
        for ref_spw, spw_sel in spw_groups.items():
            LOG.info('Processing spectral spec with spws {}'.format(spw_sel))
            extend_solint = (ref_spw in low_combined_snr_spw_list)
            if extend_solint:
                #Low SNR SpectralSpec. All intents should be solved with soilint = 1/4 scan time
                intent = str(',').join(all_intents)
                interval = solint
                result = self._do_calibrator_phasecal(interval, gaintype, combine, spw_sel, intent)
                apply_list.append(result)
            else:
                # Combined solution meets phasesnr limit. Use separate solint by intent.
                # For SNR_SOLINT_INTENTS (BANDPASS and AMPLUTUDE_
                # Use inputs.calsolint when combined SNR meets phasesnr limit.
                if len(snr_intents) > 0:
                    intent = str(',').join(snr_intents)
                    interval = self.inputs.calsolint
                    result = self._do_calibrator_phasecal(interval, gaintype, combine, spw_sel, intent)
                    apply_list.append(result)
                # NON-SNR based solint for the other sources (e.g., PHASE). Always use solint = 1/4 scan time
                # This table is used only temporary applied to generate offset caltable.
                if len(other_intents) > 0:
                    intent = str(',').join(other_intents)
                    interval = solint
                    result = self._do_calibrator_phasecal(interval, gaintype, combine, spw_sel, intent)
                    temporal_list.append(result)
        return (apply_list, temporal_list)

    # Used to calibrate "selfcaled" targets
    def _do_calibrator_phasecal(self, solint=None, gaintype=None, combine=None, spw=None, intent=None):
        inputs = self.inputs
        spw_sel = str(spw) if spw is not None else inputs.spw
        intent_sel = intent if intent is not None else inputs.intent

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.calphasetable,
            'field': inputs.field,
            'intent': intent_sel,
            'spw': spw_sel,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': inputs.calminsnr,
            'combine': combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
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
        cal_calapp = callibrary.copy_calapplication(result_calapp, intent='AMPLITUDE,BANDPASS,PHASE',
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
