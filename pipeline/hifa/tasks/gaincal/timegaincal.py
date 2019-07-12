from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks.gaincal import common
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hifa.heuristics import exptimes as gexptimes
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
        amp_diagnostic_result = self._do_caltarget_ampcal(solint=amp_calsolint)
        result.calampresult = amp_diagnostic_result

        # Compute the science target phase solution. This solution will be applied to the target, check source, and
        # phase calibrator when the result for this task is accepted.
        cal_calapp, target_calapp = self._do_target_phasecal(solint=inputs.targetsolint, gaintype=phase_gaintype,
                                                             combine=phase_combine)
        # Adopt the solutions, but don't apply them yet.
        result.pool.extend((cal_calapp, target_calapp))
        result.final.extend((cal_calapp, target_calapp))

        # Now compute the calibrator phase solution. This solution will eventually be applied to the AMPLITUDE and
        # BANDPASS calibrators, and temporarily to the PHASE calibrator within this task so we can calculate the
        # residual phase offsets.
        cal_phase_result = self._do_calibrator_phasecal(solint=phase_calsolint, gaintype=phase_gaintype,
                                                        combine=phase_combine)

        # Do a local merge of this result, thus applying the phase solution to the PHASE calibrator but only in the
        # scope of this task. Then, calculate the residuals by calculating another phase solution on the 'corrected'
        # data.
        cal_phase_result.accept(inputs.context)
        phase_residuals_result = self._do_offsets_phasecal(solint='inf', gaintype=phase_gaintype, combine='')
        result.phaseoffsetresult = phase_residuals_result

        # Direct the initial calibrator phase solution for application to the AMPLTIUDE and BANDPASS calibrators
        calphaseresult_calapp = cal_phase_result.final[0]
        calphase_calapp = callibrary.copy_calapplication(calphaseresult_calapp, intent='AMPLITUDE,BANDPASS')
        result.final.append(calphase_calapp)
        result.pool.append(calphase_calapp)

        # Compute the amplitude calibration
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

    def _do_target_phasecal(self, solint=None, gaintype=None, combine=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.targetphasetable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
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
    def _do_calibrator_phasecal(self, solint=None, gaintype=None, combine=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.calphasetable,
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
