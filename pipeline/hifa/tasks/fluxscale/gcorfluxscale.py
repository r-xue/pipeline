import collections
import operator
import os
import uuid
from functools import reduce

import numpy as np
import scipy.stats as stats

import pipeline.domain as domain
import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import FluxMeasurement
from pipeline.h.tasks.common import commonfluxresults
from pipeline.h.tasks.common import commonhelpermethods
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import gaincal
from pipeline.hif.tasks.fluxscale import fluxscale
from pipeline.hif.tasks.setmodel import setjy
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import exceptions
from pipeline.infrastructure import task_registry
from . import fluxes
from ... import heuristics

__all__ = [
    'GcorFluxscale',
    'GcorFluxscaleInputs',
    'GcorFluxscaleResults',
    'SessionGcorFluxscale',
    'SessionGcorFluxscaleInputs'
]

LOG = infrastructure.get_logger(__name__)

ORIGIN = 'gcorfluxscale'


class GcorFluxscaleResults(commonfluxresults.FluxCalibrationResults):
    def __init__(self, vis, resantenna=None, uvrange=None, measurements=None, fluxscale_measurements=None,
                 applies_adopted=False):
        super(GcorFluxscaleResults, self).__init__(vis, resantenna=resantenna, uvrange=uvrange,
                                                   measurements=measurements)
        self.applies_adopted = applies_adopted

        # To store the fluxscale derived flux measurements:
        if fluxscale_measurements is None:
            fluxscale_measurements = collections.defaultdict(list)
        self.fluxscale_measurements = fluxscale_measurements

    def merge_with_context(self, context):
        # Update the measurement set with the calibrated visibility based flux
        # measurements for later use in imaging (PIPE-644, PIPE-660).
        ms = context.observing_run.get_ms(self.vis)
        ms.derived_fluxes = self.measurements


class GcorFluxscaleInputs(fluxscale.FluxscaleInputs):
    antenna = vdp.VisDependentProperty(default='')
    hm_resolvedcals = vdp.VisDependentProperty(default='automatic')
    minsnr = vdp.VisDependentProperty(default=2.0)
    peak_fraction = vdp.VisDependentProperty(default=0.2)
    phaseupsolint = vdp.VisDependentProperty(default='int')
    refant = vdp.VisDependentProperty(default='')

    @vdp.VisDependentProperty
    def reffile(self):
        return os.path.join(self.context.output_dir, 'flux.csv')

    @vdp.VisDependentProperty
    def refspwmap(self):
        return []

    solint = vdp.VisDependentProperty(default='inf')
    # adds polarisation intent to transfer intent as required by PIPE-599
    transintent = vdp.VisDependentProperty(default='PHASE,BANDPASS,CHECK,POLARIZATION,POLANGLE,POLLEAKAGE')
    uvrange = vdp.VisDependentProperty(default='')

    def __init__(self, context, output_dir=None, vis=None, caltable=None, fluxtable=None, reffile=None, reference=None,
                 transfer=None, refspwmap=None, refintent=None, transintent=None, solint=None, phaseupsolint=None,
                 minsnr=None, refant=None, hm_resolvedcals=None, antenna=None, uvrange=None, peak_fraction=None):
        super(GcorFluxscaleInputs, self).__init__(context, output_dir=output_dir, vis=vis, caltable=caltable,
                                                  fluxtable=fluxtable, reference=reference, transfer=transfer,
                                                  refspwmap=refspwmap, refintent=refintent, transintent=transintent)
        self.reffile = reffile
        self.solint = solint
        self.phaseupsolint = phaseupsolint
        self.minsnr = minsnr
        self.refant = refant
        self.hm_resolvedcals = hm_resolvedcals
        self.antenna = antenna
        self.uvrange = uvrange
        self.peak_fraction = peak_fraction


@task_registry.set_equivalent_casa_task('hifa_gfluxscale')
@task_registry.set_casa_commands_comment(
    'The absolute flux calibration is transferred to secondary calibrator sources.'
)
class GcorFluxscale(basetask.StandardTaskTemplate):
    Inputs = GcorFluxscaleInputs

    def __init__(self, inputs):
        super(GcorFluxscale, self).__init__(inputs)

    def prepare(self, **parameters):
        inputs = self.inputs

        # Initialize results.
        result = GcorFluxscaleResults(inputs.vis, resantenna='', uvrange='')

        # Check that the measurement set does have an amplitude calibrator.
        if inputs.reference == '':
            # No point carrying on if not.
            LOG.error('%s has no data with reference intent %s' % (inputs.ms.basename, inputs.refintent))
            return result

        # Run setjy for sources in the reference list which have transfer intents.
        if inputs.ms.get_fields(inputs.reference, intent=inputs.transintent):
            self._do_setjy(reffile=inputs.reffile, field=inputs.reference)
        else:
            LOG.info('Flux calibrator field(s) {!r} in {!s} have no data with '
                     'intent {!s}'.format(inputs.reference, inputs.ms.basename, inputs.transintent))

        # set the reference antenna
        refant = inputs.refant
        if refant == '':

            # Get the reference antenna for this measurement set from the
            # context. This comes back as a string containing a ranked
            # list of antenna names.
            refant = inputs.ms.reference_antenna

            # If no reference antenna was found in the context for this measurement
            # (refant equals either None or an empty string), then raise an exception.
            if not (refant and refant.strip()):
                msg = ('No reference antenna specified and none found in context for %s' % inputs.ms.basename)
                LOG.error(msg)
                raise exceptions.PipelineException(msg)

        LOG.trace('refant: %s' % refant)

        # Get the reference spwmap for flux scaling
        refspwmap = inputs.refspwmap
        if not refspwmap:
            refspwmap = inputs.ms.reference_spwmap
            if not refspwmap:
                refspwmap = [-1]

        # Determine the spw gaincal strategy. This determines how the phase only tables will
        # be constructed.
        if inputs.ms.combine_spwmap:
            spwidlist = [spw.id for spw in inputs.ms.get_spectral_windows(science_windows_only=True)]
            fieldnamelist = [field.name for field in inputs.ms.get_fields(task_arg=inputs.transfer, intent='PHASE')]
            exptimes = heuristics.exptimes.get_scan_exptimes(inputs.ms, fieldnamelist, 'PHASE', spwidlist)
            phaseup_solint = '%0.3fs' % (min([exptime[1] for exptime in exptimes]) / 4.0)
            phase_gaintype = 'T'
            phase_combine = 'spw'
            phaseup_spwmap = inputs.ms.combine_spwmap
            phase_interp = 'linearPD,linear'
        else:
            phaseup_solint = inputs.phaseupsolint
            phase_gaintype = 'G'
            phase_combine = ''
            phaseup_spwmap = inputs.ms.phaseup_spwmap
            phase_interp = None

        # Resolved source heuristics.
        #    Needs improvement if users start specifying the input antennas.
        #    For the time being force minblperant to be 2 instead of None to
        #    avoid ACA and Tsys flagging issues.
        allantenna = inputs.antenna
        minblperant = 2

        if inputs.hm_resolvedcals == 'automatic':
            # Get the antennas to be used in the gaincals, limiting
            # the range if the reference calibrator is resolved.
            refant0 = refant.split(',')[0]  # use the first refant
            resantenna, uvrange = heuristics.fluxscale.antenna(ms=inputs.ms, refsource=inputs.reference, refant=refant0,
                                                               peak_frac=inputs.peak_fraction)

            # Do nothing if the source is unresolved.
            # If the source is resolved but the number of
            # antennas equals the total number of antennas
            # use all the antennas but pass along the uvrange
            # limit.
            if resantenna == '' and uvrange == '':
                pass
            else:
                nant = len(inputs.ms.antennas)
                nresant = len(resantenna.split(','))
                if nresant >= nant:
                    resantenna = allantenna
        else:
            resantenna = allantenna
            uvrange = inputs.uvrange

        result.resantenna = resantenna
        result.uvrange = uvrange

        # Do a phase-only gaincal on the flux calibrator using a
        # restricted set of antennas
        if resantenna == '':
            filtered_refant = refant
        else:  # filter refant if resolved calibrator or antenna selection
            resant_list = resantenna.rstrip('&').split(',')
            filtered_refant = str(',').join([ant for ant in refant.split(',') if ant in resant_list])

        r = self._do_gaincal(field=inputs.reference, intent=inputs.refintent, gaintype=phase_gaintype, calmode='p',
                             combine=phase_combine, solint=inputs.phaseupsolint, antenna=resantenna, uvrange=uvrange,
                             refant=filtered_refant, minblperant=minblperant, phaseup_spwmap=None, phase_interp=None,
                             append=False, merge=False)

        # Test for the existence of the caltable
        try:
            caltable = r.final.pop().gaintable
        except:
            caltable = r.error.pop().gaintable
            LOG.warning('Cannot compute phase solution table %s for the flux calibrator' % (os.path.basename(caltable)))

        # Do a phase-only gaincal on the remaining calibrators using the full
        # set of antennas; append this to the phase solutions caltable produced
        # for the flux calibrator; merge result into local task context so that
        # the caltable is included in pre-apply for subsequent gaincal.
        append = os.path.exists(caltable)
        r = self._do_gaincal(caltable=caltable, field=inputs.transfer, intent=inputs.transintent,
                             gaintype=phase_gaintype, calmode='p', combine=phase_combine, solint=phaseup_solint,
                             antenna=allantenna, uvrange='', minblperant=None, refant=filtered_refant,
                             phaseup_spwmap=phaseup_spwmap, phase_interp=phase_interp, append=append, merge=True)

        # Now do the amplitude-only gaincal. This will produce the caltable
        # that fluxscale will analyse
        try:
            ampcal_result = self._do_gaincal(
                field=inputs.transfer + ',' + inputs.reference,
                intent=inputs.transintent + ',' + inputs.refintent, gaintype='T', calmode='a',
                combine='', solint=inputs.solint, antenna=allantenna, uvrange='',
                refant=filtered_refant, minblperant=minblperant, phaseup_spwmap=None,
                phase_interp=None, append=False, merge=True)

            # Get the gaincal caltable from the results
            try:
                caltable = ampcal_result.final.pop().gaintable
            except:
                caltable = ' %s' % ampcal_result.error.pop().gaintable if ampcal_result.error else ''
                LOG.warning('Cannot compute compute the flux scaling table%s' % (os.path.basename(caltable)))

            # To make the following fluxscale reliable the caltable
            # should contain gains for the the same set of antennas for
            # each of the amplitude and phase calibrators - looking
            # at each spw separately.
            if os.path.exists(caltable):
                check_ok = self._check_caltable(caltable=caltable, ms=inputs.ms, reference=inputs.reference,
                                                transfer=inputs.transfer)
            else:
                check_ok = False

        except:
            caltable = ' %s' % ampcal_result.error.pop().gaintable if ampcal_result.error else ''
            LOG.warning('Cannot compute phase solution table%s for the phase '
                        'and bandpass calibrator' % (os.path.basename(caltable)))
            check_ok = False

        # If no valid amplitude caltable is available to analyse, then log an
        # error and return early.
        if not check_ok:
            LOG.error('Unable to complete flux scaling operation for MS %s' % (os.path.basename(inputs.vis)))
            return result

        # Otherwise, continue with derivation of flux densities.
        # PIPE-644: derive both fluxscale-based scaling factors, as well as
        # calibrated visibility fluxes.
        try:
            # Derive fluxscale-based flux measurements, and store in the result
            # for reporting in weblog.
            fluxscale_result = self._derive_fluxscale_flux(caltable, refspwmap)
            result.fluxscale_measurements.update(fluxscale_result.measurements)

            # Computing calibrated visibility fluxes will require a temporary
            # applycal, which is performed as part of "_derive_calvis_flux()"
            # below. To prepare for this temporary applycal, first update the
            # callibrary in the local context to replace the amplitude caltable
            # produced earlier (which used a default flux density of 1.0 Jy)
            # with the caltable produced by fluxscale, which contains
            # amplitudes set according to the derived flux values.
            self._replace_amplitude_caltable(ampcal_result, fluxscale_result)

            # Derive calibrated visibility based flux measurements
            # and store in result for reporting in weblog and merging into
            # context (into measurement set).
            calvis_fluxes = self._derive_calvis_flux()
            result.measurements.update(calvis_fluxes.measurements)
        except Exception as e:
            # Something has gone wrong, return an empty result
            LOG.error('Unable to complete flux scaling operation for MS {}'.format(inputs.ms.basename))
            LOG.exception('Flux scaling error', exc_info=e)

        return result

    def analyse(self, result):
        return result

    @staticmethod
    def _check_caltable(caltable, ms, reference, transfer):
        """
        Check that the give caltable is well-formed so that a 'fluxscale'
        will run successfully on it:

          1. Check that the caltable contains results for the reference and
             transfer fields.
        """
        # get the ids of the reference source and phase source(s)
        ref_fieldid = {field.id for field in ms.fields if field.name == reference}
        transfer_fieldids = {field.id for field in ms.fields if field.name in transfer}

        with casa_tools.TableReader(caltable) as table:
            fieldids = table.getcol('FIELD_ID')

        # warn if field IDs does not contains the amplitude and phase calibrators
        fieldids = set(fieldids)
        if fieldids.isdisjoint(ref_fieldid):
            LOG.warning('%s contains ambiguous reference calibrator field names' % os.path.basename(caltable))
        if not fieldids.issuperset(transfer_fieldids):
            LOG.warning('%s does not contain results for all transfer calibrators' % os.path.basename(caltable))

        return True

    def _compute_calvis_flux(self, fieldid, spwid):

        # Read in data from the MS, for specified field, spw, and all transfer
        # intents.
        data = self._read_data_from_ms(self.inputs.ms, fieldid, spwid, self.inputs.transintent)

        # Return if no valid data were read.
        if not data:
            return

        # Get number of correlations for this spw.
        corr_type = commonhelpermethods.get_corr_products(self.inputs.ms, int(spwid))
        ncorrs = len(corr_type)

        # Select which correlations to consider for computing the mean
        # visibility flux:
        #  - for single and dual pol, consider all correlations.
        #  - for ncorr == 4 with linear correlation products (XX, XY, etc)
        #    select the XX and YY columns.
        #  - for other values of ncorrs, or e.g. circular correlation (LL, RL)
        #    raise a warning that these are not handled.
        if ncorrs in [1, 2]:
            columns_to_select = range(ncorrs)
        elif ncorrs == 4 and set(corr_type) == {'XX', 'XY', 'YX', 'YY'}:
            columns_to_select = [corr_type.index('XX'), corr_type.index('YY')]
        else:
            LOG.warning("Unexpected polarisations found for MS {}, unable to compute mean visibility fluxes."
                        "".format(self.inputs.ms.basename))
            columns_to_select = []

        # Derive mean flux and variance for each polarisation.
        mean_fluxes = []
        variances = []
        for col in columns_to_select:
            # Select data for current polarisation.
            ampdata = np.squeeze(data['corrected_data'], axis=1)[col]
            flagdata = np.squeeze(data['flag'], axis=1)[col]
            weightdata = data['weight'][col]

            # Select for non-flagged data and non-NaN data.
            id_nonbad = np.where(np.logical_and(np.logical_not(flagdata), np.isfinite(ampdata)))
            amplitudes = ampdata[id_nonbad]
            weights = weightdata[id_nonbad]

            # If no valid data are available, skip to the next polarisation.
            if len(amplitudes) == 0:
                continue

            # Determine number of non-flagged antennas, baselines, and
            # integrations.
            ant1 = data['antenna1'][id_nonbad]
            ant2 = data['antenna2'][id_nonbad]
            n_ants = len(set(ant1) | set(ant2))
            n_baselines = n_ants * (n_ants - 1) // 2
            n_ints = len(amplitudes) // n_baselines

            # PIPE-644: Determine scale factor for variance.
            var_scale = np.mean([len(amplitudes), n_ints * n_ants])

            # Compute mean flux and stdev for current polarisation.
            mean_flux = np.abs(np.average(amplitudes, weights=weights))
            variance = np.average((np.abs(amplitudes) - mean_flux)**2, weights=weights) / var_scale

            # Store for this polarisation.
            mean_fluxes.append(mean_flux)
            variances.append(variance)

        # Compute mean flux and mean stdev for all polarisations.
        if mean_fluxes:
            mean_flux = np.mean(mean_fluxes)
            mean_std = np.sqrt(np.mean(variances))
        # If no valid data was found for any polarisation, then set flux and
        # uncertainty to zero.
        else:
            mean_flux = 0
            mean_std = 0
            LOG.debug("No valid data found for MS {}, field {}, spw {}, unable to compute mean visibility flux; flux"
                      " and uncertainty will be set to zero.".format(self.inputs.ms.basename, fieldid, spwid))

        # Combine into single flux measurement object.
        flux = domain.FluxMeasurement(spwid, mean_flux, origin=ORIGIN)
        flux.uncertainty = domain.FluxMeasurement(spwid, mean_std, origin=ORIGIN)

        return flux

    def _derive_calvis_flux(self):
        """
        Derive calibrated visibility fluxes.

        To compute the "calibrated" fluxes, this method will "temporarily"
        apply the existing calibration tables, including the the new phase and
        amplitude caltables created earlier during the hifa_gfluxscale task.

        First, create a back-up of the MS flagging state, then run an applycal
        for the necessary intents and fields. Next, compute the calibrated
        visibility fluxes. Finally, always restore the back-up of the MS
        flagging state, to undo any flags that were propagated from the applied
        caltables.

        :return: commonfluxresults.FluxCalibrationResults containing the
        calibrated visibility fluxes and uncertainties.
        """
        inputs = self.inputs

        # Identify fields and spws to derive calibrated vis for.
        transfer_fields = inputs.ms.get_fields(task_arg=inputs.transfer)
        sci_spws = set(inputs.ms.get_spectral_windows(science_windows_only=True))
        transfer_fieldids = {str(field.id) for field in transfer_fields}
        spw_ids = {str(spw.id) for field in transfer_fields for spw in field.valid_spws.intersection(sci_spws)}

        # Create back-up of MS flagging state.
        LOG.info('Creating back-up of flagging state')
        flag_backup_name = 'before_gfluxscale_calvis'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name)
        self._executor.execute(task)

        # Run computation of calibrated visibility fluxes in try/finally to
        # ensure that the MS are always restored, even in case of an exception.
        try:
            # Apply all caltables registered in the callibrary in the local
            # context to the MS.
            LOG.info('Applying pre-existing caltables and preliminary phase-up and amplitude caltables.')
            acinputs = applycal.IFApplycalInputs(context=inputs.context, vis=inputs.vis, field=inputs.transfer,
                                                 intent=inputs.transintent, flagsum=False, flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            self._executor.execute(actask)

            # Initialize result.
            result = commonfluxresults.FluxCalibrationResults(inputs.vis)

            # Compute the mean calibrated visibility flux for each field and
            # spw and add as flux measurement to the final result.
            for fieldid in transfer_fieldids:
                for spwid in spw_ids:
                    fluxes_for_field_and_spw = self._compute_calvis_flux(fieldid, spwid)
                    if fluxes_for_field_and_spw:
                        result.measurements[fieldid].append(fluxes_for_field_and_spw)
        finally:
            # Restore the MS flagging state.
            LOG.info('Restoring back-up of flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name)
            self._executor.execute(task)

        return result

    def _derive_fluxscale_flux(self, caltable, refspwmap):
        inputs = self.inputs

        # Schedule a fluxscale job using this caltable. This is the result
        # that contains the flux measurements for the context.
        # We need to write the fluxscale-derived flux densities to a file,
        # which can then be used as input for the subsequent setjy task.
        # This is the name of that file.
        # use UUID so that parallel MPI processes do not unlink the same file
        reffile = os.path.join(inputs.context.output_dir, 'fluxscale_{!s}.csv'.format(uuid.uuid4()))
        try:
            fluxscale_result = self._do_fluxscale(caltable, refspwmap=refspwmap)

            # Determine fields ids for which a model spix should be
            # set along with the derived flux. For now this is
            # restricted to BANDPASS fields
            fieldids_with_spix = [str(f.id) for f in inputs.ms.get_fields(task_arg=inputs.transfer, intent='BANDPASS')]

            # Store the results in a temporary file.
            fluxes.export_flux_from_fit_result(fluxscale_result, inputs.context, reffile,
                                               fieldids_with_spix=fieldids_with_spix)

            # Finally, do a setjy, add its setjy_settings
            # to the main result
            self._do_setjy(reffile=reffile, field=inputs.transfer)

            # Use the fluxscale measurements to get the uncertainties too.
            # This makes the (big) assumption that setjy executed exactly
            # what we passed in as arguments.
        finally:
            # clean up temporary file
            if os.path.exists(reffile):
                os.remove(reffile)

        return fluxscale_result

    def _do_gaincal(self, caltable=None, field=None, intent=None, gaintype='G', calmode=None, combine=None, solint=None,
                    antenna=None, uvrange='', refant=None, minblperant=None, phaseup_spwmap=None, phase_interp=None,
                    append=None, merge=True):
        inputs = self.inputs

        # Use only valid science spws
        fieldlist = inputs.ms.get_fields(task_arg=field)
        sci_spws = set(inputs.ms.get_spectral_windows(science_windows_only=True))
        spw_ids = {spw.id for fld in fieldlist for spw in fld.valid_spws.intersection(sci_spws)}
        spw_ids = ','.join(map(str, spw_ids))

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'field': field,
            'intent': intent,
            'spw': spw_ids,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': calmode,
            'minsnr': inputs.minsnr,
            'combine': combine,
            'refant': refant,
            'antenna': antenna,
            'uvrange': uvrange,
            'minblperant': minblperant,
            'solnorm': False,
            'append': append
        }

        # Note that field and antenna task there default values for the
        # purpose of setting up the calto object.
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)
        task = gaincal.GTypeGaincal(task_inputs)

        # Execute task
        result = self._executor.execute(task)

        # Merge
        if merge:
            overrides = {}

            # Adjust the spwmap for the combine case
            if inputs.ms.combine_spwmap and phase_interp:
                overrides['interp'] = phase_interp

            # Adjust the spw map
            if phaseup_spwmap:
                overrides['spwmap'] = phaseup_spwmap

            if overrides:
                original_calapp = result.pool[0]
                modified_calapp = callibrary.copy_calapplication(original_calapp, **overrides)
                result.pool[0] = modified_calapp
                result.final[0] = modified_calapp

            # Merge result to the local context
            result.accept(inputs.context)

        return result

    def _do_fluxscale(self, caltable=None, refspwmap=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'reference': inputs.reference,
            'transfer': inputs.transfer,
            'refspwmap': refspwmap
        }

        task_inputs = fluxscale.Fluxscale.Inputs(inputs.context, **task_args)
        task = fluxscale.Fluxscale(task_inputs)

        return self._executor.execute(task, merge=True)

    def _do_setjy(self, reffile=None, field=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'field': field,
            'intent': inputs.transintent,
            'reffile': reffile
        }

        task_inputs = setjy.Setjy.Inputs(inputs.context, **task_args)
        task = setjy.Setjy(task_inputs)

        return self._executor.execute(task, merge=True)

    @staticmethod
    def _read_data_from_ms(ms, fieldid, spwid, intent):

        # Initialize data selection.
        data_selection = {'field': fieldid,
                          'scanintent': '*%s*' % utils.to_CASA_intent(ms, intent),
                          'spw': spwid}

        # Get number of channels for this spw.
        nchans = ms.get_spectral_windows(spwid)[0].num_channels

        # Read in data from MS.
        with casa_tools.MSReader(ms.name) as openms:
            try:
                # Apply data selection.
                openms.msselect(data_selection)

                # Set channel selection to take the average of all channels.
                openms.selectchannel(1, 0, nchans, 1)

                # Extract data from MS.
                data = openms.getdata(['corrected_data', 'flag', 'antenna1', 'antenna2', 'weight'])
            except:
                # Log a warning and return without data.
                LOG.warning('Unable to compute mean vis for intent(s) {}, field {}, spw {}'
                            ''.format(intent, fieldid, spwid))
                return

        return data

    def _replace_amplitude_caltable(self, ampresult, fsresult):
        inputs = self.inputs

        # Identify the MS to process.
        vis = os.path.basename(inputs.vis)

        # predicate function to match hifa_gfluxscale amplitude caltable for this MS
        def gfluxscale_amp_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            calto_vis = {os.path.basename(v) for v in calto.vis}

            # Standard caltable filenames contain task identifiers,
            # caltable type identifiers, etc. We can use this to identify
            # caltables created by this task. As an extra check we also
            # check the caltable type.
            do_delete = 'hifa_gfluxscale' in calfrom.gaintable and 'gaincal' in calfrom.caltype and vis in calto_vis \
                and 'gacal' in calfrom.gaintable

            if do_delete:
                LOG.debug(f'Unregistering previous amplitude calibrations for {vis}')
            return do_delete

        inputs.context.callibrary.unregister_calibrations(gfluxscale_amp_matcher)

        # Add caltable from fluxscale result to local context callibrary.
        orig_calapp = ampresult.pool[0]
        new_calapp = callibrary.copy_calapplication(orig_calapp, gaintable=fsresult.inputs['fluxtable'])
        LOG.debug(f'Adding calibration to callibrary:\n{new_calapp.calto}\n{new_calapp.calfrom}')
        inputs.context.callibrary.add(new_calapp.calto, new_calapp.calfrom)


class SessionGcorFluxscaleInputs(GcorFluxscaleInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, output_dir=None, vis=None, caltable=None, fluxtable=None, reffile=None, reference=None,
                 transfer=None, refspwmap=None, refintent=None, transintent=None, solint=None, phaseupsolint=None,
                 minsnr=None, refant=None, hm_resolvedcals=None, antenna=None, uvrange=None, peak_fraction=None,
                 parallel=None):
        super(SessionGcorFluxscaleInputs, self).__init__(context, output_dir=output_dir, vis=vis, caltable=caltable,
                                                         fluxtable=fluxtable, reffile=reffile, reference=reference,
                                                         transfer=transfer, refspwmap=refspwmap, refintent=refintent,
                                                         transintent=transintent, solint=solint,
                                                         phaseupsolint=phaseupsolint, minsnr=minsnr, refant=refant,
                                                         hm_resolvedcals=hm_resolvedcals, antenna=antenna,
                                                         uvrange=uvrange, peak_fraction=peak_fraction)
        self.parallel = parallel


AMPLITUDE_MISSING = '__AMPLITUDE_MISSING__'


@task_registry.set_equivalent_casa_task('session_gfluxscale')
class SessionGcorFluxscale(basetask.StandardTaskTemplate):
    Inputs = SessionGcorFluxscaleInputs

    def __init__(self, inputs):
        super(SessionGcorFluxscale, self).__init__(inputs)

    is_multi_vis_task = True

    def prepare(self):
        inputs = self.inputs

        vis_list = sessionutils.as_list(inputs.vis)

        assessed = []
        with sessionutils.VDPTaskFactory(inputs, self._executor, GcorFluxscale) as factory:
            task_queue = [(vis, factory.get_task(vis)) for vis in vis_list]

            for (vis, (task_args, task)) in task_queue:
                # only launch jobs for MSes with amplitude calibrators.
                # The analyse() method will subsequently adopt the
                # appropriate flux calibration measurements from one of
                # the completed jobs.
                ms = inputs.context.observing_run.get_ms(vis)
                if 'AMPLITUDE' not in ms.intents:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, AMPLITUDE_MISSING))
                    continue
                try:
                    worker_result = task.get_result()
                except exceptions.PipelineException as e:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, e))
                else:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, worker_result))

        return assessed

    def analyse(self, assessed):
        # all results will be added to this object
        final_result = basetask.ResultsList()

        context = self.inputs.context

        session_groups = sessionutils.group_into_sessions(context, assessed)
        for session_id, session_results in session_groups.items():
            # we need to convert the Field ID to field name in the
            # measurements
            measurements_per_field = collect_flux_measurements(context, session_results)

            averaged = calc_averages_per_field(measurements_per_field)

            for vis, task_args, vis_result in session_results:
                if vis_result == AMPLITUDE_MISSING:
                    no_amplitude_ms = context.observing_run.get_ms(vis)

                    # find other flux calibrations for any of our fields
                    no_amplitude_field_names = {f.name for f in no_amplitude_ms.fields}
                    fields_to_adopt = no_amplitude_field_names.intersection(set(averaged.keys()))

                    if len(fields_to_adopt) is 0:
                        LOG.error('Could not find a flux calibration to adopt for '
                                  '{!s}.'.format(no_amplitude_ms.basename))
                        continue

                    LOG.info('Adopting flux calibrations for {!s}; fields: {!s}'
                             ''.format(no_amplitude_ms.basename, ', '.join(fields_to_adopt)))

                    # these are the measurements to adopt, but the spw
                    # names still need to be remapped to spw IDs for
                    # this MS
                    unmapped_adopted = {k: v for k, v in averaged.items() if k in no_amplitude_field_names}

                    mapped_adopted = map_spw_names_to_id(context, vis, unmapped_adopted)

                    fake_result = GcorFluxscaleResults(vis=vis, measurements=mapped_adopted, applies_adopted=True)
                    fake_result.inputs = task_args
                    fake_result.task = SessionGcorFluxscale

                    final_result.append(fake_result)

                elif isinstance(vis_result, Exception):
                    LOG.error('No flux calibration created for {!s}'.format(os.path.basename(vis)))

                    fake_result = GcorFluxscaleResults(vis=vis)
                    fake_result.inputs = task_args

                    final_result.append(fake_result)

                else:
                    final_result.append(vis_result)

        return final_result


def get_field_name(context, vis, identifier):
    ms = context.observing_run.get_ms(vis)
    fields = set(ms.get_fields(task_arg=identifier))
    if len(fields) is not 1:
        raise KeyError('{!r} does not uniquely identify a field: ({!s} matches found)'
                       ''.format(identifier, len(fields)))
    fields = fields.pop()
    return fields.name


def collect_flux_measurements(context, vis_result_tuples):
    """
    Compile the flux measurements from a set of results into a new
    dict data structure.

    :param context: the pipeline context
    :param vis_result_tuples: the VisResultTuples to inspect
    :type vis_result_tuples: list of VisResultTuples
    :return: dict of tuples
    :rtype: dict of {str field name: (vis, spw name, flux measurement)}
    """
    d = collections.defaultdict(list)

    for vis, _, result in vis_result_tuples:
        if result == AMPLITUDE_MISSING:
            continue

        ms = context.observing_run.get_ms(vis)

        for field_id, measurements in result.measurements.items():
            field_name = get_field_name(context, vis, field_id)

            for m in measurements:
                spws = ms.get_spectral_windows(task_arg=m.spw_id)
                assert(len(spws) is 1)
                spw = spws.pop()
                d[field_name].append((vis, spw.name, m))

    return d


def calc_averages_per_field(results):
    """
    Return a compiled and averaged flux calibrations per field.

    :param results:
    :return:
    """
    averages = collections.defaultdict(list)
    for field_name, measurement_structs in results.items():
        spw_names = {spw_name for _, spw_name, _ in measurement_structs}
        for spw_name in spw_names:
            measurements_for_spw = [measurement for _, name, measurement in measurement_structs
                                    if name == spw_name]
            if len(measurements_for_spw) == 0:
                continue

            mean = reduce(operator.add, measurements_for_spw) / len(measurements_for_spw)

            # copy the uncertainty if there's only one measurement,
            # otherwise calculate the standard error of the mean.
            if len(measurements_for_spw) == 1:
                m = measurements_for_spw[0]
                unc_I = m.uncertainty.I
                unc_Q = m.uncertainty.Q
                unc_U = m.uncertainty.U
                unc_V = m.uncertainty.V
            else:
                JY = measures.FluxDensityUnits.JANSKY
                unc_I = stats.sem([float(m.I.to_units(JY)) for m in measurements_for_spw])
                unc_Q = stats.sem([float(m.Q.to_units(JY)) for m in measurements_for_spw])
                unc_U = stats.sem([float(m.U.to_units(JY)) for m in measurements_for_spw])
                unc_V = stats.sem([float(m.V.to_units(JY)) for m in measurements_for_spw])

            # floats are interpreted as Jy, so we don't need to convert
            # SEM values
            mean.uncertainty = FluxMeasurement(spw_name, unc_I, Q=unc_Q, U=unc_U, V=unc_V, origin=ORIGIN)

            averages[field_name].append((spw_name, mean))

    return averages


def map_spw_names_to_id(context, vis, field_measurements):
    """
    Copy a flux result dict, remapping the target spectral window in
    the original result to a new measurement set.

    This function makes a copy of a dict of flux calibration results
    (keys=field names, values=FluxMeasurements), remapping the spectral
    window target in the results to the corresponding spectral window
    in the target measurement set.

    :param context: pipeline context
    :param vis: name of the measurement set to remap spws to
    :param field_measurements: flux calibrations to adopt
    :type field_measurements: dict with format {str: [FluxMeasurements]}
    :return: flux results remapped to measurement set
    :rtype: dict with format {str: [FluxMeasurements]}
    """
    ms = context.observing_run.get_ms(vis)
    science_spws = ms.get_spectral_windows(science_windows_only=True)
    spw_names_to_id = {spw.name: spw.id for spw in science_spws}
    # spw names must uniquely identify a science spw, otherwise we
    # can't create a correct spw ID mapping
    assert (len(spw_names_to_id) == len(science_spws))

    d = {field_name: [copy_flux_measurement(m, spw_id=spw_names_to_id[spw_name])
                      for spw_name, m in measurements if spw_name in spw_names_to_id]
         for field_name, measurements in field_measurements.items()}

    return d


def copy_flux_measurement(source, spw_id=None, I=None, Q=None, U=None, V=None, spix=None, uI=None, uQ=None, uU=None,
                          uV=None):
    if spw_id is None:
        spw_id = source.spw_id
    if I is None:
        I = source.I
    if Q is None:
        Q = source.Q
    if U is None:
        U = source.U
    if V is None:
        V = source.V
    if spix is None:
        spix = source.spix

    new_fm = FluxMeasurement(spw_id, I, Q=Q, U=U, V=V, spix=spix, origin=ORIGIN)

    if uI is None:
        uI = source.uncertainty.I
    if uQ is None:
        uQ = source.uncertainty.Q
    if uU is None:
        uU = source.uncertainty.U
    if uV is None:
        uV = source.uncertainty.V
    new_fm.uncertainty = FluxMeasurement(spw_id, uI, Q=uQ, U=uU, V=uV, origin=ORIGIN)

    return new_fm
