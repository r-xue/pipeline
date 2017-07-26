from __future__ import absolute_import

import os

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure

from pipeline.hif.tasks.bandpass import bandpassworker
from pipeline.hif.tasks.bandpass  import bandpassmode
from pipeline.hif.tasks import gaincal

from ..bpsolint import bpsolint

LOG = infrastructure.get_logger(__name__)

class ALMAPhcorBandpassInputs(bandpassmode.BandpassModeInputs):

    # Phaseup heuristics, options are '', 'manual' and 'snr'
    hm_phaseup = basetask.property_with_default('hm_phaseup', 'snr')
    #    'manual' mode
    #phaseupbw     = basetask.property_with_default('phaseupbw', '500MHz')
    phaseupbw     = basetask.property_with_default('phaseupbw', '')
    #    'snr' mode
    phaseupsnr = basetask.property_with_default('phaseupsnr', 20.0)
    phaseupnsols = basetask.property_with_default('phaseupnsols', 2)
    #    'manual' and 'snr' mode
    phaseupsolint = basetask.property_with_default('phaseupsolint', 'int')

    # Bandpass heuristics, options are 'fixed', 'smoothed', and 'snr'
    hm_bandpass = basetask.property_with_default('hm_bandpass', 'snr')
    #    'fixed', 'smoothed', and 'snr' mode
    solint = basetask.property_with_default('solint', 'inf')
    #    'smoothed' mode
    maxchannels = basetask.property_with_default('maxchannels', 240)
    #    'snr' mode
    evenbpints = basetask.property_with_default('evenbpints', True)
    bpsnr = basetask.property_with_default('bpsnr', 50.0)
    bpnsols = basetask.property_with_default('bpnsols', 8)
    
    @basetask.log_equivalent_CASA_call
    def __init__(self, context, mode=None, hm_phaseup=None,
                 phaseupbw=None, phaseupsolint=None,
                 phaseupsnr=None, phaseupnsols=None,
                 hm_bandpass=None, solint=None, maxchannels=None,
                 evenbpints=None, bpsnr=None, bpnsols=None,
                 **parameters):
        super(ALMAPhcorBandpassInputs, self).__init__(context, mode='channel',
            hm_phaseup=hm_phaseup, phaseupbw=phaseupbw,
            phaseupsolint=phaseupsolint, phaseupsnr=phaseupsnr, phaseupnsols=phaseupnsols,
            hm_bandpass=hm_bandpass, solint=solint, maxchannels=maxchannels,
            evenbpints=evenbpints, bpsnr=bpsnr, bpnsols=bpnsols,
            **parameters)


class ALMAPhcorBandpass(bandpassworker.BandpassWorker):
    Inputs = ALMAPhcorBandpassInputs

    def prepare(self, **parameters):
        inputs = self.inputs
        
        # Call the SNR estimater if either
        #     hm_phaseup='snr'
        # or
        #     hm_bandpass='snr'
        if inputs.hm_phaseup =='snr' or \
            inputs.hm_bandpass == 'snr':
            snr_result = self._compute_bpsolints()
        else:
            snr_result = None

        # If requested, execute a phaseup job. This will add the resulting
        # caltable to the on-the-fly calibration context, so we don't need any
        # subsequent gaintable manipulation
        if inputs.hm_phaseup != '': 
            # Determine the single best phaseup solution interval for the set of
            # science spws. The heuristic assumes that the inegration times
            # are the same
            if inputs.hm_phaseup == 'snr':
                if len(snr_result.spwids) <= 0:
                    LOG.warning('SNR based phaseup solint estimates are unavailable for MS %s' % (inputs.ms.basename))
                    phaseupsolint = inputs.phaseupsolint
                else:
                    phaseupsolint = self._get_best_phaseup_solint(snr_result)
            else:
                phaseupsolint = inputs.phaseupsolint
            phaseup_result = self._do_phaseup(phaseupsolint=phaseupsolint)

        # Now perform the bandpass
        if inputs.hm_bandpass == 'snr':
            if len(snr_result.spwids) <= 0:
                LOG.warning('SNR based bandpass solint estimates are unavailable for MS %s' % (inputs.ms.basename))
            else:
                LOG.info('Using SNR based solint estimates')
            result = self._do_snr_bandpass(snr_result)
        elif inputs.hm_bandpass == 'smoothed':
            LOG.info('Using simple bandpass smoothing solint estimates')
            result = self._do_smoothed_bandpass()
        else:
            LOG.info('Using fixed solint estimates')
            result = self._do_bandpass()
        
        # Attach the preparatory result to the final result so we have a
        # complete log of all the executed tasks. 
        if inputs.hm_phaseup != '': 
            result.preceding.append(phaseup_result.final)

        return result

    # Compute the solints required to match the SNR
    def _compute_bpsolints(self):
        inputs = self.inputs

        # Note currently the phaseup bandwidth is not supported
        bpsolint_inputs = bpsolint.BpSolint.Inputs (inputs.context,
            vis            = inputs.vis,
            field          = inputs.field,
            intent         = inputs.intent,
            spw            = inputs.spw,
            phaseupsnr     = inputs.phaseupsnr,
            minphaseupints = inputs.phaseupnsols,
            evenbpints     = inputs.evenbpints,
            bpsnr          = inputs.bpsnr,
            minbpnchan    = inputs.bpnsols
        )

        bpsolint_task = bpsolint.BpSolint(bpsolint_inputs)
        return self._executor.execute(bpsolint_task)

    # Return the best snr estimate for the phaseup solint.
    # Revert to the default of a good estimate is not available.
    def _get_best_phaseup_solint(self, snr_result):
        inputs = self.inputs

        # Number of expected results.
        nexpected = len(snr_result.spwids)
        quanta = casatools.quanta

        # Look for missing and bad solutions.
        #    Adjust the estimates for poor solutions to the
        #    best acceptable value
        #    Cap the result to the nearest even multiple of the integration time
        #    below 60s
        nmissing = 0
        tmpsolints = []
        max_solint = '60s'
        for i in range(len(snr_result.spwids)):

            # No solution available
            if not snr_result.phsolints[i]:
                nmissing = nmissing + 1
                LOG.warning('No phaseup solint estimate for spw %s in MS %s' % (snr_result.spwids[i], inputs.ms.basename))
                continue

            # An MS might have multiple scans for the phase-up intent.
            # Collect the integration intervals for each scan with
            # this intent. We expect this to return one integration
            # interval
            scans = inputs.ms.get_scans(scan_intent=inputs.intent)
            solints = set([scan.mean_interval(snr_result.spwids[i]) for scan in scans if snr_result.spwids[i] in [spw.id for spw in scan.spws]]) 

            timedelta = solints.pop()
            timedelta_solint = '%ss' % timedelta.total_seconds() 

            old_solint = snr_result.phsolints[i]

            if snr_result.nphsolutions[i] < inputs.phaseupnsols:
                LOG.warning('Phaseup solution for spw %s has only %d points in MS %s' % (snr_result.spwids[i], snr_result.nphsolutions[i], inputs.ms.basename))
                factor = float(max(1, snr_result.nphsolutions[i])) / inputs.phaseupnsols

                if old_solint == 'int':
                    # We expect an MS to have the same integration interval
                    # for each spw in the phase-up scan(s). We can't rely on 
                    # this though, so we need to check this assumption on each
                    # access 
                    
                    # But if multiple solint were were used for scans with the
                    # same intent, bail out again
                    if len(solints) != 1:
                        LOG.warning('Expected 1 solution interval for %s scans for spw %s. Got %s' % (inputs.intent, snr_result.spwids[i], solints))
                        tmpsolints.append(old_solint)
                        continue

                    # OK. We got one solution interval for all scans of the 
                    # desired intent. Overwrite old_solint with the equivalent
                    # time for 'int' in seconds. This wil be carried forward
                    # to the quanta tool calls.
                    # note the extra s to denote units, otherwise the quanta
                    # tool can't make a comparison 
                    old_solint = timedelta_solint
                    
                newsolint = quanta.tos(quanta.mul(quanta.quantity(old_solint), quanta.quantity(factor)), 3)
                LOG.warning('Rounding estimated phaseup solint for spw %s from %s to %s in MS %s' % (snr_result.spwids[i], snr_result.phsolints[i], newsolint, inputs.ms.basename))
                if quanta.gt (quanta.quantity(newsolint),  quanta.quantity(max_solint)):
                    best_solint = _constrain_phaseupsolint (newsolint, timedelta_solint, max_solint)
                    LOG.warning('Solution interval for spw %s greater than %s adjusting to %s' % (snr_result.spwids[i], max_solint, best_solint))
                else:
                    best_solint = newsolint
                tmpsolints.append(best_solint)
            else:
                if old_solint == 'int':
                    newsolint = timedelta_solint
                else:
                    newsolint = old_solint
                if quanta.gt (quanta.quantity(newsolint),  quanta.quantity(max_solint)):
                    best_solint = _constrain_phaseupsolint (newsolint, timedelta_solint, max_solint)
                    LOG.warning('Solution interval for spw %s greater than %s adjusting to %s' % (snr_result.spwids[i], max_solint, best_solint))
                else:
                    best_solint = snr_result.phsolints[i]
                tmpsolints.append(best_solint)

        # If all values are missing return default value.
        if nmissing >= nexpected:
            LOG.warning('Reverting to phaseup solint default %s for MS %s' % (inputs.phaseupsolint, inputs.ms.basename))
            return inputs.phaseupsolint

        # If phaseup solints are all the same return the first one
        if len(set(tmpsolints)) is 1:
            LOG.info("Best phaseup solint estimate is '%s'" % tmpsolints[0])
            return tmpsolints[0]

        # Find spws with the minimum number of phaseup solutions and
        # return the first phaseup solint
        best_solint = '0.0s'
        for i in range(len(tmpsolints)):
            # Test for pre-existing 'int' times
            if tmpsolints[i] == 'int':
                continue
            if quanta.gt(tmpsolints[i], best_solint):
                best_solint = tmpsolints[i]
        if best_solint == '0.0s':
            best_solint = 'int'
            
        LOG.info("Best phaseup solint estimate is '%s'" % best_solint)
        return best_solint

    # Compute the phaseup solution.
    def _do_phaseup(self, phaseupsolint='int'):
        inputs = self.inputs

        phaseup_inputs = gaincal.GTypeGaincal.Inputs(inputs.context,
            vis         = inputs.vis,
            field       = inputs.field,
            spw         = self._get_phaseup_spw(),
            antenna     = inputs.antenna,
            intent      = inputs.intent,
            #solint     = inputs.phaseupsolint,
            solint      = phaseupsolint,
            refant      = inputs.refant,
            minblperant = inputs.minblperant,
            calmode     = 'p',
            minsnr      = inputs.minsnr)

        phaseup_task = gaincal.GTypeGaincal(phaseup_inputs)
        #result = self._executor.execute(phaseup_task, merge=True)
        result = self._executor.execute(phaseup_task, merge=False)
	if not result.final:
            LOG.warning('No bandpass phaseup solution for %s' % (inputs.ms.basename))
	else:
	    result.accept(inputs.context)
	return result

    # Compute a standard bandpass
    def _do_bandpass(self):
        bandpass_task = bandpassmode.BandpassMode(self.inputs)
        return self._executor.execute(bandpass_task)

    # Compute the smoothed bandpass
    def _do_smoothed_bandpass(self):
        inputs = self.inputs

        # Store original values of some parameters.
        orig_spw = inputs.spw
        orig_solint = inputs.solint
        orig_append = inputs.append

        try:
            # initialize the caltable and list of spws
            inputs.caltable = inputs.caltable
            spwlist = inputs.ms.get_spectral_windows(orig_spw)

            # will hold the CalAppOrigins that record how each CalApp was 
            # generate. Ideally this would be a list on the CalApp itself,
            # but we don't have time to do that right now.
            calapp_origins = []
            
            # Loop through the spw appending the results of each spw
            # to the results of the previous one.
            for spw in spwlist:

                # TDM or FDM
                dd = inputs.ms.get_data_description(spw=spw)
                if dd is None:
                    LOG.debug('Missing data description for spw %s' % spw.id)
                    continue
                ncorr = len(dd.corr_axis)
                if ncorr not in [1,2,4]:
                    LOG.debug('Wrong number of correlations %s for spw %s' % \
                              (ncorr, spw.id))
                    continue

                # Smooth if FDM and if it makes sense
                if (ncorr * spw.num_channels > 256):
                    if (spw.num_channels / inputs.maxchannels) < 1:
                        inputs.solint = orig_solint
                    else:
                        bandwidth = spw.bandwidth.to_units( \
                            otherUnits=measures.FrequencyUnits.MEGAHERTZ)
                        inputs.solint = orig_solint + ',' + \
                            str(bandwidth / inputs.maxchannels) + 'MHz'
                else:
                    inputs.solint = orig_solint

                # Compute and append bandpass solution
                inputs.spw = spw.id
                bandpass_task = bandpassmode.BandpassMode(inputs)
                result = self._executor.execute(bandpass_task)
                if os.path.exists(self.inputs.caltable):
                    self.inputs.append = True
                    self.inputs.caltable = result.final[-1].gaintable                
                    calapp_origins.append(result.final[-1].origin)

            # Reset the calto spw list
            result.pool[0].calto.spw = orig_spw
	    if result.final:
                result.final[0].calto.spw = orig_spw
                result.final[0].origin = calapp_origins
            
            return result

        finally:
            inputs.spw = orig_spw
            inputs.solint = orig_solint
            inputs.append = orig_append

    # Compute the bandpass using SNR estimates
    def _do_snr_bandpass(self, snr_result):
        inputs = self.inputs
        quanta = casatools.quanta

        # Store original values of some parameters.
        orig_spw = inputs.spw
        orig_solint = inputs.solint
        orig_append = inputs.append

        try:
            # initialize the caltable and list of spws
            inputs.caltable = inputs.caltable
            spwlist = inputs.ms.get_spectral_windows(orig_spw)

            # will hold the CalAppOrigins that record how each CalApp was 
            # generate. Ideally this would be a list on the CalApp itself,
            # but we don't have time to do that right now.
            calapp_origins = []

            for spw in spwlist:

                # TDM or FDM
                dd = inputs.ms.get_data_description(spw=spw)
                if dd is None:
                    LOG.debug('Missing data description for spw %s' % spw.id)
                    continue
                ncorr = len(dd.corr_axis)
                if ncorr not in [1,2,4]:
                    LOG.debug('Wrong number of correlations %s for spw %s' % \
                              (ncorr, spw.id))
                    continue

                # Find the best solint for that window
                try:
                    solindex = snr_result.spwids.index(spw.id)
                    # Default to original 1 / 240 smoothing algorithm
                    #if snr_result.nbpsolutions[solindex] < inputs.bpnsols:
                        #solindex = -1
                except:
                    solindex = -1

                # Use the optimal solution if it is good enough otherwise
                # revert to the default smoothing algorithm
                if solindex >= 0:

                    if snr_result.nbpsolutions[solindex] < inputs.bpnsols:
                        # Recompute the solution interval to force the minimum
                        # number of solution channels
                        factor = float(max (1, snr_result.nbpsolutions[solindex])) / inputs.bpnsols
                        newsolint =  quanta.tos(quanta.mul (snr_result.bpsolints[solindex], factor))
                        LOG.warning('Too few channels: Changing recommended bandpass solint from %s to %s for spw %s' % \
                             (snr_result.bpsolints[solindex], newsolint, spw.id))
                        inputs.solint = orig_solint + ',' +  newsolint
                    else:
                        inputs.solint = orig_solint + ',' +  \
                            snr_result.bpsolints[solindex]
                    LOG.info('Setting bandpass solint to %s for spw %s' % (inputs.solint, spw.id))

                elif (ncorr * spw.num_channels > 256):
                    
                    if (spw.num_channels / inputs.maxchannels) < 1:
                        inputs.solint = orig_solint
                    else:
                        bandwidth = spw.bandwidth.to_units( \
                            otherUnits=measures.FrequencyUnits.MEGAHERTZ)
                        inputs.solint=orig_solint + ',' + \
                            str(bandwidth / inputs.maxchannels) + 'MHz'
                else:
                    inputs.solint = orig_solint
                    LOG.warning("Reverting to default bandpass solint %s for spw %s in MS %s" % (inputs.solint, spw.id, inputs.ms.basename))

                # Compute and append bandpass solution
                inputs.spw = spw.id
                bandpass_task = bandpassmode.BandpassMode(inputs)
                result = self._executor.execute(bandpass_task)
                if os.path.exists(self.inputs.caltable):
                    self.inputs.append = True
                    self.inputs.caltable = result.final[-1].gaintable              
                    calapp_origins.append(result.final[-1].origin)
                    
            # Reset the calto spw list
            result.pool[0].calto.spw = orig_spw
	    if result.final:
                result.final[0].calto.spw = orig_spw
                result.final[0].origin = calapp_origins

            return result

        finally:
            inputs.spw = orig_spw
            inputs.solint = orig_solint
            inputs.append = orig_append

    # Compute spws using bandwidth parameters
    def _get_phaseup_spw(self):
        '''
                   ms -- measurement set object 
               spwstr -- comma delimited list of spw ids
            bandwidth -- bandwidth in Hz of central channels used to
                         phaseup
        '''
        inputs = self.inputs

        # Add the channel ranges in. Note that this currently assumes no prior
        # channel selection.
        if inputs.phaseupbw == '':
            return inputs.spw

        # Convert bandwidth input to CASA quantity and then on to pipeline
        # domain Frequency object
        quanta = casatools.quanta
        bw_quantity = quanta.convert(quanta.quantity(inputs.phaseupbw), 'Hz')        
        bandwidth = measures.Frequency(quanta.getvalue(bw_quantity)[0], 
                                       measures.FrequencyUnits.HERTZ)
    
        # Loop over the spws creating a new list with channel ranges
        outspw = []
        for spw in self.inputs.ms.get_spectral_windows(self.inputs.spw):
            cen_freq = spw.centre_frequency 
            lo_freq = cen_freq - bandwidth / 2.0  
            hi_freq = cen_freq + bandwidth / 2.0
            minchan, maxchan = spw.channel_range(lo_freq, hi_freq)
            cmd = '{0}:{1}~{2}'.format(spw.id, minchan, maxchan)
            outspw.append(cmd)
    
        return ','.join(outspw)


# Constrain the solint to be the largest even number of integrations which
# are greater than a specified maximum. The inputs and outputs are times in
# seconds in string quanta format '60.0s'

def _constrain_phaseupsolint (input_solint, integration_time, max_solint):

    quanta = casatools.quanta

    input_solint_q = quanta.quantity(input_solint)
    integration_time_q = quanta.quantity (integration_time)
    max_solint_q = quanta.quantity(max_solint)

    newsolint_q = input_solint_q
    while quanta.gt(newsolint_q, max_solint_q):
         newsolint_q = quanta.sub(newsolint_q, integration_time_q)

    return quanta.tos(newsolint_q, 3)
