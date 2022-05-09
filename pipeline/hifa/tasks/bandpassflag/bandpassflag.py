import collections
import functools

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag
from pipeline.hif.tasks import gaincal
from pipeline.hifa.tasks import bandpass
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from .resultobjects import BandpassflagResults
from ..bandpass.almaphcorbandpass import ALMAPhcorBandpassInputs

__all__ = [
    'BandpassflagInputs',
    'BandpassflagResults',
    'Bandpassflag'
]

LOG = infrastructure.get_logger(__name__)


class BandpassflagInputs(ALMAPhcorBandpassInputs):
    """
    BandpassflagInputs defines the inputs for the Bandpassflag pipeline task.
    """
    # Lower sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # catchNegativeOutliers['scalardiff']
    antblnegsig = vdp.VisDependentProperty(default=3.4)

    # Upper sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # flag_nsigma['scalardiff']
    antblpossig = vdp.VisDependentProperty(default=3.2)

    # Lower sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # relaxationSigma
    antnegsig = vdp.VisDependentProperty(default=4.0)

    # Upper sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # positiveSigmaAntennaBased
    antpossig = vdp.VisDependentProperty(default=4.6)

    # Maximum number of iterations to evaluate flagging heuristics.
    niter = vdp.VisDependentProperty(default=2)

    # Relaxed value to set the threshold scaling factor to under certain
    # conditions; equivalent to:
    # relaxationFactor
    relaxed_factor = vdp.VisDependentProperty(default=2.0)

    # Threshold for maximum fraction of timestamps that are allowed
    # to contain outliers; equivalent to:
    # checkForAntennaBasedBadIntegrations
    tmantint = vdp.VisDependentProperty(default=0.063)

    # Initial threshold for maximum fraction of "bad baselines" over "all
    # baselines" that an antenna may be a part of; equivalent to:
    # tooManyBaselinesFraction
    tmbl = vdp.VisDependentProperty(default=0.175)

    # Initial threshold for maximum fraction of "outlier timestamps" over
    # "total timestamps" that a baseline may be a part of; equivalent to:
    # tooManyIntegrationsFraction
    tmint = vdp.VisDependentProperty(default=0.085)

    def __init__(self, context, output_dir=None, vis=None, caltable=None, intent=None, field=None, spw=None,
                 antenna=None, hm_phaseup=None, phaseupsolint=None, phaseupbw=None, phaseupsnr=None, phaseupnsols=None,
                 hm_bandpass=None, solint=None, maxchannels=None, evenbpints=None, bpsnr=None, minbpsnr=None, bpnsols=None,
                 combine=None, refant=None, minblperant=None, minsnr=None, solnorm=None, antnegsig=None, antpossig=None,
                 tmantint=None, tmint=None, tmbl=None, antblnegsig=None, antblpossig=None, relaxed_factor=None,
                 niter=None, mode='channel'):
        super(BandpassflagInputs, self).__init__(
            context, output_dir=output_dir, vis=vis, caltable=caltable, intent=intent, field=field, spw=spw,
            antenna=antenna, hm_phaseup=hm_phaseup, phaseupsolint=phaseupsolint, phaseupbw=phaseupbw,
            phaseupsnr=phaseupsnr, phaseupnsols=phaseupnsols, hm_bandpass=hm_bandpass, solint=solint,
            maxchannels=maxchannels, evenbpints=evenbpints, bpsnr=bpsnr, minbpsnr=minbpsnr, bpnsols=bpnsols,
            combine=combine, refant=refant, minblperant=minblperant, minsnr=minsnr, solnorm=solnorm, mode=mode
        )

        # flagging parameters
        self.antnegsig = antnegsig
        self.antpossig = antpossig
        self.tmantint = tmantint
        self.tmint = tmint
        self.tmbl = tmbl
        self.antblnegsig = antblnegsig
        self.antblpossig = antblpossig
        self.relaxed_factor = relaxed_factor
        self.niter = niter

    def as_dict(self):
        # temporary workaround to hide uvrange from Input Parameters accordion
        d = super(BandpassflagInputs, self).as_dict()
        if 'uvrange' in d:
            del d['uvrange']
        return d


@task_registry.set_equivalent_casa_task('hifa_bandpassflag')
@task_registry.set_casa_commands_comment(
    'This task performs a preliminary bandpass solution and temporarily applies it, then calls hif_correctedampflag to'
    ' evaluate the flagging heuristics, looking for outlier visibility points by statistically examining the scalar'
    ' difference of the corrected amplitudes minus model amplitudes, and then flagging those outliers. The philosophy'
    ' is that only outlier data points that have remained outliers after calibration will be flagged. Note that the'
    ' phase of the data is not assessed.'
)
class Bandpassflag(basetask.StandardTaskTemplate):
    Inputs = BandpassflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results for current MS.
        result = BandpassflagResults(inputs.vis)

        # Create a shortcut to the plotting function that pre-supplies the inputs and context.
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-bandpassflag" flagging state')
        flag_backup_name_prebpf = 'before_bpflag'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name_prebpf)
        self._executor.execute(task)

        # Run a preliminary standard phaseup and bandpass calibration:
        # Create inputs for bandpass task.
        LOG.info('Creating preliminary phased-up bandpass calibration.')
        bpinputs = bandpass.ALMAPhcorBandpass.Inputs(
            context=inputs.context, vis=inputs.vis, caltable=inputs.caltable,
            field=inputs.field, intent=inputs.intent, spw=inputs.spw,
            antenna=inputs.antenna, hm_phaseup=inputs.hm_phaseup,
            phaseupbw=inputs.phaseupbw, phaseupsnr=inputs.phaseupsnr,
            phaseupnsols=inputs.phaseupnsols,
            phaseupsolint=inputs.phaseupsolint, hm_bandpass=inputs.hm_bandpass,
            solint=inputs.solint, maxchannels=inputs.maxchannels,
            evenbpints=inputs.evenbpints, bpsnr=inputs.bpsnr, minbpsnr=inputs.minbpsnr,
            bpnsols=inputs.bpnsols, combine=inputs.combine,
            refant=inputs.refant, solnorm=inputs.solnorm,
            minblperant=inputs.minblperant, minsnr=inputs.minsnr)
        # Create and execute bandpass task.
        bptask = bandpass.ALMAPhcorBandpass(bpinputs)
        bpresult = self._executor.execute(bptask)

        # Add the phase-up table produced by the bandpass task to the
        # callibrary in the local context.
        LOG.debug('Adding preliminary phase-up and bandpass tables to temporary context.')
        for prev_result in bpresult.preceding:
            for calapp in prev_result:
                inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        # Accept the bandpass result into the local context so as to add the
        # bandpass table to the callibrary.
        bpresult.accept(inputs.context)

        # Do amplitude solve on scan interval.
        LOG.info('Create preliminary amplitude gaincal table.')
        gacalinputs = gaincal.GTypeGaincal.Inputs(context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                                                  gaintype='T', antenna='', calmode='a', solint='inf')
        gacaltask = gaincal.GTypeGaincal(gacalinputs)
        gacalresult = self._executor.execute(gacaltask)

        # CAS-10491: for scan-based amplitude solves that will be applied
        # to the calibrator, set interp to 'nearest' => modify result from
        # gaincal to update interp before merging into the local context.
        self._mod_last_interp(gacalresult.pool[0], 'nearest,linear')
        self._mod_last_interp(gacalresult.final[0], 'nearest,linear')
        LOG.debug('Adding preliminary amplitude caltable to temporary context.')
        gacalresult.accept(inputs.context)

        # Ensure that any flagging applied to the MS by applycal are reverted
        # at the end, even in the case of exceptions.
        try:
            # Apply all caltables registered in the callibrary in the local
            # context to the MS.
            LOG.info('Applying pre-existing caltables and preliminary phase-up, bandpass, and amplitude caltables.')
            acinputs = applycal.IFApplycalInputs(context=inputs.context, vis=inputs.vis, field=inputs.field,
                                                 intent=inputs.intent, flagsum=False, flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            acresult = self._executor.execute(actask)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(acresult.callib_map)

            # Create "after calibration, before flagging" plots for the weblog.
            LOG.info('Creating "after calibration, before flagging" plots')
            result.plots['before'] = plot_fn(suffix='before')

            # Call Correctedampflag to find and flag amplitude outliers.
            LOG.info('Running correctedampflag to identify outliers to flag.')
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                field=inputs.field, spw=inputs.spw, antnegsig=inputs.antnegsig,
                antpossig=inputs.antpossig, tmantint=inputs.tmantint,
                tmint=inputs.tmint, tmbl=inputs.tmbl,
                antblnegsig=inputs.antblnegsig,
                antblpossig=inputs.antblpossig,
                relaxed_factor=inputs.relaxed_factor, niter=inputs.niter)
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # If flags were found, create the "after calibration, after
            # flagging" plots for the weblog.
            cafflags = cafresult.flagcmds()
            if cafflags:
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(suffix='after')

        finally:
            # Restore the "pre-bandpassflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-bandpassflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_prebpf)
            self._executor.execute(task)

        # Store flagging task result.
        result.cafresult = cafresult

        # If new outliers were identified...
        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(context=inputs.context, vis=inputs.vis, table=inputs.vis, inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            _ = self._executor.execute(fstask)

            # Check for need to update reference antennas, and apply to local
            # copy of the MS.
            result = self._identify_refants_to_update(result)
            ms = inputs.context.observing_run.get_ms(name=inputs.vis)
            ms.update_reference_antennas(ants_to_demote=result.refants_to_demote,
                                         ants_to_remove=result.refants_to_remove)

        return result

    def analyse(self, result):
        return result

    def _mod_last_interp(self, l, interp):
        l.calfrom[-1] = self._copy_with_interp(l.calfrom[-1], interp)

    @staticmethod
    def _copy_with_interp(old_calfrom, interp):
        return callibrary.CalFrom(gaintable=old_calfrom.gaintable,
                                  gainfield=old_calfrom.gainfield,
                                  interp=interp,
                                  spwmap=old_calfrom.spwmap,
                                  caltype=old_calfrom.caltype,
                                  calwt=old_calfrom.calwt)

    def _identify_refants_to_update(self, result):
        """Updates the Bandpassflag result with lists of "bad" and "poor"
        antennas, for reference antenna update.

        Identifies "bad" antennas as those that got flagged in all spws
        (entire timestamp) which are to be removed from the reference antenna
        list.

        Identifies "poor" antennas as those that got flagged in at least
        one spw, but not all, which are to be moved to the end of the reference
        antenna list.

        :param result: BandpassflagResults object
        :return: BandpassflagResults object
        """
        # Identify bad antennas to demote/remove from refant list.
        ants_to_demote, ants_to_remove = self._identify_bad_refants(result)

        # Update result to mark antennas for demotion/removal as refant.
        result = self._mark_antennas_for_refant_update(result, ants_to_demote, ants_to_remove)

        return result

    def _identify_bad_refants(self, result):
        # Get the MS object.
        ms = self.inputs.context.observing_run.get_ms(name=self.inputs.vis)

        # Get translation dictionary for antenna id to name.
        antenna_id_to_name = self._get_ant_id_to_name_dict(ms)

        # Identify antennas to demote as refant.
        ants_to_demote, ants_fully_flagged = self._identify_ants_to_demote(result, ms, antenna_id_to_name)

        # Identify antennas to remove as refant.
        ants_to_remove = self._identify_ants_to_remove(result, ms, ants_fully_flagged, antenna_id_to_name)

        return ants_to_demote, ants_to_remove

    @staticmethod
    def _identify_ants_to_demote(result, ms, antenna_id_to_name):
        # Retrieve flags and which intents and spws were evaluated by
        # correctedampflag.
        flags = result.cafresult.flagcmds()

        # Initialize flagging state
        ants_fully_flagged = collections.defaultdict(set)

        # Create a summary of the flagging state by going through each flagging
        # command.
        for flag in flags:
            # Only consider flagging commands with a specified antenna and
            # without a specified timestamp.
            if flag.antenna is not None and flag.time is None:
                # Skip flagging commands for baselines.
                if '&' in str(flag.antenna):
                    continue
                ants_fully_flagged[(flag.intent, flag.field, flag.spw)].update([flag.antenna])

        # For each combination of intent, field, and spw that were found to
        # have antennas flagged, raise a warning.
        sorted_keys = sorted(
            sorted(ants_fully_flagged, key=lambda keys: keys[2]),
            key=lambda keys: keys[0])
        for (intent, field, spwid) in sorted_keys:
            ants_flagged = ants_fully_flagged[(intent, field, spwid)]

            # Convert antenna IDs to names and create a string.
            ants_str = ", ".join(map(str, [antenna_id_to_name[iant] for iant in ants_flagged]))

            # Convert CASA intent from flagging command to pipeline intent.
            intent_str = utils.to_pipeline_intent(ms, intent)

            # Log a warning.
            LOG.warning(
                "{msname} - for intent {intent} (field "
                "{fieldname}) and spw {spw}, the following antennas "
                "are fully flagged: {ants}".format(
                    msname=ms.basename, intent=intent_str,
                    fieldname=field, spw=spwid,
                    ants=ants_str))

        # Store the set of antennas that were fully flagged in at least
        # one spw, for any of the fields for any of the intents.
        ants_to_demote_as_refant = {
            antenna_id_to_name[iant]
            for iants in ants_fully_flagged.values()
            for iant in iants}

        return ants_to_demote_as_refant, ants_fully_flagged

    @staticmethod
    def _identify_ants_to_remove(result, ms, ants_fully_flagged, antenna_id_to_name):
        # Get the intents and the set of unique spw ids from the inputs.
        intents = result.cafresult.inputs['intent'].split(',')
        spwids = set(map(int, result.cafresult.inputs['spw'].split(',')))

        # Initialize set of antennas that are fully flagged for all spws, for any intent
        ants_fully_flagged_in_all_spws_any_intent = set()

        # Check if any antennas were found to be fully flagged in all
        # spws, for any intent.

        # Identify the unique field and intent combinations for which fully flagged
        # antennas were found.
        intent_field_found = {key[0:2] for key in ants_fully_flagged}
        for (intent, field) in intent_field_found:

            # Identify the unique spws for which fully flagged antennas were found (for current
            # intent and field).
            spws_found = {key[2] for key in ants_fully_flagged if key[0:2] == (intent, field)}

            # Only proceed if the set of spws for which flagged antennas were found
            # matches the set of spws for which correctedampflag ran.
            if spws_found == spwids:
                # Select the fully flagged antennas for current intent and field.
                ants_fully_flagged_for_intent_field = [
                    ants_fully_flagged[key]
                    for key in ants_fully_flagged
                    if key[0:2] == (intent, field)
                ]

                # Identify which antennas are fully flagged in all spws, for
                # current intent and field, and store these for later warning
                # and/or updating of refant.
                ants_fully_flagged_in_all_spws_any_intent.update(
                    set.intersection(*ants_fully_flagged_for_intent_field))

        # For the antennas that were found to be fully flagged in all
        # spws for one or more fields belonging to one or more of the intents,
        # raise a warning.
        if ants_fully_flagged_in_all_spws_any_intent:
            # Convert antenna IDs to names and create a string.
            ants_str = ", ".join(
                map(str, [antenna_id_to_name[iant]
                          for iant in ants_fully_flagged_in_all_spws_any_intent]))

            # Log a warning.
            LOG.warning(
                '{0} - the following antennas are fully flagged in all spws '
                'for one or more fields with intents among '
                '{1}: {2}'.format(ms.basename, ', '.join(intents), ants_str))

        # The following will assess if/how the list of reference antennas
        # needs to be updated based on antennas that were found to be
        # fully flagged.

        # Store the set of antennas that are fully flagged for all spws
        # in any of the intents in the result as a list of antenna
        # names.
        ants_to_remove_as_refant = {
            antenna_id_to_name[iant]
            for iant in ants_fully_flagged_in_all_spws_any_intent}

        return ants_to_remove_as_refant

    def _mark_antennas_for_refant_update(self, result, ants_to_demote, ants_to_remove):
        # Get the intents from the inputs.
        intents = result.cafresult.inputs['intent'].split(',')

        # Get the MS object
        ms = self.inputs.context.observing_run.get_ms(name=self.inputs.vis)

        # If any reference antennas were found to be candidates for
        # removal or demotion (move to end of list), then proceed...
        if ants_to_remove or ants_to_demote:

            # If a list of reference antennas was registered with the MS..
            if (hasattr(ms, 'reference_antenna') and
                    isinstance(ms.reference_antenna, str)):

                # Create list of current refants
                refant = ms.reference_antenna.split(',')

                # Identify intersection between refants and fully flagged
                # and store in result.
                result.refants_to_remove = {
                    ant for ant in refant
                    if ant in ants_to_remove}

                # If any refants were found to be removed...
                if result.refants_to_remove:

                    # Create string for log message.
                    ant_msg = utils.commafy(result.refants_to_remove, quotes=False)

                    # Check if removal of refants would result in an empty refant list,
                    # in which case the refant update is skipped.
                    if result.refants_to_remove == set(refant):

                        # Log warning that refant list should have been updated, but
                        # will not be updated so as to avoid an empty refant list.
                        LOG.warning(
                            '{0} - the following reference antennas became fully flagged '
                            'in all spws for one or more fields with intents among {1}, '
                            'but are *NOT* removed from the refant list because doing so '
                            'would result in an empty refant list: '
                            '{2}'.format(ms.basename, ', '.join(intents), ant_msg))

                        # Reset the refant removal list in the result to be empty.
                        result.refants_to_remove = set()
                    else:
                        # Log a warning if any antennas are to be removed from
                        # the refant list.
                        LOG.warning(
                            '{0} - the following reference antennas are '
                            'removed from the refant list because they became '
                            'fully flagged in all spws for one of the intents '
                            'among {1}: {2}'.format(ms.basename, ', '.join(intents), ant_msg))

                # Identify intersection between refants and candidate
                # antennas to demote, skipping those that are to be
                # removed entirely, and store this list in the result.
                # These antennas should be moved to the end of the refant
                # list (demoted) upon merging the result into the context.
                result.refants_to_demote = {
                    ant for ant in refant
                    if ant in ants_to_demote
                    and ant not in result.refants_to_remove}

                # If any refants were found to be demoted...
                if result.refants_to_demote:

                    # Create string for log message.
                    ant_msg = utils.commafy(result.refants_to_demote, quotes=False)

                    # Check if the list of refants-to-demote comprises all
                    # refants, in which case the re-ordering of refants is
                    # skipped.
                    if result.refants_to_demote == set(refant):

                        # Log warning that refant list should have been updated, but
                        # will not be updated so as to avoid an empty refant list.
                        LOG.warning(
                            '{0} - the following antennas are fully flagged '
                            'for one or more spws, in one or more fields '
                            'with intents among {1}, but since these comprise all '
                            'refants, the refant list is *NOT* updated to '
                            're-order these to the end of the refant list: '
                            '{2}'.format(ms.basename, ', '.join(intents), ant_msg))

                        # Reset the refant demotion list in the result to be empty.
                        result.refants_to_demote = set()

            # If no list of reference antennas was registered with the MS,
            # raise a warning.
            else:
                LOG.warning(
                    '{0} - no reference antennas found in MS, cannot update '
                    'the reference antenna list.'.format(ms.basename))

        return result

    @staticmethod
    def _get_ant_id_to_name_dict(ms):
        """
        Return dictionary with antenna ID mapped to antenna name.
        If no unique antenna name can be assigned to each antenna ID,
        then return empty dictionary.

        :param ms: MeasurementSet
        :return: dictionary
        """
        # Create an antenna id-to-name translation dictionary.
        antenna_id_to_name = {ant.id: ant.name
                              for ant in ms.antennas
                              if ant.name.strip()}

        # Check that each antenna ID is represented by a unique non-empty
        # name, by testing that the unique set of antenna names is same
        # length as list of IDs. If not, then unset the translation
        # dictionary to revert back to flagging by ID.
        if len(set(antenna_id_to_name.values())) != len(ms.antennas):
            LOG.info('No unique name available for each antenna ID:'
                     ' flagging by antenna ID instead of by name.')
            antenna_id_to_name = {}

        return antenna_id_to_name

    @staticmethod
    def _copy_calfrom_with_gaintable(old_calfrom, gaintable):
        return callibrary.CalFrom(gaintable=gaintable,
                                  gainfield=old_calfrom.gainfield,
                                  interp=old_calfrom.interp,
                                  spwmap=old_calfrom.spwmap,
                                  caltype=old_calfrom.caltype,
                                  calwt=old_calfrom.calwt)


def create_plots(inputs, context, suffix=''):
    """
    Return amplitude vs time and amplitude vs UV distance plots for the given
    data column.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis, spw=inputs.spw)
    output_dir = context.output_dir

    amp_uvdist_plots = AmpVsXChart('uvdist', context, output_dir, calto, suffix=suffix).plot()
    amp_time_plots = AmpVsXChart('time', context, output_dir, calto, suffix=suffix).plot()

    return {'uvdist': amp_uvdist_plots, 'time': amp_time_plots}


class AmpVsXChart(applycal_displays.SpwSummaryChart):
    """
    Plotting class that creates an amplitude vs X plot for each spw, where X
    is given as a constructor argument.
    """
    def __init__(self, xaxis, context, output_dir, calto, **overrides):
        plot_args = {
            'ydatacolumn': 'corrected',
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'coloraxis': 'corr',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0]
        }
        plot_args.update(**overrides)

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp', intent='BANDPASS',
                                          **plot_args)
