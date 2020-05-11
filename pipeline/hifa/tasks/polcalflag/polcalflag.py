import collections
import functools

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag

LOG = infrastructure.get_logger(__name__)


class PolcalflagResults(basetask.Results):
    def __init__(self):
        super(PolcalflagResults, self).__init__()

        self.vis = None
        self.cafresult = None
        self.plots = dict()

        # list of antennas that should be moved to the end
        # of the refant list
        self.refants_to_demote = set()

        # list of entirely flagged antennas that should be removed from refants
        self.refants_to_remove = set()

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        # Update reference antennas for MS.
        ms = context.observing_run.get_ms(name=self.vis)
        ms.update_reference_antennas(ants_to_demote=self.refants_to_demote,
                                     ants_to_remove=self.refants_to_remove)

    def __repr__(self):
        return 'PolcalflagResults'


class PolcalflagInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_polcalflag')
@task_registry.set_casa_commands_comment('Flag polarization calibrator outliers.')
class Polcalflag(basetask.StandardTaskTemplate):
    Inputs = PolcalflagInputs

    def prepare(self):

        inputs = self.inputs

        # Initialize results.
        result = PolcalflagResults()

        # Store the vis in the result
        result.vis = inputs.vis

        # Create a shortcut to the plotting function that pre-supplies the inputs and context
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Check for any polarization intents
        eb_intents = inputs.context.observing_run.get_ms(inputs.vis).intents
        if 'POLARIZATION' not in eb_intents and 'POLANGLE' not in eb_intents and 'POLLEAKAGE' not in eb_intents:
            LOG.info('No polarization intents found.')
            return result

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-polcalflag" flagging state')
        flag_backup_name_prepcf = 'before_pcflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_prepcf)
        self._executor.execute(task)

        # Ensure that any flagging applied to the MS by this applycal is
        # reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags
            LOG.info('Applying pre-existing cal tables.')
            acinputs = applycal.IFApplycalInputs(
                context=inputs.context, vis=inputs.vis, intent='POLARIZATION,POLANGLE,POLLEAKAGE', flagsum=False,
                flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            acresult = self._executor.execute(actask, merge=True)

            # Create back-up of flags after applycal but before correctedampflag.
            LOG.info('Creating back-up of "after_pcflag_applycal" flagging state')
            flag_backup_name_after_pcflag_applycal = 'after_pcflag_applycal'
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='save',
                                          versionname=flag_backup_name_after_pcflag_applycal)
            self._executor.execute(task)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify polarization calibrator outliers to flag.')

            # This task is called by the framework for each EB in the vis list.

            # Call correctedampflag for the polarization calibrator intent.
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent='POLARIZATION,POLANGLE,POLLEAKAGE')
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # Store correctedampflag result
            result.cafresult = cafresult

            # Get new flag commands
            cafflags = cafresult.flagcmds()

            # If new outliers were identified...make "after flagging" plots that
            # include both applycal flags and correctedampflag flags
            if cafflags:
                # Make "after calibration, after flagging" plots for the weblog
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(suffix='after')

        finally:
            if cafflags:
                # Restore the "after_pcflag_applycal" backup of the flagging
                # state, so that the "before plots" only show things needing
                # to be flagged by correctedampflag
                LOG.info('Restoring back-up of "after_pcflag_applycal" flagging state.')
                task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore',
                                              versionname=flag_backup_name_after_pcflag_applycal)
                self._executor.execute(task)
                # Make "after calibration, before flagging" plots for the weblog
                LOG.info('Creating "after calibration, before flagging" plots')
                result.plots['before'] = plot_fn(suffix='before')

            # Restore the "pre-polcalflag" backup of the flagging state.
            LOG.info('Restoring back-up of "pre-polcalflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_prepcf)
            self._executor.execute(task)

            # If new outliers were identified...
            if cafflags:
                # Make "after calibration, before flagging" plots for the weblog
                LOG.info('Creating "after calibration, before flagging" plots')
                result.plots['before'] = plot_fn(suffix='before')

                # Re-apply the newly found flags from correctedampflag.
                LOG.info('Re-applying flags from correctedampflag.')
                fsinputs = FlagdataSetter.Inputs(
                    context=inputs.context, vis=inputs.vis, table=inputs.vis, inpfile=[])
                fstask = FlagdataSetter(fsinputs)
                fstask.flags_to_set(cafflags)
                fsresult = self._executor.execute(fstask)

                # Check for need to update reference antennas, and apply to local
                # copy of the MS.
                result = self._identify_refants_to_update(result)
                ms = inputs.context.observing_run.get_ms(name=inputs.vis)
                ms.update_reference_antennas(ants_to_demote=result.refants_to_demote,
                                             ants_to_remove=result.refants_to_remove)

        return result

    def analyse(self, results):
        return results

    def _identify_refants_to_update(self, result):
        """Updates the Polcalflag result with lists of "bad" and "poor"
        antennas, for reference antenna update.

        Identifies "bad" antennas as those that got flagged in all spws
        (entire timestamp) which are to be removed from the reference antenna
        list.

        Identifies "poor" antennas as those that got flagged in at least
        one spw, but not all, which are to be moved to the end of the reference
        antenna list.

        :param result: PolcalflagResults object
        :return: PolcalflagResults object
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
                    else:
                        # Log a warning if any antennas are to be demoted from
                        # the refant list.
                        LOG.warning(
                            '{0} - the following antennas are moved to the end '
                            'of the refant list because they are fully '
                            'flagged for one or more spws, in one or more '
                            'fields with intents among {1}: '
                            '{2}'.format(ms.basename, ', '.join(intents), ant_msg))

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


def create_plots(inputs, context, suffix=''):
    """
    Return amplitude vs time plots for the given data column.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis)
    output_dir = context.output_dir

    amp_time_plots = AmpVsXChart('time', context, output_dir, calto, suffix=suffix).plot()

    return {'time': amp_time_plots}


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

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp',
                                          intent='POLARIZATION,POLANGLE,POLLEAKAGE', **plot_args)
