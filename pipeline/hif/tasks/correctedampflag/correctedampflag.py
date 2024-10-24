import collections
import copy
import os
from statistics import mode
from typing import Dict, List, Optional, Tuple

import numpy as np
from numpy.typing import NDArray

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.domain.measurementset import MeasurementSet
from pipeline.h.tasks.common import commonhelpermethods, mstools
from pipeline.h.tasks.common.arrayflaggerbase import FlagCmd
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.infrastructure import task_registry
from .resultobjects import CorrectedampflagResults

LOG = infrastructure.get_logger(__name__)


def _consolidate_flags(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags").
    """
    # Consolidate by polarisation.
    flags = _consolidate_flags_with_same_pol(flags)

    # Consolidate by timestamps.
    flags = _consolidate_flags_by_timestamps(flags)

    # Consolidate by antennas.
    flags = _consolidate_flags_by_antennas(flags)

    # Consolidate duplicate flags.
    flags = _consolidate_duplicate_flags(flags)

    return flags


def _consolidate_flags_with_same_pol(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags") by removing
    flags that differ only in polarisation.

    This function belongs to correctedampflag, by making assumptions on which
    properties of the FlagCmd it needs to compare.
    """
    # Get flag commands.
    flagcmds = [flag.flagcmd for flag in flags]

    # If all flag commands are unique, then there is nothing to
    # consolidate.
    if len(flagcmds) == len(set(flagcmds)):
        cflags = flags

    # If duplicate flag commands exist, go through each one, verify that
    # the duplication is just due to difference in polarisation, and for
    # those where this is true, replace them with a single flag.
    else:
        # Identify the flags that have non-unique flagging commands:
        uval, uind, ucnt = np.unique(flagcmds, return_inverse=True, return_counts=True)

        # Build new list of flags.
        cflags = []
        for ind, cnt in enumerate(ucnt):
            # For flags that appear twice...
            if cnt == 2:
                # Identify which flags these were.
                flag1, flag2 = [flags[i]
                                for i, val in enumerate(uind)
                                if val == ind]
                # Check that the flag commands differ only in polarisation
                if (flag1.filename == flag2.filename and
                        flag1.spw == flag2.spw and
                        flag1.antenna == flag2.antenna and
                        flag1.intent == flag2.intent and
                        flag1.time == flag2.time and
                        flag1.field == flag2.field and
                        flag1.reason == flag2.reason and
                        flag1.pol != flag2.pol):

                    # Copy across just first flag, but set its polarisation
                    # to empty.
                    flag1.pol = ''
                    cflags.append(flag1)
                    LOG.debug('Consolidated 2 duplicate flags that '
                              'differed only in polarisation.')
                # If they differed in a non-anticipated manner, copy across
                # both.
                else:
                    cflags.extend([flag1, flag2])
                    LOG.debug('Unable to consolidate 2 flags with same flag '
                              'command, appear to differ in unanticipated '
                              'manner.')

            # If flags do not appear twice, they either appear once
            # (commonly expected) or more than twice (never expected).
            # Either way, don't attempt any consolidation for these cases
            # and just copy them to the output array.
            else:
                cflags.extend([flags[i]
                               for i, val in enumerate(uind)
                               if val == ind])
                if cnt > 2:
                    # If the flag appeared more than twice, something must
                    # have gone wrong, insofar that flags differed by a
                    # metric that is not the polarisation but that was not
                    # included in flagging command. This should not happen,
                    # but log it as a "trace" message for potential
                    # debugging.
                    LOG.debug('Unable to consolidate 3+ flags with same flag '
                              'command, unanticipated case.')

    return cflags


def _consolidate_flags_by_timestamps(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags") by removing
    flags with timestamps if for the same (spw, field, intent), the antenna
    and/or baseline is covered by a flagging command without timestamp
    (covering same baseline, or an antenna from baseline, or all antennas).

    This function belongs to correctedampflag, by making assumptions on which
    properties of the FlagCmd it needs to compare.
    """

    # Identify list of properties of flag commands without timestamps.
    flags_without_timestamps = collections.defaultdict(list)
    for flag in flags:
        if flag.time is None:
            flags_without_timestamps[(flag.filename, flag.spw, flag.intent, flag.field)].append(flag.antenna)

    # If flag commands without timestamp exist, go through every flag
    # command and remove the ones that are cover the same
    # (filename, intent, field, spw, antenna), ignoring differences in
    # reason and polarisation.
    if flags_without_timestamps:

        # Build new list of flags, and preserve skipped flag commands to
        # report.
        cflags = []
        skipped_flagcmds = []

        # Go through list of flags.
        for flag in flags:

            # If the current flag matches in properties with one of the
            # flagging commands without timestamps:
            idx = (flag.filename, flag.spw, flag.intent, flag.field)
            if idx in flags_without_timestamps:

                # If flag for current file, spw, intent, and field is
                # time-stamp specific, check if any of matching
                # no-timestamp flags cover an antenna that also appears in
                # the current flag command (same antenna, same baseline,
                # and/or an antenna in current baseline).
                if flag.time is not None:
                    # Skip if antenna or baseline is covered by no-timestamp flags.
                    if flag.antenna in flags_without_timestamps[idx]:
                        skipped_flagcmds.append(flag.flagcmd)
                    # Skip if any ant in baseline is covered by no-timestamp flags.
                    elif any(str(ant) in str(flag.antenna).split('&') for ant in flags_without_timestamps[idx]):
                        skipped_flagcmds.append(flag.flagcmd)
                    else:
                        cflags.append(flag)
                else:
                    cflags.append(flag)

            # If the current flag does not match with any of the flagging
            # commands without timestamps, then preserve it.
            else:
                cflags.append(flag)

        # Log a summary of consolidated flag commands:
        if len(skipped_flagcmds) > 0:
            LOG.debug('The following {} time-specific flag commands were '
                      'consolidated into one or more flag commands '
                      'without a timestamp:\n'
                      '{}'.format(len(skipped_flagcmds), '\n'.join(skipped_flagcmds)))

    # If no flag commands with timestamp exist, then there is nothing to
    # consolidate => return flags unmodified.
    else:
        cflags = flags

    return cflags


def _consolidate_flags_by_antennas(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags")
    by antennas.

    This function belongs to correctedampflag, by making assumptions on which
    properties of the FlagCmd it needs to compare.
    """
    # Consolidate flags matching a non-antenna specific flag.
    flags = _consolidate_flags_non_antenna_specific(flags)

    # Consolidate flags with baselines matching a flag for one of
    # the baseline ants.
    flags = _consolidate_flags_for_ant_in_baselines(flags)

    return flags


def _consolidate_flags_non_antenna_specific(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags") by removing
    flags with antennas/baselines if the same (timestamp, spw, field,
    intent) is covered by a flagging command without antennas (i.e. all
    antennas).

    This function belongs to correctedampflag, by making assumptions on which
    properties of the FlagCmd it needs to compare.
    """
    # Identify list of properties of flag commands without antenna.
    flags_without_ant = collections.defaultdict(list)
    for flag in flags:
        if flag.antenna is None:
            flags_without_ant[(flag.filename, flag.spw, flag.intent, flag.field)].append(flag.time)

    # If flag commands without antenna exist, go through every flag
    # command and remove the ones that are cover the same
    # (filename, intent, field, spw, timestamp), ignoring differences in
    # reason and polarisation.
    if flags_without_ant:

        # Build new list of flags, and preserve skipped flag commands to
        # report.
        cflags = []
        skipped_flagcmds = []

        # Go through list of flags.
        for flag in flags:

            # If the current flag matches in properties with one of the
            # flagging commands without antennas:
            idx = (flag.filename, flag.spw, flag.intent, flag.field)
            if idx in flags_without_ant:

                # If the current flag is antenna-specific but its timestamp
                # is covered by one of the no-antenna flags, then skip this
                # flag; otherwise preserve it.
                if flag.antenna is not None and flag.time in flags_without_ant[idx]:
                    skipped_flagcmds.append(flag.flagcmd)
                else:
                    cflags.append(flag)

                # If the current flag does not match with any of the flagging
                # commands without antennas, then preserve it.
            else:
                cflags.append(flag)

        if len(skipped_flagcmds) > 0:
            LOG.debug('The following {} antenna-specific flag commands were '
                      'consolidated into one or more flag commands '
                      'without an antenna:\n'
                      '{}'.format(len(skipped_flagcmds), '\n'.join(skipped_flagcmds)))

    # If no flag commands with timestamp exist, then there is nothing to
    # consolidate => return flags unmodified.
    else:
        cflags = flags

    return cflags


def _consolidate_flags_for_ant_in_baselines(flags: List[FlagCmd]) -> List[FlagCmd]:
    """Function to consolidate a list of FlagCmd objects ("flags") by removing
    flags with baselines if the same (timestamp, spw, field, intent) is
    covered by a flagging command for one of the antennas in the baseline.

    This function belongs to correctedampflag, by making assumptions on which
    properties of the FlagCmd it needs to compare.
    """
    # Identify list of properties of flag commands with a single antenna.
    flags_for_single_ant = collections.defaultdict(list)
    for flag in flags:
        if flag.antenna is not None and '&' not in str(flag.antenna):
            flags_for_single_ant[(flag.filename, flag.spw, flag.intent, flag.field, flag.time)].append(
                str(flag.antenna))

    # If flag commands for a single antenna exist, go through every flag
    # command and remove the ones that are cover the same
    # (filename, intent, field, spw, timestamp) for which their baseline
    # includes an antenna covered by a single antenna (ignoring differences in
    # reason and polarisation).
    if flags_for_single_ant:

        # Build new list of flags, and preserve skipped flag commands to
        # report.
        cflags = []
        skipped_flagcmds = []

        # Go through list of flags.
        for flag in flags:

            # If the current flag matches in properties with one of the
            # flagging commands without antennas:
            idx = (flag.filename, flag.spw, flag.intent, flag.field, flag.time)
            if (idx in flags_for_single_ant and
                    str(flag.antenna) not in flags_for_single_ant[idx] and
                    any(ant in str(flag.antenna).split('&') for ant in flags_for_single_ant[idx])):
                skipped_flagcmds.append(flag.flagcmd)
            else:
                cflags.append(flag)

        if len(skipped_flagcmds) > 0:
            LOG.debug('The following {} baseline-specific flag commands were '
                      'consolidated into one or more flag commands '
                      'covering one of its antennas:\n'
                      '{}'.format(len(skipped_flagcmds), '\n'.join(skipped_flagcmds)))

    # If no flag commands with timestamp exist, then there is nothing to
    # consolidate => return flags unmodified.
    else:
        cflags = flags

    return cflags


def _consolidate_duplicate_flags(flags: List[FlagCmd]) -> List[FlagCmd]:
    """
    Function to consolidate a list of FlagCmd objects ("flags") by removing
    duplicate flags that result in the same flagging command.
    """
    # Build new list of flags, and preserve skipped flag commands to
    # report.
    cflags = []
    keep_flagcmds = []
    skipped_flagcmds = []

    for flag in flags:
        if flag.flagcmd not in keep_flagcmds:
            cflags.append(flag)
            keep_flagcmds.append(flag.flagcmd)
        else:
            skipped_flagcmds.append(flag.flagcmd)

    if len(skipped_flagcmds) > 0:
        LOG.debug('The following {} flag commands were consolidated as duplicates.'
                  '\n:{}'.format(len(skipped_flagcmds), '\n'.join(skipped_flagcmds)))

    return cflags


def _propagate_phase_flags(flags: List[FlagCmd], ms: MeasurementSet, antenna_id_to_name: Dict) -> List[FlagCmd]:
    """
    Function to take a list of FlagCmd objects ("flags") and propagate
    flags with reason = 'bad baseline' and intent = PHASE to intents
    TARGET and CHECK.
    """
    # Intents to propagate to.
    intents_propto = ["TARGET", "CHECK"]

    # Check for presence of intents in current MS, and if valid intent,
    # retrieve corresponding fields from MS.
    valid_intents = []
    valid_intents_fields = {}
    for intent in intents_propto:
        casa_intent = utils.to_CASA_intent(ms, intent)
        if casa_intent:
            valid_intents.append(casa_intent)
            fields = {field.name
                      for field in ms.get_fields(intent=intent)}
            valid_intents_fields[casa_intent] = ','.join(fields)

    # Proceed if there are valid intents to propagate to.
    propagated_flags = []
    nr_propagated_flags = 0
    if valid_intents:

        # Go through each flag, looking for 'bad baseline' reason and
        # "PHASE" intent...
        for flag in flags:
            if (flag.reason == 'bad baseline'
                    and flag.intent == utils.to_CASA_intent(ms, "PHASE")):

                nr_propagated_flags += 1

                # If a match was found, propagate to each of the valid
                # intents.
                for intent in valid_intents:
                    propagated_flags.append(
                        FlagCmd(
                            filename=flag.filename,
                            spw=flag.spw,
                            antenna=flag.antenna,
                            intent=intent,
                            pol=flag.pol,
                            field=valid_intents_fields[intent],
                            reason='bad baseline propagated from PHASE',
                            antenna_id_to_name=antenna_id_to_name))

    if propagated_flags:
        LOG.info('Propagated {} flagging command(s) with reason '
                 '\"bad baseline\" from PHASE intent to TARGET '
                 'and CHECK intent (where present).'.format(nr_propagated_flags))
        flags.extend(propagated_flags)

    return flags


class CorrectedampflagInputs(vdp.StandardInputs):
    """
    CorrectedampflagInputs defines the inputs for the Correctedampflag pipeline task.
    """
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
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

    @vdp.VisDependentProperty
    def field(self):
        # By default, return the fields corresponding to the input
        # intents.
        fieldids = [field.name
                    for field in self.ms.get_fields(intent=self.intent)]
        return ','.join(fieldids)

    intent = vdp.VisDependentProperty(default='BANDPASS')

    niter = vdp.VisDependentProperty(default=2)

    # Relaxed value to set the threshold scaling factor to under certain
    # conditions; equivalent to:
    # relaxationFactor
    relaxed_factor = vdp.VisDependentProperty(default=2.0)

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(
            science_windows_only=True)
        return ','.join([str(spw.id) for spw in science_spws])

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

    def __init__(self, context, output_dir=None, vis=None, intent=None, field=None, spw=None, antnegsig=None,
                 antpossig=None, tmantint=None, tmint=None, tmbl=None, antblnegsig=None, antblpossig=None,
                 relaxed_factor=None, niter=None):
        super(CorrectedampflagInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.field = field
        self.intent = intent
        self.spw = spw

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


@task_registry.set_equivalent_casa_task('hif_correctedampflag')
@task_registry.set_casa_commands_comment(
    'This task identifies, for one or more specified calibrator source intents, baselines and antennas with a '
    'significant fraction of outlier integrations, by statistically examining the scalar difference of the corrected '
    'amplitudes minus model amplitudes.'
)
class Correctedampflag(basetask.StandardTaskTemplate):
    Inputs = CorrectedampflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results.
        result = CorrectedampflagResults()

        # Store the vis in the result
        result.vis = inputs.vis

        # Get the MS object.
        ms = inputs.context.observing_run.get_ms(name=inputs.vis)

        # Get translation dictionary for antenna id to name.
        antenna_id_to_name = self._get_ant_id_to_name_dict(ms)

        # Initialize list of all newly found flags, and before/after flagging
        # summaries.
        allflags = []
        stats_before, stats_after = {}, {}

        # Start iterative flagging.
        counter = 1
        while counter <= inputs.niter:
            LOG.info("Evaluating flagging heuristics for {}, iteration {}"
                     "".format(os.path.basename(inputs.vis), counter))

            # Identify new flags.
            newflags = self._run_flagging_iteration(ms, antenna_id_to_name)

            # Run step that will propagate flags under certain conditions.
            # PIPE-1630: run this within iterative flagging loop to ensure that
            # propagated flags show up in the "after" flagging summary.
            newflags = _propagate_phase_flags(newflags, ms, antenna_id_to_name)

            # Add flags to overall list.
            allflags.extend(newflags)

            # Apply intermediate flags; on first iteration, always include "before" summary.
            if counter == 1:
                # If new flags are found, but there will be another iteration,
                # then skip "after" summary; otherwise include the "after" summary.
                if newflags and counter < inputs.niter:
                    stats_before, _ = self._apply_flags(newflags, sum_before=True)
                else:
                    stats_before, stats_after = self._apply_flags(newflags, sum_before=True, sum_after=True)

            # On subsequent iterations, if new flags are found, but there are
            # more iterations, then skip summaries.
            elif newflags and counter < inputs.niter:
                self._apply_flags(newflags)

            # On subsequent iterations, if no new flags are found, or this is
            # the final iteration, then include the "after" summary.
            else:
                _, stats_after = self._apply_flags(newflags, sum_after=True)

            if not newflags:
                LOG.info("Evaluation of flagging heuristics for {}, iteration {} resulted 0 new flagging commands."
                         "".format(os.path.basename(inputs.vis), counter))
                break
            else:
                LOG.info("Evaluation of flagging heuristics for {}, iteration {} resulted in {} new"
                         " flagging commands.".format(os.path.basename(inputs.vis), counter, len(newflags)))

            counter += 1

        # After iterative evaluation of heuristics, if any flags were found:
        if allflags:
            # Consolidate final list of all flagging commands.
            allflags = _consolidate_flags(allflags)

            # Report final number of new flags (CAS-7336: show as info message instead of warning).
            LOG.info("Evaluation of flagging heuristics for {} raised total of {} flagging command(s)"
                     "".format(os.path.basename(inputs.vis), len(allflags)))

        # Store final list of flags in result.
        result.addflags(allflags)

        # Attach flagging summaries to result
        result.summaries = [stats_before, stats_after]

        return result

    def analyse(self, result):
        return result

    @staticmethod
    def _get_ant_id_to_name_dict(ms: MeasurementSet) -> Dict:
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
        # length as list of IDs. If not, then set the translation
        # dictionary to an empty dictionary (to revert back to flagging by ID.)
        if len(set(antenna_id_to_name.values())) != len(ms.antennas):
            LOG.info('No unique name available for each antenna ID:'
                     ' flagging by antenna ID instead of by name.')
            antenna_id_to_name = {}

        return antenna_id_to_name

    def _run_flagging_iteration(self, ms: MeasurementSet, antenna_id_to_name: Dict) -> List[FlagCmd]:
        inputs = self.inputs

        # Get the spws to use.
        spwids = list(map(int, inputs.spw.split(',')))

        # Initialize list of newly found flags.
        newflags = []

        # Evaluate flagging heuristics separately for each intent.
        for intent in inputs.intent.split(','):

            # For current intent, identify which fields from inputs are valid.
            if intent == 'TARGET':
                # Use field IDs to loop over individual mosaic pointings for
                # science targets (PIPE-337).
                valid_fields = [str(field.id)
                                for field in ms.get_fields(intent=intent)
                                if field.name in list(utils.safe_split(inputs.field))]
            else:
                # Use field names for calibrators.
                valid_fields = [field.name
                                for field in ms.get_fields(intent=intent)
                                if field.name in list(utils.safe_split(inputs.field))]

            # If no valid fields were found, raise warning, and continue to
            # next intent. The following intents are optional and do not require
            # a warning: CHECK (PIPE-281), POLANGLE, POLLEAKAGE (PIPE-607),
            # DIFFGAINREF, DIFFGAINSRC (PIPE-2082, PIPE-2145).
            if not valid_fields:
                if intent not in ['CHECK', 'DIFFGAINREF', 'DIFFGAINSRC', 'POLANGLE', 'POLLEAKAGE']:
                    LOG.warning("Invalid data selection for given intent(s) and field(s): fields {} do not include"
                                " intent \'{}\'.".format(utils.commafy(utils.safe_split(inputs.field)), intent))
                continue

            # Evaluate heuristic for each valid field.
            for field in valid_fields:

                # Evaluate flagging heuristics separately for each spw.
                for spwid in spwids:

                    flags_for_intent_field_spw = self._evaluate_heuristic(
                        ms, intent, field, spwid, antenna_id_to_name)
                    newflags.extend(flags_for_intent_field_spw)

        LOG.debug("Flagging commands from current iteration, before consolidation:\n{}"
                  "".format('\n'.join([flag.flagcmd for flag in newflags])))

        # Consolidate flagging commands from current iteration to minimize
        # request for flagdata.
        newflags = _consolidate_flags(newflags)

        return newflags

    def _evaluate_heuristic(self, ms: MeasurementSet, intent: str, field: str, spwid: int,
                            antenna_id_to_name: Dict) -> List[FlagCmd]:
        # Initialize flags.
        allflags = []

        # Determine which baseline sets to evaluate separately.
        baseline_sets = self._identify_baseline_sets(ms)

        # If no baseline sets were received, then evaluate heuristics for
        # all baselines at once.
        if not baseline_sets:
            allflags.extend(self._evaluate_heuristic_for_baseline_set(
                ms, intent, field, spwid, antenna_id_to_name))
        else:
            # Evaluate heuristic for each set of baselines.
            for baseline_set in baseline_sets:
                newflags = self._evaluate_heuristic_for_baseline_set(
                    ms, intent, field, spwid, antenna_id_to_name, baseline_set)
                allflags.extend(newflags)

        return allflags

    @staticmethod
    def _identify_baseline_sets(ms: MeasurementSet) -> List[Tuple[str, str]]:
        # Determine unique antenna diameters.
        uniq_diams = {ant.diameter for ant in ms.antennas}

        if len(uniq_diams) <= 1:
            # If dataset contains only antennas with the same antenna diameter,
            # then heuristics should be evaluated for all baselines together.
            baseline_sets = []
        else:
            # If the dataset contains antennas of different diameters, then
            # check that these are ALMA with the expected diameters.
            if ms.antenna_array.name == 'ALMA' and uniq_diams == {7.0, 12.0}:

                # For 12m antennas, identify number of "PM*" and "D*" antennas.
                ant_names = [ant.name for ant in ms.antennas]
                n_pm = len([name for name in ant_names if "PM" in name])
                n_d = len([name for name in ant_names if "D" in name])

                # Always add the mixed-array 7m-12m as a baseline set.
                bl_str = []
                if n_pm > 0:
                    bl_str.append("CM*&PM*")
                if n_d > 0:
                    bl_str.append("CM*&D*")
                baseline_sets = [('7m-12m', ';'.join(bl_str))]

                # If more than one 7m antenna is present, add these as a
                # separate baseline set.
                if len([ant for ant in ms.antennas if ant.diameter == 7.0]) > 1:
                    baseline_sets.append(('7m-7m', "CM*&CM*"))

                # If more than one 12m antenna is present, add these as a
                # separate baseline set.
                if n_pm + n_d > 1:
                    bl_str = []
                    if n_pm > 1:
                        bl_str.append("PM*&PM*")
                    if n_d > 1:
                        bl_str.append("D*&D*")
                    if n_pm > 0 and n_d > 0:
                        bl_str.append("PM*&D*")
                    baseline_sets.append(('12m-12m', ';'.join(bl_str)))
            # If the mixed-array dataset is not recognized as ALMA diameters,
            # then continue with evaluating all baselines at once.
            else:
                baseline_sets = []
                LOG.warning("Found mixed-array with multiple antenna diameters, but unanticipated non-ALMA diameters"
                            " (do not know how to select data). Continuing with heuristic evaluation for all baselines"
                            " at once.")

        return baseline_sets

    def _uvbinFactor(self, uvmin: float, totalpts: int) -> float:
        # Determine the uvrange bin width for searching for outliers in TARGET data.
        # ACA snapshot mosaics can have small number of visibility points per field that would 
        # not support the option with finer bins at short baselines (18 bins from 7m-36m).  
        # So, we must use the fixed sqrt(2) bin width in this case, which will lead to only 5 bins.
        # Where to set the threshold?
        # Example: 11 antenna array (one 30sec scan/6sec integrations)*11*10/2 = 275 points
        # Example: 12 antenna array (one 30sec scan/6sec integrations)*12*11/2 = 330 points
        # Example: 16 antenna array (one 30sec scan/6sec integrations)*12*11/2 = 600 points
        # Compared to 12m array:
        # Example: 43 antenna array: 5integrations * 43*42/2 = 4515
        # Example: 27 antenna array: 5integrations * 27*26/2 = 1755
        # We set threshold to 1000 points, so that we need at least 2 scans per field
        # with a 16-antenna array, or 4 scans per field of an 11-antenna array.
        if totalpts > 1000:
            # use faster increments at longer baselines
            if uvmin < 40:
                factor = 1.1
            elif uvmin < 90:
                factor = 1.2
            else:
                factor = 1.26
        elif totalpts > 500:
            factor = np.sqrt(2)
            LOG.info('Using uvbinFactor=%.2f' % (factor))
        else:
            factor = 1.6
            LOG.info('Using uvbinFactor=%.2f' % (factor))
        return factor
        
    def _evaluate_heuristic_for_baseline_set(self, ms: MeasurementSet, intent: str, field: str, spwid: int,
                                             antenna_id_to_name: Dict,
                                             baseline_set: Optional[List] = None) -> List[FlagCmd]:

        inputs = self.inputs

        # Set "default" scale factor by which the thresholds
        # tmint and tmbl should be scaled.
        thresh_scale_factor = 1.0

        # Set threshold for maximum fraction of outlier baseline scans that
        # a single antenna can be involved in.
        max_frac_outlier_scans = 0.5

        # Set threshold for maximum fraction of antennas that can be
        # "affected", by being involved in the most, or close to the most,
        # number of outlier baseline scans.
        ants_in_outlier_baseline_scans_thresh = 1.0/3.0

        # Set threshold for maximum fraction of antennas that can be either
        # "affected" or "partially affected", where the latter are antennas
        # that are not "affected" but still involved in at least one outlier
        # baseline scan.
        ants_in_outlier_baseline_scans_partial_thresh = 0.5

        # Set threshold for minimum number of "bad baselines" than an antenna
        # may be a part of without getting flagged.
        tmbl_minbadnr = 4.0

        # Set sigma threshold for identifying very high outliers.
        antveryhighsig = 10.0

        # Set sigma thresholds for identifying ultra low/high outliers.
        antultrahighsig = 12.0
        antultralowsig = 13.0

        # Set sigma outlier threshold for purpose of evaluating
        # whether to relax the threshold scaling factor.
        relaxsig = 6.5

        # Get number of scans in MS for this intent.
        nscans = len(ms.get_scans(scan_intent=intent, spw=spwid))

        # Get number of antennas.
        nants = len(ms.antennas)

        # If there are multiple scans for this intent, then scale up the
        # threshold for timestamps with outliers.
        if nscans > 1:
            tmantint = inputs.tmantint * 2 * (1 + np.log10(nscans))
        else:
            tmantint = inputs.tmantint

        # Initialize flags.
        newflags = []

        # Read in data from MS. If no valid data could be read, return early with no flags.
        data = mstools.read_channel_averaged_data_from_ms(ms, field, spwid, intent,
            ['corrected_data', 'model_data', 'antenna1', 'antenna2', 'flag', 'time', 'uvdist'], baseline_set)
        if not data:
            return newflags

        # Remove the channel dimension (should be of length 1 as we asked
        # for average across all channels).
        corrdata = np.squeeze(data['corrected_data'], axis=1)
        modeldata = np.squeeze(data['model_data'], axis=1)
        flag_all = np.squeeze(data['flag'], axis=1)
        uvdist_all = np.squeeze(data['uvdist'])

        # Compute "scalar difference" between corrected data and model data.
        cmetric_all = np.abs(corrdata) - np.abs(modeldata)

        # Select non-autocorrelations.
        id_nonac = np.where(data['antenna1'] != data['antenna2'])
        time = data['time'][id_nonac]
        ant1 = data['antenna1'][id_nonac]
        ant2 = data['antenna2'][id_nonac]

        # Get number of correlations for this spw.
        corr_type = commonhelpermethods.get_corr_products(ms, spwid)
        ncorrs = len(corr_type)

        # CAS-12011: For multi-scan observations, analyze the mean of the
        # metric for XX and YY, and for XY and YX (if ncorrs = 4). Combine the
        # flagging state of the individual correlations by only marking the
        # mean metric as flagged where both individual contributing
        # correlations are marked as flagged (by floor-dividing by 2).
        if nscans > 1:
            cmetric_mask = np.ma.array(cmetric_all, mask=flag_all)
            if ncorrs == 2:
                cmetric_all = np.ma.mean(cmetric_mask, axis=0, keepdims=True)
                flag_all = np.sum(flag_all, axis=0, keepdims=True) // 2
                ncorrs = 1
            elif ncorrs == 4 and set(corr_type) == {'XX', 'XY', 'YX', 'YY'}:
                # Create mean of XX and YY polarization, and combine flags.
                col_sel = [corr_type.index('XX'), corr_type.index('YY')]
                cmetric_copol = np.ma.mean(cmetric_mask[col_sel, :], axis=0, keepdims=True)
                flag_copol = np.sum(flag_all[col_sel, :], axis=0, keepdims=True) // 2
                # Create mean of XY and YX polarization, and combine flags.
                col_sel = [corr_type.index('XY'), corr_type.index('YX')]
                cmetric_crosspol = np.ma.mean(cmetric_mask[col_sel, :], axis=0, keepdims=True)
                flag_crosspol = np.sum(flag_all[col_sel, :], axis=0, keepdims=True) // 2
                # Create new scalar difference array with the mean data, and
                # corresponding flagging array.
                cmetric_all = np.concatenate((cmetric_copol, cmetric_crosspol), axis=0)
                flag_all = np.concatenate((flag_copol, flag_crosspol), axis=0)
                ncorrs = 2

        # Evaluate flagging heuristics separately for each polarisation.
        for icorr in range(ncorrs):

            # Select non-autocorrelations from corrected and model data.
            cmetric = cmetric_all[icorr][id_nonac]
            flag = flag_all[icorr][id_nonac]

            # Compute the threshold for maximum number of timestamps that are allowed
            # to contain outliers, based on maximum fractional threshold and
            # number of unique timestamps, while setting to a minimum of 1.
            n_time_with_highsig_thresh_min = 1
            n_time_with_highsig_thresh_frac = tmantint * len(np.unique(time))
            n_time_with_highsig_max = np.max([n_time_with_highsig_thresh_min, n_time_with_highsig_thresh_frac])

            # Select for non-flagged data and non-NaN data.
            id_nonbad = np.where(np.logical_and(
                np.logical_not(flag),
                np.isfinite(cmetric)))
            cmetric_sel = cmetric[id_nonbad]
            time_sel = time[id_nonbad]
            ant1_sel = ant1[id_nonbad]
            ant2_sel = ant2[id_nonbad]

            # Compute the median. Assuming the distribution is normal,
            # compute a robust estimate of the standard deviation as
            # 1.4826 x the median absolute deviation from the median.
            med = np.median(cmetric_sel)
            mad = np.median(np.abs(cmetric_sel - np.median(cmetric_sel))) * 1.4826
            #
            # Evaluate whether the threshold scaling factor should be
            # set to the relaxed value.
            #

            # Based on the "relaxation" sigma outlier threshold, identify
            # outliers. If an antenna-based bad integrations fraction was
            # provided, then identify both negative and positive outliers,
            # otherwise just identify negative outliers.
            if tmantint > 0:
                id_relaxsig = np.where(
                    np.logical_or(
                        cmetric_sel < (med - mad * relaxsig),
                        cmetric_sel > (med + mad * relaxsig)))[0]
            else:
                id_relaxsig = np.where(
                    cmetric_sel < (med - mad * inputs.relaxsig))[0]

            # If any outliers were found...
            if len(id_relaxsig) > 0:
                # Relax threshold scale factor if not testing for positive
                # outliers.
                if tmantint <= 0:
                    thresh_scale_factor = inputs.relaxed_factor
                else:
                    # First check if a few antennas (up to 10%) explain most of
                    # the outlier timestamps (>90%), before determining the
                    # maximum threshold of outlier timestamps.
                    n_time_max_scale_factor = 1

                    # Track which antennas appear most frequently among outlier
                    # timestamps.
                    frequent_ants = []

                    # Select antennas for outlier timestamps.
                    ant1_remaining = ant1_sel[id_relaxsig]
                    ant2_remaining = ant2_sel[id_relaxsig]

                    # Iterate for up to 10% of the nr. of antennas.
                    for i in range(np.max([1, nants // 10])):
                        # Add antenna that appears most frequently.
                        frequent_ant = mode(np.concatenate([ant1_remaining, ant2_remaining]))
                        frequent_ants.append(antenna_id_to_name[frequent_ant])

                        # Identify which antennas remain excluding the newly
                        # found most frequent antenna.
                        ant1_to_keep = np.where(ant1_remaining != frequent_ant)
                        ant2_to_keep = np.where(ant2_remaining != frequent_ant)
                        ants_to_keep = np.intersect1d(ant1_to_keep, ant2_to_keep)

                        # Compute what fraction of outliers the most frequent
                        # antennas account for.
                        percentage = 100 * (1 - float(len(ants_to_keep)) / len(id_relaxsig))
                        LOG.info(f"{ms.basename}, corr {icorr}: {i + 1} antenna(s)"
                                 f" ({utils.commafy(frequent_ants, quotes=False)}) account for {percentage:.1f} percent"
                                 f" of relaxsig outliers (PIPE-1000).")

                        # If the remaining outliers are less than 10%, then
                        # set a new scaling factor based on nr. of antennas
                        # that cause the most outliers, and break out of this
                        # for loop.
                        if len(ants_to_keep) < len(id_relaxsig) * 0.1:
                            n_time_max_scale_factor = 4 - i
                            LOG.info(f"{ms.basename}, corr {icorr}: setting scaling factor for maximum threshold for"
                                     f" outlier timestamps to: {n_time_max_scale_factor} (PIPE-1000).")
                            break

                        # If still iterating, re-select the remaining antennas
                        # for outlier timestamps.
                        ant1_remaining = ant1_remaining[ants_to_keep]
                        ant2_remaining = ant2_remaining[ants_to_keep]

                    # Identify number of unique outlier timestamps.
                    time_sel_relaxsig = time_sel[id_relaxsig]
                    time_sel_relaxsig_uniq = np.unique(time_sel_relaxsig)

                    # Set maximum threshold for outlier timestamps.
                    # Currently set equal to the threshold for high sigma
                    # outlier timestamps used in flagging heuristic, scaled by
                    # the scaling factor based on whether a small fraction of
                    # antennas are responsible for most outliers.
                    n_time_with_relaxsig_max = n_time_with_highsig_max * n_time_max_scale_factor

                    # If the number of unique outlier timestamps exceeds the
                    # threshold, then relax the threshold scale factor.
                    if len(time_sel_relaxsig_uniq) > n_time_with_relaxsig_max:
                        thresh_scale_factor = inputs.relaxed_factor

            #
            # Start with evaluation of flagging heuristics.
            #

            #
            # The following part considers timestamps separately, evaluating a
            # series of antenna-based heuristics to flag one or more antennas
            # within a timestamp or to flag an entire timestamp if necessary.
            #
            # If outliers are identified, but not flagged by the antenna-based
            # heuristics, then any existing ultra high outlier
            # baseline-timestamp combination will be flagged.
            #

            # Based on the input antenna based negative and positive sigma
            # outlier thresholds, identify outliers. If an antenna-based
            # bad integrations fraction was provided, then identify both
            # negative and positive outliers, otherwise just identify
            # negative outliers.
            if tmantint > 0:
                id_highsig = np.where(
                    np.logical_or(
                        cmetric_sel < (med - mad * inputs.antnegsig),
                        cmetric_sel > (med + mad * inputs.antpossig)))[0]
            else:
                id_highsig = np.where(
                    cmetric_sel < (med - mad * inputs.antnegsig))[0]

            # Based on the "very high" sigma outlier threshold, identify
            # both negative and positive outliers.
            id_veryhighsig = np.where(
                np.logical_or(
                    cmetric_sel < (med - mad * antveryhighsig),
                    cmetric_sel > (med + mad * antveryhighsig)))[0]

            # Based on the "ultra low/high" sigma outlier thresholds, identify
            # both negative and positive outliers.
            if intent == 'TARGET':
                # Identify UV bins.
                uvdist = uvdist_all[id_nonac]
                uvdist_sel = uvdist[id_nonbad]
                if uvdist_sel.shape == (0,):
                    continue  # go on to the next polarization
                uvmin = np.min(uvdist_sel)
                uvmax = np.max(uvdist_sel)
                uvbins = []
                uv1 = uvmin
                while uv1 < uvmax:
                    uv0 = uv1
                    uv1 *= self._uvbinFactor(uv0, len(uvdist_sel))
                    uvbins.append([uv0, uv1])
                LOG.info('%s: Defined %d uvbins for field %s spw %d: %s'
                         '' % (ms, len(uvbins), str(field), spwid, str(uvbins)))

                # Build another set of uvbins, shifted by 1/2 bin from the original set
                # Any visibilities in the first half of the original bin will be ignored by
                # the second set of bins, while the final bin will be half the width of the 
                # original final bin.
                uvbins2 = []
                uv1 = np.mean(uvbins[0])
                while uv1 < uvbins[-1][1]:
                    uv0 = uv1
                    i = len(uvbins2)
                    if i < len(uvbins)-1:
                        uv1 = np.mean([uvbins[i+1][0], uvbins[i+1][1]])
                    else:
                        uv1 = uvbins[i][1]
                    uvbins2.append([uv0, uv1])
                LOG.info('%s: Defined %d offset uvbins for field %s spw %d: %s'
                         '' % (ms, len(uvbins), str(field), spwid, str(uvbins2)))
                uvbinsets = [uvbins, uvbins2]

                id_ultrahighsig_dict = {0: [], 1: []}

                # Define minimum points for an accurate median/MAD. Note: a
                # single integration of a 7-antenna array would produce only 6
                # points per field.
                minimumPoints = 22
                # Establish count to set the initial uvstart.
                npts = minimumPoints + 1
                # Iterate through set of UV bins.
                for v, uvbins in enumerate(uvbinsets):
                    previousLength = 0  # only used for LOG message
                    prior_uvstart = uvbins[0][0]
                    for u, uvbin in enumerate(uvbins):
                        # Advance uvstart, but only if prior bin contained enough points to be evaluated.
                        # (This avoids leaving a small number of orphaned points unevaluated.)
                        if npts >= minimumPoints: 
                            uvstart = uvbin[0]
                        id_uvbin = np.where(
                            np.logical_and(
                                uvdist_sel >= uvstart,
                                uvdist_sel < uvbin[1]))[0]
                        npts = len(id_uvbin)
                        if npts < minimumPoints and u+1 == len(uvbins) and u > 0:
                            LOG.info('Final bin (%d) has too few points (%d), including them into prior successful bin'
                                     ' with uvstart=%f.' % (u, npts, prior_uvstart))
                            id_uvbin = np.where(
                                np.logical_and(
                                    uvdist_sel >= prior_uvstart,
                                    uvdist_sel < uvbin[1]))[0]
                        npts = len(id_uvbin)
                        if npts < minimumPoints: 
                            # If the logic is correct above, we should never arrive here while in the final bin, 
                            # and thus we will never leave any data uninspected.
                            LOG.info('uvbin%d) has too few points (%d), including them into next bin.' % (u, npts))
                            continue

                        # It is now safe to set prior_uvstart because this is a "successful" bin.
                        prior_uvstart = uvstart
                        maxInThisBin = np.max(cmetric_sel[id_uvbin])
                        if len(uvdist_sel) > 1000:
                            Q1 = np.percentile(cmetric_sel[id_uvbin], 25, interpolation='midpoint')
                            Q3 = np.percentile(cmetric_sel[id_uvbin], 75, interpolation='midpoint')
                            if npts >= 20:
                                D1 = np.percentile(cmetric_sel[id_uvbin], 10, interpolation='midpoint')
                                D9 = np.percentile(cmetric_sel[id_uvbin], 90, interpolation='midpoint')
                                IQR = 0.5*(Q3-Q1)
                                IDR = 0.5*(D9-D1)/1.9004
                                mad = np.max([IQR, np.min([2*IQR, IDR])])
                                if IQR > IDR:
                                    LOG.info('uvbin%d) using interquartile range %f>%f (npts=%d, max=%f)'
                                             '' % (u, IQR, IDR, npts, maxInThisBin))
                                else:
                                    LOG.info('uvbin%d) using scaled interdecile range %f>%f (npts=%d, max=%f)'
                                             '' % (u, IDR, IQR, npts, maxInThisBin))
                            else:
                                LOG.info('uvbin%d) using IQR npts=%d<20, max=%f' % (u, npts, maxInThisBin))
                                # use half the interquartile spread instead of MAD
                                mad = 0.5*(Q3-Q1)
                            # use midpoint of interquartile spread (the "midhinge") instead of median
                            med = np.mean([Q1, Q3])
                        else:
                            LOG.info('uvbin%d) using median & MAD (npts=%d, max=%f)' % (u, npts, maxInThisBin))
                            med = np.median(cmetric_sel[id_uvbin])
                            bufferFactor = 2
                            mad = bufferFactor*np.median(np.abs(cmetric_sel[id_uvbin] - med)) * 1.4826
                            LOG.info('uv%dbin%d) using median=%f & %.2f*MAD=%f (npts=%d, maxInThisBin=%f)'
                                     '' % (v, u, med, bufferFactor, mad, npts, maxInThisBin))
                        if tmantint > 0:
                            id_ultrahighsig_dict[v] += list(id_uvbin[np.where(
                                np.logical_or(
                                    cmetric_sel[id_uvbin] < (med - mad * antultralowsig),
                                    cmetric_sel[id_uvbin] > (med + mad * antultrahighsig)))[0]])
                        else:
                            id_ultrahighsig_dict[v] += list(id_uvbin[np.where(
                                cmetric_sel[id_uvbin] < (med - mad * antultrahighsig))[0]])
                        if len(id_ultrahighsig_dict[v]) > previousLength:
                            LOG.info('spw %d: Found %d outliers out of %d points in uvbin %d'
                                     '' % (spwid, len(id_ultrahighsig_dict[v])-previousLength, npts, u))
                        previousLength = len(id_ultrahighsig_dict[v])
                    # end loop over this uvbin set
                # end loop over uvbinsets
                LOG.info('spw %d: found %d outliers in uvbinset0 and %d in uvbinset1'
                         '' % (spwid, len(id_ultrahighsig_dict[0]), len(id_ultrahighsig_dict[1])))

                id_uvbin_firsthalf_firstbin = np.where(
                    np.logical_and(
                        uvdist_sel >= uvbinsets[0][0][0],  # start of first bin of first group
                        uvdist_sel < uvbinsets[1][0][0]))[0]  # start of first bin of second group
                id_ultrahighsig = np.intersect1d(id_ultrahighsig_dict[0], id_ultrahighsig_dict[1])
                firsthalf_firstbin_flags = np.intersect1d(id_ultrahighsig_dict[0], id_uvbin_firsthalf_firstbin)
                LOG.info('spw %d: %d outliers are in common and will be flagged, along with %d outliers from the first'
                         ' half of the first bin' % (spwid, len(id_ultrahighsig), len(firsthalf_firstbin_flags)))
                id_ultrahighsig = np.union1d(id_ultrahighsig, firsthalf_firstbin_flags)
                id_ultrahighsig = np.array(id_ultrahighsig, dtype=int)
            else:
                id_ultrahighsig = np.where(
                    np.logical_or(
                        cmetric_sel < (med - mad * antultralowsig),
                        cmetric_sel > (med + mad * antultrahighsig)))[0]

            # If outliers were found and checking for positive outliers...
            if len(id_highsig) > 0 and tmantint > 0:
                # Check whether the outliers were concentrated in only one or a small
                # fraction (set by bad_int_frac) of timestamps.

                # Identify timestamps with outliers
                time_sel_highsig = time_sel[id_highsig]
                time_sel_highsig_uniq = np.unique(time_sel_highsig)

                # Identify timestamps with very high outliers.
                time_sel_veryhighsig = time_sel[id_veryhighsig]
                time_sel_veryhighsig_uniq = np.unique(time_sel_veryhighsig)

                # Set maximum threshold for "very high" outlier timestamps.
                # Currently set equal to the threshold for high sigma
                # outlier timestamps used in flagging heuristic.
                n_time_with_veryhighsig_max = n_time_with_highsig_max

                # The antenna based heuristics shall be done for calibrators
                # only (PIPE-337).
                if intent != 'TARGET':
                    # If all outliers were concentrated within a small number of
                    # timestamps set by a threshold, then evaluate the antenna
                    # based heuristics for those timestamps.
                    if 0 < len(time_sel_highsig_uniq) <= n_time_with_highsig_max:
                        new_antbased_flags = self._evaluate_antbased_heuristics(
                            ms, spwid, intent, icorr, field,
                            ants_in_outlier_baseline_scans_thresh,
                            ants_in_outlier_baseline_scans_partial_thresh,
                            max_frac_outlier_scans,
                            antenna_id_to_name, ant1_sel, ant2_sel, nants,
                            id_highsig, time_sel_highsig, time_sel_highsig_uniq)
                        newflags.extend(new_antbased_flags)
                    # If all very high outliers were concentrated within a small
                    # number of timestamps set by a threshold, then evaluate the
                    # antenna based heuristics for those timestamps.
                    elif 0 < len(time_sel_veryhighsig_uniq) <= n_time_with_veryhighsig_max:
                        new_antbased_flags = self._evaluate_antbased_heuristics(
                            ms, spwid, intent, icorr, field,
                            ants_in_outlier_baseline_scans_thresh,
                            ants_in_outlier_baseline_scans_partial_thresh,
                            max_frac_outlier_scans,
                            antenna_id_to_name, ant1_sel, ant2_sel, nants,
                            id_veryhighsig, time_sel_veryhighsig, time_sel_veryhighsig_uniq)
                        newflags.extend(new_antbased_flags)

            # Flag any ultra high outliers for corresponding baseline/timestamp.
            if len(id_ultrahighsig) > 0:
                bad_timestamps = time_sel[id_ultrahighsig]
                bad_bls = list(zip(ant1_sel[id_ultrahighsig], ant2_sel[id_ultrahighsig]))
                newflags.extend(
                    self._create_flags_for_ultrahigh_baselines_timestamps(
                        ms, spwid, intent, icorr, field, bad_timestamps, bad_bls, antenna_id_to_name))

            #
            # The following part considers all timestamps at once, and
            # identifies "bad baselines" as the baselines that contain outliers
            # in a number of timestamps that exceeds the maximum threshold (set
            # by scale factor). For each of these "bad" baselines, it then:
            #
            #  a.) identifies "bad antennas" as those antennas that are
            #  part of a number of "bad baselines" that exceeds a
            #  maximum threshold. Each of these "bad antennas" are
            #  entirely flagged for all timestamps.
            #
            #  b.) identifies remaining "bad baselines" as those that
            #  do not contain one of the "bad antennas", but that do
            #  contain outliers in a number of timestamps that exceeds
            #  the maximum threshold set by the relaxed scale factor.
            #  Each of these baselines are flagged for all timestamps.
            #
            # This heuristic shall be done for calibrators only (PIPE-337).
            if intent != 'TARGET':
                # If requested, identify both positive and negative
                # outliers, otherwise just identify positive outliers; also
                # select for non-flagged and non-NaN data.
                if inputs.antblnegsig > 0:
                    id_flagsig = np.where(
                        np.all((np.logical_not(flag),
                                np.isfinite(cmetric),
                                np.logical_or(
                                    cmetric < (med - mad * inputs.antblnegsig),
                                    cmetric > (med + mad * inputs.antblpossig))), axis=0))[0]
                else:
                    id_flagsig = np.where(
                        np.all((np.logical_not(flag),
                                np.isfinite(cmetric),
                                cmetric > (med + mad * inputs.antblpossig)), axis=0))[0]

                # Proceed if outliers were found...
                if len(id_flagsig) > 0:
                    # Identify baselines involved in each baseline/timestamp
                    # outlier; this list may contain multiples of the same
                    # baseline when it was an outlier in more than one
                    # timestamp.
                    outlier_ant1 = ant1[id_flagsig]
                    outlier_ant2 = ant2[id_flagsig]
                    outlier_bl = list(zip(outlier_ant1, outlier_ant2))

                    # Compute for each baseline how many outlier timestamps
                    # it is a part of. This creates a dictionary with
                    # baselines as keys, and number of outlier timestamps
                    # as values.
                    outlier_bl_counts = collections.Counter(outlier_bl)

                    # Compute for each baseline how many timestamps
                    # it is a part of. This creates a dictionary with
                    # baselines as keys, and number of timestamps
                    # as values.
                    baselines = list(zip(ant1, ant2))
                    bl_counts = collections.Counter(baselines)

                    # Compute final threshold for maximum fraction of "outlier
                    # timestamps" over "total timestamps" that a baseline can
                    # be a part of. Scale up the threshold for multi-scan
                    # observations.
                    tmint_scaled = inputs.tmint * thresh_scale_factor
                    if nscans > 1:
                        if icorr == 0:
                            tmint_scaled = tmint_scaled * 2**0.5
                        else: # increase the threshold for cross-polar visibilities, which can show an offset in scans taken near transit
                            tmint_scaled = tmint_scaled * 3

                    # Identify "bad baselines" as those baselines whose number
                    # of timestamps with outliers exceeds the threshold.
                    bad_bls = [bl for bl, count in outlier_bl_counts.items()
                               if count > np.max([1, bl_counts[bl] * tmint_scaled])]

                    # Compute for each antenna how many "bad baselines" it is
                    # a part of.
                    ant_in_bad_bl_count = np.bincount(sum(bad_bls, ()),
                                                      minlength=nants)

                    # Compute final threshold for maximum number of "bad
                    # baselines" that an antenna may be a part of:
                    # this is based on the scaled fractional threshold
                    # times the number of baselines that the antenna is
                    # part of, with a minimum number threshold set by
                    # "tmbl_minbadnr".
                    tmbl_nr_thresh = max(
                        tmbl_minbadnr,
                        inputs.tmbl * thresh_scale_factor * (nants - 1))

                    # Identify "bad antennas" as those antennas involved in a number of
                    # "bad baselines" that exceeds the threshold.
                    bad_ants = [ant for ant, count in enumerate(ant_in_bad_bl_count)
                                if count > tmbl_nr_thresh]

                    # Create flagging command for each identified bad antenna.
                    for bad_ant in bad_ants:
                        newflags.append(
                            FlagCmd(
                                filename=ms.name,
                                spw=spwid,
                                antenna=bad_ant,
                                intent=utils.to_CASA_intent(ms, intent),
                                pol=icorr,
                                field=field,
                                reason='bad antenna',
                                antenna_id_to_name=antenna_id_to_name))

                    # Compute final outlier timestamps per baseline threshold,
                    # forcibly always using the relaxed threshold scale factor,
                    # and setting the minimum fraction to 1, such that a
                    # baseline with 100% outlier timestamps will get flagged
                    # (even if dynamic threshold exceeded beyond 1.0).
                    tmint_relaxed = np.min(
                        [1.0,
                         inputs.tmint * inputs.relaxed_factor])

                    # Compute fraction of outlier timestamps for each bad baseline.
                    bad_bls_timestamp_fraction = {
                        bl: float(outlier_bl_counts[bl]) / bl_counts[bl]
                        for bl in bad_bls}

                    # For each bad baseline, check if it was already covered by
                    # one of the bad antennas, and otherwise flag it explicitly
                    # if the fraction of outlier timestamps for this baseline
                    # equals-or-exceeds the threshold.
                    for bl in bad_bls:
                        if (bl[0] not in bad_ants
                                and bl[1] not in bad_ants
                                and bad_bls_timestamp_fraction[bl] >= tmint_relaxed):
                            newflags.append(
                                FlagCmd(
                                    filename=ms.name,
                                    spw=spwid,
                                    antenna="%s&%s" % bl,
                                    intent=utils.to_CASA_intent(ms, intent),
                                    pol=icorr,
                                    field=field,
                                    reason='bad baseline',
                                    antenna_id_to_name=antenna_id_to_name))

        return newflags

    @staticmethod
    def _create_flags_for_ultrahigh_baselines_timestamps(ms: MeasurementSet, spwid: int, intent: str, icorr: int,
                                                         field: str, timestamps: List, baselines: List,
                                                         antenna_id_to_name: Dict) -> List[FlagCmd]:
        newflags = []
        for idx in range(len(baselines)):
            newflags.append(
                FlagCmd(
                    filename=ms.name,
                    spw=spwid,
                    antenna="%s&%s" % baselines[idx],
                    intent=utils.to_CASA_intent(ms, intent),
                    pol=icorr,
                    time=timestamps[idx],
                    field=field,
                    reason='ultrahigh baseline timestamp',
                    antenna_id_to_name=antenna_id_to_name))

        return newflags

    @staticmethod
    def _evaluate_antbased_heuristics(ms: MeasurementSet, spwid: int, intent: str, icorr: int, field: str,
                                      ants_in_outlier_baseline_scans_thresh: float,
                                      ants_in_outlier_baseline_scans_partial_thresh: float,
                                      max_frac_outlier_scans: float, antenna_id_to_name: Dict, ant1_sel: NDArray,
                                      ant2_sel: NDArray, nants: int, id_highsig: NDArray, time_sel_highsig: NDArray,
                                      time_sel_highsig_uniq: NDArray) -> List[FlagCmd]:

        # Initialize flags.
        newflags = []

        # For each of the few bad timestamps...
        for timestamp in time_sel_highsig_uniq:

            # Identify baseline scans within this timestamp.
            id_outlier_scans_in_timestamp = np.where(
                time_sel_highsig == timestamp)[0]
            n_outlier_scans_in_timestamp = len(id_outlier_scans_in_timestamp)

            # Get a list of antennas involved in outlier scans
            # matching the timestamp.
            ants_in_olscans_in_tstamp = list(np.concatenate(
                [ant1_sel[id_highsig][id_outlier_scans_in_timestamp],
                 ant2_sel[id_highsig][id_outlier_scans_in_timestamp]]))

            # Identify number of outlier scans within current
            # timestamp that each antenna is involved in.
            antcnts = np.bincount(ants_in_olscans_in_tstamp, minlength=nants)

            # Identify the ants involved in the largest number
            # of outliers as well as 1 count less (while ignoring
            # ants involved in 0 outliers).
            id_affected_ants = np.where(antcnts >= max([1, antcnts.max() - 1]))[0]

            # Identify the ants that are at least partially affected,
            # by being involved in at least one outlier, but excluding
            # antennas already identified as "affected".
            id_partly_affected_ants = np.setdiff1d(
                np.where(antcnts >= 1)[0],
                id_affected_ants)

            # If the number of affected antennas is a significant fraction of
            # all antennas, then flag the entire timestamp.
            if len(id_affected_ants) > ants_in_outlier_baseline_scans_thresh * nants:
                # Create a flagging command for all antennas
                # in this timestamp (for given spw, intent, pol).
                newflags.append(
                    FlagCmd(
                        filename=ms.name,
                        spw=spwid,
                        intent=utils.to_CASA_intent(ms, intent),
                        pol=icorr,
                        time=timestamp,
                        field=field,
                        reason='bad timestamp'))
            # Evaluate a slightly more restrictive threshold, this time
            # on the total number of affected and partly affected antennas,
            # while still requiring half the original threshold.
            elif (len(id_affected_ants) > 0.5 * ants_in_outlier_baseline_scans_thresh * nants
                    and len(id_affected_ants) + len(id_partly_affected_ants) >
                    ants_in_outlier_baseline_scans_partial_thresh * nants):

                # Create a flagging command for all antennas
                # in this timestamp (for given spw, intent, pol).
                newflags.append(
                    FlagCmd(
                        filename=ms.name,
                        spw=spwid,
                        intent=utils.to_CASA_intent(ms, intent),
                        pol=icorr,
                        time=timestamp,
                        field=field,
                        reason='bad timestamp'))
            # If there was no significant fraction of affected antennas,
            # then proceed to check if the antenna(s) with the highest number
            # of outlier scans (within this timestamp) equals-or-exceeds the
            # threshold and flag the corresponding antenna(s).
            elif (antcnts.max() >= max_frac_outlier_scans * n_outlier_scans_in_timestamp
                    and n_outlier_scans_in_timestamp > 5):

                # Identify which antennas matched the highest counts,
                # and create a flagging command for each.
                id_ants_highest_cnts = np.where(antcnts == antcnts.max())[0]
                for ant in id_ants_highest_cnts:
                    # Create a flagging command for this antenna
                    newflags.append(
                        FlagCmd(
                            filename=ms.name,
                            spw=spwid,
                            antenna=ant,
                            intent=utils.to_CASA_intent(ms, intent),
                            pol=icorr,
                            time=timestamp,
                            field=field,
                            reason='bad antenna timestamp',
                            antenna_id_to_name=antenna_id_to_name))
            # Heuristic for catching cross-CAI-dependent issues:
            # If there are affected antennas, and total number of affected
            # and partially affected antennas exceeds the larger of 6 or
            # 20% of the antennas, then flag the timestamp. The minimum
            # threshold of 6 is there to prevent over-application on ACA 7m
            # datasets.
            elif (len(id_affected_ants) + len(id_partly_affected_ants) > np.max([6, 0.2 * nants])
                    and len(id_affected_ants) > 0):

                # Create flags only for the "affected" antennas for this timestamp
                # because CAI-dependent issues do not affect all antennas.
                for ant in id_affected_ants:
                    # Create a flagging command for this antenna.
                    newflags.append(
                        FlagCmd(
                            filename=ms.name,
                            spw=spwid,
                            antenna=ant,
                            intent=utils.to_CASA_intent(ms, intent),
                            pol=icorr,
                            time=timestamp,
                            field=field,
                            reason='bad CAI-dependent data',
                            antenna_id_to_name=antenna_id_to_name))

        return newflags

    def _apply_flags(self, flags: List[FlagCmd], sum_before: bool = False, sum_after: bool = False) \
            -> Tuple[Dict, Dict]:

        inputs = self.inputs

        # Initialize flagging summaries.
        stats_before, stats_after = {}, {}

        # Initialize list of flagdata commands.
        allflagcmds = []

        # If requested, add the "before" summary.
        if sum_before:
            allflagcmds.append("mode='summary' name='before'")

        # If new flags were found, apply these as part of the flagdata call,
        # and add an "after" summary if requested.
        if flags:
            LOG.info('Applying newly found flags.')
            allflagcmds.extend(flags)
            if sum_after:
                allflagcmds.append("mode='summary' name='after'")
        else:
            # If an "after" summary is requested, but no "before" summary,
            # then run "after" summary explicitly.
            if sum_after and not sum_before:
                allflagcmds.append("mode='summary' name='after'")

        # Run flagdata to create summaries and set flags.
        fsinputs = FlagdataSetter.Inputs(
            context=inputs.context, vis=inputs.vis, table=inputs.vis,
            inpfile=[])
        fstask = FlagdataSetter(fsinputs)
        fstask.flags_to_set(allflagcmds)
        fsresult = self._executor.execute(fstask)

        # Extract "before" and/or "after" summary
        if all(['report' in k for k in fsresult.results[0]]):
            # Go through dictionary of reports...
            for report in fsresult.results[0]:
                if fsresult.results[0][report]['name'] == 'before':
                    stats_before = fsresult.results[0][report]
                if fsresult.results[0][report]['name'] == 'after':
                    stats_after = fsresult.results[0][report]
        else:
            # Go through single report.
            if fsresult.results[0]['name'] == 'before':
                stats_before = fsresult.results[0]
            if fsresult.results[0]['name'] == 'after':
                stats_after = fsresult.results[0]

        # If no new flags were supplied, and both a "before" and "after"
        # summary was requested, then create a copy of the "before" summary.
        if not flags and sum_before and sum_after:
            stats_after = copy.deepcopy(stats_before)

        return stats_before, stats_after
