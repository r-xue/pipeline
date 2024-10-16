import copy
import itertools
import math
import operator
import os
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.exceptions as exceptions
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common import arrayflaggerbase
from pipeline.h.tasks.common import flaggableviewresults
from pipeline.h.tasks.common import ozone
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.utils import find_ranges

LOG = infrastructure.get_logger(__name__)


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
    # length as list of IDs. If not, then set the translation
    # dictionary to an empty dictionary (to revert back to flagging by ID.)
    if len(set(antenna_id_to_name.values())) != len(ms.antennas):
        LOG.info('No unique name available for each antenna ID:'
                 ' flagging by antenna ID instead of by name.')
        antenna_id_to_name = {}

    return antenna_id_to_name


def _log_outlier(msg, level=logging.DEBUG):
    """
    Pipeline DEBUG messages are only logged to the terminal unless the
    CASA logging priority level is also lowered. This method will log
    the outlier message as well as record it in the CASA log, so it can
    be referenced afterwards.
    """
    if LOG.isEnabledFor(level):
        LOG.log(level, msg)
        # Log outliers directly to CASA log (CAS-11313)
        casa_tools.post_to_log(msg)


class MatrixFlaggerInputs(vdp.StandardInputs):
    prepend = vdp.VisDependentProperty(default='')
    skip_fully_flagged = vdp.VisDependentProperty(default=True)
    use_antenna_names = vdp.VisDependentProperty(default=True)

    def __init__(self, context, output_dir=None, vis=None, datatask=None, viewtask=None, flagsettertask=None,
                 rules=None, niter=None, extendfields=None, extendbaseband=None, iter_datatask=None,
                 use_antenna_names=None, prepend=None, skip_fully_flagged=None):
        super(MatrixFlaggerInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # solution parameters
        self.datatask = datatask
        self.extendbaseband = extendbaseband
        self.extendfields = extendfields
        self.flagsettertask = flagsettertask
        self.iter_datatask = iter_datatask
        self.niter = niter
        self.prepend = prepend
        self.rules = rules
        self.skip_fully_flagged = skip_fully_flagged
        self.use_antenna_names = use_antenna_names
        self.viewtask = viewtask


class MatrixFlaggerResults(basetask.Results,
                           flaggableviewresults.FlaggableViewResults):
    def __init__(self, vis=None):
        """
        Construct and return a new MatrixFlaggerResults.
        """
        basetask.Results.__init__(self)
        flaggableviewresults.FlaggableViewResults.__init__(self)

        self.vis = vis
        self.dataresult = None
        self.viewresult = None

    def merge_with_context(self, context):
        pass

    def __repr__(self):
        s = 'MatrixFlaggerResults'
        return s


class MatrixFlagger(basetask.StandardTaskTemplate):
    Inputs = MatrixFlaggerInputs

    flag_reason_index = {'max abs': 1,
                         'min abs': 2,
                         'nmedian': 3,
                         'outlier': 4,
                         'high outlier': 5,
                         'low outlier': 6,
                         'too many flags': 7,
                         'bad quadrant': 8,
                         'bad antenna': 9,
                         'too many entirely flagged': 10}
    flag_reason_key = {value: key for (key, value) in flag_reason_index.items()}

    # override the inherited __init__ method so that references to the
    # task objects can be kept outside self.inputs. Later on self.inputs
    # will be replaced by a copy which breaks the connection between
    # its references to the tasks and the originals.
    def __init__(self, inputs):
        self.inputs = inputs

    def prepare(self):
        inputs = self.inputs

        # Initialize result.
        result = MatrixFlaggerResults(vis=inputs.vis)

        # Expand flag commands to larger scope, if requested, by removing
        # selection in specified fields
        if inputs.extendfields:
            LOG.info("{} flagcmds will be extended by removing selection in following fields: {}"
                     "".format(inputs.prepend, inputs.extendfields))

        # Expand flag commands to include all spws in a baseband, if requested
        if inputs.extendbaseband:
            LOG.info("{} flagcmds will be extended to include all spws within baseband.".format(inputs.prepend))

        # Initialize flags, flag_reason, and iteration counter
        flags = []
        flag_reason_plane = {}
        newflags = []
        counter = 1
        include_before = True
        dataresult = None
        viewresult = None

        # Start iterative flagging
        while counter <= inputs.niter:

            # Run the data task if needed
            if counter == 1:
                # Always run data task on first iteration
                dataresult = self._executor.execute(inputs.datatask)
            elif inputs.iter_datatask is True:
                # If requested to re-run datatask on iteration, then
                # run the flag-setting task which modifies the data
                # and then re-run the data task

                # If no "before summary" was done, include this in the flag
                # setting task
                if include_before:
                    stats_before, _ = self.set_flags(newflags,
                                                     summarize_before=True)
                    include_before = False
                else:
                    _, _ = self.set_flags(newflags)

                dataresult = self._executor.execute(inputs.datatask)
            else:
                # If not iterating the datatask, the previous
                # data result will be re-used, but marked here as no
                # longer new.
                dataresult.new = False

            # Create flagging view
            viewresult = inputs.viewtask(dataresult)

            # If a view could be created, continue with flagging
            if viewresult.descriptions():

                # Import the views from viewtask into the final result
                result.importfrom(viewresult)

                # Flag the view
                newflags, newflags_reason = self.flag_view(viewresult, inputs.rules)

                # Report how many flags were found in this iteration and
                # stop iteration if no new flags were found
                if len(newflags) == 0:
                    # If no new flags are found, report as a log message
                    LOG.info("{0}{1} iteration {2} raised {3} flagging commands"
                             "".format(inputs.prepend, os.path.basename(inputs.vis), counter, len(newflags)))
                    break
                else:
                    # Report newly found flags (CAS-7336: show as info message instead of warning).
                    LOG.info("{0}{1} iteration {2} raised {3} flagging commands"
                             "".format(inputs.prepend, os.path.basename(inputs.vis), counter, len(newflags)))

                # Accumulate new flags and flag reasons
                flags += newflags
                for description in newflags_reason:
                    if description in flag_reason_plane:
                        flag_reason_plane[description][newflags_reason[description] > 0] = \
                            newflags_reason[description][newflags_reason[description] > 0]
                    else:
                        flag_reason_plane[description] = newflags_reason[description]

                counter += 1
            else:
                # If no view could be created, exit the iteration
                LOG.warning('No flagging view was created!')
                break

        # Create final set of flags by removing duplicates from our accumulated
        # flags
        flags = list(set(flags))

        # If flags were found...
        if len(flags) > 0:

            # If newflags were found on last iteration loop, we need to still
            # set these.
            if len(newflags) > 0:

                # If datatask needs to be iterated...
                if inputs.iter_datatask is True:

                    # First set the new flags that were found on the last
                    # iteration. If the "before" summary was not yet created,
                    # then include this here; always include the "after"
                    # summary.
                    if include_before:
                        # Set flags, and include "before" and "after" summary.
                        stats_before, stats_after = self.set_flags(
                            newflags, summarize_before=True,
                            summarize_after=True)
                    else:
                        # Set flags, and include "after" summary
                        _, stats_after = self.set_flags(
                            newflags, summarize_after=True)

                    # After setting the latest flags, re-run the data task.
                    dataresult = self._executor.execute(inputs.datatask)

                # If the datatask did not need to be iterated, then no flags
                # were set yet and no "before" summary was performed yet, so
                # set all flags and include both "before" and "after" summary.
                else:
                    stats_before, stats_after = self.set_flags(
                        flags, summarize_before=True, summarize_after=True)

                # Create final post-flagging view
                viewresult = inputs.viewtask(dataresult)

                # Import the post-flagging view into the final result
                result.importfrom(viewresult)

            # If flags were found, but no newflags were found on last iteration
            # then the dataresult is already up-to-date, and all that is needed
            # is to ensure the flags are set, and that summaries are created.
            else:
                # If datatask needs to be iterated, then the "before" summary has
                # already been done, and the flags have already been set, so only
                # need to do an "after" summary.
                if inputs.iter_datatask is True:
                    _, stats_after = self.set_flags([], summarize_after=True)
                # If the datatask did not need to be iterated, then no flags
                # were set yet and no "before" summary was performed yet,
                # so set all flags and include both "before" and "after" summary.
                else:
                    stats_before, stats_after = self.set_flags(
                        flags, summarize_before=True, summarize_after=True)

            # Store the final set of flags in the final result
            result.addflags(flags)

            # Store the flag reasons in the last (i.e. post-flagging) view in
            # the final result
            result.add_flag_reason_plane(flag_reason_plane,
                                         self.flag_reason_key)

        # if no flags were found at all
        else:
            # Run a single flagging summary and use the result as both the "before"
            # and "after" summary.
            stats_before, _ = self.set_flags(flags, summarize_before=True)
            stats_after = copy.deepcopy(stats_before)

        # Store in the final result the name of the measurement set or caltable
        # to which any potentially found flags would need to be applied to.
        result.table = inputs.flagsettertask.inputs.table

        # Store in the final result the final data task result and the final
        # view task result.
        result.dataresult = dataresult
        result.viewresult = viewresult

        # Store the flagging summaries in the final result
        result.summaries = [stats_before, stats_after]

        # Sort the final list of flagging commands.
        result.sort_flagcmds()

        return result

    def analyse(self, result):
        return result

    def flag_view(self, view, rules):
        newflags = []
        newflags_reason = {}
        descriptionlist = sorted(view.descriptions())
        for description in descriptionlist:
            image = view.last(description)
            # get flags for this view according to the rules
            theseflags, this_flag_reason_plane = self.generate_flags(image, rules)

            # update flagging record
            newflags += theseflags
            newflags_reason[description] = this_flag_reason_plane

        return newflags, newflags_reason

    def set_flags(self, flags, summarize_before=False, summarize_after=False):
        # Initialize flag commands.
        allflagcmds = []

        # Add the "before" summary to the flagging commands.
        if summarize_before:
            allflagcmds = ["mode='summary' name='before'"]

        # Add the flagging commands.
        allflagcmds.extend(flags)

        # Add the "before" summary to the flagging commands.
        if summarize_after:
            allflagcmds.append("mode='summary' name='after'")

        # Update flag setting task with all flagging commands.
        self.inputs.flagsettertask.flags_to_set(allflagcmds)

        # Run flag setting task
        flagsetterresult = self._executor.execute(self.inputs.flagsettertask)

        # Initialize "before" and/or "after" summaries.
        stats_before = {}
        stats_after = {}

        # If the flagsetter returned results from the CASA flag data task,
        # then proceed to extract "before" and/or "after" flagging summaries;
        # if no "real" flagsetter results were returned (e.g. by
        # WvrgcalFlagSetter), then there will have been no real flagging
        # summaries created, in which case empty summaries are returned.
        if flagsetterresult.results:
            # CAS-10407: if MPI version of flagdata failed and returned invalid
            # results, then raise an exception.
            if flagsetterresult.results[0] is None:
                raise exceptions.PipelineException("Results from flagdata are empty, cannot continue.")
            if all(['report' in k for k in flagsetterresult.results[0]]):
                # Go through dictionary of reports.
                for report in flagsetterresult.results[0]:
                    if flagsetterresult.results[0][report]['name'] == 'before':
                        stats_before = flagsetterresult.results[0][report]
                    if flagsetterresult.results[0][report]['name'] == 'after':
                        stats_after = flagsetterresult.results[0][report]
            else:
                # Go through single report.
                if flagsetterresult.results[0]['name'] == 'before':
                    stats_before = flagsetterresult.results[0]
                if flagsetterresult.results[0]['name'] == 'after':
                    stats_after = flagsetterresult.results[0]

        return stats_before, stats_after

    @staticmethod
    def make_flag_rules(
            flag_hilo=False, fhl_limit=5.0, fhl_minsample=5,
            flag_hi=False, fhi_limit=5.0, fhi_minsample=5,
            flag_lo=False, flo_limit=5.0, flo_minsample=5,
            flag_tmf1=False, tmf1_axis='Time', tmf1_limit=1.0,
            tmf1_excess_limit=10000000,
            flag_tmf2=False, tmf2_axis='Time', tmf2_limit=1.0,
            tmf2_excess_limit=10000000,
            flag_tmef1=False, tmef1_axis='Antenna1', tmef1_limit=1.0,
            flag_nmedian=False, fnm_lo_limit=0.7, fnm_hi_limit=1.3,
            flag_maxabs=False, fmax_limit=0.1,
            flag_minabs=False, fmin_limit=0.0,
            flag_bad_quadrant=False, fbq_hilo_limit=7.0,
            fbq_antenna_frac_limit=0.5, fbq_baseline_frac_limit=0.5,
            flag_bad_antenna=False, fba_lo_limit=7.0,
            fba_frac_limit=0.05, fba_number_limit=3, fba_minsample=5):
        """
        Generate a list of flagging rules from a set of flagging parameters.
        Added detailed docs here.
        """

        # Construct rules from flag properties. If niter is set to curtail
        # the flagging loop then the order that the rules are applied
        # can be important. For example, 'too many flags' should run after
        # the other rules, 'bad quadrant' or 'bad antenna' should be run
        # before the others.
        rules = []
        if flag_bad_quadrant:
            rules.append({'name': 'bad quadrant', 'hilo_limit': fbq_hilo_limit,
                          'frac_limit': fbq_antenna_frac_limit,
                          'baseline_frac_limit': fbq_baseline_frac_limit})
        if flag_bad_antenna:
            rules.append({'name': 'bad antenna', 'lo_limit': fba_lo_limit,
                          'frac_limit': fba_frac_limit,
                          'number_limit': fba_number_limit,
                          'minsample': fba_minsample})
        if flag_maxabs:
            rules.append({'name': 'max abs', 'limit': fmax_limit})
        if flag_minabs:
            rules.append({'name': 'min abs', 'limit': fmin_limit})
        if flag_nmedian:
            rules.append({'name': 'nmedian', 'lo_limit': fnm_lo_limit,
                          'hi_limit': fnm_hi_limit})
        if flag_hilo:
            rules.append({'name': 'outlier', 'limit': fhl_limit,
                          'minsample': fhl_minsample})
        if flag_hi:
            rules.append({'name': 'high outlier', 'limit': fhi_limit,
                          'minsample': fhi_minsample})
        if flag_lo:
            rules.append({'name': 'low outlier', 'limit': flo_limit,
                          'minsample': flo_minsample})
        if flag_tmf1:
            rules.append({'name': 'too many flags',
                          'axis': str.upper(tmf1_axis),
                          'limit': tmf1_limit,
                          'excess limit': tmf1_excess_limit})
        if flag_tmf2:
            rules.append({'name': 'too many flags',
                          'axis': str.upper(tmf2_axis),
                          'limit': tmf2_limit,
                          'excess limit': tmf2_excess_limit})
        if flag_tmef1:
            rules.append({'name': 'too many entirely flagged',
                          'axis': str.upper(tmef1_axis),
                          'limit': tmef1_limit})

        return rules

    def generate_flags(self, matrix, rules):
        """
        Calculate the statistics of a matrix and flag the data according
        to a list of specified rules.

        Keyword arguments:
        matrix - ImageResult object containing data to be flagged.
        rules - Rules to be applied.
        """

        # Get the attributes - ensure all arrays are numpy arrays
        # as some subsequent processing depends on numpy array indexing
        data = np.array(matrix.data)
        flag = np.array(matrix.flag)
        nodata = np.array(matrix.nodata)
        xtitle = matrix.axes[0].name
        xdata = np.array(matrix.axes[0].data)
        ytitle = matrix.axes[1].name
        ydata = np.array(matrix.axes[1].data)
        spw = matrix.spw
        table = matrix.filename
        pol = matrix.pol
        antenna = matrix.ant

        # Initialize flags
        newflags = []
        flag_reason = np.zeros(np.shape(flag), int)

        # If there is no valid (non-flagged) data, then return early.
        if np.all(flag) and self.inputs.skip_fully_flagged:
            return newflags, flag_reason

        # If requested to use antenna names instead of IDs antenna,
        # create an id-to-name translation and check to make sure this
        # would result in unique non-empty names for all IDs, otherwise
        # revert back to flagging by ID
        if self.inputs.use_antenna_names:
            antenna_id_to_name = _get_ant_id_to_name_dict(self.inputs.ms)
        else:
            antenna_id_to_name = {}

        # If requested, expand current spw to all spws within the same
        # baseband, thus changing spw from an integer to a list of integers
        if self.inputs.extendbaseband:
            ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
            baseband = ms.get_spectral_window(spw).baseband
            spw = [spw.id for spw in ms.get_spectral_windows()
                   if spw.baseband == baseband]

        # Calculate statistics for valid (non-flagged) data.
        valid_data = data[np.logical_not(flag)]
        nvalid = len(valid_data)
        data_median, data_mad = arrayflaggerbase.median_and_mad(valid_data)

        # Index arrays
        i, j = np.indices(np.shape(data))

        # flag data according to each rule in turn
        for rule in rules:
            rulename = rule['name']

            if rulename == 'outlier':

                # Stop evaluating rule if sample is too small.
                if nvalid < rule['minsample']:
                    continue

                # Check limits.
                mad_max = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data - data_median), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_threshold = mad_max * data_mad
                data_masked = np.ma.masked_greater(data_masked, outlier_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flagged data.
                if not np.any(new_flag):
                    continue

                # PIPE-344: If the flagged channels fall within ozone lines, then ignore these outliers
                # (but still display a notification message that no action was taken)
                ozone_channels = ozone.get_ozone_channels_for_spw(self.inputs.ms, spw)
                new_flag_unfiltered = new_flag.copy()
                new_flag[ozone_channels] = False
                rejected_flagging_all_baselines = new_flag != new_flag_unfiltered

                # If the view is for a specific set of antennas, then include these in the warning
                if antenna:
                    ants_as_str = ", ant {}".format(antenna)
                else:
                    ants_as_str = ""

                if np.any(rejected_flagging_all_baselines):
                    # collapse the array of rejected flags from num_channels*num_baselines to num_channels*num_antennas
                    nchan = data.shape[0]
                    nant = int(math.sqrt(data.shape[1]))
                    rejected_flagging = np.logical_or(
                        np.any(rejected_flagging_all_baselines.reshape(nchan, nant, nant), axis=1),
                        np.any(rejected_flagging_all_baselines.reshape(nchan, nant, nant), axis=2))

                    # compressed (human-readable) list of channels in which at least one antenna was affected
                    rejected_channel_ranges = find_ranges(np.where(np.any(rejected_flagging, axis=1))[0])

                    # list of affected antennas (those for which flagging was rejected in at least one channel)
                    rejected_antenna_names = ','.join([
                        (antenna_id_to_name[ant] if antenna_id_to_name else str(ant))
                        for ant in np.where(np.any(rejected_flagging, axis=0))[0]
                    ])

                    msg = ("Outliers provisionally found with flagging rule '{}' for {}, spw {}, pol {}{}, channel {}, "
                           "antenna {}, were not flagged because they overlap with known atmospheric ozone lines"
                           "".format(rulename, os.path.basename(table), spw, pol, ants_as_str,
                                     rejected_channel_ranges, rejected_antenna_names))

                    # add a message at the "attention" level, creating a notification banner in the weblog
                    _log_outlier(msg, logging.ATTENTION)

                # check again if any outliers remained after excluding those within ozone lines
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data], reverse=True))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}{}.\n"
                       "Data: median = {}, MAD = {}. Max MAD threshold = {}, corresponding to {}.\n"
                       "{} outlier(s) found (highest to lowest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, ants_as_str, data_median, data_mad,
                                 mad_max, outlier_threshold, len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag command to flag data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='outlier', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'low outlier':

                # Stop evaluating rule if sample is too small.
                if nvalid < rule['minsample']:
                    continue

                # Check limits.
                mad_max = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(data_median - data, mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_threshold = mad_max * data_mad
                data_masked = np.ma.masked_greater(data_masked, outlier_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flagged data.
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data], reverse=True))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Data: median = {}, MAD = {}. Max MAD threshold = {}, corresponding to {}.\n"
                       "{} outlier(s) found (highest to lowest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, data_median, data_mad, mad_max,
                                 outlier_threshold, len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag commands to flag data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='low_outlier', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'high outlier':

                # Stop evaluating rule if sample is too small.
                if nvalid < rule['minsample']:
                    continue

                # Get threshold limit.
                mad_max = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(data - data_median, mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_threshold = mad_max * data_mad
                data_masked = np.ma.masked_greater(data_masked, outlier_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flags
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data], reverse=True))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Data: median = {}, MAD = {}. Max MAD threshold = {}, corresponding to {}.\n"
                       "{} outlier(s) found (highest to lowest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, data_median, data_mad, mad_max,
                                 outlier_threshold, len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag commands to flag data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='high_outlier', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'min abs':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Check limits.
                limit = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                data_masked = np.ma.masked_less(data_masked, limit)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flags
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data]))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Minimum threshold = {}.\n"
                       "{} outlier(s) found (lowest to highest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, limit,
                                 len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag commands to flag data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='min_abs', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'max abs':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Check limits.
                limit = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                data_masked = np.ma.masked_greater(data_masked, limit)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flags
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data], reverse=True))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Maximum threshold = {}.\n"
                       "{} outlier(s) found (highest to lowest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, limit,
                                 len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag commands to flag data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='max_abs', filename=table, rulename=rulename,  spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'too many flags':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                maxfraction = rule['limit']
                maxexcessflags = rule['excess limit']
                axis = rule['axis']
                axis = axis.upper().strip()

                if axis == xtitle.upper().strip():

                    # Compute median number flagged
                    num_flagged = np.zeros([np.shape(data)[1]], int)
                    for iy in np.arange(len(ydata)):
                        num_flagged[iy] = len(data[:, iy][flag[:, iy]])
                    median_num_flagged = np.median(num_flagged)

                    # look along x axis
                    for iy in np.arange(len(ydata)):
                        if all(flag[:, iy]):
                            continue

                        # Compute fraction flagged
                        len_data = len(xdata)
                        len_no_data = len(data[:, iy][nodata[:, iy]])
                        len_flagged = len(data[:, iy][flag[:, iy]])
                        fractionflagged = (
                          float(len_flagged - len_no_data) /
                          float(len_data - len_no_data))
                        if fractionflagged > maxfraction:
                            i2flag = i[:, iy][np.logical_not(flag[:, iy])]
                            j2flag = j[:, iy][np.logical_not(flag[:, iy])]
                        else:
                            i2flag = np.zeros([0], int)
                            j2flag = np.zeros([0], int)

                        # likewise for maxexcessflags
                        if len_flagged > median_num_flagged + maxexcessflags:
                            i2flag = np.concatenate((i2flag, i[:, iy][np.logical_not(flag[:, iy])]))
                            j2flag = np.concatenate((j2flag, j[:, iy][np.logical_not(flag[:, iy])]))

                        # If row did not have too many flags, skip row.
                        if len(i2flag) <= 0:
                            continue

                        # Log a debug message about outliers.
                        msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                               "Max fraction threshold {}; max nr excess flags above median nr of flags {}.\n"
                               "Data: medium nr flagged {}.\n"
                               "For row {}, number flagged = {}, fraction flagged = {}, exceeding thresholds; "
                               " entire row will be flagged."
                               "".format(rulename, os.path.basename(table), spw, pol, maxfraction, maxexcessflags,
                                         median_num_flagged, iy, len_flagged, fractionflagged))
                        _log_outlier(msg)

                        # Add new flag commands to flag data underlying
                        # the view.
                        for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                            newflags.append(arrayflaggerbase.FlagCmd(
                                reason='too_many_flags', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                                axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                        # Flag the view, for any subsequent rules being evaluated.
                        flag[i2flag, j2flag] = True
                        flag_reason[i2flag, j2flag] =\
                            self.flag_reason_index[rulename]

                elif axis == ytitle.upper().strip():

                    # Compute median number flagged
                    num_flagged = np.zeros([np.shape(data)[0]], int)
                    for ix in np.arange(len(xdata)):
                        num_flagged[ix] = len(data[ix, :][flag[ix, :]])
                    median_num_flagged = np.median(num_flagged)

                    # look along y axis
                    for ix in np.arange(len(xdata)):
                        if all(flag[ix, :]):
                            continue

                        len_data = len(ydata)
                        len_no_data = len(data[ix, :][nodata[ix, :]])
                        len_flagged = len(data[ix, :][flag[ix, :]])
                        fractionflagged = (
                            float(len_flagged - len_no_data) /
                            float(len_data - len_no_data))
                        if fractionflagged > maxfraction:
                            i2flag = i[ix, :][np.logical_not(flag[ix, :])]
                            j2flag = j[ix, :][np.logical_not(flag[ix, :])]
                        else:
                            i2flag = np.zeros([0], int)
                            j2flag = np.zeros([0], int)

                        len_flagged = len(data[ix, :][flag[ix, :]])
                        if len_flagged > median_num_flagged + maxexcessflags:
                            i2flag = np.concatenate((i2flag, i[ix, :][np.logical_not(flag[ix, :])]))
                            j2flag = np.concatenate((j2flag, j[ix, :][np.logical_not(flag[ix, :])]))

                        # If no column had too many flags, skip column.
                        if len(i2flag) <= 0:
                            continue

                        # Log a debug message about outliers.
                        msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                               "Max fraction threshold {}; max nr excess flags above median nr of flags {}.\n"
                               "Data: medium nr flagged {}.\n"
                               "For column {}, number flagged = {}, fraction flagged = {}, exceeding thresholds; "
                               " entire column will be flagged."
                               "".format(rulename, os.path.basename(table), spw, pol, maxfraction, maxexcessflags,
                                         median_num_flagged, ix, len_flagged, fractionflagged))
                        _log_outlier(msg)

                        # Add new flag commands to flag data underlying
                        # the view.
                        for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                            newflags.append(arrayflaggerbase.FlagCmd(
                                reason='too_many_flags', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                                axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                        # Flag the view, for any subsequent rules being evaluated.
                        flag[i2flag, j2flag] = True
                        flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'too many entirely flagged':

                # Stop evaluating rule if all data is flagged, unless
                # explicitly overridden.
                if np.all(flag) and self.inputs.skip_fully_flagged:
                    continue

                maxfraction = rule['limit']
                axis = rule['axis']
                axis = axis.upper().strip()

                # if flagging for each element on x-axis (i.e. evaluate column by column)
                if axis == xtitle.upper().strip():

                    # Determine fraction of columns that are entirely flagged
                    frac_ef = np.count_nonzero(np.all(flag, axis=1)) / float(flag.shape[0])

                    # If the fraction of "entirely flagged" columns exceeds the limit, then
                    # all non-flagged data will need to be flagged.
                    if frac_ef >= maxfraction:

                        # Indices to flag are all those that are currently not flagged
                        i2flag = i[np.logical_not(flag)]
                        j2flag = j[np.logical_not(flag)]

                        # PIPE-566: if the entire view was already flagged,
                        # potentially because no valid data were available in
                        # caltable, then create an explicit flagging command
                        # for the flagged columns; depending on "table" input
                        # parameter of the flagsetter task, this will either
                        # re-flag the same data (unnecessary, but harmless), or
                        # it may be used to flag the underlying data in the MS.
                        if frac_ef == 1.0:
                            i2flag = i[flag]
                            j2flag = j[flag]

                        # Log a debug message about outliers.
                        msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                               "Threshold for entirely flagged columns: {}.\n"
                               "Fraction of entirely flagged columns {} reached or exceeded threshold, entire view will"
                               " be flagged.".format(rulename, os.path.basename(table), spw, pol, maxfraction, frac_ef))
                        _log_outlier(msg)

                        # Add new flag commands to flag data underlying the view.
                        for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                            newflags.append(arrayflaggerbase.FlagCmd(
                                reason='too_many_flags', filename=table, rulename=rulename,  spw=spw, antenna=antenna,
                                axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                        # Flag the view, for any subsequent rules being evaluated.
                        flag[i2flag, j2flag] = True
                        flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'nmedian':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Check limits.
                lo_limit = rule['lo_limit']
                hi_limit = rule['hi_limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(data, mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_high_threshold = hi_limit * data_median
                outlier_low_threshold = lo_limit * data_median
                data_masked = np.ma.masked_greater(data_masked, outlier_high_threshold)
                data_masked = np.ma.masked_less(data_masked, outlier_low_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                new_flag = np.logical_and(np.ma.getmask(data_masked),
                                          np.logical_not(flag))

                # No flags
                if not np.any(new_flag):
                    continue

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[new_flag].data]))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Data: median = {}. Low, high nmedian thresholds = {}, {}, corresponding to {}, {}.\n"
                       "{} outlier(s) found (lowest to highest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, data_median, lo_limit,
                                 hi_limit, outlier_low_threshold, outlier_high_threshold,
                                 len(data_masked[new_flag].data), outliers_as_str))
                _log_outlier(msg)

                i2flag = i[new_flag]
                j2flag = j[new_flag]

                # Add new flag commands to flag the data underlying the view.
                for flagcoord in zip(xdata[i2flag], ydata[j2flag]):
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='nmedian', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                # Flag the view, for any subsequent rules being evaluated.
                flag[i2flag, j2flag] = True
                flag_reason[i2flag, j2flag] = self.flag_reason_index[rulename]

            elif rulename == 'bad antenna':
                # this test should be run before the others as it depends on no other
                # flags having been set by other rules before it
                # (because the number of unflagged points on entry are part of the test)

                # Stop evaluating rule if all data is flagged or if the x-axis is not for antenna.
                if np.all(flag) or 'ANTENNA' not in xtitle.upper():
                    continue

                # Check limits.
                mad_max = rule['lo_limit']
                frac_limit = rule['frac_limit']
                number_limit = rule['number_limit']
                minsample = rule['minsample']

                # For every antenna on the x-axis...
                for iant in range(np.shape(flag)[0]):
                    # For current antenna, create references to the
                    # corresponding column in data, flag, and flag_reason.
                    ant_data = data[iant, :]
                    ant_flag = flag[iant, :]
                    ant_flag_reason = flag_reason[iant, :]

                    # Identify valid (non-flagged) data.
                    valid_ant_data = ant_data[np.logical_not(ant_flag)]

                    # If the sample of unflagged datapoints is smaller than
                    # the minimum threshold, skip this antenna.
                    if len(valid_ant_data) < minsample:
                        continue

                    # Create masked array with flagged data masked.
                    ant_data_masked = np.ma.array(data_median - ant_data, mask=ant_flag)

                    # Create new masked array from masked array with outliers
                    # masked. This should avoid performing a comparison with
                    # flagged data that could include NaNs (that would cause a
                    # RuntimeWarning).
                    outlier_threshold = mad_max * data_mad
                    ant_data_masked = np.ma.masked_greater(ant_data_masked, outlier_threshold)

                    # Get indices to flag as the masked elements that were not
                    # already flagged, i.e. the newly masked elements.
                    new_flag = np.logical_and(np.ma.getmask(ant_data_masked), np.logical_not(ant_flag))

                    # If no low outliers were found, skip this antenna.
                    if not np.any(new_flag):
                        continue

                    j2flag_lo = j[iant, :][new_flag]

                    # Determine number of points found to be low outliers that
                    # were not previously flagged.
                    nflags = len(j2flag_lo)

                    # Determine fraction of newly found low outliers over
                    # total number of data points in current antenna data
                    # selection.
                    flagsfrac = float(nflags) / float(np.shape(ant_flag)[0])

                    # If the number of newly found low outliers equals-or-exceeds
                    # a minimum threshold number, and the fraction of newly
                    # found flags exceeds a minimum threshold fraction, then
                    # proceed with actually generating flagging commands.
                    if nflags >= number_limit or flagsfrac > frac_limit:

                        # If we get here, then a sufficient number and
                        # fraction of low outliers were identified for
                        # the current antenna, such that the antenna is
                        # considered "bad" and should be flagged entirely.

                        # In this case, the low outlier data points are
                        # explicitly flagged as "low outlier", while the
                        # remaining non-flagged data points for this antenna
                        # are flagged as "bad antenna".

                        # For current antenna data selection, flag the points
                        # that were identified to be low outliers and not
                        # already flagged, and set corresponding flag reason.
                        ant_flag[j2flag_lo] = True
                        ant_flag_reason[j2flag_lo] =\
                            self.flag_reason_index['low outlier']

                        # Log a debug message with outliers.
                        outliers_as_str = ", ".join(sorted([str(ol) for ol in ant_data_masked[new_flag].data]))
                        msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                               "Data: median = {}, MAD = {}. Max MAD threshold = {}, corresponding to {}.\n"
                               "For antenna {}: {} low outlier(s) found, representing {} fraction of its data"
                               " points, which is above number threshold ({}) for number and/or above fraction"
                               " threshold ({}); flagging these data points for antenna {} as low outliers: {}."
                               "".format(rulename, os.path.basename(table), spw, pol, data_median, data_mad,
                                         mad_max, outlier_threshold, iant, nflags, flagsfrac, number_limit,
                                         frac_limit, iant, outliers_as_str))
                        _log_outlier(msg)

                        # Create a flagging command that flags these
                        # low outliers in the data.
                        for flagcoord in zip(xdata[[iant]], ydata[j2flag_lo]):
                            newflags.append(arrayflaggerbase.FlagCmd(
                                reason='low outlier', filename=table, rulename='low outlier', spw=spw, antenna=antenna,
                                axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                        # For current antenna data selection, identify the
                        # remaining non-flagged data points.
                        j2flag_bad = j[iant, :][np.logical_not(ant_flag)]

                        # Flag the remaining non-flagged data points as
                        # "bad antenna"; these are references to original view
                        # which is thus updated for any subsequent rules being evaluated.
                        ant_flag[j2flag_bad] = True
                        ant_flag_reason[j2flag_bad] = self.flag_reason_index['bad antenna']

                        # Log a debug message for this antenna.
                        LOG.debug("Flagging remaining data points for antenna {} as 'bad antenna'.".format(iant))

                        # Create a flagging command that flags the remaining
                        # data points as "bad antenna".
                        for flagcoord in zip(xdata[[iant]], ydata[j2flag_bad]):
                            newflags.append(arrayflaggerbase.FlagCmd(
                                reason='bad antenna', filename=table, rulename='bad antenna', spw=spw, antenna=antenna,
                                axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'bad quadrant':
                # this test should be run before the others as it depends on no other
                # flags having been set by other rules before it
                # (because the number of unflagged points on entry are part of the test)

                # a quadrant is one quarter of the extent of the x-axis

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Check limits.
                hilo_limit = rule['hilo_limit']
                frac_limit = rule['frac_limit']
                baseline_frac_limit = rule['baseline_frac_limit']

                # find outlier flags first
                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data - data_median), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                data_masked = np.ma.masked_greater(data_masked, hilo_limit * data_mad)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                provisional_new_flag = np.logical_and(data_masked.mask, np.logical_not(flag))

                # No flagged data.
                if not np.any(provisional_new_flag):
                    continue

                # PIPE-344: If the flagged channels fall within ozone lines, then ignore (filter out) these outliers;
                # store the "unfiltered" provisional list of flags and later compare it with the "filtered" one
                # in which ozone lines are removed, in order to show a notification message when such removal
                # affects the outcome of the "bad quadrant" rule (at this moment, it is not yet known
                # whether a flagging would happen, because it depends on all baselines for a given antenna).
                ozone_channels = ozone.get_ozone_channels_for_spw(self.inputs.ms, spw)
                provisional_new_flag_unfiltered = provisional_new_flag.copy()
                provisional_new_flag[ozone_channels] = False

                # check again if any outliers remained after excluding those within ozone lines
                if not np.any(provisional_new_flag):
                    continue

                # store the previous flagging state in 'previous_flag',
                # then examine the provisional new flags one antenna and quadrant at a time,
                # checking if the fraction of new flags in all baselines involving this antenna
                # is above threshold (frac_limit) or if this fraction in individual baselines
                # is above another threshold (baseline_frac_limit).
                # If so, flag the entire quadrant for this antenna or for individual baseline.
                previous_flag = np.copy(flag)

                # look for bad antenna/quadrants in view copy
                data_shape = np.shape(data)
                nchan = data_shape[0]
                nbaseline = data_shape[1]
                nant = int(math.sqrt(nbaseline))

                quadrant = [
                    [0, nchan//4],
                    [nchan//4, nchan//2],
                    [nchan//2, nchan*3//4],
                    [nchan*3//4, nchan],
                ]
                rejected_flagging = np.zeros((nchan, nant), bool)

                for ant in range(nant):
                    # baselines involving this antenna
                    baselines = np.array([baseline
                                          for baseline in range(nbaseline)
                                          if (ant*nant <= baseline < (ant+1)*nant)
                                          or (baseline % nant == ant)])

                    for iquad in range(4):
                        # first check all baselines involving this antenna in this quadrant,
                        # examining the ratio of the number of provisional new flags
                        # to the number of previously unflagged channels
                        quad_slice = slice(quadrant[iquad][0], quadrant[iquad][1])
                        num_provisional_new_flag = np.count_nonzero(
                            provisional_new_flag[quad_slice, baselines])
                        num_provisional_new_flag_unfiltered = np.count_nonzero(
                            provisional_new_flag_unfiltered[quad_slice, baselines])
                        num_previous_unflagged = np.count_nonzero(np.logical_not(
                            previous_flag[quad_slice, baselines]))
                        if num_previous_unflagged:
                            frac = num_provisional_new_flag * 1.0 / num_previous_unflagged
                            frac_unfiltered = num_provisional_new_flag_unfiltered * 1.0 / num_previous_unflagged
                        else:
                            frac = frac_unfiltered = 0.0

                        if frac > frac_limit:
                            # Add new flag commands to flag the data underlying the view.
                            # These will flag the entire quadrant/antenna.
                            # If the quadrant is not bad, then any provisional outlier points
                            # found earlier will not be flagged.
                            flagcoords = []
                            channels_to_flag = list(range(quadrant[iquad][0], quadrant[iquad][1]))
                            for chan in channels_to_flag:
                                flagcoords.append((chan, ant))

                            # Log a debug message with outliers.
                            msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                                   "Threshold for maximum number of outliers per channel quadrant per antenna: {}.\n"
                                   "For antenna {}, channels quadrant {}: fraction outliers = {}, exceeding "
                                   "threshold => entire quadrant will be flagged."
                                   "".format(rulename, os.path.basename(table), spw, pol, frac_limit, ant, iquad, frac))
                            _log_outlier(msg)

                            for flagcoord in flagcoords:
                                newflags.append(arrayflaggerbase.FlagCmd(
                                    reason='bad quadrant', filename=table, rulename=rulename, spw=spw,
                                    antenna=antenna, axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                    extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                            # update flagging view with 'bad quadrant' flags
                            where = np.logical_not(previous_flag[quad_slice, baselines])
                            i2flag = i[quad_slice, baselines][where]
                            j2flag = j[quad_slice, baselines][where]
                            flag[i2flag, j2flag] = True
                            flag_reason[i2flag, j2flag] = self.flag_reason_index['bad quadrant']

                            # whole antenna/quadrant flagged, no need to check individual baselines
                            continue

                        elif frac_unfiltered > frac_limit:
                            # fraction of provisional new flags is below the threshold,
                            # but had ozone lines not been rejected, it would have been above the threshold:
                            # in this case, collect the information about channel/antenna pairs that were removed from
                            # the flagging list add a notification message after all such cases have been identified.

                            # determine indices of channels in this quadrant and antenna
                            # that were initially flagged but subsequently removed from the flagging list
                            rejected_channels = np.where(np.logical_and(
                                ozone_channels[quad_slice],
                                np.count_nonzero(
                                    provisional_new_flag_unfiltered[quad_slice, baselines] !=
                                    provisional_new_flag[quad_slice, baselines],
                                    axis=1) > 0))[0]

                            # mark up these channels in the overall table, adding a correct offset for the quadrant
                            rejected_flagging[rejected_channels + quadrant[iquad][0], ant] = True

                        # if the entire antenna was not flagged, look for individual bad baselines in this quadrant
                        for baseline in baselines:
                            num_provisional_new_flag = np.count_nonzero(
                                provisional_new_flag[quad_slice, baseline])
                            num_provisional_new_flag_unfiltered = np.count_nonzero(
                                provisional_new_flag_unfiltered[quad_slice, baseline])
                            num_previous_unflagged = np.count_nonzero(np.logical_not(
                                previous_flag[quad_slice, baseline]))
                            if num_previous_unflagged:
                                frac = num_provisional_new_flag * 1.0 / num_previous_unflagged
                                frac_unfiltered = num_provisional_new_flag_unfiltered * 1.0 / num_previous_unflagged
                            else:
                                frac = frac_unfiltered = 0.0

                            if frac > baseline_frac_limit:
                                # Add new flag commands to flag the data underlying the view.
                                # These will flag the entire quadrant/baseline.
                                # If the quadrant is not bad, then any provisional outlier points
                                # found earlier will not be flagged.
                                flagcoords = []
                                for chan in range(quadrant[iquad][0], quadrant[iquad][1]):
                                    flagcoords.append((chan, ydata[baseline]))

                                # Log a debug message with outliers.
                                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                                       "Threshold for maximum number of outliers per channel quadrant per baseline:"
                                       " {}.\n"
                                       "For baseline {}, channels quadrant {}: fraction outliers = {}, exceeding "
                                       "threshold => entire quadrant will be flagged."
                                       "".format(rulename, os.path.basename(table), spw, pol, baseline_frac_limit,
                                                 baseline, iquad, frac))
                                _log_outlier(msg)

                                for flagcoord in flagcoords:
                                    newflags.append(arrayflaggerbase.FlagCmd(
                                        reason='bad quadrant', filename=table, rulename=rulename, spw=spw,
                                        antenna=antenna, axisnames=[xtitle, ytitle], flagcoords=flagcoord, pol=pol,
                                        extendfields=self.inputs.extendfields, antenna_id_to_name=antenna_id_to_name))

                                # update flagging view with 'bad quadrant' flags
                                where = np.logical_not(previous_flag[quad_slice, baseline])
                                i2flag = i[quad_slice, baseline][where]
                                j2flag = j[quad_slice, baseline][where]
                                flag[i2flag, j2flag] = True
                                flag_reason[i2flag, j2flag] = self.flag_reason_index['bad quadrant']

                            elif frac_unfiltered > baseline_frac_limit:
                                # determine which channels were initially flagged but subsequently rejected
                                rejected_channels = np.where(np.logical_and(
                                    ozone_channels[quad_slice],
                                    provisional_new_flag_unfiltered[quad_slice, baseline] !=
                                    provisional_new_flag[quad_slice, baseline]))[0]

                                # mark up these channels in the overall table, adding a correct offset for the quadrant
                                # (do not memorize the individual baselines, but only the antenna index)
                                rejected_flagging[rejected_channels + quadrant[iquad][0], ant] = True

                # PIPE-344: add a notification message if the bad_quadrant flagging was rejected due to ozone lines.
                # do not report all possible channel/antenna combinations, but only the 1d projections of this 2d matrix
                if np.any(rejected_flagging):
                    # compressed (human-readable) list of channels in which at least one antenna was affected
                    rejected_channel_ranges = find_ranges(np.where(np.any(rejected_flagging, axis=1))[0])

                    # list of affected antennas (those for which flagging was rejected in at least one channel)
                    rejected_antenna_names = ','.join([
                        (antenna_id_to_name[ant] if antenna_id_to_name else str(ant))
                        for ant in np.where(np.any(rejected_flagging, axis=0))[0]
                    ])

                    msg = ("Outliers provisionally found with flagging rule '{}' for {}, spw {}, pol {}, channel {}, "
                           "antenna {}, were not flagged because they overlap with known atmospheric ozone lines"
                           "".format(rulename, os.path.basename(table), spw, pol,
                                     rejected_channel_ranges, rejected_antenna_names))
                    _log_outlier(msg, logging.ATTENTION)

            else:
                raise NameError('bad rule: %s' % rule)

        # consolidate flagcmds that specify individual channels into fewer flagcmds that specify ranges
        newflags = arrayflaggerbase.consolidate_flagcmd_channels(newflags, antenna_id_to_name=antenna_id_to_name)

        return newflags, flag_reason


class VectorFlaggerInputs(vdp.StandardInputs):
    prepend = vdp.VisDependentProperty(default='')
    use_antenna_names = vdp.VisDependentProperty(default=True)

    def __init__(self, context, output_dir=None, vis=None, datatask=None, viewtask=None, flagsettertask=None,
                 rules=None, niter=None, iter_datatask=None, use_antenna_names=None, prepend=None):
        super(VectorFlaggerInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # solution parameters
        self.datatask = datatask
        self.flagsettertask = flagsettertask
        self.iter_datatask = iter_datatask
        self.niter = niter
        self.prepend = prepend
        self.rules = rules
        self.use_antenna_names = use_antenna_names
        self.viewtask = viewtask


class VectorFlaggerResults(basetask.Results, flaggableviewresults.FlaggableViewResults):
    def __init__(self, vis=None):
        """
        Construct and return a new VectorFlaggerResults.
        """
        basetask.Results.__init__(self)
        flaggableviewresults.FlaggableViewResults.__init__(self)

        self.vis = vis
        self.dataresult = None
        self.viewresult = None

    def merge_with_context(self, context):
        pass

    def __repr__(self):
        s = 'VectorFlaggerResults'
        return s


class VectorFlagger(basetask.StandardTaskTemplate):
    Inputs = VectorFlaggerInputs

    # override the inherited __init__ method so that references to the
    # task objects can be kept outside self.inputs. Later on self.inputs
    # will be replaced by a copy which breaks the connection between
    # its references to the tasks and the originals.
    def __init__(self, inputs):
        self.inputs = inputs

    def prepare(self):
        inputs = self.inputs

        # Initialize result.
        result = VectorFlaggerResults()

        # Initialize flags and iteration counter
        flags = []
        newflags = []
        counter = 1
        include_before = True
        dataresult = None
        viewresult = None

        # Start iterative flagging
        while counter <= inputs.niter:

            # Run the data task if needed
            if counter == 1:
                # Always run data task on first iteration
                dataresult = self._executor.execute(self.inputs.datatask)
            elif inputs.iter_datatask is True:
                # If requested to re-run datatask on iteration, then
                # run the flag-setting task which modifies the data
                # and then re-run the data task

                # If no "before summary" was done, include this in the flag setting task
                if include_before:
                    stats_before, _ = self.set_flags(newflags, summarize_before=True)
                    include_before = False
                else:
                    _, _ = self.set_flags(newflags)

                dataresult = self._executor.execute(inputs.datatask)
            else:
                # If not iterating the datatask, the previous
                # data result will be re-used, but marked here as no
                # longer new.
                dataresult.new = False

            # Create flagging view
            viewresult = inputs.viewtask(dataresult)

            # If a view could be created, continue with flagging
            if viewresult.descriptions():

                # Import the views from viewtask into the final result
                result.importfrom(viewresult)

                # Flag the view
                newflags = self.flag_view(viewresult)

                # Report how many flags were found in this iteration and
                # stop iteration if no new flags were found
                if len(newflags) == 0:
                    # If no new flags are found, report as a log message
                    LOG.info("{0}{1} iteration {2} raised {3} flagging commands"
                             "".format(inputs.prepend, os.path.basename(inputs.vis), counter, len(newflags)))
                    break
                else:
                    # Report newly found flags (CAS-7336: show as info message instead of warning).
                    LOG.info("{0}{1} iteration {2} raised {3} flagging commands"
                             "".format(inputs.prepend, os.path.basename(inputs.vis), counter, len(newflags)))

                # Accumulate new flags
                flags += newflags

                counter += 1
            else:
                # If no view could be created, exit the iteration
                LOG.warning('No flagging view was created!')
                break

        # Create final set of flags by removing duplicates from our accumulated flags
        flags = list(set(flags))

        # If flags were found...
        if len(flags) > 0:

            # If newflags were found on last iteration loop, we need to still set
            # these.
            if len(newflags) > 0:

                # If datatask needs to be iterated...
                if inputs.iter_datatask is True:

                    # First set the new flags that were found on the last
                    # iteration. If the "before" summary was not yet created,
                    # then include this here; always include the "after"
                    # summary.
                    if include_before:
                        # Set flags, and include "before" and "after" summary.
                        stats_before, stats_after = self.set_flags(
                            newflags, summarize_before=True, summarize_after=True)
                    else:
                        # Set flags, and include "after" summary
                        _, stats_after = self.set_flags(
                            newflags, summarize_after=True)

                    # After setting the latest flags, re-run the data task.
                    dataresult = self._executor.execute(inputs.datatask)

                # If the datatask did not need to be iterated, then no flags
                # were set yet and no "before" summary was performed yet, so
                # set all flags and include both "before" and "after" summary.
                else:
                    stats_before, stats_after = self.set_flags(
                        flags, summarize_before=True, summarize_after=True)

                # Create final post-flagging view
                viewresult = inputs.viewtask(dataresult)

                # Import the post-flagging view into the final result
                result.importfrom(viewresult)

            # If flags were found, but no newflags were found on last iteration
            # then the dataresult is already up-to-date, and all that is needed
            # is to ensure the flags are set, and that summaries are created.
            else:

                # If datatask needs to be iterated, then the "before" summary has
                # already been done, and the flags have already been set, so only
                # need to do an "after" summary.
                if inputs.iter_datatask is True:
                    _, stats_after = self.set_flags([], summarize_after=True)
                # If the datatask did not need to be iterated, then no flags
                # were set yet and no "before" summary was performed yet,
                # so set all flags and include both "before" and "after" summary.
                else:
                    stats_before, stats_after = self.set_flags(
                        flags, summarize_before=True, summarize_after=True)

            # Store the final set of flags in the final result
            result.addflags(flags)

        # if no flags were found at all
        else:
            # Run a single flagging summary and use the result as both the "before"
            # and "after" summary.
            stats_before, _ = self.set_flags(flags, summarize_before=True)
            stats_after = copy.deepcopy(stats_before)

        # Store in the final result the name of the measurement set or caltable
        # to which any potentially found flags would need to be applied to.
        result.table = inputs.flagsettertask.inputs.table

        # Store in the final result the final data task result and the final
        # view task result.
        result.dataresult = dataresult
        result.viewresult = viewresult

        # Store the flagging summaries in the final result
        result.summaries = [stats_before, stats_after]

        # Sort the final list of flagging commands.
        result.sort_flagcmds()

        return result

    def analyse(self, result):
        return result

    def flag_view(self, view):
        newflags = []
        descriptionlist = sorted(view.descriptions())
        for description in descriptionlist:
            image = view.last(description)
            # get flags for this view according to the rules
            newflags += self.generate_flags(image)

        return newflags

    def set_flags(self, flags, summarize_before=False, summarize_after=False):
        # Initialize flagging summaries
        allflagcmds = []

        # Add the "before" summary to the flagging commands
        if summarize_before:
            allflagcmds = ["mode='summary' name='before'"]

        # Add the flagging commands
        allflagcmds.extend(flags)

        # Add the "before" summary to the flagging commands
        if summarize_after:
            allflagcmds.append("mode='summary' name='after'")

        # Update flag setting task with all flagging commands
        self.inputs.flagsettertask.flags_to_set(allflagcmds)

        # Run flag setting task
        flagsetterresult = self._executor.execute(self.inputs.flagsettertask)

        # Initialize "before" and/or "after" summaries. If "real" flagsetter
        # results are returned (e.g. by WvrgcalFlagSetter), then there will
        # have been no real flagging summaries created, in which case empty
        # dictionaries will be returned as empty flagging summaries.
        stats_before = {}
        stats_after = {}

        # If the flagsetter returned results from the CASA flag data task,
        # then proceed to extract "before" and/or "after" flagging summaries.
        if flagsetterresult.results:
            # CAS-10407: if MPI version of flagdata failed and returned invalid
            # results, then raise an exception.
            if flagsetterresult.results[0] is None:
                raise exceptions.PipelineException("Results from flagdata are empty, cannot continue.")
            if all(['report' in k for k in flagsetterresult.results[0]]):
                # Go through dictionary of reports...
                for report in flagsetterresult.results[0]:
                    if flagsetterresult.results[0][report]['name'] == 'before':
                        stats_before = flagsetterresult.results[0][report]
                    if flagsetterresult.results[0][report]['name'] == 'after':
                        stats_after = flagsetterresult.results[0][report]
            else:
                # Go through single report.
                if flagsetterresult.results[0]['name'] == 'before':
                    stats_before = flagsetterresult.results[0]
                if flagsetterresult.results[0]['name'] == 'after':
                    stats_after = flagsetterresult.results[0]

        return stats_before, stats_after

    @staticmethod
    def make_flag_rules(
            flag_edges=False, edge_limit=2.0,
            flag_minabs=False, fmin_limit=0.0,
            flag_nmedian=False, fnm_lo_limit=0.7, fnm_hi_limit=1.3,
            flag_hilo=False, fhl_limit=5.0, fhl_minsample=5,
            flag_sharps=False, sharps_limit=0.05,
            flag_diffmad=False, diffmad_limit=10, diffmad_nchan_limit=4,
            flag_tmf=None, tmf_frac_limit=0.1, tmf_nchan_limit=4):
        """
        Generate a list of flagging rules from a set of flagging parameters.
        Added detailed docs here.
        """

        # Construct rules from flag properties
        rules = []
        if flag_edges:
            rules.append({'name': 'edges', 'limit': edge_limit})
        if flag_minabs:
            rules.append({'name': 'min abs', 'limit': fmin_limit})
        if flag_nmedian:
            rules.append({'name': 'nmedian', 'lo_limit': fnm_lo_limit, 'hi_limit': fnm_hi_limit})
        if flag_hilo:
            rules.append({'name': 'outlier', 'limit': fhl_limit, 'minsample': fhl_minsample})
        if flag_sharps:
            rules.append({'name': 'sharps', 'limit': sharps_limit})
        if flag_diffmad:
            rules.append({'name': 'diffmad', 'limit': diffmad_limit, 'nchan_limit': diffmad_nchan_limit})
        if flag_tmf:
            rules.append({'name': 'tmf', 'frac_limit': tmf_frac_limit, 'nchan_limit': tmf_nchan_limit})

        return rules

    def generate_flags(self, vector):
        """
        Calculate the statistics of a vector and flag the data according
        to a list of specified rules.

        Keyword arguments:
        vector - SpectrumResult object containing data to be flagged.
        rules - Rules to be applied.
        """

        # Get the attributes - ensure all arrays are numpy arrays
        # as some subsequent processing depends on numpy array indexing
        data = np.array(vector.data)
        flag = np.array(vector.flag)
        spw = vector.spw
        pol = vector.pol
        antenna = vector.ant
        if antenna is not None:
            # deal with antenna id not name
            antenna = antenna[0]
        table = vector.filename

        # Initialize flags
        newflags = []

        # If there is no valid (non-flagged) data, then return early.
        if np.all(flag):
            return newflags

        # If requested to use antenna names instead of IDs antenna,
        # create an id-to-name translation and check to make sure this
        # would result in unique non-empty names for all IDs, otherwise
        # revert back to flagging by ID
        if self.inputs.use_antenna_names:
            antenna_id_to_name = _get_ant_id_to_name_dict(self.inputs.ms)
        else:
            antenna_id_to_name = {}

        # any flags found will apply to this subset of the data
        axisnames = []
        flagcoords = []
        if antenna is not None:
            axisnames.append('ANTENNA1')
            flagcoords.append(antenna)
        axisnames = ['channels']

        # Identify valid (non-flagged) data.
        valid_data = data[np.logical_not(flag)]

        # Calculate statistics for valid data.
        data_median, data_mad = arrayflaggerbase.median_and_mad(valid_data)

        # Create channel array.
        nchannels = len(data)
        channels = np.arange(nchannels)

        # flag data according to each rule in turn
        for rule in self.inputs.rules:
            rulename = rule['name']

            if rulename == 'edges':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                limit = rule['limit']

                # find left edge
                left_edge = VectorFlagger._find_small_diff(data, flag, limit, vector.description)

                # and right edge
                reverse_data = data[-1::-1]
                reverse_flag = flag[-1::-1]
                right_edge = VectorFlagger._find_small_diff(reverse_data, reverse_flag, limit, vector.description)

                # flag the 'view', for any subsequent rules being evaluated.
                flag[:left_edge] = True
                if right_edge > 0:
                    flag[-right_edge:] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[np.logical_or(channels < left_edge, channels > (nchannels-1-right_edge))]
                flagcoords = [list(channels_flagged)]

                if len(channels_flagged) > 0:
                    # Log a debug message with outliers.
                    flagged_as_str = ", ".join([str(ol) for ol in channels_flagged])
                    msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                           "The following {} edge channels were flagged: {}"
                           "".format(rulename, os.path.basename(table), spw, pol, len(channels_flagged),
                                     flagged_as_str))
                    _log_outlier(msg)

                    # Add new flag command to flag data underlying the
                    # view.
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='edges', filename=table, rulename=rulename, spw=spw, axisnames=axisnames,
                        flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'min abs':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                limit = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                data_masked = np.ma.masked_less(data_masked, limit)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                ind2flag = np.logical_and(np.ma.getmask(data_masked), np.logical_not(flag))

                # No flags
                if not np.any(ind2flag):
                    continue

                # flag the 'view', for any subsequent rules being evaluated.
                flag[ind2flag] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[ind2flag]
                flagcoords = [list(channels_flagged)]

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[ind2flag].data]))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Minimum threshold = {}.\n"
                       "{} outlier(s) found (lowest to highest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, limit,
                                 len(data_masked[ind2flag].data), outliers_as_str))
                _log_outlier(msg)

                # Add new flag command to flag data underlying the
                # view.
                newflags.append(arrayflaggerbase.FlagCmd(
                    reason='min_abs', filename=table, rulename=rulename, spw=spw, axisnames=axisnames,
                    flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'nmedian':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                lo_limit = rule['lo_limit']
                hi_limit = rule['hi_limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(data, mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_high_threshold = hi_limit * data_median
                outlier_low_threshold = lo_limit * data_median
                data_masked = np.ma.masked_greater(data_masked, outlier_high_threshold)
                data_masked = np.ma.masked_less(data_masked, outlier_low_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                ind2flag = np.logical_and(np.ma.getmask(data_masked), np.logical_not(flag))

                # No flags
                if not np.any(ind2flag):
                    continue

                # flag the 'view', for any subsequent rules being evaluated.
                flag[ind2flag] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[ind2flag]
                flagcoords = [list(channels_flagged)]

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[ind2flag].data]))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Data: median = {}. Low, high nmedian thresholds = {}, {}, corresponding to {}, {}.\n"
                       "{} outlier(s) found (lowest to highest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, data_median, lo_limit,
                                 hi_limit, outlier_low_threshold, outlier_high_threshold,
                                 len(data_masked[ind2flag].data), outliers_as_str))
                _log_outlier(msg)

                # Add new flag command to flag data underlying the
                # view.
                newflags.append(arrayflaggerbase.FlagCmd(
                    reason='nmedian', filename=table, rulename=rulename, spw=spw, axisnames=axisnames,
                    flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'outlier':

                minsample = rule['minsample']

                # Stop evaluating rule if sample is too small.
                if len(valid_data) < minsample:
                    continue

                # Get limits.
                limit = rule['limit']

                # Create masked array with flagged data masked.
                data_masked = np.ma.array(np.abs(data - data_median), mask=flag)

                # Create new masked array from masked array with outliers
                # masked. This should avoid performing a comparison with
                # flagged data that could include NaNs (that would cause a
                # RuntimeWarning).
                outlier_threshold = limit * data_mad
                data_masked = np.ma.masked_greater(data_masked, outlier_threshold)

                # Get indices to flag as the masked elements that were not
                # already flagged, i.e. the newly masked elements.
                ind2flag = np.logical_and(np.ma.getmask(data_masked), np.logical_not(flag))

                # No flags
                if not np.any(ind2flag):
                    continue

                # flag the 'view', for any subsequent rules being evaluated.
                flag[ind2flag] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[ind2flag]
                flagcoords = [list(channels_flagged)]

                # Log a debug message with outliers.
                outliers_as_str = ", ".join(sorted([str(ol) for ol in data_masked[ind2flag].data]))
                msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                       "Data: median = {}, MAD = {}. Max MAD threshold = {}, corresponding to {}.\n"
                       "{} outlier(s) found (lowest to highest): {}"
                       "".format(rulename, os.path.basename(table), spw, pol, data_median, data_mad, limit,
                                 outlier_threshold, len(channels_flagged), outliers_as_str))
                _log_outlier(msg)

                # Add new flag command to flag data underlying the
                # view.
                newflags.append(arrayflaggerbase.FlagCmd(
                    reason='outlier', filename=table, rulename=rulename, spw=spw, pol=pol, antenna=antenna,
                    axisnames=axisnames, flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'sharps':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                limit = rule['limit']

                # Compute channel-to-channel difference, and corresponding flag array.
                diff = abs(data[1:] - data[:-1])
                diff_flag = (flag[1:] | flag[:-1])

                # flag channels whose slope is greater than the
                # limit for a 'sharp feature'
                newflag = (diff > limit) & np.logical_not(diff_flag)

                # If no new flags were found, stop evaluating; otherwise,
                # retrieve the ozone line exclusion array.
                if not np.any(newflag):
                    continue
                else:
                    ozone_channels = ozone.get_ozone_channels_for_spw(self.inputs.ms, spw)

                # Prepare string for antenna.
                ant_msg = ""
                if antenna is not None:
                    ant_msg = "ant {} ({}), ".format(antenna, antenna_id_to_name[antenna])

                # now broaden the flags until the diff falls below
                # 2 times the median diff, to catch the wings of
                # sharp features
                if np.any([np.logical_not(diff_flag | newflag)]):
                    median_diff = np.median(diff[np.logical_not(diff_flag | newflag)])
                    median_flag = ((diff > 2 * median_diff) & np.logical_not(diff_flag))
                else:
                    median_flag = newflag

                start = None
                for i in np.arange(len(median_flag)):
                    if median_flag[i]:
                        end = i
                        if start is None:
                            start = i
                    else:
                        if start is not None:
                            # have found start and end of a block
                            # of contiguous True flags. Does the
                            # block contain a sharp feature? If
                            # so broaden the sharp feature flags
                            # to include the whole block
                            if np.any(newflag[start:end]):
                                newflag[start:end] = True
                            start = None

                # Convert new channels-to-flag based on difference array to channels-to-flag within
                # the original array.
                flag_chan = np.zeros([len(newflag)+1], bool)
                flag_chan[:-1] = newflag
                flag_chan[1:] = (flag_chan[1:] | newflag)

                # CAS-12242: reject sharps that could be due to ozone lines.
                # Find ranges of contiguous channels among new channels-to-flag.
                for _, g in itertools.groupby(enumerate(np.where(flag_chan)[0]), lambda i_x: i_x[0] - i_x[1]):
                    rng = list(map(operator.itemgetter(1), g))
                    # Check if single channel or range of channels overlaps with known ozone lines. If so, then
                    # mark that channel (or range of channels) as no longer newly flagged, and log a message.
                    if len(rng) == 1:
                        if ozone_channels[rng[0]]:
                            flag_chan[rng[0]] = False
                            LOG.info("Rejected potential outlier found with flagging rule '{}' for {}, {}"
                                     "spw {}, pol {}, channel {}, since this channel overlaps with an atmospheric "
                                     "ozone line.".format(rulename, os.path.basename(table), ant_msg, spw, pol, rng[0]))
                    else:
                        if np.any(ozone_channels[rng[0]:rng[-1]+1]):
                            flag_chan[rng[0]:rng[-1]+1] = False
                            LOG.info("Rejected potential outlier found with flagging rule '{}' for {}, {}"
                                     "spw {}, pol {}, channels {}-{}, since one or more of these "
                                     "channels overlaps with an atmospheric ozone line."
                                     "".format(rulename, os.path.basename(table), ant_msg, spw, pol, rng[0], rng[-1]))

                # flag the 'view', for any subsequent rules being evaluated.
                flag[flag_chan] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[flag_chan]
                flagcoords = [list(channels_flagged)]

                if len(channels_flagged) > 0:
                    # Log a debug message with outliers.
                    flagged_as_str = ", ".join([str(ol) for ol in channels_flagged])
                    msg = ("Outliers found with flagging rule '{}' for {}, {}spw {}, pol {}.\n"
                           "Sharp feature limit: {}.\n"
                           "The following {} channels will be flagged: {}"
                           "".format(rulename, os.path.basename(table), ant_msg, spw, pol, limit,
                                     len(channels_flagged), flagged_as_str))
                    _log_outlier(msg)

                    # Add new flag command to flag data underlying the
                    # view.
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='sharps', filename=table, rulename=rulename, spw=spw, antenna=antenna,
                        axisnames=axisnames, flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'diffmad':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                limit = rule['limit']
                nchan_limit = rule['nchan_limit']

                # Compute channel-to-channel difference (and associated median
                # and MAD), and corresponding flag array.
                diff = data[1:] - data[:-1]
                diff_flag = np.logical_or(flag[1:], flag[:-1])
                median_diff = np.median(diff[diff_flag == 0])
                mad = np.median(np.abs(diff[diff_flag == 0] - median_diff))

                # first, flag channels further from the median than
                # limit * MAD
                newflag = ((abs(diff-median_diff) > limit*mad) & (diff_flag == 0))

                # second, flag all channels if more than nchan_limit
                # were flagged by the first stage
                if np.count_nonzero(newflag) >= nchan_limit:
                    newflag = np.ones(diff.shape, bool)

                # set channels flagged
                flag_chan = np.zeros([len(newflag)+1], bool)
                flag_chan[:-1] = newflag
                flag_chan[1:] = np.logical_or(flag_chan[1:], newflag)

                # flag the 'view', for any subsequent rules being evaluated.
                flag[flag_chan] = True

                # now compose a description of the flagging required on
                # the MS
                channels_flagged = channels[flag_chan]
                flagcoords = [list(channels_flagged)]

                if len(channels_flagged) > 0:
                    # Log a debug message with outliers.
                    flagged_as_str = ", ".join([str(ol) for ol in channels_flagged])
                    msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                           "The following {} channels were flagged: {}"
                           "".format(rulename, os.path.basename(table), spw, pol,
                                     len(channels_flagged), flagged_as_str))
                    _log_outlier(msg)

                    # Add new flag command to flag data underlying the
                    # view.
                    newflags.append(arrayflaggerbase.FlagCmd(
                        reason='diffmad', filename=table, rulename=rulename, spw=spw, pol=pol, antenna=antenna,
                        axisnames=axisnames, flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            elif rulename == 'tmf':

                # Stop evaluating rule if all data is flagged.
                if np.all(flag):
                    continue

                # Get limits.
                frac_limit = rule['frac_limit']
                nchan_limit = rule['nchan_limit']

                # flag all channels if fraction already flagged
                # is greater than tmf_limit of total
                if (float(np.count_nonzero(flag)) / len(data) >= frac_limit or
                        np.count_nonzero(flag) >= nchan_limit):

                    newflag = np.logical_not(flag)

                    # flag the 'view', for any subsequent rules being evaluated.
                    flag[newflag] = True

                    # now compose a description of the flagging required on
                    # the MS
                    channels_flagged = channels[newflag]
                    flagcoords = [list(channels_flagged)]

                    if len(channels_flagged) > 0:
                        # Log a debug message with outliers.
                        flagged_as_str = ", ".join([str(ol) for ol in channels_flagged])
                        msg = ("Outliers found with flagging rule '{}' for {}, spw {}, pol {}.\n"
                               "Limit on fraction channels flagged before all channels are flagged: {}\n"
                               "Limit on number channels flagged before all channels are flagged: {}\n"
                               "The following {} channels were flagged: {}"
                               "".format(rulename, os.path.basename(table), spw, pol, frac_limit,
                                         nchan_limit, len(channels_flagged), flagged_as_str))
                        _log_outlier(msg)

                        # Add new flag command to flag data underlying the
                        # view.
                        newflags.append(arrayflaggerbase.FlagCmd(
                            reason='tmf', filename=table, rulename=rulename, spw=spw, pol=pol, antenna=antenna,
                            axisnames=axisnames, flagcoords=flagcoords, antenna_id_to_name=antenna_id_to_name))

            else:
                raise NameError('bad rule: %s' % rule)

        return newflags

    @staticmethod
    def _find_noise_edge(mad, flag):
        """Return the index in the mad array where the noise first
        dips below the median value.

        Keyword arguments:
        mad    -- The noise array to be examined.
        flag   -- Array whose elements are True where mad is invalid.

        The index of the first point where the noise dips below twice the median
        for the first half of the spectrum. Looking at half the spectrum
        handles the case where the spectrum is a composite of 2 subbands,
        with different noise levels; it's a fudge in that some spectra may
        be composed of more than 2 subbands.
        """

        noise_edge = None

        nchan = len(mad)
        median_mad = np.median(mad[:nchan//4][np.logical_not(flag[:nchan//4])])
        for i in range(nchan):
            if not flag[i] and mad[i] < 2.0 * median_mad:
                noise_edge = i
                break

        return noise_edge

    @staticmethod
    def _find_small_diff(data, flag, limit=2.0, description='unknown'):
        """Return the index in the first quarter of the data array where the
        point to point difference first falls below a threshold, where
        the threshold is defined as the "limit" * the median point-to-point
        difference.

        Keyword arguments:
        data -- The data array to be examined.
        flag -- Array whose elements are True where list_data is bad.
        limit -- Multiple of median value where the 'edge' will be set.

        Returns:
        The index of the first point where the point to point difference
        first falls below 'limit' times the median value.
        """
        result = None

        nchan = len(data)
        good_data = data[:nchan//4][np.logical_not(flag[:nchan//4])]
        good_data_index = np.arange(nchan//4)[np.logical_not(flag[:nchan//4])]
        good_data_diff = abs(good_data[1:] - good_data[:-1])
        median_diff = np.median(good_data_diff)

        for i, diff in enumerate(good_data_diff):
            if diff < limit * median_diff:
                result = good_data_index[i]
                break

        if result is None:
            LOG.warning('edge finder failed for:%s' % description)
            # flag one edge channel - sole purpose of this is to ensure
            # that a plot is made in the weblog so that the problem
            # can be understood
            result = 1

        return result
