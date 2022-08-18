import os

import numpy as np

import pipeline.domain as domain
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common import calibrationtableaccess as caltableaccess
from pipeline.h.tasks.common import commonresultobjects
from pipeline.h.tasks.common import viewflaggers
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import bandpass
from pipeline.hif.tasks import gaincal
from pipeline.infrastructure import task_registry
from .resultobjects import LowgainflagDataResults
from .resultobjects import LowgainflagResults
from .resultobjects import LowgainflagViewResults

__all__ = [
    'LowgainflagInputs',
    'Lowgainflag'
]

LOG = infrastructure.get_logger(__name__)


class LowgainflagInputs(vdp.StandardInputs):
    """
    LowgainflagInputs defines the inputs for the Lowgainflag pipeline task.
    """
    flag_nmedian = vdp.VisDependentProperty(default=True)
    fnm_hi_limit = vdp.VisDependentProperty(default=1.5)
    fnm_lo_limit = vdp.VisDependentProperty(default=0.5)

    @vdp.VisDependentProperty
    def intent(self):
        # default to the intent that would be used for bandpass
        # calibration
        bp_inputs = bandpass.PhcorBandpass.Inputs(context=self.context, vis=self.vis, intent=None)
        return bp_inputs.intent

    # Flagging view is created if number of antennas in a set equals-or-exceeds
    # the threshold.
    min_nants_threshold = vdp.VisDependentProperty(default=5)
    niter = vdp.VisDependentProperty(default=2)

    @vdp.VisDependentProperty
    def refant(self):
        # we cannot find the context value without the measurement set
        if not self.ms:
            return None

        # get the reference antenna for this measurement set
        ant = self.ms.reference_antenna
        if isinstance(ant, list):
            ant = ant[0]

        # return the antenna name/id if this is an Antenna domain object
        if isinstance(ant, domain.Antenna):
            return getattr(ant, 'name', ant.id)

        # otherwise return whatever we found. We assume the calling function
        # knows how to handle an object of this type.
        return ant

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(with_channels=True, science_windows_only=True)
        return ','.join([str(spw.id) for spw in science_spws])

    def __init__(self, context, output_dir=None, vis=None, intent=None, spw=None, refant=None, flag_nmedian=None,
                 fnm_lo_limit=None, fnm_hi_limit=None, niter=None, min_nants_threshold=None):
        super(LowgainflagInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.intent = intent
        self.spw = spw
        self.refant = refant
        self.min_nants_threshold = min_nants_threshold

        # flagging parameters
        self.flag_nmedian = flag_nmedian
        self.fnm_hi_limit = fnm_hi_limit
        self.fnm_lo_limit = fnm_lo_limit
        self.niter = niter


@task_registry.set_equivalent_casa_task('hif_lowgainflag')
@task_registry.set_casa_commands_comment(
    'Sometimes antennas have significantly lower gain than nominal. Even when calibrated, it is better for ALMA data to'
    ' flag these antennas. The pipeline detects this by calculating a long solint amplitude gain on the bandpass '
    'calibrator.  First, temporary phase and bandpass solutions are calculated, and then that temporary bandpass is '
    'used to calculate a short solint phase and long solint amplitude solution.'
)
class Lowgainflag(basetask.StandardTaskTemplate):
    Inputs = LowgainflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize result and store vis in result
        result = LowgainflagResults(vis=inputs.vis)

        # Construct the task that will read the data.
        datainputs = LowgainflagDataInputs(
            context=inputs.context, output_dir=inputs.output_dir,
            vis=inputs.vis, intent=inputs.intent, spw=inputs.spw,
            refant=inputs.refant)
        datatask = LowgainflagData(datainputs)

        # Construct the generator that will create the view of the data
        # that is the basis for flagging.
        viewtask = LowgainflagView(
            context=inputs.context, vis=inputs.vis, intent=inputs.intent,
            spw=inputs.spw, refant=inputs.refant,
            min_nants_threshold=inputs.min_nants_threshold)

        # Construct the task that will set any flags raised in the
        # underlying data.
        flagsetterinputs = FlagdataSetter.Inputs(
            context=inputs.context, vis=inputs.vis, table=inputs.vis,
            inpfile=[])
        flagsettertask = FlagdataSetter(flagsetterinputs)

        # Define which type of flagger to use.
        flagger = viewflaggers.MatrixFlagger

        # Translate the input flagging parameters to a more compact
        # list of rules.
        rules = flagger.make_flag_rules(
            flag_nmedian=inputs.flag_nmedian,
            fnm_lo_limit=inputs.fnm_lo_limit,
            fnm_hi_limit=inputs.fnm_hi_limit)

        # PIPE-566: append a rule to detect when a flagging view has no usable
        # (unflagged) data at all for a spw (e.g. because gaincal could not
        # compute any solutions). In this case the underlying (bad) data in the
        # MS for this spw should get flagged explicitly.
        rules.extend(flagger.make_flag_rules(flag_tmef1=True, tmef1_axis='Antenna1', tmef1_limit=1.0))

        # Construct the flagger task around the data view task and the
        # flagsetter task. 
        matrixflaggerinputs = flagger.Inputs(
            context=inputs.context, output_dir=inputs.output_dir,
            vis=inputs.vis, datatask=datatask, viewtask=viewtask,
            flagsettertask=flagsettertask, rules=rules, niter=inputs.niter,
            extendfields=['field', 'scan'], iter_datatask=True, skip_fully_flagged=False)
        flaggertask = flagger(matrixflaggerinputs)

        # Execute the flagger task.
        flaggerresult = self._executor.execute(flaggertask)

        # Import views, flags, and "measurement set or caltable to flag"
        # into final result
        result.importfrom(flaggerresult)

        # Copy flagging summaries to final result
        result.summaries = flaggerresult.summaries

        return result

    def analyse(self, result):
        """
        Analyses the Lowgainflag result.

        :param result: LowgainflagResults object
        :return: LowgainflagResults object
        """
        result = self._update_reference_antennas(result)

        return result

    def _update_reference_antennas(self, result):
        """
        Updates the Lowgainflag result to mark any antennas that were found to
        be fully flagged in any of the flagging views to be demoted when
        result gets accepted.

        :param result: LowgainflagResults object
        :return: LowgainflagResults object
        """
        # First summarize which antennas are fully flagged in any flagging view.
        ants_flagged_in_any_spw = self._summarize_fully_flagged_antennas(result)

        # If any fully flagged antennas were found, then update result to mark
        # these antennas for demotion.
        if ants_flagged_in_any_spw:
            result = self._mark_antennas_for_demotion(result, ants_flagged_in_any_spw)

        return result

    @staticmethod
    def _summarize_fully_flagged_antennas(result):
        """
        Create a summary of fully flagged antennas based on all flagging views
        in the result.

        :param result: LowgainflagResults object
        :return: list of int, representing IDs of antennas that are fully
        flagged.
        """
        ants_flagged_in_any_view = set()

        for description in result.descriptions():
            # Get final view.
            view = result.last(description)

            # Identify antennas fully flagged for all scans, mapping the
            # array indices to the original antenna IDs using the flagging view
            # x-axis data.
            antids_fully_flagged = view.axes[0].data[
                np.where(np.all(view.flag, axis=1))[0]]

            # Update set of fully flagged antennas based on current view.
            ants_flagged_in_any_view.update(antids_fully_flagged)

        return ants_flagged_in_any_view

    def _mark_antennas_for_demotion(self, result, ants_to_demote):
        """
        Modify result to set antennas to be demoted if/when result gets
        accepted into the pipeline context. If list of antennas to demote
        comprises all antennas, then skip demotion but raise a warning.

        :param result: LowgainflagResults object
        :param ants_to_demote: list of ints, representing IDs of antennas to
        demote.
        :return: LowgainflagResults object
        """
        # Get the MS object
        ms = self.inputs.context.observing_run.get_ms(name=self.inputs.vis)

        # Proceed only if a list of reference antennas was registered with the MS.
        if (hasattr(ms, 'reference_antenna') and
                isinstance(ms.reference_antenna, str)):

            # Create list of current refants
            refant = ms.reference_antenna.split(',')

            # Create translation dictionary, reject empty antenna name strings.
            antenna_id_to_name = {ant.id: ant.name for ant in ms.antennas if ant.name.strip()}

            # Translate IDs of antennas-to-demote to antenna names.
            ants_to_demote_as_refant = {
                antenna_id_to_name[ant_id]
                for ant_id in ants_to_demote
            }

            # Compute intersection between refants and ants to demote as
            # refant.
            refants_to_demote = {
                ant for ant in refant
                if ant in ants_to_demote_as_refant
            }

            # If the intersection is not empty, then there are existing
            # refants that need to be demoted.
            if refants_to_demote:
                # Create string for log message.
                ant_msg = utils.commafy(refants_to_demote, quotes=False)

                # Check if the list of refants-to-demote comprises all
                # refants, in which case the re-ordering of refants is
                # skipped.
                if refants_to_demote == set(refant):

                    # Log warning that refant list should have been updated, but
                    # will not be updated so as to avoid an empty refant list.
                    LOG.warning(
                        '{} - the following antennas are fully flagged '
                        'for one or more spws, but since these comprise all '
                        'refants, the refant list is *NOT* updated to '
                        're-order these to the end of the refant list: '
                        '{}'.format(ms.basename, ant_msg))
                else:
                    # Log a warning if any antennas are to be demoted from
                    # the refant list.
                    LOG.warning(
                        '{} - the following antennas have been fully flagged '
                        'in one or more spws, and moved to the end '
                        'of the refant list: {}'.format(ms.basename, ant_msg))

                    # Update result to set the refants to demote:
                    result.refants_to_demote = refants_to_demote

        # If no list of reference antennas was registered with the MS,
        # raise a warning.
        else:
            LOG.warning(
                '{} - no reference antennas found in MS, cannot update '
                'the reference antenna list.'.format(ms.basename))

        return result


class LowgainflagDataInputs(vdp.StandardInputs):
    def __init__(self, context, output_dir=None, vis=None, intent=None,
                 spw=None, refant=None):
        super(LowgainflagDataInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.intent = intent
        self.spw = spw
        self.refant = refant


class LowgainflagData(basetask.StandardTaskTemplate):
    Inputs = LowgainflagDataInputs

    def __init__(self, inputs):
        super(LowgainflagData, self).__init__(inputs)

    def prepare(self):
        inputs = self.inputs

        # Initialize result structure
        result = LowgainflagDataResults()
        result.vis = inputs.vis

        # Calculate a phased-up bpcal
        bpcal_inputs = bandpass.PhcorBandpass.Inputs(
            context=inputs.context, vis=inputs.vis, intent=inputs.intent,
            spw=inputs.spw, refant=inputs.refant, solint='inf,7.8125MHz')
        bpcal_task = bandpass.PhcorBandpass(bpcal_inputs)
        bpcal = self._executor.execute(bpcal_task, merge=False)
        if not bpcal.final:
            LOG.warning("No bandpass solution computed for {}".format(inputs.ms.basename))
        else:
            bpcal.accept(inputs.context)

        # Calculate gain phases
        gpcal_inputs = gaincal.GTypeGaincal.Inputs(context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                                                   spw=inputs.spw, refant=inputs.refant, calmode='p', minsnr=2.0,
                                                   solint='int')
        gpcal_task = gaincal.GTypeGaincal(gpcal_inputs)
        gpcal = self._executor.execute(gpcal_task, merge=False)
        if not gpcal.final:
            LOG.warning("No phase time solution computed for {}".format(inputs.ms.basename))
        else:
            gpcal.accept(inputs.context)

        # Calculate gain amplitudes
        gacal_inputs = gaincal.GTypeGaincal.Inputs(
            context=inputs.context, vis=inputs.vis,
            intent=inputs.intent, spw=inputs.spw,
            refant=inputs.refant,
            calmode='a', minsnr=2.0, solint='inf', gaintype='T')
        gacal_task = gaincal.GTypeGaincal(gacal_inputs)
        gacal = self._executor.execute(gacal_task, merge=False)
        if not gacal.final:
            gatable = list(gacal.error)
            gatable = gatable[0].gaintable
            LOG.warning("No amplitude time solution computed for {}".format(inputs.ms.basename))
            result.table = gatable
            result.table_available = False
        else:
            gacal.accept(inputs.context)
            gatable = gacal.final
            gatable = gatable[0].gaintable
            result.table = gatable
            result.table_available = True

        return result

    def analyse(self, result):
        return result


class LowgainflagView(object):

    def __init__(self, context, vis=None, intent=None, spw=None, refant=None,
                 min_nants_threshold=None):

        self.context = context
        self.vis = vis
        self.intent = intent
        self.spw = spw
        self.refant = refant
        self.min_nants_threshold = min_nants_threshold

    def __call__(self, data):

        # Initialize result structure
        self.result = LowgainflagViewResults()

        if data.table_available:

            # Calculate the view
            gatable = data.table
            LOG.info('Computing flagging metrics for caltable {0}'.format(
                os.path.basename(gatable)))
            self.calculate_view(gatable)

        # Add visibility name to result
        self.result.vis = self.vis

        return self.result

    def calculate_view(self, table):
        """
        table -- Name of gain table to be analysed.
        """

        # Open gains caltable.
        gtable = caltableaccess.CalibrationTableDataFiller.getcal(table)

        # Get range of scans covered.
        scans = set()
        for row in gtable.rows:
            # The gain table is T, should be no pol dimension
            npols = np.shape(row.get('CPARAM'))[0]
            if npols != 1:
                raise Exception('table has polarization results')
            scans.update([row.get('SCAN_NUMBER')])
        scans = np.sort(list(scans))

        # Create translation of scan ID to flagging view axis ID.
        scanid_to_axisid = {scan_id: axis_id for axis_id, scan_id in enumerate(scans)}

        # Get the MS domain object.
        ms = self.context.observing_run.get_ms(name=self.vis)

        # Get spw IDs from MS: the task input "spw" arg may contain channel
        # specification, so let MS parse input and read spw ID from the
        # SpectralWindow domain objects that are returned.
        spwids = [spw.id for spw in ms.get_spectral_windows(self.spw)]

        # Identify set of unique antenna diameters present in MS.
        ant_diameters = {antenna.diameter for antenna in ms.antennas}

        # Create separate flagging view for each set of antennas with
        # same diameter.
        for antdiam in ant_diameters:
            # Identify antennas with current diameter.
            antenna_ids = sorted([antenna.id for antenna in ms.antennas if antenna.diameter == antdiam])

            # Create translation of antenna ID to flagging view axis ID.
            antid_to_axisid = {ant_id: axis_id for axis_id, ant_id in enumerate(antenna_ids)}
            nants = len(antenna_ids)

            # If the number of antennas is below the threshold, then skip
            # flagging for the set with current diameter.
            if nants < self.min_nants_threshold:
                LOG.warning(
                    "Number of antennas with diameter of {:.1f} m is"
                    " below the minimum threshold ({}), skipping"
                    " flagging for these antennas (no flagging view"
                    " created).".format(antdiam, self.min_nants_threshold))
            else:
                # Create flagging view for each spwid.
                for spwid in spwids:

                    # Initialize arrays for flagging view.
                    data = np.zeros([nants, len(scans)])
                    flag = np.ones([nants, len(scans)], np.bool)

                    for row in gtable.rows:
                        ant = row.get('ANTENNA1')
                        if row.get('SPECTRAL_WINDOW_ID') == spwid and ant in antenna_ids:
                            gain = row.get('CPARAM')[0][0]
                            gainflag = row.get('FLAG')[0][0]
                            scan = row.get('SCAN_NUMBER')
                            if not gainflag:
                                data[antid_to_axisid[ant], scanid_to_axisid[scan]] = np.abs(gain)
                                flag[antid_to_axisid[ant], scanid_to_axisid[scan]] = 0

                    axes = [
                        commonresultobjects.ResultAxis(name='Antenna1', units='id', data=np.asarray(antenna_ids)),
                        commonresultobjects.ResultAxis(name='Scan', units='id', data=[str(scan) for scan in scans],
                                                       channel_width=1)
                    ]

                    # associate the result with a generic filename - using
                    # specific names gives confusing duplicates on the weblog
                    # display
                    ants = ','.join([str(ant) for ant in antenna_ids])
                    viewresult = commonresultobjects.ImageResult(
                        filename='%s(gtable)' % os.path.basename(gtable.vis),
                        intent=self.intent, data=data, flag=flag, axes=axes,
                        datatype='gain amplitude', spw=spwid,
                        ant=ants)

                    # add the view results and their children results to the
                    # class result structure
                    self.result.addview(viewresult.description, viewresult)
