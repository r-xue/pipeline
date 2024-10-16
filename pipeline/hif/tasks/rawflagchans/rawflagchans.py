import os

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common import commonhelpermethods
from pipeline.h.tasks.common import commonresultobjects
from pipeline.h.tasks.common import viewflaggers
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from .resultobjects import RawflagchansResults, RawflagchansDataResults, RawflagchansViewResults

__all__ = [
    'Rawflagchans',
    'RawflagchansInputs'
]

LOG = infrastructure.get_logger(__name__)


class RawflagchansInputs(vdp.StandardInputs):
    """
    RawflagchansInputs defines the inputs for the Rawflagchans pipeline task.
    """
    fbq_antenna_frac_limit = vdp.VisDependentProperty(default=0.2)
    fbq_baseline_frac_limit = vdp.VisDependentProperty(default=1.0)
    fbq_hilo_limit = vdp.VisDependentProperty(default=8.0)
    flag_bad_quadrant = vdp.VisDependentProperty(default=True)
    flag_hilo = vdp.VisDependentProperty(default=True)
    fhl_limit = vdp.VisDependentProperty(default=20.0)
    fhl_minsample = vdp.VisDependentProperty(default=5)

    @vdp.VisDependentProperty
    def intent(self):
        # if the spw was set, look to see which intents were observed in that
        # spectral window and return the intent based on our order of
        # preference: BANDPASS
        preferred_intents = ('BANDPASS',)
        if self.spw:
            for spw in self.ms.get_spectral_windows(self.spw):
                for intent in preferred_intents:
                    if intent in spw.intents:
                        if intent != preferred_intents[0]:
                            LOG.warning('%s spw %s: %s not present, %s used instead' %
                                        (os.path.basename(self.vis), spw.id,
                                         preferred_intents[0], intent))
                        return intent

        # spw was not set, so look through the spectral windows
        for intent in preferred_intents:
            intentok = True
            for spw in self.ms.spectral_windows:
                if intent not in spw.intents:
                    intentok = False
                    break
                if not intentok:
                    break
            if intentok:
                if intent != preferred_intents[0]:
                    LOG.warning('%s %s: %s not present, %s used instead' %
                                (os.path.basename(self.vis), spw.id, preferred_intents[0],
                                 intent))
                return intent

        # As a fallback, return 'BANDPASS' as the intent.
        return 'BANDPASS'

    niter = vdp.VisDependentProperty(default=1)

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    def __init__(self, context, output_dir=None, vis=None, spw=None, intent=None, flag_hilo=None,
                 fhl_limit=None, fhl_minsample=None, flag_bad_quadrant=None, fbq_hilo_limit=None,
                 fbq_antenna_frac_limit=None, fbq_baseline_frac_limit=None, niter=None):
        super(RawflagchansInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.spw = spw
        self.intent = intent

        # flagging parameters
        self.flag_hilo = flag_hilo
        self.fhl_limit = fhl_limit
        self.fhl_minsample = fhl_minsample
        self.flag_bad_quadrant = flag_bad_quadrant
        self.fbq_hilo_limit = fbq_hilo_limit
        self.fbq_antenna_frac_limit = fbq_antenna_frac_limit
        self.fbq_baseline_frac_limit = fbq_baseline_frac_limit
        self.niter = niter


@task_registry.set_equivalent_casa_task('hif_rawflagchans')
class Rawflagchans(basetask.StandardTaskTemplate):
    Inputs = RawflagchansInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize result and store vis in result
        result = RawflagchansResults()
        result.vis = inputs.vis

        # Construct the task that will read the data.
        datainputs = RawflagchansDataInputs(
            context=inputs.context, vis=inputs.vis, spw=inputs.spw,
            intent=inputs.intent)
        datatask = RawflagchansData(datainputs)

        # Construct the generator that will create the view of the data
        # that is the basis for flagging.
        viewtask = RawflagchansView()

        # Construct the task that will set any flags raised in the
        # underlying data.
        flagsetterinputs = FlagdataSetter.Inputs(
            context=inputs.context,
            vis=inputs.vis, table=inputs.vis, inpfile=[])
        flagsettertask = FlagdataSetter(flagsetterinputs)

        # Define which type of flagger to use.
        flagger = viewflaggers.MatrixFlagger

        # Translate the input flagging parameters to a more compact
        # list of rules.
        rules = flagger.make_flag_rules(
            flag_hilo=inputs.flag_hilo, fhl_limit=inputs.fhl_limit,
            fhl_minsample=inputs.fhl_minsample,
            flag_bad_quadrant=inputs.flag_bad_quadrant,
            fbq_hilo_limit=inputs.fbq_hilo_limit,
            fbq_antenna_frac_limit=inputs.fbq_antenna_frac_limit,
            fbq_baseline_frac_limit=inputs.fbq_baseline_frac_limit)

        # Construct the flagger task around the data view task and the
        # flagger task. 
        flaggerinputs = flagger.Inputs(
            context=inputs.context, output_dir=inputs.output_dir,
            vis=inputs.vis, datatask=datatask, viewtask=viewtask,
            flagsettertask=flagsettertask, rules=rules, niter=inputs.niter,
            iter_datatask=False)
        flaggertask = flagger(flaggerinputs)

        # Execute the flagger task.
        flaggerresult = self._executor.execute(flaggertask)

        # Import views, flags, and "measurement set or caltable to flag"
        # into final result.
        result.importfrom(flaggerresult)

        # Copy flagging summaries to final result, and update
        # names to match expectations by renderer.
        result.summaries = flaggerresult.summaries
        result.summaries[0]['name'] = 'before'
        result.summaries[-1]['name'] = 'after'

        return result

    def analyse(self, result):
        return result


class RawflagchansDataInputs(vdp.StandardInputs):

    def __init__(self, context, vis=None, spw=None, intent=None):
        """
        RawflagchansDataInputs defines the inputs for the RawflagchansData task.

        Keyword arguments:
        spw    -- views are created for these spws.
        intent -- views are created for this intent.
        """
        super(RawflagchansDataInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis

        # data selection arguments
        self.intent = intent
        self.spw = spw


class RawflagchansData(basetask.StandardTaskTemplate):
    Inputs = RawflagchansDataInputs

    def __init__(self, inputs):
        super(RawflagchansData, self).__init__(inputs)

    def prepare(self):

        # Initialize result structure
        result = RawflagchansDataResults()
        result.data = {}
        result.intent = self.inputs.intent
        result.new = True

        # Take vis from inputs, and add to result.
        result.table = self.inputs.vis

        LOG.info('Reading flagging view data for vis {0}'.format(
            self.inputs.vis))

        # Get the MS object.
        ms = self.inputs.context.observing_run.get_ms(name=self.inputs.vis)

        # Get antenna names, ids, store in result.
        _, antenna_ids = commonhelpermethods.get_antenna_names(ms)
        ants = np.array(antenna_ids)

        # Create a list of antenna baselines.
        baselines = []
        for ant1 in ants:
            for ant2 in ants: 
                baselines.append('%s&%s' % (ant1, ant2))
        nbaselines = len(baselines)

        # Create a separate flagging view for each spw.
        for spwid in map(int, self.inputs.spw.split(',')):

            LOG.info('Reading flagging view data for spw {0}'.format(spwid))

            # Get the correlation products.
            corrs = commonhelpermethods.get_corr_products(ms, spwid)

            # Get the spw object from the MS and the number of channels
            # within the spw.
            spw = ms.get_spectral_window(spwid)
            nchans = spw.num_channels

            # Initialize the data, and number of data points
            # used, for the flagging view.
            data = np.zeros([len(corrs), nchans, nbaselines], complex)
            ndata = np.zeros([len(corrs), nchans, nbaselines], int)

            # Open MS and read in required data.       
            with casa_tools.MSReader(ms.name) as openms:
                try:
                    openms.msselect({'scanintent': '*%s*' % self.inputs.intent, 'spw': str(spwid)})
                except:
                    LOG.warning('Unable to compute flagging view for spw %s' % spwid)
                    openms.close()
                    # Continue to next spw.
                    continue

                # Read in the MS data in chunks of limited number of rows at a time.
                openms.iterinit(maxrows=500)
                openms.iterorigin()
                iterating = True
                while iterating:
                    rec = openms.getdata(
                        ['data', 'flag', 'antenna1', 'antenna2'])
                    if 'data' not in rec:
                        break

                    for row in range(np.shape(rec['data'])[2]):
                        ant1 = rec['antenna1'][row]
                        ant2 = rec['antenna2'][row]

                        if ant1 == ant2:
                            continue

                        baseline1 = ant1 * (ants[-1] + 1) + ant2
                        baseline2 = ant2 * (ants[-1] + 1) + ant1

                        for icorr in range(len(corrs)):

                            data[icorr, :, baseline1][
                              rec['flag'][icorr, :, row] == False]\
                              += rec['data'][icorr, :, row][
                              rec['flag'][icorr, :, row] == False]

                            ndata[icorr, :, baseline1][
                              rec['flag'][icorr, :, row] == False] += 1

                            data[icorr, :, baseline2][
                              rec['flag'][icorr, :, row] == False]\
                              += rec['data'][icorr, :, row][
                              rec['flag'][icorr, :, row] == False]

                            ndata[icorr, :, baseline2][
                              rec['flag'][icorr, :, row] == False] += 1

                    iterating = openms.iternext()

            # Calculate the average data value, suppress any divide by 0
            # error messages (RuntimeWarning).
            with np.errstate(divide='ignore', invalid='ignore'):
                data /= ndata

            # Take the absolute value of the data points.
            data = np.abs(data)

            # Set flagging state to "True" wherever no data points were available.
            flag = ndata == 0

            # Store data and related information for this spwid.
            result.data[spwid] = {
                'data': data,
                'flag': flag,
                'baselines': baselines,
                'nchans': nchans,
                'corrs': corrs
            }

        return result

    def analyse(self, result):
        return result


class RawflagchansView(object):

    def __init__(self):
        """
        Creates an RawflagchansView instance.
        """

        # Initialize result structure. By initializing here,
        # it is ensured that repeated calls to this instance will
        # have access to all previously generated results,
        # which is used to "refine" the data view in case the 
        # incoming data task result is not new.
        self.result = RawflagchansViewResults()

    def __call__(self, dataresult):
        """
        When called, the RawflagchansView object calculates flagging views
        for the vis / table provided by RawflagchansDataResults.

        dataresult  -- RawflagchansDataResults object.

        Returns:
        RawflagchansViewResults object containing the flagging view.
        """

        # Take vis from data task results, and add to result.
        self.result.vis = dataresult.table

        LOG.info('Computing flagging metrics for vis {0}'.format(
            self.result.vis))

        # Check if dataresult is new for current iteration, or created during 
        # a previous iteration.

        if dataresult.new:
            # If the dataresult is from a newly run datatask, then 
            # create the flagging view.

            # Get intent from dataresult
            intent = dataresult.intent

            # The dataresult should have stored separate results
            # for each spwid it could find data for in the MS.
            # Create a separate flagging view for each spw.
            for spwid, spwdata in dataresult.data.items():

                LOG.info('Calculating flagging view for spw %s' % spwid)

                # Create axes for flagging view.
                axes = [
                    commonresultobjects.ResultAxis(
                        name='channels', units='',
                        data=np.arange(spwdata['nchans'])),
                    commonresultobjects.ResultAxis(
                        name='Baseline', units='',
                        data=np.array(spwdata['baselines']), channel_width=1)
                ]

                # From the data array, create a view for each polarisation.
                for icorr, corrlist in enumerate(spwdata['corrs']):
                    corr = corrlist[0]

                    self.refine_view(spwdata['data'][icorr], spwdata['flag'][icorr])

                    viewresult = commonresultobjects.ImageResult(
                        filename=self.result.vis, data=spwdata['data'][icorr],
                        flag=spwdata['flag'][icorr], axes=axes,
                        datatype='Mean amplitude', spw=spwid, pol=corr,
                        intent=intent)

                    # add the view results to the result structure
                    self.result.addview(viewresult.description, viewresult)
        else:
            # If the dataresult is from a previously run datatask, then
            # the datatask is not being iterated, and instead the 
            # flagging view will be created as a refinement from an 
            # earlier flagging view.

            # Check in the result structure stored in this instance of RawflagchansView
            # for the presence of previous result descriptions.
            prev_descriptions = self.result.descriptions()
            if prev_descriptions:
                # If result descriptions are present, then this instance was called 
                # before, and views were already calculated previously. 
                # Since the current view will be very similar to the last, it is
                # approximated to be identical, which saves having to recalculate.
                for description in prev_descriptions:

                    # Get a deep copy of the last result belonging to the description.
                    copy_of_prev_result = self.result.last(description)

                    # re-refine the data to take into account possible
                    # new flagging
                    self.refine_view(copy_of_prev_result.data, copy_of_prev_result.flag)

                    # Store the refined view as the new view in the result structure.
                    self.result.addview(description, copy_of_prev_result)
            else:
                LOG.error('Not iterating datatask, but no previous flagging '
                          ' views available to create a refined view from. '
                          'Cannot create flagging views.')

        return self.result

    @staticmethod
    def refine_view(data, flag):
        """Refine the data view by subtracting the median from each
        baseline row, then subtracting the median from each channel
        column.
        """

        for baseline in range(np.shape(data)[1]):
            valid = data[:, baseline][flag[:, baseline] == False]
            if len(valid):
                data[:, baseline] -= np.median(valid)

        for chan in range(np.shape(data)[0]):
            valid = data[chan, :][flag[chan, :] == False]
            if len(valid):
                data[chan, :] -= np.median(valid)
