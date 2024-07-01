from typing import List

import numpy as np
from astropy.coordinates import AltAz, EarthLocation, SkyCoord
from astropy.time import Time

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.domain.measurementset import MeasurementSet
from pipeline.extern.adopted import getMedianPWV
from pipeline.h.tasks.common import atmutil
from pipeline.h.tasks.common.arrayflaggerbase import channel_ranges
from pipeline.h.tasks.flagging import flagdeterbase
from pipeline.infrastructure import casa_tools, task_registry
from pipeline.infrastructure.utils import utils

__all__ = [
    'FlagDeterALMA',
    'FlagDeterALMAInputs',
    'FlagDeterALMAResults',
]

LOG = infrastructure.get_logger(__name__)


class FlagDeterALMAResults(flagdeterbase.FlagDeterBaseResults):
    pass


class FlagDeterALMAInputs(flagdeterbase.FlagDeterBaseInputs):
    """
    FlagDeterALMAInputs defines the inputs for the FlagDeterALMA pipeline task.
    """
    tolerance = vdp.VisDependentProperty(default=0.0)
    edgespw = vdp.VisDependentProperty(default=True)
    flagbackup = vdp.VisDependentProperty(default=True)
    fracspw = vdp.VisDependentProperty(default=0.03125)
    # PIPE-1028: in hifa_flagdata, flag integrations with only partial
    # polarization products.
    partialpol = vdp.VisDependentProperty(default=True)
    # PIPE-624: parameters for flagging low transmission.
    lowtrans = vdp.VisDependentProperty(default=True)
    mintransnonrepspws = vdp.VisDependentProperty(default=0.1)
    mintransrepspw = vdp.VisDependentProperty(default=0.05)
    template = vdp.VisDependentProperty(default=True)

    # new property for ACA correlator
    fracspwfps = vdp.VisDependentProperty(default=0.048387)

    # New property for QA0 / QA2 flags
    qa0 = vdp.VisDependentProperty(default=True)
    qa2 = vdp.VisDependentProperty(default=True)

    def __init__(self, context, vis=None, output_dir=None, flagbackup=None, autocorr=None, shadow=None, tolerance=None,
                 scan=None, scannumber=None, intents=None, edgespw=None, fracspw=None, fracspwfps=None, online=None,
                 partialpol=None, lowtrans=None, mintransnonrepspws=None, mintransrepspw=None,
                 fileonline=None, template=None, filetemplate=None, hm_tbuff=None, tbuff=None, qa0=None, qa2=None):
        super(FlagDeterALMAInputs, self).__init__(
            context, vis=vis, output_dir=output_dir, flagbackup=flagbackup, autocorr=autocorr, shadow=shadow,
            tolerance=tolerance, scan=scan, scannumber=scannumber, intents=intents, edgespw=edgespw, fracspw=fracspw,
            fracspwfps=fracspwfps, online=online, fileonline=fileonline, template=template,
            filetemplate=filetemplate, hm_tbuff=hm_tbuff, tbuff=tbuff, partialpol=partialpol,
            lowtrans=lowtrans, mintransnonrepspws=mintransnonrepspws, mintransrepspw=mintransrepspw)

        # solution parameters
        self.qa0 = qa0
        self.qa2 = qa2


@task_registry.set_equivalent_casa_task('hifa_flagdata')
@task_registry.set_casa_commands_comment(
    'Flags generated by the online telescope software, by the QA0 process, and manually set by the pipeline user.'
)
class FlagDeterALMA(flagdeterbase.FlagDeterBase):
    Inputs = FlagDeterALMAInputs

    # PIPE-425: define allowed bandwidths of ACA spectral windows for which to
    # perform the ACA FDM edge channel flagging heuristic, and define
    # corresponding thresholds.
    #
    # Note: at present, February 2020, ALMA correlators produce
    # spectral windows whose bandwidths are quantized:
    #
    #   Baseline correlator (BLC) widths:
    #     1875, 937.5, 468.75, 234.375, 117.1875, 58.5375 MHz
    #   ACA correlator widths:
    #     2000, 1000, 500, 250, 125, 62.5 MHz
    #
    # This edge channel heuristic is defined for all ACA correlator widths
    # except the 2000 MHz, where the latter is already covered by the heuristic
    # covering bandwidths > 1875 MHz, introduced in CAS-5231.
    _aca_edge_flag_thresholds = {
        # spw bandwidth (MHz): threshold frequency
        62.5: measures.Frequency(5.0, measures.FrequencyUnits.MEGAHERTZ),
        125: measures.Frequency(10.0, measures.FrequencyUnits.MEGAHERTZ),
        250: measures.Frequency(20.0, measures.FrequencyUnits.MEGAHERTZ),
        500: measures.Frequency(40.0, measures.FrequencyUnits.MEGAHERTZ),
        1000: measures.Frequency(62.5, measures.FrequencyUnits.MEGAHERTZ),
    }

    # PIPE-624: Threshold for fraction of data with low transmission above
    # which a SpW is flagged for low transmission.
    _max_frac_low_trans = 0.6

    def prepare(self):
        # PIPE-1759: this list collects the spws with missing basebands subsequently used to create a QA score
        self.missing_baseband_spws = []

        # Wrap results from parent in hifa_flagdata specific result to enable
        # separate QA scoring.
        results = super().prepare()
        results = FlagDeterALMAResults(results.summaries, results.flagcmds())

        # PIPE-1759: store the list of spws with missing basebands for a subsequent QA score
        results.missing_baseband_spws = self.missing_baseband_spws
        return results

    def get_fracspw(self, spw):
        # From T. Hunter on PIPE-425: in early ALMA Cycles, the ACA
        # correlator's frequency profile synthesis (fps) algorithm produced TDM
        # spws that had 64 channels in full-polarisation, 124 channels in dual
        # pol, and 248 channels in single-pol.
        # TODO: find out whether it should be 62 (as in code) or 64 (as per above comment)
        #
        # By comparison, the baseline correlator (BLC) standard values are 128
        # channels for dual pol, and 256 channels for single pol, and in more
        # recent cycles, the ACA TDM spws agree in channel number and bandwidth
        # with the BLC TDM spws.
        #
        # The following override is preserved to handle the old "FPS" generated
        # ACA TDM spws, for which a different threshold is used.
        if spw.num_channels in (62, 124, 248):
            return self.inputs.fracspwfps
        else:
            return self.inputs.fracspw

    def verify_spw(self, spw):
        # Override the default verifier:
        #  - first run the default verifier
        #  - then run extra test to skip flagging of TDM windows
        super(FlagDeterALMA, self).verify_spw(spw)

        # Test whether the spw is TDM or FDM. If it is FDM, then raise a
        # ValueError. From T. Hunter on PIPE-425:
        # TDM spectral windows have either 4*64 channels in full polarisation,
        # 2*128 for dual polarisation, or 1*256 channels for single
        # polarisation; i.e., in all cases, a TDM spw has ncorr*nchans = 256.
        #
        # By comparison, the smallest number of channels in an FDM spectral
        # window is 4*120 (full pol), 2*240 (dual-pol), or 1*480 (single-pol)
        # when online binning is set to 16.
        #
        # Based on this, it is assumed that any spw with ncorr*nchans <= 256 is
        # TDM, and any spw with ncorr*nchans > 256 is FDM.
        dd = self.inputs.ms.get_data_description(spw=spw)
        ncorr = len(dd.corr_axis)
        if ncorr * spw.num_channels > 256:
            raise ValueError('Spectral window {} is an FDM spectral window, skipping the TDM edge flagging'
                             'heuristics.'.format(spw.id))

    def _get_partialpol_cmds(self):
        """
        ALMA specific step to identify and flag data where only part of the
        polarization products are flagged.

        Returns:
            List of flagging commands.
        """
        return load_partialpols_alma(self.inputs.ms)

    def _get_lowtrans_cmds(self) -> List:
        """
        ALMA specific step to identify and flag data with low atmospheric
        transmission.

        Returns:
            List of flagging commands.
        """
        return lowtrans_alma(self.inputs.ms, mintransrepspw=self.inputs.mintransrepspw,
                             mintransnonrepspws=self.inputs.mintransnonrepspws,
                             max_frac_low_trans=self._max_frac_low_trans)

    def _get_edgespw_cmds(self):
        # Run default edge channel flagging first.
        to_flag = super(FlagDeterALMA, self)._get_edgespw_cmds()

        # Loop over the spectral windows, generate a flagging command for each
        # spw in the ms. Calling get_spectral_windows() with no arguments
        # returns just the science windows, which is exactly what we want.
        for spw in self.inputs.ms.get_spectral_windows():
            try:
                # Test whether this spw should be evaluated for edge channel
                # flagging.
                self.verify_spw(spw)
            except ValueError:
                # If we get here, then the spw verification failed. Proceed
                # with alternate edge channel flagging.
                LOG.debug('Proceeding with FDM edge channel flagging heuristics for spw {}'
                          ''.format(spw.id))

                # CAS-5231: FDM edge channel flagging heuristic for spectral
                # windows whose bandwidth exceeds 1875 MHz.
                threshold = measures.Frequency(1875, measures.FrequencyUnits.MEGAHERTZ)
                if spw.bandwidth > threshold:
                    to_flag.extend(self._get_fdm_edgespw_cmds(spw, threshold))

                # PIPE-425: run a separate edge channel flagging heuristic for
                # ACA spectral windows with shorter bandwidth (defined above),
                # to ensure that channels tuned too close to the edge of the
                # baseband are flagged. Assume that the ACA spw bandwidths are
                # stored in MHz.
                #
                # TODO: in CASA 6.1, the correlator type should start to be
                # propagated from ASDM to MS. Once available, this test could
                # be future-proofed (w.r.t. possible new correlators) by
                # explicitly checking whether the spw is an ACA spw
                # (e.g., self.inputs.ms.correlator_name == 'ALMA_ACA')
                spw_bw_in_mhz = spw.bandwidth.to_units(otherUnits=measures.FrequencyUnits.MEGAHERTZ)
                if spw_bw_in_mhz in self._aca_edge_flag_thresholds.keys():
                    to_flag.extend(self._get_aca_edgespw_cmds(spw, self._aca_edge_flag_thresholds[spw_bw_in_mhz]))

        return to_flag

    def _get_fdm_edgespw_cmds(self, spw, threshold):
        """
        FDM spectral window edge flagging heuristic.

        Returns a list containing a flagging command that will flag all channels
        that lie beyond "threshold/2" from the center frequency.

        For example: if the threshold is 1875 MHz, this will flag any channels
        +- 937.5 MHz from the center frequency.

        :param spw: spectral window to evaluate
        :param threshold: bandwidth threshold
        :return: list containing flagging command as string
        :rtype: list[str]
        """
        LOG.debug('Bandwidth greater than {} for spw {}. Proceeding with flagging all channels beyond +-{} from the'
                  ' center frequency.'.format(str(threshold), spw.id, str(threshold / 2.0)))

        cen_freq = spw.centre_frequency

        # channel range lower than threshold
        lo_freq = cen_freq - spw.bandwidth / 2.0
        hi_freq = cen_freq - threshold / 2.0
        minchan_lo, maxchan_lo = spw.channel_range(lo_freq, hi_freq)

        # upper range higher than threshold
        lo_freq = cen_freq + threshold / 2.0
        hi_freq = cen_freq + spw.bandwidth / 2.0
        minchan_hi, maxchan_hi = spw.channel_range(lo_freq, hi_freq)

        # Append to flag list
        # Clean up order of channel ranges high to low
        chan1 = '{0}~{1}'.format(minchan_lo, maxchan_lo)
        chan2 = '{0}~{1}'.format(minchan_hi, maxchan_hi)
        chans = sorted([chan1, chan2])
        cmd = '{0}:{1};{2}'.format(spw.id, chans[0], chans[1])
        to_flag = [cmd]

        return to_flag

    def _get_aca_edgespw_cmds(self, spw, threshold):
        """
        ACA FDM spectral window edge flagging heuristic.

        Return a list containing a flagging command that will flag all channels
        that lie too close to the edge of the baseband.

        :param spw: spectral window to evaluate
        :param threshold: threshold frequency range used to determine whether
        spectral window channels are too close to edge of baseband
        :return: list containing flagging command as string
        :rtype: list[str]
        """
        LOG.debug('Spectral window {} is an ACA FDM spectral window. Proceeding with flagging channels'
                  ' that are too close to the baseband edge.'.format(spw.id))

        # For the given spectral window, identify the corresponding SQLD
        # spectral window(s) with TARGET intent taken in same baseband with same
        # spectral tuning (SpectralSpec, PIPE-1991).
        bb_spw = [s for s in self.inputs.ms.get_spectral_windows(science_windows_only=False)
                  if s.baseband == spw.baseband and s.spectralspec == spw.spectralspec and s.type == 'SQLD'
                  and 'TARGET' in s.intents]

        # If no baseband spw could be identified, add the spw to the list of missing basebands
        # and return with no new flagging commands.
        if not bb_spw:
            self.missing_baseband_spws.append(spw.id)
            return []

        # Compute frequency ranges for which any channel that falls within the
        # range should be flagged; these ranges are set by the baseband edges
        # and the provided threshold.
        bb_edges = [measures.FrequencyRange(bb_spw[0].min_frequency, bb_spw[0].min_frequency + threshold),
                    measures.FrequencyRange(bb_spw[0].max_frequency - threshold, bb_spw[0].max_frequency)]

        # Compute list of channels to flag as those channels that have an
        # overlap with either edge range of the baseband.
        to_flag = [str(i) for i, ch in enumerate(spw.channels)
                   if ch.frequency_range.overlaps(other=bb_edges[0])
                   or ch.frequency_range.overlaps(other=bb_edges[1])]

        # If flagging is needed, turn the list of channels into a list of
        # a flagging command with spectral window id, and consolidated channel
        # ranges. utils.find_ranges returns comma separated ranges, but for
        # multiple channel ranges within a single spw, CASA's flagdata expects
        # these to be separated by semi-colon.
        if to_flag:
            chan_to_flag = utils.find_ranges(to_flag).replace(',', ';')
            LOG.attention('{} - Flagging edge channels for ACA spectral window {}, channel(s) {}, due to proximity'
                          ' to edge of baseband.'.format(self.inputs.ms.basename, spw.id, chan_to_flag))
            to_flag = ['{}:{}'.format(spw.id, chan_to_flag)]

        return to_flag


def load_partialpols_alma(ms):
    """Retrieve the relevant data to extend partial polarization flagging to all the polarizations (see PIPE-1028).
    It returns the list of flagging commands required to flag the partial polarization.

    :param ms: Measurement set to load
    :return: list containing flagging commands as strings
    :rtype: List[str]
    """

    # Get the spw IDs and corresponding DATA DESC IDs for which to assess the
    # partial polarization flagging.
    # TODO: This function takes care of translating spws to DATA_DESC_IDs but it may not be necessary and it
    #  is possible that it can be replaced by the pipeline domain object function, such as:
    #  all_spws = ms.get_spectral_windows()
    spws_ids, datadescids = get_partialpol_spws(ms.name)

    # PIPE-1245: restrict evaluation of partial polarization flagging to scans
    # that do not cover the following intents:
    unwanted_intents = {'ATMOSPHERE', 'FOCUS', 'POINTING'}
    scan_ids = [scan.id for scan in ms.get_scans() if not scan.intents.intersection(unwanted_intents)]
    spw_scan_selections = iter((spw, dd, scan) for spw, dd in zip(spws_ids, datadescids) for scan in scan_ids)

    # Initialize flagging command parameters.
    params = []

    with casa_tools.TableReader(ms.name) as table:
        # Run evaluation for each combination of SpW id (with corresponding
        # data_desc_id) and scan id.
        for spw, ddid, scan in spw_scan_selections:  # Iterate over relevant spws

            # Create table selection for current spw and get flag column.
            table_sel = table.query(f"DATA_DESC_ID == {ddid} && SCAN_NUMBER == {scan}")
            flags = table_sel.getcol('FLAG')

            # Number of polarisations present.
            n_pol = len(flags)

            # For multi-pol data, assess if there are any rows where part of
            # the polarisation is flagged.
            if n_pol > 1:
                LOG.debug(f"Multiple polarization data found for DATA_DESC_IDs {ddid}, scan {scan}, checking if any"
                          f" polarization data are partially flagged.")

                # Count how often no pols, some pols, and/or all pols are
                # flagged.
                npolflag = dict.fromkeys(range(n_pol + 1), 0)
                for k, v in zip(*np.unique(np.count_nonzero(flags, axis=0), return_counts=True)):
                    npolflag[k] = v

                # Report how often n number of pols are flagged, and assess
                # if any there are any occurrences where polarisation data is
                # partially flagged.
                do_partial_pol = False
                for nflag, n in npolflag.items():
                    if nflag == 0:
                        LOG.debug(f" Polarization data completely unflagged: N = {n}")
                    elif nflag < n_pol:
                        LOG.debug(f" Polarization data partially flagged - {nflag} out of {n_pol}: N = {n}")
                        if n > 0:
                            do_partial_pol = True
                    else:
                        LOG.debug(f" Polarization data completely flagged: N = {n}")

                # Continue with actual flagging of partial polarization rows.
                if do_partial_pol:
                    ant1 = table_sel.getcol('ANTENNA1')
                    ant2 = table_sel.getcol('ANTENNA2')
                    time = table_sel.getcol('TIME')
                    interval = table_sel.getcol('INTERVAL')
                    params_spw = get_partialpol_flag_cmd_params(flags, ant1, ant2, time, interval)
                    # Add the spw and time_unit to the params
                    time_unit = table_sel.getcolkeyword('TIME', 'QuantumUnits')[0]
                    # Add the spw and time_unit to the dictionaries
                    updated_params_spw = [{**d, "spw": spw, "time_unit": time_unit} for d in params_spw]
                    params.extend(updated_params_spw)
            else:
                LOG.debug(f"No multiple polarization data found for DATA_DESC_IDs {ddid}, scan {scan}.")

            # Free resources held by table selection.
            table_sel.close()

    commands = convert_params_to_commands(ms, params)
    return commands


def get_partialpol_spws(ms_name):
    """Obtain the spws and DATA_DESC_IDs required for the Partial Polarization flagging agent.
    According to the comments in PIPE-1028 they are all non-WVR/non-SQLD spws with an intent that
    starts with OBSERVE_TARGET or CALIBRATE_PHASE (which includes the Science spws).

    Note that there is a chance that this function can be refactored using one on the pipeline domain object
    functions. If the translation of spw to DATA_DESC_ID is not required, this function may not be needed at all.

    :param ms_name: Name of the Measurement Set
    :return: List of spws.ids and list of DATA_DESC_IDs (with the same length)
    """

    # Note: In all the examples checked, spw id and DATA_DESC_ID are the same, but as this may not be always true and
    #  Todd uses this translation in his code, both sets of data (spws and DATA_DESC_IDs) are retrieved.
    # Note: In the examples checked, non-Science spws do not have usable data.
    with casa_tools.MSMDReader(ms_name) as msmd:
        spws_fdm_tdm = msmd.almaspws(fdm=True, tdm=True)
        spws_observe_target = msmd.spwsforintent('OBSERVE_TARGET*')
        spws_calibrate_phase = msmd.spwsforintent('CALIBRATE_PHASE*')
        spws = np.intersect1d(np.union1d(spws_observe_target, spws_calibrate_phase), spws_fdm_tdm)
        datadescids = [msmd.datadescids(spw=spw)[0] for spw in spws]
    return list(spws), datadescids


def get_partialpol_flag_cmd_params(flags, ant1, ant2, time, interval):
    """Get the parameters that identify points where partial polarizations need to be flagged.
    This function should be called only if the data presents more than one polarization.
    At the moment it only handles data with 3 dimensions (n_pol, n_channels, n_params).

    :param flags: numpy array with the flags with shape (n_pol, n_channels, n_params)
    :param ant1: numpy array with the antenna1s with shape (n_params, )
    :param ant2: numpy array with the antenna2s with shape (n_params, )
    :param time: numpy array with the times with shape (n_params, )
    :param interval: numpy array with the intervals with shape (n_params, )
    :return: List of dictionaries with the set of params to identify partial polarizations.
      The dictionaries contain the keys:
       * "ant1" - ID of the antenna1,
       * "ant2" - ID of the antenna2,
       * "time" - Central time of the 'scan',
       * "interval" - Duration of the 'scan', and
       * "channels"- a list of numerical values of affected channels that can be compressed later.
    :rtype: List[Dict]
    """
    shape = np.shape(flags)
    # Check: Is there any chance that there are data with only 2 dimensions?
    if len(shape) != 3:
        LOG.error("Partial Polarization flagging: Data with no channel information are not handled by the pipeline yet")
    n_pol, n_channels, n_params = shape
    # Create a table of shape (n_channels, n_params) with the number of pols flagged
    folded_flags = np.sum(flags, axis=0)
    # Identify where polarization data is partially flagged.
    to_extend_idx = (folded_flags > 0) & (folded_flags < n_pol)
    # Identify the sets of parameters that have partial polarizations for any
    # of the channels.
    param_sets_to_check = np.where(np.any(to_extend_idx, axis=0))[0]
    params = []
    # Iterate only through the sets of parameters with partial polarizations
    for param_set_idx in param_sets_to_check:
        # Get the numerical values of the channels that have partial polarizations
        channel_sel = list(np.where(to_extend_idx[:, param_set_idx])[0])
        params.append({
            "ant1": ant1[param_set_idx],
            "ant2": ant2[param_set_idx],
            "time": time[param_set_idx],
            "interval": interval[param_set_idx],
            "channels": channel_sel
        })
    return params


def convert_params_to_commands(ms, params, ant_id_map=None):
    """Convert the identified partial polarization parameters to flagging commands.

    :param ms: Measurement Set to get the antenna id map (it can be None if ant_id_map is entered)
    :param params: List of dictionaries with the parameters
    :param ant_id_map: Dictionary mapping antenna IDs to their names (optional; overrides data from ms)
    :return: List of flagging commands
    :rtype: List[str]
    """
    if ant_id_map is None:
        ant_id_map = {ant.id: ant.name for ant in ms.antennas}
    commands = []
    for param in params:
        # Antennas
        antenna_str = "{}&&{}".format(ant_id_map[param["ant1"]], ant_id_map[param["ant2"]])
        # spw and channels
        ch_str = '{}:'.format(param["spw"])
        ch_ranges = channel_ranges(param["channels"])
        ch_str += ";".join(["{}~{}".format(down, up) if down != up else "{}".format(down) for down, up in ch_ranges])
        # Times
        time_start = casa_tools.quanta.quantity(param["time"] - 0.5 * param["interval"], param["time_unit"])
        time_end = casa_tools.quanta.quantity(param["time"] + 0.5 * param["interval"], param["time_unit"])
        time_str_start = casa_tools.quanta.time(time_start, prec=9, form="ymd")[0]
        time_str_end = casa_tools.quanta.time(time_end, prec=9, form="ymd")[0]
        time_str = "{}~{}".format(time_str_start, time_str_end)
        # Combine
        command = "antenna='{}' spw='{}' timerange='{}' reason='partialpol'".format(antenna_str, ch_str, time_str)
        commands.append(command)
    return commands


def lowtrans_alma(ms: MeasurementSet, mintransrepspw: float, mintransnonrepspws: float,
                  max_frac_low_trans: float) -> List[str]:
    """
    Create flagging commands to flag science spectral windows with low
    atmospheric transmission (PIPE-624).

    Args:
        ms: Measurement Set to evaluate.
        mintransrepspw: Atmospheric transmissivity threshold used to flag
            the representative science spectral window when fraction of data
            with low transmissivity exceeds "frac_low_trans".
        mintransnonrepspws: Atmospheric transmissivity threshold used to flag
            the non-representative science spectral window(s) when fraction of
            data with low transmissivity exceeds "frac_low_trans".
        max_frac_low_trans: Threshold fraction of data with low transmission
            at-or-above which a SpW is flagged for low transmission.

    Returns:
        List of flagging commands.
    """
    # Initialize flagging commands.
    commands = []

    # Compute the PWV for current MS. If this PWV value is invalid, then skip
    # the rest of the heuristic.
    pwv, _ = getMedianPWV(vis=ms.name)
    if pwv == 1.0 or np.isnan(pwv) or pwv < 0:
        LOG.debug(f'Invalid value for PWV ({pwv}) encountered during evaluation of low atmospheric transmission'
                  f' flagging, no flagging commands generated.')
        return commands

    # Get list of science scans and science SpWs, and representative SpW.
    scans = ms.get_scans(scan_intent="TARGET")
    scispws = ms.get_spectral_windows()
    _, repr_spwid = ms.get_representative_source_spw()

    # Compute the mean airmass for each science scan.
    airmass_for_scan = {scan.id: get_airmass_for_alma_scan(scan) for scan in scans}

    # Initializes atmospheric profile for a defined set of atmospheric
    # parameters from PIPE-624, that are common for all scans and SpWs.
    myat = casa_tools.atmosphere
    atmutil.init_atm(myat, altitude=5059.0, humidity=20.0, temperature=273.0, pressure=563.0, max_altitude=48.0,
                     delta_p=5.0, delta_pm=1.1, h0=1.0, atmtype=atmutil.AtmType.tropical)

    # Evaluate low transmission for each SpW:
    for spw in scispws:
        # Set transmission threshold based on whether current SpW is the
        # representative SpW.
        thresh_transm = mintransrepspw if spw.id == repr_spwid else mintransnonrepspws

        # Initialize spectral window setting in atmosphere tool for current SpW,
        # and update atmospheric profile to set PWV (needs to happen after SpW is
        # initialized).
        atmutil.init_spw(myat, fcenter=float(spw.mean_frequency.to_units(measures.FrequencyUnits.GIGAHERTZ)),
                         nchan=spw.num_channels,
                         resolution=float(spw.bandwidth.to_units(measures.FrequencyUnits.GIGAHERTZ) / spw.num_channels))
        myqa = casa_tools.quanta
        myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

        # Get wet and dry opacity from atmospheric profile, for channels of
        # current SpW.
        dry_opacity = atmutil.get_dry_opacity(myat)
        wet_opacity = atmutil.get_wet_opacity(myat)

        # Get transmission spectrum for opacity profiles for current SpW and
        # airmass of each scan.
        transm = np.asarray([atmutil.calc_transmission(airmass_for_scan[scan.id], dry_opacity, wet_opacity)
                             for scan in scans])

        # For collection of transmission spectra for current SpW, assess the
        # fraction of data points (channels, scans) that fall below the
        # threshold.
        n_low_trans = len(np.where(transm < thresh_transm)[0])
        frac_below_thresh = n_low_trans / transm.size

        # If the fraction of data points below transmission threshold is equal
        # to or higher than the given (fraction) threshold, then generate a
        # flagging command to flag the current SpW entirely.
        if frac_below_thresh >= max_frac_low_trans:
            LOG.info(f"{ms.basename}, SpW {spw.id}: fraction of data with low transmission = {n_low_trans} /"
                     f" {transm.size} = {frac_below_thresh:.2f}; this is equal/above the max fraction of"
                     f" {max_frac_low_trans} therefore this SpW will become flagged.")

            # Add new flagging command for current SpW.
            command = f"mode='manual' spw='{spw.id}' reason='low_transmission'"
            commands.append(command)
        else:
            LOG.info(
                f"{ms.basename}, SpW {spw.id}: fraction of data with low transmission = {n_low_trans} / {transm.size} ="
                f" {frac_below_thresh:.2f}; this is below the max fraction of {max_frac_low_trans}, therefore"
                f" therefore this SpW will not be flagged.")

    return commands


def get_elevation_for_alma_scan(scan, edge):
    """Get the elevation for the beginning or the end of an ALMA scan.

    Args:
        scan: Scan object.
        edge: Selects which edge of the scan, either "start" or "end".

    Returns:
        Astropy quantity corresponding to the elevation.
    """
    alma_site = EarthLocation.from_geocentric(x=2225015.30883296, y=-5440016.41799762, z=-2481631.27428014, unit='m')
    scan_field = next(iter(scan.fields))
    coords = SkyCoord(
        scan_field.mdirection['m0']['value'],  # RA
        scan_field.mdirection['m1']['value'],  # DEC
        frame=scan_field.mdirection['refer'].lower(),
        unit=(scan_field.mdirection['m0']['unit'], scan_field.mdirection['m1']['unit']))
    if edge == "start":
        time = Time(scan.start_time['m0']['value'], format='mjd')
    elif edge == "end":
        time = Time(scan.end_time['m0']['value'], format='mjd')
    else:
        raise RuntimeError('The parameter edge should be either "start" or "end".')
    coords_altaz = coords.transform_to(AltAz(obstime=time, location=alma_site))
    return coords_altaz.alt


def get_airmass_for_alma_scan(scan):
    """Compute the airmass corresponding to an ALMA observation scan.

    Args:
        scan: Scan object.

    Returns:
        Mean airmass of the scan.
    """
    start_elevation = get_elevation_for_alma_scan(scan, "start")
    end_elevation = get_elevation_for_alma_scan(scan, "end")
    start_airmass = atmutil.calc_airmass(start_elevation.deg)
    end_airmass = atmutil.calc_airmass(end_elevation.deg)
    return (start_airmass + end_airmass)/2.
