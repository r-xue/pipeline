import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.flagging import flagdeterbase
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.utils import utils

__all__ = [
    'FlagDeterALMA',
    'FlagDeterALMAInputs'
]

LOG = infrastructure.get_logger(__name__)


class FlagDeterALMAInputs(flagdeterbase.FlagDeterBaseInputs):
    """
    FlagDeterALMAInputs defines the inputs for the FlagDeterALMA pipeline task.
    """
    edgespw = vdp.VisDependentProperty(default=True)
    flagbackup = vdp.VisDependentProperty(default=True)
    fracspw = vdp.VisDependentProperty(default=0.03125)
    template = vdp.VisDependentProperty(default=True)

    # new property for ACA correlator
    fracspwfps = vdp.VisDependentProperty(default=0.048387)

    # New property for QA0 / QA2 flags
    qa0 = vdp.VisDependentProperty(default=True)
    qa2 = vdp.VisDependentProperty(default=True)

    def __init__(self, context, vis=None, output_dir=None, flagbackup=None, autocorr=None, shadow=None, scan=None,
                 scannumber=None, intents=None, edgespw=None, fracspw=None, fracspwfps=None, online=None,
                 fileonline=None, template=None, filetemplate=None, hm_tbuff=None, tbuff=None, qa0=None, qa2=None):
        super(FlagDeterALMAInputs, self).__init__(
            context, vis=vis, output_dir=output_dir, flagbackup=flagbackup, autocorr=autocorr, shadow=shadow, scan=scan,
            scannumber=scannumber, intents=intents, edgespw=edgespw, fracspw=fracspw, fracspwfps=fracspwfps,
            online=online, fileonline=fileonline, template=template, filetemplate=filetemplate, hm_tbuff=hm_tbuff,
            tbuff=tbuff)

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

    def get_fracspw(self, spw):    
        # From T. Hunter on PIPE-425: in early ALMA Cycles, the ACA
        # correlator's frequency profile synthesis (fps) algorithm produced TDM
        # spws that had 64 channels in full-polarisation, 124 channels in dual
        # pol, and 248 channels in single-pol.
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
                # explicitly checking whether the spw is an ACA spw.
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
        # spectral window with TARGET intent, which is representative of the
        # baseband.
        bb_spw = [s for s in self.inputs.ms.get_spectral_windows(science_windows_only=False)
                  if s.baseband == spw.baseband and s.type == 'SQLD' and 'TARGET' in s.intents]

        # If no baseband spw could be identified, log warning and return
        # with no new flagging commands.
        if not bb_spw:
            LOG.warning("{} - Unable to determine baseband range for spw {}, skipping ACA FDM edge flagging."
                        "".format(self.inputs.ms.basename, spw.id))
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
            LOG.warning('{} - Flagging edge channels for ACA spectral window {}, channel(s) {}, due to proximity'
                        ' to edge of baseband.'.format(self.inputs.ms.basename, spw.id, chan_to_flag))
            to_flag = ['{}:{}'.format(spw.id, chan_to_flag)]

        return to_flag
