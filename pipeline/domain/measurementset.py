"""Provide a class to store logical representation of MeasurementSet."""
import collections
import contextlib
import itertools
import operator
import os
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
if TYPE_CHECKING: # Avoid circular import. Used only for type annotation.
    from pipeline.infrastructure.tablereader import RetrieveByIndexContainer

from . import measures
from . import spectralwindow
from .antennaarray import AntennaArray
from .datatype import DataType

LOG = infrastructure.get_logger(__name__)


class MeasurementSet(object):
    """
    A class to store logical representation of a MeasurementSet (MS).

    Attributes:
        name: A path to MeasurementSet
        session: Session name of MS
        antenna_array: Antenna array information
        array_name: Name of array configuration
        derived_fluxes: Flux measurements
        flagcmds: A list of flag commands
        filesize: Disk size of MS
        representative_target: A tuple of the name of representative source,
            frequency and bandwidth.
        representative_window: A representative spectral window name
        science_goals: A science goal information consists of min/max
            acceptable angular resolution, max allowed beam ratio, sensitivity,
            dynamic range and SB name.
        data_descriptions: A list of DataDescription objects associated with MS
        spectral_windows: A list of SpectralWindow objects associated with MS
        spectralspec_spwmap: SpectralSpec mapping
        fields: A list of Field objects associated with MS
        states: A list of State objects associated with MS
        reference_spwmap: Reference spectral window map
        phaseup_spwmap: Spectral window mapping used in spwphaseup calibration
        combine_spwmap: Spectral window mapping used to increase S/N ratio
        data_column: A dictionary to store data type (key) and corresponding
            data column (value)
        reference_antenna_locked: If True, reference antenna is locked to
            prevent modification
        is_imaging_ms: If True, the MS is for imaging (interferometry only)
        origin_ms: A path to the first generation MeasurementSet from which
            the current MS is generated.
        software_version: placeholder name for version of ALMA software to be read in and added to the weblog
    """

    def __init__(self, name: str, session: Optional[str]=None):
        """
        Initialize MeasurmentSet class.

        Args:
            name: A path to MS
            session: Session name of MS
        """
        self.name: str = name
        self.session: Optional[str] = session
        self.antenna_array: Optional[AntennaArray] = None
        self.array_name: str = None
        self.derived_fluxes: Optional[collections.defaultdict] = None
        self.flagcmds: List[str] = []
        self.filesize: measures.FileSize = self._calc_filesize()
        self.representative_target: Tuple[Optional[str], Optional[dict],
                                          Optional[dict]] = (None, None, None)
        self.representative_window: Optional[str] = None
        self.science_goals: dict = {}
        self.data_descriptions: Union[RetrieveByIndexContainer, list] = []
        self.spectral_windows: Union[RetrieveByIndexContainer, list] = []
        self.spectralspec_spwmap: dict = {}
        self.fields: Union[RetrieveByIndexContainer, list] = []
        self.states: Union[RetrieveByIndexContainer, list] = []
        self.reference_spwmap: Optional[List[int]] = None
        self.phaseup_spwmap: Optional[List[int]] = None
        self.combine_spwmap: Optional[List[int]] = None
        self.is_imaging_ms: bool = False
        self.origin_ms: str = name
        self.data_column: dict = {}
        self.acs_software_version = None
        self.software_build_version = None


        # Polarisation calibration requires the refant list be frozen, after
        # which subsequent gaincal calls are executed with
        # refantmode='strict'.
        #
        # To meet this requirement we make the MS refant list lockable. When
        # locked, the refant list cannot be changed. Additionally, gaincal
        # checks the lock status to know whether to set refantmode to strict.
        #
        # The backing property for the refant list.
        self._reference_antenna: Optional[str] = None
        # The refant lock. Setting reference_antenna_locked to True prevents
        # the reference antenna list from being modified. I would have liked
        # to put the lock on a custom refant list class, but some tasks check
        # the type of reference_antenna directly which prevents that approach.
        self.reference_antenna_locked: bool = False

    def _calc_filesize(self):
        """
        Calculate the disk usage of this measurement set.
        """
        total_bytes = 0
        for dirpath, _, filenames in os.walk(self.name):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_bytes += os.path.getsize(fp)

        return measures.FileSize(total_bytes, 
                                 measures.FileSizeUnits.BYTES)

    def __str__(self):
        return 'MeasurementSet({0})'.format(self.name)

    @property
    def intents(self):
        intents = set()
        # we look to field rather than state as VLA datasets don't have state
        # entries
        for field in self.fields:
            intents.update(field.intents)
        return intents

    @property
    def antennas(self):
        # return a copy rather than the underlying list
        return list(self.antenna_array.antennas)

    @property
    def basename(self):
        return os.path.basename(self.name)

    def get_antenna(self, search_term=''):
        if search_term == '':
            return self.antennas

        return [a for a in self.antennas
                if a.id in utils.ant_arg_to_id(self.name, search_term, self.antennas)]

    def get_state(self, state_id=None):
        match = [state for state in self.states if state.id == state_id]
        if match:
            return match[0]
        else:
            return None

    def get_scans(self, scan_id=None, scan_intent=None, field=None, spw=None):
        pool = self.scans

        if scan_id is not None:
            # encase raw numbers in a tuple
            if not isinstance(scan_id, collections.Sequence):
                scan_id = (scan_id,)
            pool = [s for s in pool if s.id in scan_id]

        if scan_intent is not None:
            if isinstance(scan_intent, str):
                if scan_intent in ('', '*'):
                    # empty string equals all intents for CASA
                    scan_intent = ','.join(self.intents)
                scan_intent = scan_intent.split(',')
            scan_intent = set(scan_intent) 
            pool = [s for s in pool if not s.intents.isdisjoint(scan_intent)]

        if field is not None:
            fields_with_name = frozenset(self.get_fields(task_arg=field))
            pool = [s for s in pool if not fields_with_name.isdisjoint(s.fields)]

        if spw is not None:
            if not isinstance(spw, collections.Sequence):
                spw = (spw,)
            if isinstance(spw, str):
                if spw in ('', '*'):
                    spw = ','.join(str(spw.id) for spw in self.spectral_windows)
                spw = spw.split(',')
            spw = {int(i) for i in spw}
            pool = {scan for scan in pool for scan_spw in scan.spws if scan_spw.id in spw}
            pool = list(pool)

        return pool

    def get_data_description(self, spw=None, id=None):
        match = None
        if spw is not None:
            if isinstance(spw, spectralwindow.SpectralWindow):
                match = [dd for dd in self.data_descriptions
                         if dd.spw is spw]
            elif isinstance(spw, int):
                match = [dd for dd in self.data_descriptions
                         if dd.spw.id == spw]
        if id is not None:
            match = [dd for dd in self.data_descriptions if dd.id == id]

        if match:
            return match[0]
        else:
            return None

    def get_representative_source_spw(self, source_name=None, source_spwid=None):
        qa = casa_tools.quanta
        cme = casa_tools.measures

        # Get the representative target source object
        #   Use user name if source_name is supplied by user and it has TARGET intent.
        #   Otherwise use the source defined in the ASDM SBSummary table if it has TARGET intent.
        #   Otherwise use the first source in the source list with TARGET intent
        if source_name:
            # Use the first target source that matches the user defined name
            target_sources = [source for source in self.sources
                              if source.name == source_name
                              and 'TARGET' in source.intents]
            if len(target_sources) > 0:
                LOG.info('Selecting user defined representative target source %s for data set %s' % \
                    (source_name, self.basename))
                target_source = target_sources[0]
            else:
                LOG.warning('User defined representative target source %s not found in data set %s' % \
                    (source_name, self.basename))
                target_source = None
        elif self.representative_target[0]:
            # Use the first target source that matches the representative source name in the ASDM
            # SBSummary table
            source_name = self.representative_target[0]
            target_sources = [source for source in self.sources
                              if source.name == source_name 
                              and 'TARGET' in source.intents] 
            if len(target_sources) > 0:
                LOG.info('Selecting representative target source %s for data set %s' % \
                    (self.representative_target[0], self.basename))
                target_source = target_sources[0]
            else:
                LOG.warning('Representative target source %s not found in data set %s' % \
                    (self.representative_target[0], self.basename))
                # Try to fall back first target source
                target_sources = [source for source in self.sources
                                  if 'TARGET' in source.intents] 
                if len(target_sources) > 0:
                    target_source = target_sources[0]
                    LOG.info('Falling back to first target source (%s) for data set %s' % \
                        (target_source.name, self.basename))
                else:
                    LOG.warning('No target sources observed for data set %s' % (self.basename))
                    target_source = None
        else:
            # Use first target source no matter what it is
            target_sources = [source for source in self.sources
                              if 'TARGET' in source.intents] 
            if len(target_sources) > 0:
                target_source = target_sources[0]
                LOG.info('Undefined representative target source, defaulting to first target source (%s) for data set %s' % \
                    (target_source.name, self.basename))
            else:
                LOG.warning('No target sources observed for data set %s' % (self.basename))
                target_source = None

        # Target source not found
        if target_source is None:
            return (None, None)

        # Target source name
        target_source_name = target_source.name

        # Check the user defined spw and return it if it was observed for the
        # representative source. If it was not observed quit.
        if source_spwid:
            found = False
            for f in target_source.fields:
                valid_spwids = [spw.id for spw in list(f.valid_spws)]
                if source_spwid in valid_spwids:
                    found = True
                    break
            if found:
                LOG.info('Selecting user defined representative spw %s for data set %s' % \
                    (str(source_spwid), self.basename))
                return (target_source_name, source_spwid)
            else:
                LOG.warning('No target source data for representative spw %s in data set %s' % \
                    (str(source_spwid), self.basename))
                return (target_source_name, None)

        target_spwid = None
        # Check for representative spw from ASDM (>= Cycle 7)
        if self.representative_window is not None:
            try:
                target_spwid = [s.id for s in self.get_spectral_windows() if s.name == self.representative_window][0]
            except:
                LOG.warning('Could not translate spw name %s to ID. Trying frequency matching heuristics.' % (self.representative_window))

        if target_spwid is not None:
            return (target_source_name, target_spwid)

        # Get the representative bandwidth
        #     Return if there isn't one
        if self.representative_target[2]:
            target_bw = cme.frequency('TOPO',
                qa.quantity(qa.getvalue(self.representative_target[2]),
                qa.getunit(self.representative_target[2])))
        else:
            LOG.warning('Undefined representative bandwidth for data set %s' % (self.basename))
            return (target_source_name, None)

        # Get the representative frequency
        #     Return if there isn't one
        if self.representative_target[1]:
            target_frequency = cme.frequency('BARY',
                qa.quantity(qa.getvalue(self.representative_target[1]),
                qa.getunit(self.representative_target[1])))
        else:
            LOG.warning('Undefined representative frequency for data set %s' % (self.basename))
            return (target_source_name, None)

        # Convert BARY frequency to TOPO
        #    Use the start time of the first target source scan
        #    Note some funny business with source and field names
        cme.doframe(cme.observatory(self.antenna_array.name))
        cme.doframe(target_source.direction)
        target_scans = [
            scan for scan in self.get_scans(scan_intent='TARGET')
            if target_source.id in [f.source_id for f in scan.fields]]
        if len(target_scans) > 0:
            cme.doframe(target_scans[0].start_time)
            target_frequency_topo = cme.measure(target_frequency, 'TOPO')
        else:
            LOG.error('Unable to convert representative frequency to TOPO for data set %s' % \
                (self.basename))
            target_frequency_topo = None
        cme.done()

        # No representative frequency
        if not target_frequency_topo:
            return (target_source_name, None)

        # Find the science spw 

        # Get the science spw ids
        science_spw_ids = [spw.id for spw in self.get_spectral_windows()]

        # Get the target science spws observed for the target source
        target_spws = {spw for f in target_source.fields for spw in f.valid_spws
                       if spw.id in science_spw_ids}

        # Now find all the target_spws that have a channel width less than
        # or equal to the representative bandwidth
        # Note that the representative bandwidth can be slightly smaller than the actual channel width.
        # This is due to the details of online Hanning smoothing and is technically correct. We thus
        # apply a 1% margin (see CAS-11710).
        target_spws_bw = [spw for spw in target_spws
                          if spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ) <= 1.01 * target_bw['m0']['value']]

        if len(target_spws_bw) <= 0:
            LOG.warning('No target spws have channel width <= representative bandwidth in data set %s' % \
                (self.basename))

            # Now find all the target spws that contain the representative frequency.
            target_spws_freq = [spw for spw in target_spws
                                if target_frequency_topo['m0']['value'] >= spw.min_frequency.value and
                                target_frequency_topo['m0']['value'] <= spw.max_frequency.value]

            if len(target_spws_freq) <= 0:
                LOG.warning('No target spws overlap the representative frequency in data set %s' % \
                    (self.basename))

                # Now find the closest match to the center frequency
                max_freqdiff = np.finfo('d').max
                for spw in target_spws:
                    freqdiff = abs(float(spw.centre_frequency.value) - target_frequency_topo['m0']['value'])
                    if freqdiff < max_freqdiff:
                        target_spwid = spw.id
                        max_freqdiff = freqdiff
                LOG.info('Selecting spw %s which is closest to the representative frequency in data set %s' % \
                    (str(target_spwid), self.basename))
            else:
                min_chanwidth = None
                for spw in target_spws_freq:
                    chanwidth = spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ)
                    if not min_chanwidth or chanwidth < min_chanwidth:
                        target_spwid = spw.id
                LOG.info('Selecting the narrowest chanwidth spw id %s which overlaps the representative frequency in data set %s' % \
                    (str(target_spwid), self.basename))

            return (target_source_name, target_spwid)

        # Now find all the spw that contain the representative frequency
        target_spws_freq = [spw for spw in target_spws_bw if target_frequency_topo['m0']['value'] >= spw.min_frequency.value and \
            target_frequency_topo['m0']['value'] <= spw.max_frequency.value]
        if len(target_spws_freq) <= 0:
            LOG.warning('No target spws with channel spacing <= representative bandwith overlap the representative frequency in data set %s' % (self.basename))
            max_chanwidth = None
            for spw in target_spws_bw:
                chanwidth = spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ)
                if (max_chanwidth is None) or (chanwidth > max_chanwidth):
                #if not_max_chanwidth or chanwidth > max_chanwidth:
                    target_spwid = spw.id
            LOG.info('Selecting widest channel width spw id {} with channel width <= representative bandwidth in data'
                     ' set {}'.format(str(target_spwid), self.basename))

            return (target_source_name, target_spwid)

        # For all the spws with channel width less than or equal
        # to the representative bandwidth which contain the
        # representative frequency select the one with the spw
        # with the greatest bandwidth.
        bestspw = None
        target_spwid = None
        for spw in sorted(target_spws_freq, key=lambda x: x.id):
            if not bestspw:
                bestspw = spw
            elif spw.bandwidth.value > bestspw.bandwidth.value: 
                bestspw = spw
        target_spwid = bestspw.id

        return target_source_name, target_spwid

    def get_fields(self, task_arg=None, field_id=None, name=None, intent=None):
        """
        Get Fields from this MeasurementSet matching the given criteria. If no
        criteria are given, all Fields in the MeasurementSet will be returned.

        Arguments can be given as either single items of the expected type,
        sequences of the expected type, or in the case of name or intent, as
        comma separated strings. For instance, name could be 'HOIX', 
        'HOIX,0841+708' or ('HOIX','0841+708').

        :param field_id: field ID(s) to match
        :param name: field name(s) to match
        :param intent: observing intent(s) to match
        :rtype: a (potentially empty) list of :class:`~pipeline.domain.field.Field` \
             objects
        """
        pool = self.fields

        if task_arg not in (None, ''):
            field_id_for_task_arg = utils.field_arg_to_id(self.name, task_arg, self.fields)
            pool = [f for f in pool if f.id in field_id_for_task_arg]

        if field_id is not None:
            # encase raw numbers in a tuple
            if not isinstance(field_id, collections.Sequence):
                field_id = (field_id,)
            pool = [f for f in pool if f.id in field_id]

        if name is not None:
            if isinstance(name, str):
                name = name.split(',')
            name = set(name) 
            pool = [f for f in pool if f.name in name]

        if intent is not None:
            if isinstance(intent, str):
                if intent in ('', '*'):
                    # empty string equals all intents for CASA
                    intent = ','.join(self.intents)
                intent = intent.split(',')
            intent = set(intent) 
            pool = [f for f in pool if not f.intents.isdisjoint(intent)]

        return pool

    def get_spectral_window(self, spw_id):
        if spw_id is not None:
            spw_id = int(spw_id)
            match = [spw for spw in self.spectral_windows 
                     if spw.id == spw_id]
            if match:
                return match[0]
            else:
                raise KeyError('No spectral window with ID \'{0}\' found in '
                               '{1}'.format(spw_id, self.basename))

    def get_spectral_windows(self, task_arg='', with_channels=False, num_channels=(), science_windows_only=True,
                             spectralspecs=None):
        """
        Return the spectral windows corresponding to the given CASA-style spw
        argument, filtering out windows that may not be science spectral 
        windows (WVR windows, channel average windows etc.).
        """
        spws = self.get_all_spectral_windows(task_arg, with_channels)

        # if requested, filter spws by number of channels
        if num_channels:
            spws = [w for w in spws if w.num_channels in num_channels] 

        # If requested, filter spws by spectral specs.
        if spectralspecs is not None:
            spws = [w for w in spws if w.spectralspec in spectralspecs]

        if not science_windows_only:
            return spws

        if self.antenna_array.name == 'ALMA':
            science_intents = {'TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE',
                               'POLARIZATION', 'POLANGLE', 'POLLEAKAGE',
                               'CHECK'}
            return [w for w in spws if w.num_channels not in (1, 4)
                    and not science_intents.isdisjoint(w.intents)]

        if self.antenna_array.name == 'VLA' or self.antenna_array.name == 'EVLA':
            science_intents = {'TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE',
                               'POLARIZATION', 'POLANGLE', 'POLLEAKAGE',
                               'CHECK'}
            return [w for w in spws if w.num_channels not in (1, 4)
                    and not science_intents.isdisjoint(w.intents) and 'POINTING' not in w.intents]

        if self.antenna_array.name == 'NRO':
            science_intents = {'TARGET'}
            return [w for w in spws if not science_intents.isdisjoint(w.intents)]

        return spws

    def get_spectral_specs(self) -> List[str]:
        """Return list of all spectral specs used in the MS."""
        return list(self.spectralspec_spwmap.keys())

    def get_all_spectral_windows(self, task_arg='', with_channels=False):
        """Return the spectral windows corresponding to the given CASA-style
        spw argument.
        """
        # we may have more spectral windows in our MeasurementSet than have
        # data in the measurement set on disk. Ask for all 
        if task_arg in (None, ''):
            task_arg = '*'

        # expand spw tuples into a range per spw, eg. spw9 : 1,2,3,4,5
        selected = collections.defaultdict(set)
        for (spw, start, end, step) in utils.spw_arg_to_id(self.name, task_arg, self.spectral_windows):
            selected[spw].update(set(range(start, end+1, step))) 

        if not with_channels:
            return [spw for spw in self.spectral_windows if spw.id in selected]

        spws = []
        for spw_id, channels in selected.items():
            spw_obj = self.get_spectral_window(spw_id)
            proxy = spectralwindow.SpectralWindowWithChannelSelection(spw_obj, 
                                                                      channels)
            spws.append(proxy)
        return spws

    def get_original_intent(self, intent=None):
        """
        Get the original obs_modes that correspond to the given pipeline
        observing intents.
        """
        obs_modes = [state.get_obs_mode_for_intent(intent)
                     for state in self.states]
        return set(itertools.chain(*obs_modes))

    @property
    def start_time(self):
        earliest, _ = min([(scan, utils.get_epoch_as_datetime(scan.start_time)) for scan in self.scans],
                          key=operator.itemgetter(1))
        return earliest.start_time

    @property
    def end_time(self):
        latest, _ = max([(scan, utils.get_epoch_as_datetime(scan.end_time)) for scan in self.scans],
                        key=operator.itemgetter(1))
        return latest.end_time

    def get_vla_max_integration_time(self):
        """Get the integration time used by the original VLA scripts

           Returns -- The max integration time used
        """

        vis = self.name

        # with casa_tools.TableReader(vis + '/FIELD') as table:
        #     numFields = table.nrows()
        #     field_positions = table.getcol('PHASE_DIR')
        #     field_ids = range(numFields)
        #     field_names = table.getcol('NAME')

        # with casa_tools.TableReader(vis) as table:
        #     scanNums = sorted(np.unique(table.getcol('SCAN_NUMBER')))
        #     field_scans = []
        #     for ii in range(0,numFields):
        #         subtable = table.query('FIELD_ID==%s'%ii)
        #         field_scans.append(list(np.unique(subtable.getcol('SCAN_NUMBER'))))
        #         subtable.close()

        # field_scans is now a list of lists containing the scans for each field.
        # so, to access all the scans for the fields, you'd:
        #
        # for ii in range(0,len(field_scans)):
        #    for jj in range(0,len(field_scans[ii]))
        #
        # the jj'th scan of the ii'th field is in field_scans[ii][jj]

        # Identify intents

        with casa_tools.TableReader(vis + '/STATE') as table:
            intents = table.getcol('OBS_MODE')

        """Figure out integration time used"""

        with casa_tools.MSReader(vis) as ms:
            scan_summary = ms.getscansummary()
            # ms_summary = ms.summary()
        # startdate=float(ms_summary['BeginTime'])

        integ_scan_list = []
        for scan in scan_summary:
            integ_scan_list.append(int(scan))
        sorted_scan_list = sorted(integ_scan_list)

        # find max and median integration times
        #
        integration_times = []
        for ii in sorted_scan_list:
            integration_times.append(scan_summary[str(ii)]['0']['IntegrationTime'])

        maximum_integration_time = max(integration_times)
        median_integration_time = np.median(integration_times)

        int_time = maximum_integration_time

        return int_time

    def get_vla_datadesc(self):
        """Generate VLA data description index"""

        vis = self.name

        cordesclist = ['Undefined', 'I', 'Q', 'U', 'V',
                       'RR', 'RL', 'LR', 'LL',
                       'XX', 'XY', 'YX', 'YY',
                       'RX', 'RY', 'LX', 'LY',
                       'XR', 'XL', 'YR', 'YL',
                       'PP', 'PQ', 'QP', 'QQ',
                       'RCircular', 'LCircular',
                       'Linear', 'Ptotal',
                       'Plinear', 'PFtotal',
                       'PFlinear', 'Pangle']

        # From Steve Myers buildscans function
        with casa_tools.TableReader(vis + '/DATA_DESCRIPTION') as table:
            # tb.open(msfile+"/DATA_DESCRIPTION")
            ddspwarr = table.getcol("SPECTRAL_WINDOW_ID")
            ddpolarr = table.getcol("POLARIZATION_ID")
            # tb.close()
        ddspwlist = ddspwarr.tolist()
        ddpollist = ddpolarr.tolist()
        ndd = len(ddspwlist)

        with casa_tools.TableReader(vis + '/SPECTRAL_WINDOW') as table:
            # tb.open(msfile+"/SPECTRAL_WINDOW")
            nchanarr = table.getcol("NUM_CHAN")
            spwnamearr = table.getcol("NAME")
            reffreqarr = table.getcol("REF_FREQUENCY")
            # tb.close()
        nspw = len(nchanarr)
        spwlookup = {}
        for isp in range(nspw):
            spwlookup[isp] = {}
            spwlookup[isp]['nchan'] = nchanarr[isp]
            spwlookup[isp]['name'] = str(spwnamearr[isp])
            spwlookup[isp]['reffreq'] = reffreqarr[isp]

        with casa_tools.TableReader(vis + '/POLARIZATION') as table:
            # tb.open(msfile+"/POLARIZATION")
            ncorarr = table.getcol("NUM_CORR")
            npols = len(ncorarr)
            polindex = {}
            poldescr = {}
            for ip in range(npols):
                cort = table.getcol("CORR_TYPE", startrow=ip, nrow=1)
                (nct, nr) = cort.shape
                cortypes = []
                cordescs = []
                for ict in range(nct):
                    cct = cort[ict][0]
                    cde = cordesclist[cct]
                    cortypes.append(cct)
                    cordescs.append(cde)
                polindex[ip] = cortypes
                poldescr[ip] = cordescs

        ddindex = {}
        ncorlist = ncorarr.tolist()
        for idd in range(ndd):
            ddindex[idd] = {}
            isp = ddspwlist[idd]
            ddindex[idd]['spw'] = isp
            ddindex[idd]['spwname'] = spwlookup[isp]['name']
            ddindex[idd]['nchan'] = spwlookup[isp]['nchan']
            ddindex[idd]['reffreq'] = spwlookup[isp]['reffreq']
            #
            ipol = ddpollist[idd]
            ddindex[idd]['ipol'] = ipol
            ddindex[idd]['npol'] = ncorlist[ipol]
            ddindex[idd]['corrtype'] = polindex[ipol]
            ddindex[idd]['corrdesc'] = poldescr[ipol]

        return ddindex

    def get_vla_corrstring(self):
        """Get correlation string for VLA"""

        """
        Prep string listing of correlations from dictionary created by method buildscans
        For now, only use the parallel hands.  Cross hands will be implemented later.
        """

        ddindex = self.get_vla_datadesc()

        corrstring_list = ddindex[0]['corrdesc']
        removal_list = ['RL', 'LR', 'XY', 'YX']
        corrstring_list = list(set(corrstring_list).difference(set(removal_list)))
        corrstring = ','.join(corrstring_list)

        return corrstring

    def get_alma_corrstring(self):
        """Get correlation string for ALMA for the science windows"""

        sci_spwlist = self.get_spectral_windows(science_windows_only=True)
        sci_spwids = [spw.id for spw in sci_spwlist]

        datadescs = [dd for dd in self.data_descriptions if dd.spw.id in sci_spwids]

        numpols = len(datadescs[0].polarizations)

        if numpols == 1:
            corrstring = 'XX'
        else:
            corrstring = 'XX,YY'

        return corrstring

    def get_vla_spw2band(self):

        ddindex = self.get_vla_datadesc()

        spw2band = {}

        for spw in ddindex:

            strelems = list(ddindex[spw]['spwname'])
            # print strelems
            bandname = strelems[5]
            if bandname in '4PLSCXUKAQ':
                spw2band[spw] = strelems[5]
            # Check for U / KU
            if strelems[5] == 'K' and strelems[6] == 'U':
                spw2band[spw] = 'U'
            if strelems[5] == 'K' and strelems[6] == 'A':
                spw2band[spw] = 'A'

        return spw2band

    def vla_minbaselineforcal(self):

        #return max(4, int(len(self.antennas) / 2.0))
        return 4

    def vla_spws_for_field(self, field):
        """VLA spws for field"""

        vis = self.name

        # get observed DDIDs for specified field from MAIN
        with casa_tools.TableReader(vis) as table:
            st = table.query('FIELD_ID=='+str(field))
            ddids = np.unique(st.getcol('DATA_DESC_ID'))
            st.close()

        # get SPW_IDs corresponding to those DDIDs
        with casa_tools.TableReader(vis+'/DATA_DESCRIPTION') as table:
            spws = table.getcol('SPECTRAL_WINDOW_ID')[ddids]

        # return as a list
        return list(spws)

    def get_vla_field_ids(self):
        """Find field ids for VLA"""

        vis = self.name

        with casa_tools.TableReader(vis+'/FIELD') as table:
            numFields = table.nrows()
            field_ids = list(range(numFields))

        return field_ids

    def get_vla_field_names(self):
        """Find field names for VLA"""

        vis = self.name

        with casa_tools.TableReader(vis+'/FIELD') as table:
            field_names = table.getcol('NAME')

        return field_names

    def get_vla_field_spws(self, spwlist=[]):
        """Find field spws for VLA"""

        vis = self.name

        # with casa_tools.TableReader(vis+'/FIELD') as table:
        #     numFields = table.nrows()

        # Map field IDs to spws
        field_spws = []
        # for ii in range(numFields):
        #     field_spws.append(self.vla_spws_for_field(ii))

        spwlistint = [int(spw) for spw in spwlist]

        with casa_tools.MSMDReader(vis) as msmd:
            spwsforfieldsall = msmd.spwsforfields()

            if spwlist != []:
                spwsforfields = {}
                for field, spws in spwsforfieldsall.items():
                    spwsforfields[field] = [spw for spw in spws if spw in spwlistint]
            else:
                spwsforfields = spwsforfieldsall

            spwfieldkeys = sorted([int(i) for i in spwsforfields])
            spwfieldkeys = [str(i) for i in spwfieldkeys]

            for key in spwfieldkeys:
                field_spws.append(spwsforfields[key])

        return field_spws

    def get_vla_numchan(self):
        """Get number of channels for VLA"""

        vis = self.name

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            channels = table.getcol('NUM_CHAN')

        return channels

    def get_vla_tst_bpass_spw(self, spwlist=[]):
        """Get VLA test bandpass spws"""

        vis = self.name
        tst_delay_spw = ''

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            channels = table.getcol('NUM_CHAN')

        numSpws = len(channels)

        ispwlist = [int(spw) for spw in spwlist]
        #for ispw in range(numSpws):
        for ispw in ispwlist:
            endch1 = int(channels[ispw]/3.0)
            endch2 = int(2.0*channels[ispw]/3.0)+1
            #if ispw < max(range(numSpws)):
            if ispw < max(ispwlist):
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)+','
                # all_spw=all_spw+str(ispw)+','
            else:
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)
                # all_spw=all_spw+str(ispw)

        tst_bpass_spw = tst_delay_spw

        return tst_bpass_spw

    def get_vla_tst_delay_spw(self, spwlist=[]):
        """Get VLA test bandpass spws"""

        vis = self.name
        tst_delay_spw = ''

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            channels = table.getcol('NUM_CHAN')

        numSpws = len(channels)

        ispwlist = [int(spw) for spw in spwlist]
        # for ispw in range(numSpws):
        for ispw in ispwlist:
            endch1 = int(channels[ispw]/3.0)
            endch2 = int(2.0*channels[ispw]/3.0)+1
            #if ispw < max(range(numSpws)):
            if ispw < max(ispwlist):
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)+','
                # all_spw=all_spw+str(ispw)+','
            else:
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)
                # all_spw=all_spw+str(ispw)

        return tst_delay_spw

    def get_vla_quackingscans(self):
        """Find VLA scans for quacking.  Quack! :)"""

        vis = self.name
        with casa_tools.MSReader(vis) as ms:
            scan_summary = ms.getscansummary()

        integ_scan_list = []
        for scan in scan_summary:
            integ_scan_list.append(int(scan))
        sorted_scan_list = sorted(integ_scan_list)

        scan_list = [1]
        old_scan = scan_summary[str(sorted_scan_list[0])]['0']

        old_field = old_scan['FieldId']
        old_spws = old_scan['SpwIds']
        for ii in range(1, len(sorted_scan_list)):
            new_scan = scan_summary[str(sorted_scan_list[ii])]['0']
            new_field = new_scan['FieldId']
            new_spws = new_scan['SpwIds']
            if ((new_field != old_field) or (set(new_spws) != set(old_spws))):
                scan_list.append(sorted_scan_list[ii])
                old_field = new_field
                old_spws = new_spws
        quack_scan_string = ','.join(["%s" % ii for ii in scan_list])

        return quack_scan_string

    def get_vla_critfrac(self):
        """Identify bands/basebands/spws"""

        vis = self.name

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            spw_names = table.getcol('NAME')

        # If the dataset is too old to have the bandname in it, assume that
        # either there are 8 spws per baseband (and allow for one or two for
        # pointing), or that this is a dataset with one spw per baseband

        if len(spw_names) >= 8:
            critfrac = 0.9/int(len(spw_names)/8.0)
        else:
            critfrac = 0.9/float(len(spw_names))

        if '#' in spw_names[0]:
            #
            # i assume that if any of the spw_names have '#', they all do...
            #
            bands_basebands_subbands = []
            for spw_name in spw_names:
                receiver_name, baseband, subband = spw_name.split('#')
                receiver_band = (receiver_name.split('_'))[1]
                bands_basebands_subbands.append([receiver_band, baseband, int(subband)])
            spws_info = [[bands_basebands_subbands[0][0], bands_basebands_subbands[0][1], [], []]]
            bands = [bands_basebands_subbands[0][0]]
            for ii in range(len(bands_basebands_subbands)):
                band, baseband, subband = bands_basebands_subbands[ii]
                found = -1
                for jj in range(len(spws_info)):
                    oband, obaseband, osubband, ospw_list = spws_info[jj]
                    if band == oband and baseband == obaseband:
                        osubband.append(subband)
                        ospw_list.append(ii)
                        found = jj
                        break
                if found >= 0:
                    spws_info[found] = [oband, obaseband, osubband, ospw_list]
                else:
                    spws_info.append([band, baseband, [subband], [ii]])
                    bands.append(band)
            # logprint("Bands/basebands/spws are:", logfileout='logs/msinfo.log')
            for spw_info in spws_info:
                spw_info_string = spw_info[0] + '   ' + spw_info[1] + '   [' + ','.join(["%d" % ii for ii in spw_info[2]]) + ']   [' + ','.join(["%d" % ii for ii in spw_info[3]]) + ']'
                # logprint(spw_info_string, logfileout='logs/msinfo.log')

            # Critical fraction of flagged solutions in delay cal to avoid an
            # entire baseband being flagged on all antennas
            critfrac = 0.9/float(len(spws_info))
        elif ':' in spw_names[0]:
            print("old spw names with :")
            # logprint("old spw names with :", logfileout='logs/msinfo.log')
        else:
            print("unknown spw names")

        return critfrac

    def get_vla_baseband_spws(self, science_windows_only=True,
                              return_select_list=True, warning=True):
        """Get the SPW information from individual VLA band/baseband.

        Args:
            science_windows_only (bool, optional): Defaults to True.
            return_select_list (bool, optional): return spw list of each baseband. Defaults to True.
            warning (bool, optional): Defaults to True.

        Returns:
            baseband_spws: spws info of individual basebands as baseband_spws[band][baseband]
            baseband_spws_list: spw_list of individual basebands
                e.g., [[0,1,2,3],[4,5,6,7]]
        """

        baseband_spws = collections.defaultdict(lambda: collections.defaultdict(list))

        for spw in self.get_spectral_windows(science_windows_only=science_windows_only):
            try:
                band = spw.name.split('#')[0].split('_')[1]
                baseband = spw.name.split('#')[1]
                min_freq = spw.min_frequency
                max_freq = spw.max_frequency
                mean_freq = spw.mean_frequency
                chan_width = spw.channels[0].getWidth()
                baseband_spws[band][baseband].append({spw.id: (min_freq, max_freq, mean_freq, chan_width)})
            except Exception as ex:
                if warning:
                    LOG.warn("Exception: Baseband name cannot be parsed. {!s}".format(str(ex)))
                else:
                    pass

        if return_select_list:
            baseband_spws_list = []
            for band in baseband_spws.values():
                for baseband in band.values():
                    baseband_spws_list.append([[*spw_info][0] for spw_info in baseband])
            return baseband_spws, baseband_spws_list
        else:
            return baseband_spws

    def get_median_integration_time(self, intent=None):
        """Get the median integration time used to get data for the given
        intent.

        Keyword arguments:
        intent  -- The intent of the data of interest.

        Returns -- The median integration time used.
        """
        LOG.debug('inefficiency - MSFlagger reading file to get integration '
                  'time')

        # get the field IDs and state IDs for fields in the measurement set,
        # filtering by intent if necessary
        if intent:    
            field_ids = [field.id for field in self.fields 
                         if intent in field.intents]
            state_ids = [state.id for state in self.states
                         if intent in state.intents]
#        if intent:
#            re_intent = intent.replace('*', '.*')
#            re_intent = re.compile(re_intent)
#            field_ids = [field.id for field in self.fields 
#                         if re_intent.match(str(field.intents))]
#            state_ids = [state.id for state in self.states
#                         if re_intent.match(str(state.intents))]
        else:
            field_ids = [field.id for field in self.fields]
            state_ids = [state.id for state in self.states]

        # VLA datasets have an empty STATE table; in the main table such rows
        # have a state ID of -1.
        if not state_ids:
            state_ids = [-1] 

        with casa_tools.TableReader(self.name) as table:
            taql = '(STATE_ID IN %s AND FIELD_ID IN %s)' % (state_ids, field_ids)
            with contextlib.closing(table.query(taql)) as subtable:
                integration = subtable.getcol('INTERVAL')          
            return np.median(integration)

    def get_median_science_integration_time(self, intent=None, spw=None):
        """Get the median integration time for science targets used to get data for the given
        intent.

        Keyword arguments:
        intent  -- The intent of the data of interest.
        spw     -- spw string list - '1,7,11,18'

        Returns -- The median integration time used.
        """
        LOG.debug('inefficiency - MSFlagger reading file to get median integration '
                  'time for science targets')

        if spw is None:
            spws = self.spectral_windows
        else: 

            try:
                # Put csv string of spws into a list
                spw_string_list = spw.split(',')

                # Get all spw objects
                all_spws = self.spectral_windows

                # Filter out the science spw objects
                spws = [ispw for ispw in all_spws if str(ispw.id) in spw_string_list]
            except:
                LOG.error("Incorrect spw string format.")

        # now get the science spws, those used for scientific intent
        science_spws = [
            ispw for ispw in spws
            if ispw.num_channels not in [1, 4]
            and not ispw.intents.isdisjoint(['BANDPASS', 'AMPLITUDE', 'PHASE',
                                             'TARGET'])]
        LOG.debug('science spws are: %s' % [ispw.id for ispw in science_spws])

        # and the science fields/states
        science_field_ids = [
            field.id for field in self.fields
            if not set(field.intents).isdisjoint(['BANDPASS', 'AMPLITUDE',
                                                  'PHASE', 'TARGET'])]
        science_state_ids = [
            state.id for state in self.states
            if not set(state.intents).isdisjoint(['BANDPASS', 'AMPLITUDE',
                                                  'PHASE', 'TARGET'])]

        science_spw_dd_ids = [self.get_data_description(spw).id for spw in science_spws]

        with casa_tools.TableReader(self.name) as table:
            taql = '(STATE_ID IN %s AND FIELD_ID IN %s AND DATA_DESC_ID in %s)' % (science_state_ids, science_field_ids, science_spw_dd_ids)
            with contextlib.closing(table.query(taql)) as subtable:
                integration = subtable.getcol('INTERVAL')          
            return np.median(integration)

    @property
    def reference_antenna(self):
        """
        Get the reference antenna list for this MS. The refant value is
        a comma-separated string.

        Example: 'DV01,DV02,DV03'
        """
        return self._reference_antenna

    @reference_antenna.setter
    def reference_antenna(self, value):
        """
        Set the reference antenna list for this MS.

        If this property is in R/O mode, signified by reference_antenna_locked
        being set True, an AttributeError will be raised.
        """
        if self.reference_antenna_locked:
            # AttributeError is raised for R/O properties, which seems
            # appropriate for this scenario
            raise AttributeError(f'Refant list for {self.basename} is locked')
        self._reference_antenna = value

    def update_reference_antennas(self, ants_to_demote=None, ants_to_remove=None):
        """Update the reference antenna list by demoting and/or removing
        specified antennas.

        If the same antenna is specified to be demoted and to be removed, it
        is removed.

        :param ants_to_demote: list of antenna names to demote
        :param ants_to_remove: list of antenna names to remove
        """
        if ants_to_demote is None:
            ants_to_demote = []
        if ants_to_remove is None:
            ants_to_remove = []

        # Return early if no refants are registered (None, or empty string).
        if not (self.reference_antenna and self.reference_antenna.strip()):
            LOG.warning("No reference antennas registered set for MS {}, "
                        "cannot update its reference antenna list."
                        "".format(self.name))
            return

        # Create updated refant list.
        refants_to_keep = []
        refants_to_move = []
        for ant in self.reference_antenna.split(','):
            if ant not in ants_to_remove:
                if ant in ants_to_demote:
                    refants_to_move.append(ant)
                else:
                    refants_to_keep.append(ant)
        refants_to_keep.extend(refants_to_move)

        # Update refant list.
        self.reference_antenna = ','.join(refants_to_keep)

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value):
        if value is None:
            value = 'session_1'
        self._session = value

    def set_data_column(self, dtype: DataType, column: str,
                        spw: Optional[str]=None, field: Optional[str]=None,
                        overwrite: bool=False):
        """
        Set data type and column.

        Set data type and column to MS domain object or to selected spectral
        window and field. If both spw and field are None, data column
        information of MS domain object is set. If both spw and field are not
        None, data column information of both spectral windows and fields
        selected by the string selection syntaxes are set.

        Args:
            dtype: data type to set
            column: name of column in MS associated with the data type
            spw: spectral window selection string
            field: field selection string
            overwrite: if True existing data colum is overwritten by the new
                column. If False and if type is already associated with other
                column, the function raises ValueError.

        Raises:
            ValueError: An error raised when the column does not exist
                or the type is already associated with a column and would not
                be overwritten.
        """
        # Check existence of the column
        with casa_tools.TableReader(self.name) as table:
            cols = table.colnames()
        if column not in cols:
            raise ValueError('Column {} does not exists in {}'.format(column, self.basename))
        if spw is None and field is None: # Update MS domain object
            if not overwrite and dtype in self.data_column:
                raise ValueError('Data type {} is already associated with {} in {}'.format(dtype, self.get_data_column(dtype), self.basename))
            self.data_column[dtype] = column
            LOG.info('Updated data column information of {}. Set {} to column, {}'.format(self.basename, dtype, column))
            return
        # Update Spw
        if spw is not None:
            for s in self.get_spectral_windows(task_arg=spw, science_windows_only=False):
                if not overwrite and dtype in s.data_column.keys():
                    raise ValueError('Data type {} is already associated with {} in spw {}'.format(dtype, s.data_column[dtype], s.id))
                s.data_column[dtype] = column
        # Update field
        if field is not None:
            for f in self.get_fields(field):
                if not overwrite and dtype in f.data_column.keys():
                    raise ValueError('Data type {} is already associated with {} in field {}'.format(dtype, f.data_column[dtype], f.id))
                f.data_column[dtype] = column

    def get_data_column(self, dtype: DataType) -> Optional[str]:
        """
        Retun a column name associated with a DataType in MS domain object.

        Args:
            dtype: DataType to fetch column name for

        Returns:
            A name of column of a dtype. Returns None if dtype is not defined
            in the MS.
        """
        if not (dtype in self.data_column.keys()):
            return None
        return self.data_column[dtype]

    def get_software_version() -> Optional[str]:
        #TODO: Try to get table info, else, return None
        return None