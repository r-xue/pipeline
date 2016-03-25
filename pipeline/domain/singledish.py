from __future__ import absolute_import
import decimal
import numpy
import os
import re

from . import antenna
from . import measures
from . import observingrun
from . import source
from . import spectralwindow
from .datatable import DataTableImpl as DataTable

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools

LOG = infrastructure.get_logger(__name__)


def to_numeric_freq(m, unit=measures.FrequencyUnits.HERTZ):
    return float(m.convert_to(unit).value)

class ScantableList(observingrun.ObservingRun, list):
    def __init__(self):
        super(ScantableList, self).__init__()
        self.reduction_group = {}
        self.grid_position = {}
        self.datatable_instance = None
        self.datatable_name = None  # os.path.join(context.name,'DataTable.tbl')

    def __getstate__(self):
        mydict = self.__dict__.copy()
        # not to pickle DataTable instance
        mydict['datatable_instance'] = None
        return mydict

    def __setstate__(self, d):
        self.__dict__ = d
        name = self.__dict__['datatable_name']
        if name is not None and self.__dict__['datatable_instance'] is None:
            datatable = DataTable(name)
            self.__dict__['datatable_instance'] = datatable
        

    @property
    def start_time(self):
        if len(self) > 0:
            obj = self
        elif len(self.measurement_sets) > 0:
            obj = self.measurement_sets
        else:
            return None
        qt = casatools.quanta
        s = sorted(obj,
                   key=lambda st: st.start_time['m0'],
                   cmp=lambda x, y: 1 if qt.gt(x, y) else 0 if qt.eq(x, y) else -1)
        return s[0].start_time

    @property
    def end_time(self):
        if len(self) > 0:
            obj = self
        elif len(self.measurement_sets) > 0:
            obj = self.measurement_sets
        else:
            return None
        qt = casatools.quanta
        s = sorted(obj,
                   key=lambda st: st.end_time['m0'],
                   cmp=lambda x, y: 1 if qt.gt(x, y) else 0 if qt.eq(x, y) else -1)
        return s[-1].end_time

    @property
    def st_names(self):
        return [st.basename for st in self]

    def merge_inspection(self, instance, name=None, reduction_group=None,
                         calibration_strategy=None, beam_size=None,
                         grid_position=None, observing_pattern=None):
        self.datatable_instance = instance
        if name is not None:
            self.datatable_name = name

        self.reduction_group = reduction_group
        self.grid_position = grid_position
        
        for idx in xrange(len(self)):
            self[idx].calibration_strategy = calibration_strategy[idx]
            self[idx].beam_size = beam_size[idx]
            self[idx].pattern = observing_pattern[idx]
            self[idx].work_data = self[idx].name
            self[idx].baseline_source = self[idx].name

    def add_scantable(self, s):
        if s.basename in self.st_names:
            msg = '%s is already in the pipeline context' % (s.name)
            LOG.error(msg)
            raise Exception, msg

        self.append(s)
        if s.ms_name:
            ms = self.get_ms(name=s.ms_name)
            if hasattr(ms, 'scantables'):
                ms.scantables.append(s)
            else:
                ms.scantables = [s]

    def get_spw_for_wvr(self, name):
        st = self.get_scantable(name)
        spw = st.spectral_window
        return self.__get_spw_from_condition(spw, lambda v: v.type == 'WVR')

    def get_spw_without_wvr(self, name):
        st = self.get_scantable(name)
        spw = st.spectral_window
        return self.__get_spw_from_condition(spw, lambda v: v.type != 'WVR')

    def get_spw_for_science(self, name):
        st = self.get_scantable(name)
        spw = st.spectral_window
        return self.__get_spw_from_condition(spw, lambda v: v.is_target and v.type != 'WVR' and v.nchan > 1)

    def get_spw_for_caltsys(self, name):
        st = self.get_scantable(name)
        spw = st.spectral_window
        return self.__get_spw_from_condition(spw, lambda v: (v.is_target or v.is_atmcal) and v.type != 'WVR' and v.nchan > 1)

    def __get_spw_from_condition(self, spw_list, condition):
        return [k for (k, v) in spw_list.items() if condition(v) is True]

    def get_calmode(self, name):
        st = self.get_scantable(name)
        return st.calibration_strategy['calmode']
        
    def get_scantable(self, name):
        if isinstance(name, str):
            for entry in self:
                if entry.basename == name:
                    return entry
            return None
        else:
            # should be integer index
            return self[name]

    

class SingleDishBase(object):
    def __repr__(self):
        s = '%s:\n' % (self.__class__.__name__)
        for (k, v) in self.__dict__.items():
            if k[0] == '_':
                key = k[1:]
            else:
                key = k
            s += '\t%s=%s\n' % (key, v)
        return s

    def _init_properties(self, properties={}, kw_ignore=['self']):
        for (k, v) in properties.items():
            if k not in kw_ignore:
                setattr(self, k, v)

class ScantableRep(SingleDishBase):
    tolerance = 1.0e-3

    def __init__(self, name, ms=None, session=None):
        self.name = name.rstrip('/')
        self.ms = ms
        self.session = session

        # observation property
        self.observer = None
        self.start_time = None
        self.end_time = None

        # polarization property
        self.polarization = {}

        # spectral window property
        self.spectral_window = {}

        # calibration property
        self.tsys_transfer = None
        self.tsys_transfer_list = []

        # source property
        self.source = {}

        # antenna property
        self.antenna = None
        
        # exported ms for imaging
        self.exported_ms = None

    @property
    def basename(self):
        return os.path.basename(self.name)

    @property
    def ms_name(self):
        if self.ms is not None:
            return self.ms.basename
        else:
            return None
            
    @property
    def baselined_name(self):
        return self.name + '_work'

    @property
    def scaled_name(self):
        return self.name + '_corrected'

    def __repr__(self):
        return 'Scantable(%s)' % (self.name)

class Polarization(SingleDishBase):
    to_polid = {'XX': 0, 'YY': 1, 'XY': 2, 'YX': 3,
                'RR': 0, 'LL': 1, 'RL': 2, 'LR': 3,
                'I' : 0, 'Q': 1, 'U' : 2, 'V' : 3} 
    to_polenum = {'XX':  9, 'YY': 12, 'XY': 10, 'YX': 11,
                  'RR':  5, 'LL':  8, 'RL':  6, 'LR':  7,
                  'I' :  1, 'Q':  2, 'U' :  3, 'V' :  4}
    polarization_map = { 'linear': { 0: ['XX', 9],
                                     1: ['YY', 12],
                                     2: ['XY', 10],
                                     3: ['YX', 11] },
                         'circular': { 0: ['RR', 5],
                                       1: ['LL', 8],
                                       2: ['RL', 6],
                                       3: ['LR', 7] },
                         'stokes': { 0: ['I', 1],
                                     1: ['Q', 2],
                                     2: ['U', 3],
                                     3: ['V', 4] },
                         'linpol': { 0: ['Ptotal', 28],
                                     1: ['Plinear', 29],
                                     2: ['PFtotal', 30],
                                     3: ['PFlinear', 31],
                                     4: ['Pangle', 32] } }
    @staticmethod
    def from_data_desc(datadesc):
        npol = datadesc.num_polarizations
        corr_axis = datadesc.corr_axis
        corr_type = datadesc.polarizations
        if 'X' in corr_type or 'Y' in corr_type:
            poltype = 'linear'
        elif 'R' in corr_type or 'L' in corr_type:
            poltype = 'circular'
        else:
            poltype = 'stokes'
        polno = [Polarization.to_polid[x] for x in corr_axis]
        corr_enum = [Polarization.to_polenum[x] for x in corr_axis]
        entry = Polarization(type=poltype,
                             polno=polno,
                             spw_association=[datadesc.spw.id],
                             corr_string=corr_axis,
                             corr_index=corr_enum)
        return entry
        

    def __init__(self, type=None, polno=None, corr_index=None, corr_string=None, spw_association=[]):
        self._init_properties(vars())

        
class SpectralWindowAdapter:

    frame_map = { 0: 'REST',
                  1: 'LSRK',
                  2: 'LSRD',
                  3: 'BARY',
                  4: 'GEO',
                  5: 'TOPO',
                  6: 'GALACTO',
                  7: 'LGROUP',
                  8: 'CMB' }
    
    __slots__ = ('spw', '_frame', '_freq_max', '_freq_min',
                 '_intent', '_intents', '_pol_association',
                 '_rest_frequencies', 'deviation_mask')

    def __getstate__(self):
        state_dictionary = self.__dict__.copy()
        for attribute in self.__slots__:
            if hasattr(self, attribute):
                state_dictionary[attribute] = getattr(self, attribute)
        return state_dictionary

    def __setstate__(self, d):
        for (k, v) in d.items():
            if not hasattr(self, k):
                setattr(self, k, v)
        self.__dict__ = d
    
    @staticmethod
    def from_spectral_window(spw):
        entry = SpectralWindowAdapter(spw=spw)
        return entry
    
    def __init__(self, spw):
        self.spw = spw
        self.deviation_mask = None
        
    @property
    def band(self):
        return self.spw.band
    
    @property
    def bandwidth(self):
        # if not hasattr(self, '_bandwidth') or self._bandwidth is None:
        #    self._bandwidth = to_numeric_freq(self.spw.bandwidth)
        # return self._bandwidth
        return self.spw.bandwidth
    
    @property
    def baseband(self):
        return self.spw.baseband
    
    @property
    def centre_frequency(self):
        return self.spw.centre_frequency
    
    @property
    def chan_freqs(self):
        chan_freqs = self.spw._chan_freqs
        if isinstance(chan_freqs, spectralwindow.ArithmeticProgression):
            chan_freqs = numpy.array(list(spectralwindow.expand_ap(chan_freqs)))
        return chan_freqs
        
    @property
    def chan_widths(self):
        chan_widths = self.spw._chan_widths
        if isinstance(chan_widths, spectralwindow.ArithmeticProgression):
            chan_widths = numpy.array(list(spectralwindow.expand_ap(chan_widths)))
        return chan_widths
    
    @property
    def channels(self):
        return self.spw.channels
        # return []
    
    @property
    def frame(self):
        if hasattr(self, '_frame'):
            return self._frame
        else:
            return None
    
    @frame.setter
    def frame(self, value):
        self._frame = value
    
    @property
    def freq_max(self):
        return float(self.spw._max_frequency.value)
                                 
    @property
    def freq_min(self):
        return float(self.spw._min_frequency.value)
    
    @property
    def frequency_range(self):
        return [self.freq_min, self.freq_max]
    
    # @property
    # def hif_spw(self):
    #    return None
     
    @property
    def id(self):
        return self.spw.id
    
    @property
    def increment(self):
        if not hasattr(self, '_increment') or self._increment is None:
            if self.nchan == 1:
                chan_widths = self.spw._chan_widths
                if isinstance(chan_widths, spectralwindow.ArithmeticProgression):
                    self._increment = chan_widths.start
                else:
                    self._increment = chan_widths[0]
            else:
                chan_freqs = self.spw._chan_freqs
                if isinstance(chan_freqs, spectralwindow.ArithmeticProgression):
                    self._increment = chan_freqs.delta
                else:
                    self._increment = chan_freqs[1] - chan_freqs[0]
        return self._increment
   
    @property
    def intent(self):
        if hasattr(self, '_intent'):
            return self._intent
        else:
            return ':'.join(self.intents)
        
    @intent.setter
    def intent(self, value):
        self._intent = value
    
    @property
    def intents(self):
        if not hasattr(self, '_intents') or self._intents is None:
            self._intents = self.spw.intents.copy()
            if self.type == 'WVR':
                self._intents.add(self.type)
        return self._intents
    
    @property
    def is_target(self):
        # return (self.type == 'SP' and self.intent.find('TARGET') != -1)
        return (self.intent.find('TARGET') != -1)

    @property
    def is_atmcal(self):
        return (self.type == 'SP' and self.intent.find('ATMOSPHERE') != -1)
    
    @property
    def max_frequency(self):
        return self.spw.max_frequency
    
    @property
    def mean_freq(self):
        return float(self.spw.mean_frequency.value)
    
    @property
    def mean_frequency(self):
        return self.spw.mean_frequency
    
    @property
    def min_frequency(self):
        return self.spw.min_frequency

    @property
    def name(self):
        return self.spw.name

    @property
    def num_channels(self):
        return self.spw.num_channels

    @property
    def nchan(self):
        return self.spw.num_channels
    
    @property
    def pol_association(self):
        if not hasattr(self, '_pol_association') or self._pol_association is None:
            self._pol_association = []
        return self._pol_association

    @pol_association.setter
    def pol_association(self, value):
        self._pol_association = [] if value is None else value

    @property
    def ref_frequency(self):
        return self.spw.ref_frequency
    
    @property
    def ref_freq(self):
        return self.mean_freq

    @property
    def refpix(self):
        return 0
    
    @property
    def refval(self):
        if not hasattr(self, '_refval') or self._refval is None:
            chan_freqs = self.spw._chan_freqs
            if isinstance(chan_freqs, spectralwindow.ArithmeticProgression):
                self._refval = chan_freqs.start
            else:
                self._refval = chan_freqs[0]
        return self._refval
    
    @property
    def rest_frequencies(self):
        if hasattr(self, '_rest_frequencies'):
            return self._rest_frequencies
        else:
            return None
    
    @rest_frequencies.setter
    def rest_frequencies(self, value):
        self._rest_frequencies = value

    @property
    def sideband(self):
        return self.spw.sideband
    
    @property
    def type(self):
        return ('TP' if self.nchan == 1 else ('WVR' if self.nchan == 4 else 'SP'))

    def __repr__(self):
        args = map(str, [self.id, self.centre_frequency, self.bandwidth,
                         self.type])
        return 'SpectralWindow({0})'.format(', '.join(args))
    
# 2015/07/03 TN
# Frequency object is replaced with SpectralWindowAdapter object
#
# class Frequencies(spectralwindow.SpectralWindow, SingleDishBase):
# 
#     frame_map = { 0: 'REST',
#                   1: 'LSRK',
#                   2: 'LSRD',
#                   3: 'BARY',
#                   4: 'GEO',
#                   5: 'TOPO',
#                   6: 'GALACTO',
#                   7: 'LGROUP',
#                   8: 'CMB' }
# 
#     def __getstate__(self):
#         state_dictionary = self.__dict__.copy()
#         for attribute in self.__slots__:
#             state_dictionary[attribute] = getattr(self, attribute)
#         return state_dictionary
# 
#     def __setstate__(self, d):
#         for (k,v) in d.items():
#             if not hasattr(self, k):
#                 setattr(self, k, v)
#         self.__dict__ = d
#     
#     @staticmethod
#     def from_spectral_window(spw):
#         nchan = spw.num_channels
#         spw_type = ('TP' if nchan == 1 else \
#                     ('WVR' if nchan == 4 else 'SP'))
#         center_freq0 = spw._chan_freqs[0]
#         refpix = 0
#         refval = center_freq0
#         if nchan == 1:
#             increment = spw._chan_widths[0]
#         else:
#             center_freq1 = spw._chan_freqs[1]
#             increment = center_freq1 - center_freq0
# 
#         entry = Frequencies(id=spw.id,
#                             type=spw_type,
#                             nchan=nchan,
#                             bandwidth=float(to_numeric_freq(spw.bandwidth)),
#                             refpix=refpix,
#                             refval=refval,
#                             increment=increment,
#                             freq_min=to_numeric_freq(spw.min_frequency),
#                             freq_max=to_numeric_freq(spw.max_frequency),
#                             name=spw.name,
#                             sideband=spw.sideband,
#                             baseband=spw.baseband)
#         return entry
#         
#     def __init__(self, id=None, type=None, frame=None, nchan=None, refpix=None, refval=None, increment=None, bandwidth=None, intent=None, freq_min=None, freq_max=None, pol_association=None, rest_frequencies=None,name=None,sideband=None,baseband=None, hif_spw=None):
#         if increment is not None and nchan is not None:
#             chan_widths = [increment] * nchan
#         else:
#             chan_widths = None
#         if refpix is not None and refval is not None:
#             chan_freqs = [refval + refpix * increment * ichan for ichan in xrange(nchan)]
#         else:
#             chan_freqs = None
# 
# #       spectralwindow.SpectralWindow.__init__(self, id, bandwidth, freq_min, chan_widths, chan_freqs, name, sideband, baseband)
#         # assume reference frequency and mean frequency are one and the same
#         mean_freq = numpy.mean(chan_freqs)
#         ref_freq = mean_freq
#         spectralwindow.SpectralWindow.__init__(self, id, name, type, bandwidth, ref_freq, mean_freq, chan_freqs, chan_widths, sideband, baseband)
# 
#         self._init_properties(vars(),kw_ignore=['self','bandwidth'])
#         intents = self.intent.split(':')
#         for intent in intents:
#             if self.type == 'WVR':
#                 self.intents.add(self.type)
#             else:
#                 self.intents.add(intent)
# 
#     @property
#     def frequency_range(self):
#         return [self.freq_min, self.freq_max]
# 
#     @property
#     def intent(self):
#         return self._intent
# 
#     @intent.setter
#     def intent(self, value):
#         self._intent = '' if value is None else value
# 
#     @property
#     def pol_association(self):
#         return self._pol_association
# 
#     @pol_association.setter
#     def pol_association(self, value):
#         self._pol_association = [] if value is None else value
# 
#     @property
#     def is_target(self):
#         #return (self.type == 'SP' and self.intent.find('TARGET') != -1)
#         return (self.intent.find('TARGET') != -1)
# 
#     @property
#     def is_atmcal(self):
#         return (self.type == 'SP' and self.intent.find('ATMOSPHERE') != -1)


class ReductionGroupMember(object):
    def __init__(self, antenna, spw, pols):
        self.antenna = antenna
        self.spw = spw
        self.pols = pols
        self.iteration = [0 for p in xrange(self.npol)]
        self.linelist = [[] for p in xrange(self.npol)]
        self.channelmap_range = [[] for p in xrange(self.npol)]

    @property
    def npol(self):
        return len(self.pols)

    def iter_countup(self, pols=None):
        for i in self.__gen_pols(pols):
            self.iteration[i] += 1

    def iter_reset(self):
        self.iteration = [0 for p in self.pols]

    def add_linelist(self, linelist, pols=None, channelmap_range=None):
        for i in self.__gen_pols(pols):
            self.linelist[i] = linelist
            if channelmap_range is not None:
                self.channelmap_range[i] = channelmap_range
            else:
                self.channelmap_range[i] = linelist

    def __gen_pols(self, pols):
        if pols is None:
            for i in xrange(self.npol):
                yield i
        elif isinstance(pols, int):
            yield pols
        else:
            for i in pols:
                yield self.pols.index(i)

    def __repr__(self):
        return 'ReductionGroupMember(antenna=%s, spw=%s, pols=%s)' % (self.antenna, self.spw, self.pols)

    def __eq__(self, other):
        return other.antenna == self.antenna and other.spw == self.spw and other.pols == self.pols 
        
class ReductionGroupDesc(list):
    def __init__(self, frequency_range=None, nchan=None):
        self.frequency_range = frequency_range
        self.nchan = nchan

    def add_member(self, antenna, spw, pols):
        new_member = ReductionGroupMember(antenna, spw, pols)
        if not new_member in self:
            self.append(new_member)

    def get_iteration(self, antenna, spw, pol=None):
        member = self[self.__search_member(antenna, spw)]
        if pol is None:
            return max(member.iteration)
        else:
            return member.iteration[pol]
            
    def iter_countup(self, antenna, spw, pols=None):
        member = self[self.__search_member(antenna, spw)]
        member.iter_countup(pols)

    def add_linelist(self, linelist, antenna, spw, pols=None, channelmap_range=None):
        member = self[self.__search_member(antenna, spw)]
        member.add_linelist(linelist, pols, channelmap_range=channelmap_range)

    def __search_member(self, antenna, spw):
        for indx in xrange(len(self)):
            member = self[indx]
            if member.antenna == antenna and member.spw == spw:
                return indx
                break

    def __repr__(self):
        return 'ReductionGroupDesc(frequency_range=%s, nchan=%s, member=%s)' % (self.frequency_range, self.nchan, self[:])


class MSReductionGroupMember(object):
    def __init__(self, ms, antenna, spw):
        self.ms = ms
        self.antenna = antenna
        self.spw = spw
        self.iteration = 0
        self.linelist = []
        self.channelmap_range = []

    @property
    def npol(self):
        return 1

    def iter_countup(self):
        self.iteration += 1

    def iter_reset(self):
        self.iteration = 0

    def add_linelist(self, linelist, pols=None, channelmap_range=None):
        self.linelist = linelist
        if channelmap_range is not None:
            self.channelmap_range = channelmap_range
        else:
            self.channelmap_range = linelist

    def __repr__(self):
        return 'MSReductionGroupMember(ms=\'%s\', antenna=%s, spw=%s)' % (self.ms.basename, self.antenna, self.spw)

    def __eq__(self, other):
        LOG.debug('MSReductionGroupMember.__eq__')
        return other.ms.name == self.ms.name and other.antenna == self.antenna and other.spw == self.spw

    def __ne__(self, other):
        return other.ms.name != self.ms.name or other.antenna != self.antenna or other.spw != self.spw
     
class MSReductionGroupDesc(list):
    def __init__(self, min_frequency=None, max_frequency=None, nchan=None):
        self.max_frequency = max_frequency
        self.min_frequency = min_frequency
        self.nchan = nchan
        
    @property
    def frequency_range(self):
        return [self.min_frequency, self.max_frequency]

    def merge(self, other):
        assert self == other
        for member in other:
            LOG.info('ms.name=\"%s\" antenna=%s spw=%s'%(member.ms.name, member.antenna, member.spw))
            if not member in self:
                LOG.info('Adding (%s, %s, %s)'%(member.ms.name,member.antenna,member.spw))
                self.append(member)

    def add_member(self, ms, antenna, spw):
        new_member = MSReductionGroupMember(ms, antenna, spw)
        if not new_member in self:
            self.append(new_member)

    def get_iteration(self, ms, antenna, spw):
        member = self[self.__search_member(ms, antenna, spw)]
        return member.iteration
            
    def iter_countup(self, antenna, spw, pols=None):
        member = self[self.__search_member(antenna, spw)]
        member.iter_countup(pols)

    def add_linelist(self, linelist, antenna, spw, pols=None, channelmap_range=None):
        member = self[self.__search_member(antenna, spw)]
        member.add_linelist(linelist, pols, channelmap_range=channelmap_range)

    def __search_member(self, ms, antenna, spw):
        for indx in xrange(len(self)):
            member = self[indx]
            if member.ms.name == ms.name and member.antenna == antenna and member.spw == spw:
                return indx
                break
            
    def __eq__(self, other):
        LOG.debug('MSReductionGroupDesc.__eq__')
        return self.max_frequency == other.max_frequency \
            and self.min_frequency == other.min_frequency \
            and self.nchan == other.nchan
            
    def __ne__(self, other):
        return self.max_frequency != other.max_frequency \
            or self.min_frequency != other.min_frequency \
            or self.nchan != other.nchan

    def __repr__(self):
        return 'MSReductionGroupDesc(frequency_range=%s, nchan=%s, member=%s)' % (self.frequency_range, self.nchan, self[:])
