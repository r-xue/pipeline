from __future__ import absolute_import 

import sys
import os
import numpy
import contextlib
import re
import time

from logging import CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET

LogLevelMap = {'critical': 0,
               'error': 0,
               'warning': 1,
               'info': 2,
               'debug': 3,
               'todo': 4,
               'trace': 4}

LogLevelMap2 = {'critical': CRITICAL, # 50
                'error': ERROR,       # 40
                'warning': WARNING,   # 30
                'info': INFO,         # 20
                'debug': DEBUG,       # 10
                'todo': NOTSET,       # 0
                'trace': NOTSET }     # 0

import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
from pipeline.domain.datatable import OnlineFlagIndex

LOG = infrastructure.get_logger(__name__)

def asdm_name(scantable_object):
    """
    Return ASDM name that target scantable belongs to.
    Assumptions are:
       - scantable is generated from MS
       - MS is generated from ASDM
       - MS name is <uid>.ms
    """
    return asdm_name_from_ms(scantable_object.ms)

def asdm_name_from_ms(ms_domain):
    """
    Return ASDM name that target ms originates from.
    Assumptions are:
       - MS is generated from ASDM
       - MS name is <uid>.ms
    """
    ms_basename = ms_domain.basename
    index_for_suffix = ms_basename.rfind('.')
    asdm = ms_basename[:index_for_suffix] if index_for_suffix > 0 \
           else ms_basename
    return asdm

def get_parent_ms_idx(context, msname):
    """
    Returns index of corresponding ms in context
    The method maps both work_data and original MS to a proper index
    The return value is -1 if no match found.
    """
    mslist = context.observing_run.measurement_sets
    idx_found = -1
    for idx in xrange(len(mslist)):
        msobj = mslist[idx]
        search_list = [msobj.name, msobj.basename]
        if hasattr(msobj, "work_data"):
            search_list += [msobj.work_data, os.path.basename(msobj.work_data)]
        if msname in search_list:
            idx_found = idx
            break
    return idx_found    

def get_parent_ms_name(context, msname):
    """
    Returns name of corresponding parent ms in context
    The method maps both work_data and original MS to a proper index
    The return value is "" if no match found.
    """
    idx = get_parent_ms_idx(context, msname)
    return context.observing_run.measurement_sets[idx].name if idx >=0 else ""

####
# ProgressTimer
#
# Show the progress bar on the console if LogLevel is lower than or equal to 2.
#
####
class ProgressTimer(object):
    def __init__(self, length=80, maxCount=80, LogLevel='info'):
        """
        Constructor:
            length: length of the progress bar (default 80 characters)
        """
        self.currentLevel = 0
        self.maxCount = maxCount
        self.curCount = 0
        self.scale = float(length)/float(maxCount)
        if isinstance(LogLevel, str):
            self.LogLevel = LogLevelMap2[LogLevel] if LogLevelMap2.has_key(LogLevel) else INFO
        else:
            # should be integer
            self.LogLevel = LogLevel
        if self.LogLevel >= INFO:
            print '\n|' + '='*((length-8)/2) + ' 100% ' + '='*((length-8)/2) + '|'

    def __del__(self):
        if self.LogLevel >= INFO:
            print '\n'

    def count(self, increment=1):
        if self.LogLevel >= INFO:
            self.curCount += increment
            newLevel = int(self.curCount * self.scale)
            if newLevel != self.currentLevel:
                print '\b' + '*' * (newLevel - self.currentLevel),
                sys.stdout.flush()
                self.currentLevel = newLevel

# parse edge parameter to tuple
def parseEdge(edge):
    if isinstance(edge,int) or isinstance(edge,float):
        EdgeL = edge
        EdgeR = edge
    elif len(edge) == 0:
        EdgeL = 0
        EdgeR = 0
    elif len(edge) == 1:
        EdgeL = edge[0]
        EdgeR = edge[0]
    else:
        (EdgeL, EdgeR) = edge[:2]
    return(EdgeL, EdgeR)

def mjd_to_datestring( t, unit='sec' ):
    """
    MJD ---> date string

    t: MJD
    unit: sec or day
    """
    if unit == 'sec':
        mjd = t
    elif unit == 'day':
        mjd = t * 86400.0
    else:
        mjd = 0.0
    import time
    import datetime
    mjdzero=datetime.datetime(1858,11,17,0,0,0)
    zt=time.gmtime(0.0)
    timezero=datetime.datetime(zt.tm_year,zt.tm_mon,zt.tm_mday,zt.tm_hour,zt.tm_min,zt.tm_sec)
    dtd = timezero-mjdzero
    dtsec=mjd-(float(dtd.days)*86400.0+float(dtd.seconds)+float(dtd.microseconds)*1.0e-6)
    mjdstr=time.asctime(time.gmtime(dtsec))+' UTC'
    return mjdstr 


def to_list(s):
    if s is None:
        return None
    elif isinstance(s, list) or isinstance(s, numpy.ndarray):
        return s
    elif isinstance(s, str):
        if s.startswith('['):
            if s.lstrip('[')[0].isdigit():
                return eval(s)
            else:
                # maybe string list
                return eval(s.replace('[','[\'').replace(']','\']').replace(',','\',\''))
        else:
            try:
                return [float(s)]
            except:
                return [s]
    else:
        return [s]

def to_bool(s):
    if s is None:
        return None
    elif isinstance(s, bool):
        return s
    elif isinstance(s, str):
        if s.upper() == 'FALSE' or s == 'F':
            return False
        elif s.upper() == 'TRUE' or s == 'T':
            return True
        else:
            return s
    else:
        return bool(s)

def to_numeric(s):
    if s is None:
        return None
    elif isinstance(s, str):
        try:
            return float(s)
        except:
            return s
    else:
        return s

def get_mask_from_flagtra(flagtra):
    """Convert FLAGTRA (unsigned char) to a mask array (1=valid, 0=flagged)"""
    return (numpy.asarray(flagtra) == 0).astype(int)


@contextlib.contextmanager
def temporary_filename(name='_heuristics.temporary.table', removesubfiles=False):
    try:
        yield name
    finally:
        command = 'rm -rf %s'%(name)
        if removesubfiles:
            command += '*'
        os.system(command)

def _get_index_list(datatable, antenna, spw, pols=None, srctype=None):
    LOG.info('get_index_list_org start')
    start_time = time.time()
    assert len(antenna) == len(spw)
    table1 = datatable.tb1
    table2 = datatable.tb2
    antenna_column = table1.getcol('ANTENNA')
    spw_column = table1.getcol('IF')
    pol_column = table1.getcol('POL')
    srctype_column = table1.getcol('SRCTYPE')
    permanent_flag = table2.getcol('FLAG_PERMANENT')
    # skip if the row is flagged by online flagging
    f = lambda i, j: antenna_column[i] == antenna[j] and spw_column[i] == spw[j] and permanent_flag[OnlineFlagIndex,i] == 1
    if pols is None or len(pols) == 0:
        g = f
    else:
        g = lambda i, j: f(i,j) and (pols[j] is None or pol_column[i] in pols[j])
    if srctype is None:
        #f = lambda i, j: antenna_column[i] == antenna[j] and spw_column[i] == spw[j]
        sel = g
    else:
        #f = lambda i, j: antenna_column[i] == antenna[j] and spw_column[i] == spw[j] and srctype_column[i] == srctype
        sel = lambda i, j: g(i,j) and srctype_column[i] == srctype

    nrow = table1.nrows()
    nval = len(antenna)
    for irow in xrange(nrow):
        for ival in xrange(nval):
            if sel(irow, ival):
                yield irow
    end_time = time.time()
    LOG.info('get_index_list_org end: Elapsed time %s sec'%(end_time - start_time))
    
def _get_index_list2(datatable, antenna, spw, pols):
    assert pols is not None
    online_flag = datatable.tb2.getcolslice('FLAG_PERMANENT', [OnlineFlagIndex], [OnlineFlagIndex], 1)[0]
    for (_ant, _spw, _pols) in zip(antenna, spw, pols):
        for _pol in _pols:
            time_table = datatable.get_timetable(_ant, _spw, _pol)
            # time table separated by large time gap
            the_table = time_table[1]
            for group in the_table:
                for row in group[1]:
                    if online_flag[row] == 1:
                        yield row

def get_index_list(datatable, antenna, spw, pols=None, srctype=None):
    LOG.info('new get_index_list start')
    start_time = time.time()
    
    if pols is None or pols.count(None) > 0 or srctype is None:
        index_list = list(_get_index_list(datatable, antenna, spw, pols, srctype))
    else:
        index_list = list(_get_index_list2(datatable, antenna, spw, pols))
    index_list.sort()
    
    end_time = time.time()
    LOG.info('new get_index_list end: Elapsed time %s sec'%(end_time - start_time))
    
    return index_list

def get_index_list_for_ms(datatable, vis_list, antennaid_list, fieldid_list, 
                          spwid_list, srctype=None):
    return numpy.fromiter(_get_index_list_for_ms(datatable, vis_list, fieldid_list, antennaid_list, 
                                                spwid_list, srctype), dtype=numpy.int64)
    
def _get_index_list_for_ms(datatable, vis_list, antennaid_list, fieldid_list, 
                          spwid_list, srctype=None):
    # use time_table instead of data selection
    online_flag = datatable.tb2.getcolslice('FLAG_PERMANENT', [OnlineFlagIndex], [OnlineFlagIndex], 1)[0]
    for (_vis, _field, _ant, _spw) in zip(vis_list, fieldid_list, antennaid_list, spwid_list):
        time_table = datatable.get_timetable(_ant, _spw, None, os.path.basename(_vis), _field)
        # time table separated by large time gap
        the_table = time_table[1]
        for group in the_table:
            for row in group[1]:
                if online_flag[row] == 1:
                    yield row    

def get_valid_members(group_desc, antenna_filter, spwid_filter):
    for i in xrange(len(group_desc)):
        member = group_desc[i]
        antenna = member.antenna
        spwid = member.spw
        _spwid_filter = _get_spwid_filter(spwid_filter, antenna)
        LOG.debug('_spwid_filter=%s'%(_spwid_filter))
        if antenna in antenna_filter:
            if _spwid_filter is None or len(_spwid_filter) == 0 or spwid in _spwid_filter:
                yield i
                
def get_valid_ms_members(group_desc, ms_filter, ant_selection, field_selection, spw_selection):
    for member_id in xrange(len(group_desc)):
        member = group_desc[member_id]
        spw_id = member.spw
        field_id = member.field_id
        ant_id = member.antenna
        ms = member.ms
        if ms.name in ms_filter:
            mssel = casatools.ms.msseltoindex(vis=ms.name, spw=spw_selection, field=field_selection, baseline=ant_selection)
            spwsel = mssel['spw']
            fieldsel = mssel['field']
            antsel = mssel['antenna1']
            if (len(spwsel) == 0 or spw_id in spwsel) and (len(fieldsel) == 0 or field_id in fieldsel) and (len(antsel) == 0 or ant_id in antsel):
                yield member_id

def _get_spwid_filter(spwid_filter, file_id):
    if spwid_filter is None:
        return None
    elif isinstance(spwid_filter, str):
        return selection_to_list(spwid_filter)
    elif file_id < len(spwid_filter):
        _spwid_filter = spwid_filter[file_id]
        if _spwid_filter is None:
            return None
        else:
            return selection_to_list(spwid_filter[file_id])
    else:
        _spwid_filter = spwid_filter[0]
        if _spwid_filter is None:
            return None
        else:
            return selection_to_list(spwid_filter[0])

def _collect_logrecords(logger):
    capture_handlers = [h for h in logger.handlers if h.__class__.__name__ == 'CapturingHandler']
    logrecords = []
    for handler in capture_handlers:
        logrecords.extend(handler.buffer[:])
    return logrecords

def selection_to_list(sel, maxid=99):
    """Convert idx selction string to a list of ids"""
    def _selection_to_list(sel):
        elements = sel.split(',')
        for _elem in elements:
            elem = _elem.strip()
            if elem.isdigit():
                yield int(elem)
            elif re.match('^[0-9]+~[0-9]+$', elem):
                s = [int(e) for e in elem.split('~')]
                for i in xrange(s[0], s[1]+1):
                    yield i
            elif re.match('^<[0-9]+$', elem):
                for i in xrange(int(elem[1:])):
                    yield i
            elif re.match('^<=[0-9]+$', elem):
                for i in xrange(int(elem[2:])+1):
                    yield i
            elif re.match('^>[0-9]+$', elem):
                for i in xrange(int(elem[1:])+1,maxid):
                    yield i
            elif re.match('^>=[0-9]+$', elem):
                for i in xrange(int(elem[2:]),maxid):
                    yield i
    l = set(_selection_to_list(sel))
    return list(l)

def list_to_selection(rows, dosort=True):
    unique_list = numpy.unique(rows)
    sorted_list = numpy.sort(unique_list) if dosort else unique_list
    if len(sorted_list) == 0:
        sel = ''
    elif len(sorted_list) == 1:
        sel = str(sorted_list[0])
    else:
        index = 0
        sel = ''
        #print 'test: %s'%(sorted_list)
        increments = sorted_list[1:] - sorted_list[:-1]
        #print increments
        while index < len(sorted_list):
            start = index
            while index < len(increments) and increments[index] == 1:
                index += 1
            #print start, index
            
            if index - start < 2:
                _sel = ','.join(map(str,sorted_list[start:index+1]))
            else:
                _sel = '%s~%s'%(sorted_list[start],sorted_list[index])

            if len(sel) == 0:
                sel = _sel
            else:
                sel = ','.join([sel,_sel])

            #print '\'%s\''%(sel)
            index += 1

    return sel

def intersection(sel1, sel2):
    if sel1 is None or len(sel1) == 0:
        return '' if sel2 is None else sel2
    elif sel2 is None or len(sel2) == 0:
        return sel1
    else:
        set1 = set(selection_to_list(sel1))
        set2 = set(selection_to_list(sel2))
        l = list(set1 & set2)
        if len(l) == 0:
            return None
        else:
            return list_to_selection(l)
    
def pol_filter(group_desc, filter_func):
    for m in group_desc:
        filter = filter_func(m.antenna)
        if len(filter) == 0:
            yield m.pols
        else:
            yield list(set(filter) & set(m.pols))

def polstring(pols):
    if pols == [0, 1]:
        polstr = 'XXYY'
    elif pols == [0] or pols == 0:
        polstr = 'XX'
    elif pols == [1] or pols == 1:
        polstr = 'YY'
    else:
        polstr = 'I'
    return polstr

@contextlib.contextmanager
def TableSelector(name, query):
    with casatools.TableReader(name) as tb:
        tsel = tb.query(query)
        yield tsel
        tsel.close()
    
def nonscience_spw(spectral_windows):
    # detect spws for WVR and square-law detector (SQLD)
    pattern = '(SQLD|WVR)'
    for (spwid, spw) in spectral_windows.items():
        spw_name = spw.name
        match_by_name = re.search(pattern, spw_name) is not None
        match_by_intents = spw.is_target is False
        if match_by_name or match_by_intents:
            yield spwid

def science_spw(spectral_windows):
    # exclude spws for WVR and square-law detector (SQLD)
    pattern = '(SQLD|WVR|CH_AVG)'
    for (spwid, spw) in spectral_windows.items():
        spw_name = spw.name
        match_by_name = re.search(pattern, spw_name) is None
        match_by_intents = spw.is_target 
        if match_by_name and match_by_intents:
            yield spwid

@contextlib.contextmanager
def asap_force_storage(storage='disk'):
    import asap as sd
    key = 'scantable.storage'
    storage_org = sd.rcParams[key]
    sd.rcParams[key] = storage  
    yield None
    sd.rcParams[key] = storage_org
    
# helper functions for parallel execution
def create_serial_job(task_cls, task_args, context):
    inputs = task_cls.Inputs(context, **task_args)
    task = task_cls(inputs)
    job = mpihelpers.SyncTask(task)
    LOG.debug('Serial Job: %s'%(task))
    return job

def create_parallel_job(task_cls, task_args, context):
    context_path = os.path.join(context.output_dir, context.name + '.context')
    if not os.path.exists(context_path):
        context.save(context_path)
    task = mpihelpers.Tier0PipelineTask(task_cls, task_args, context_path)
    job = mpihelpers.AsyncTask(task)
    LOG.debug('Parallel Job: %s'%(task))
    return job
