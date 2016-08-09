import os
import shutil
import numpy

from taskinit import gentools

import pipeline.infrastructure as infrastructure
from pipeline.domain.datatableold import map_spwchans

LOG = infrastructure.get_logger(__name__)

def __coldesc( vtype, option, maxlen,
             ndim, comment, unit=None, measinfo=None ):
    d={'dataManagerGroup': 'StandardStMan',
       'dataManagerType': 'StandardStMan'}
    d['valueType'] = vtype
    d['option'] = option
    if ndim > 0:
        d['ndim'] = ndim
    d['maxlen'] = maxlen
    d['comment'] = comment
    if unit is not None:
        d['keywords'] = {'QuantumUnits': [unit] }
    if measinfo is not None:
        if not d.has_key('keywords'):
            d['keywords'] = {}
        d['keywords']['MEASINFO'] = measinfo
    return d

def tbobj():
    return gentools(['tb'])[0]

# generate column description, default is scalar column
def coldesc(colname, vtype, option=0, maxlen=0, ndim=-1, comment='', unit=None, measinfo=None):
    desc = __coldesc(vtype, option, maxlen, ndim, comment, unit, measinfo)
    return colname, desc

TABLE_DESC = dict([
    coldesc('SCANNO', 'uint', comment='Scan number'),
    coldesc('CYCLENO', 'uint', comment='Cycle number'),
    coldesc('IFNO', 'uint', comment='IF number'),
    coldesc('BEAMNO', 'uint', comment='Beam number'),
    coldesc('POLNO', 'uint', comment='Pol number'),
    coldesc('FREQ_ID', 'uint', comment='Key for FREQUENCIES table'),
    coldesc('TIME', 'double', comment='Time', unit='d', measinfo={'type': 'epoch', 'Ref': 'UTC'}),
    coldesc('TSYS', 'float', ndim=1, comment='Tsys'),
    coldesc('FLAGTRA', 'uchar', ndim=1, comment='Channel flag'),
    coldesc('ELEVATION', 'float', comment='Elevation')
])

TABLE_KEYWORD = {'VERSION': 1,
                 'ScantableName': '{name}',
                 'ApplyType': 'CALTSYS',
                 'FREQUENCIES': 'Table: {name}'}

# workaround for CAS-8724
# set column keywords after creating table
def create_table(table, name):
    ret = table.create(name, TABLE_DESC, memtype='plain', nrow=0)
    assert ret == True
    for (_colname, _coldesc) in TABLE_DESC.items():
        if _coldesc.has_key('keywords'):
            table.putcolkeywords(_colname, _coldesc['keywords'])

def map_without_average(prefix, caltable, reftable):
    # initial check
    check(caltable)
    
    tb = tbobj()
    antenna = antennanames(caltable)
    names = {}
    for (antenna_id, antenna_name) in enumerate(antenna):
        name = '.'.join([prefix, antenna_name, 'tsyscal.tbl'])
        names[antenna_name] = name
        create_table(tb, name)
        try:
            fill(tb, caltable, antenna_id)
            keywords = TABLE_KEYWORD.copy()
            if reftable is not None:
                src = os.path.join(reftable, 'FREQUENCIES')
                dst = os.path.join(name, 'FREQUENCIES')
                if os.path.exists(src):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
            scntable = '.'.join([prefix, antenna_name, 'asap'])
            keywords['ScantableName'] = os.path.abspath(scntable)
            keywords['FREQUENCIES'] = 'Table: %s'%(os.path.abspath(dst))
            putkeyword(tb, keywords)
        finally:
            tb.close()
    return names

def map_with_average(prefix, caltable, reftable, atm_spw, science_spw):
    # initial check
    check(caltable)
    
    tb = tbobj()
    antenna = antennanames(caltable)
    names = {}
    for (antenna_id, antenna_name) in enumerate(antenna):
        name = '.'.join([prefix, antenna_name, 'spw%s'%(science_spw.id), 'tsyscal.tbl'])
        names[antenna_name] = name
        create_table(tb, name)
        try:
            fill_with_average(tb, caltable, antenna_id, atm_spw, science_spw)
            keywords = TABLE_KEYWORD.copy()
            if reftable is not None:
                src = os.path.join(reftable, 'FREQUENCIES')
                dst = os.path.join(name, 'FREQUENCIES')
                if os.path.exists(src):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
            scntable = '.'.join([prefix, antenna_name, 'asap'])
            keywords['ScantableName'] = os.path.abspath(scntable)
            keywords['FREQUENCIES'] = 'Table: %s'%(os.path.abspath(dst))
            putkeyword(tb, keywords)
        finally:
            tb.close()
    return {science_spw.id: names}

def check(caltable):
    # Make sure caltable type is B TSYS
    tb = tbobj()
    tb.open(caltable)
    viscal = tb.getkeyword('VisCal')
    tb.close()

    assert viscal == 'B TSYS'

def putkeyword(table, keywords):
    for (k,v) in keywords.items():
        table.putkeyword(k, v)
    
def fill(table, caltable, antenna_id):
    tb = tbobj()
    tb.open(caltable)
    try:
        tsel = tb.query('ANTENNA1==%s'%(antenna_id))
        rows = tsel.rownumbers()
        tsel.close()
        for row in rows:
            t = tb.getcell('TIME', row)
            spw = tb.getcell('SPECTRAL_WINDOW_ID', row)
            scan = tb.getcell('SCAN_NUMBER', row)
            tsys = tb.getcell('FPARAM', row)
            flag = tb.getcell('FLAG', row)
            npol = tsys.shape[0]
            flagtra = flag * 128
            for ipol in xrange(npol):
                idx = table.nrows()
                table.addrows()
                table.putcell('TIME', idx, t / 86400.0)
                table.putcell('IFNO', idx, spw)
                table.putcell('FREQ_ID', idx, spw)
                table.putcell('SCANNO', idx, scan)
                table.putcell('CYCLENO', idx, 0)
                table.putcell('POLNO', idx, ipol)
                table.putcell('TSYS', idx, tsys[ipol])
                table.putcell('FLAGTRA', idx, flagtra[ipol])
                table.putcell('ELEVATION', idx, 0.0)
    finally:
        tb.close()

def fill_with_average(table, caltable, antenna_id, atm_spw, science_spw):
    tb = tbobj()
    tb.open(caltable)
    try:
        tsel = tb.query('ANTENNA1==%s && SPECTRAL_WINDOW_ID==%s'%(antenna_id,atm_spw.id))
        rows = tsel.rownumbers()
        tsel.close()
        failed_list = []
        for row in rows:
            t = tb.getcell('TIME', row)
            spw = tb.getcell('SPECTRAL_WINDOW_ID', row)
            scan = tb.getcell('SCAN_NUMBER', row)
            tsys = tb.getcell('FPARAM', row)
            flag = tb.getcell('FLAG', row)
            npol = tsys.shape[0]
            flagtra = flag * 128
            start_chan, end_chan = map_spwchans(atm_spw, science_spw)
            LOG.info('atm_spw %s science_spw %s: start_chan=%s, end_chan=%s'%(atm_spw.id, science_spw.id, start_chan, end_chan))
            for ipol in xrange(npol):
                idx = table.nrows()
                table.addrows()
                table.putcell('TIME', idx, t / 86400.0)
                table.putcell('IFNO', idx, spw)
                table.putcell('FREQ_ID', idx, spw)
                table.putcell('SCANNO', idx, scan)
                table.putcell('CYCLENO', idx, 0)
                table.putcell('POLNO', idx, ipol)
                #mean_tsys = numpy.mean(tsys[ipol][start_chan:end_chan])
                masked_tsys = numpy.ma.masked_array(tsys[ipol][start_chan:end_chan], flag[ipol][start_chan:end_chan])
                mean_tsys = masked_tsys.mean()
                if numpy.ma.is_masked(mean_tsys) or not numpy.isfinite(mean_tsys):
                    flagtra[ipol,:] = 128
                    mean_tsys = 0.0
                    failed_list.append((row, ipol))
                tsys[ipol][:] = mean_tsys
                table.putcell('TSYS', idx, tsys[ipol])
                table.putcell('FLAGTRA', idx, flagtra[ipol])
                table.putcell('ELEVATION', idx, 0.0)
        if len(failed_list) > 0:
            vis = tb.getkeyword('MSName')
            tb2 = tbobj()
            tb2.open(os.path.join(caltable, 'ANTENNA'))
            antenna_name = tb2.getcell('NAME', antenna_id)
            LOG.error('Wrong averaged Tsys is found in %s antenna %s spw %s, probably because channels that overlaps with science spw are all flagged. Flagged %s'%(vis, antenna_name, atm_spw.id, ', '.join(map(lambda x: '(row %s, pol %s)'%(x), failed_list))))
            
    finally:
        tb.close()

def antennaname(name):
    tb = tbobj()
    tb.open(name)
    antenna = tb.getkeyword('AntennaName')
    tb.close()
    site, ant = antenna.split('//')
    ant, station = ant.split('@')
    return ant

def antennanames(caltable):
    tb = tbobj()
    tb.open(os.path.join(caltable, 'ANTENNA'))
    names = tb.getcol('NAME')
    tb.close()
    return names
