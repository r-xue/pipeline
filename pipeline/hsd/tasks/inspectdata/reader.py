from __future__ import absolute_import

import os
import re
import numpy

import asap as sd

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
from pipeline.domain.datatable import DataTableImpl as DataTable
from pipeline.domain.datatable import DataTableColumnMaskList as ColMaskList

LOG = infrastructure.get_logger(__name__)

class DataTableReader(object):
    def __init__(self, table_name):
        self.table_name = table_name
        self.datatable = DataTable(name=self.table_name)
        self.vAnt = 0

    def get_datatable(self):
        return self.datatable

    def export_datatable(self, minimal=False):
        self.datatable.exportdata(minimal=minimal)

    def execute(self, name):
        Rad2Deg = 180. / 3.141592653
        
        LOG.info('name=%s'%(name))
        if self.datatable.has_key('FILENAMES'):
            filenames = self.datatable.getkeyword('FILENAMES')
            filenames = numpy.concatenate((filenames,[name]))
        else:
            filenames = [name]
        self.datatable.putkeyword('FILENAMES',filenames)
        s = sd.scantable(name, average=False)
        nrow = s.nrow()
        npol = s.npol()
        nbeam = s.nbeam()
        nif = s.nif()
        vorg=sd.rcParams['verbose']
        sd.rcParams['verbose']=False
        #if self.casa_version > 302:
        #    sd.asaplog.disable()
        sd.asaplog.disable()
        Tsys = s.get_tsys()
        sd.rcParams['verbose']=vorg
        #if self.casa_version > 302:
        #    sd.asaplog.enable()
        sd.asaplog.enable()
        if s.get_azimuth()[0] == 0: s.recalc_azel()
        
        with casatools.TableReader(name) as tb:
            Texpt = tb.getcol('INTERVAL')
            # ASAP doesn't know the rows for cal are included in s.nrow()
            # nrow = len(Ttime)
            Tscan = tb.getcol('SCANNO')
            Tif = tb.getcol('IFNO')
            Tpol = tb.getcol('POLNO')
            Tbeam = tb.getcol('BEAMNO')
            Tsrctype = tb.getcol('SRCTYPE')
            # 2009/10/19 nchan for scantable is not correctly set
            NchanArray = numpy.zeros(nrow, numpy.int)
            for row in range(nrow):
                NchanArray[row] = len(tb.getcell('SPECTRA', row))

        # 2011/10/23 GK List of DataTable for multiple antennas
        #self.datatable = {}
        # Save file name to be able to load the special setup needed for the
        # flagging based on the expected RMS.
        # 2011/10/23 GK List of DataTable for multiple antennas
        #if self.datatable.has_key('FileName') == False: self.datatable['FileName']  = ['']
        #self.datatable.putkeyword('FileName',[''])

        #self.datatable['FileName'] = rawFile
        #self.datatable['FileName'].append(rawFile)

        #if 'FileName' in self.datatable.tb.keywordnames():
        #    l = self.datatable.getkeyword('FileName').tolist()
        #    self.datatable.putkeyword('FileName',l+[rawFile])
        #else:
        #    self.datatable.putkeyword('FileName',[rawFile])

##         rawFileList.append(rawFile)
        #self.Row2ID[self.vAnt] = {}
##         outfile = open(TableOut, 'w')
##         outfile.write("!ID,Row,Scan,IF,Pol,Beam,Date,MJD,ElapsedTime,ExpTime,RA,Dec,Az,El,Nchan,Tsys,TargetName,AntennaID\n")

        # 2011/10/23 GK List of DataTable for multiple antennas
        #ID = len(self.datatable)-1
        ID = len(self.datatable)
        #ID = len(self.datatable)
##         self.LogMessage('INFO',Msg='ID=%s'%(ID))
        LOG.info('ID=%s'%(ID))
        #ID = 0
        ROWs = []
        IDs = []
        # 2009/7/16 to speed-up, get values as a list
        # 2011/11/8 get_direction -> get_directionval
        Sdir = s.get_directionval()
        #Sdir = s.get_direction()
        if ( sd.__version__ == '2.1.1' ):
            Stim = s.get_time()
        else:
            Stim = s.get_time(-1, True)
        Ssrc = s.get_sourcename()
        Saz = s.get_azimuth()
        Sel = s.get_elevation()

        # 2012/08/31 Temporary
##         if os.path.isdir(outTbl):
##             os.system('rm -rf %s' % outTbl)
##         TBL = createExportTable(outTbl, nrow)
        #TBL = gentools(['tb'])[0]
        #TBL.open(self.TableOut, nomodify=False)
        #TBL.addrows(nrow)

        self.datatable.addrows( nrow )
        # column based storing
        intArr = numpy.arange(nrow,dtype=int)
        self.datatable.putcol('ROW',intArr,startrow=ID)
        self.datatable.putcol('SCAN',Tscan,startrow=ID)
        self.datatable.putcol('IF',Tif,startrow=ID)
        self.datatable.putcol('POL',Tpol,startrow=ID)
        self.datatable.putcol('BEAM',Tbeam,startrow=ID)
        self.datatable.putcol('EXPOSURE',Texpt,startrow=ID)
        dirNP = numpy.array(Sdir,dtype=float) * Rad2Deg
        self.datatable.putcol('RA',dirNP[:,0],startrow=ID)
        self.datatable.putcol('DEC',dirNP[:,1],startrow=ID)
        azNP = numpy.array(Saz,dtype=float) * Rad2Deg
        self.datatable.putcol('AZ',azNP,startrow=ID)
        elNP = numpy.array(Sel,dtype=float) * Rad2Deg
        self.datatable.putcol('EL',elNP,startrow=ID)
        self.datatable.putcol('NCHAN',NchanArray,startrow=ID)
        self.datatable.putcol('TSYS',Tsys,startrow=ID)
        self.datatable.putcol('TARGET',Ssrc,startrow=ID)
        intArr[:] = 1
        self.datatable.putcol('FLAG_SUMMARY',intArr,startrow=ID)
        intArr[:] = 0
        self.datatable.putcol('NMASK',intArr,startrow=ID)
        intArr[:] = -1
        self.datatable.putcol('NOCHANGE',intArr,startrow=ID)
        self.datatable.putcol('POSGRP',intArr,startrow=ID)
        self.datatable.putcol('TIMEGRP_S',intArr,startrow=ID)
        self.datatable.putcol('TIMEGRP_L',intArr,startrow=ID)
        intArr[:] = self.vAnt
        self.datatable.putcol('ANTENNA',intArr,startrow=ID)
        self.datatable.putcol('SRCTYPE',Tsrctype,startrow=ID)
        # row base storing
        stats = [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, -1.0]
        flags = [1, 1, 1, 1, 1, 1, 1]
        pflags = [1, 1, 1]
        masklist = ColMaskList.NoMask
        for x in range(nrow):
            Ttime = Stim[x]
            sDate = ("%4d-%02d-%02d" % (Ttime.year, Ttime.month, Ttime.day))
            # Calculate MJD
            sTime = ("%4d/%02d/%02d/%02d:%02d:%.1f" % (Ttime.year, Ttime.month, Ttime.day, Ttime.hour, Ttime.minute, Ttime.second))
            qTime = casatools.quanta.quantity(sTime)
            MJD = qTime['value']
            if x == 0: MJD0 = MJD
            self.datatable.putcell('DATE',ID,sDate)
            self.datatable.putcell('TIME',ID,MJD)
            self.datatable.putcell('ELAPSED',ID,(MJD-MJD0)*86400.0)
            self.datatable.putcell('STATISTICS',ID,stats)
            self.datatable.putcell('FLAG',ID,flags)
            self.datatable.putcell('FLAG_PERMANENT',ID,pflags)
            self.datatable.putcell('MASKLIST',ID,masklist)
##             if Ssrc[x].find('_calon') < 0:
##                 outfile.write("%d,%d,%d,%d,%d,%d,%s,%.8f,%.3f,%.3f,%.8f,%.8f,%.3f,%.3f,%d,%f,%s,%d\n" % \
##                          (ID, x, Tscan[x], Tif[x], Tpol[x], Tbeam[x], \
##                          sDate, MJD, (MJD - MJD0) * 86400., Texpt[x], \
##                           dirNP[x,0],dirNP[x,1], \
##                           azNP[x], elNP[x], \
##                          NchanArray[x], Tsys[x], Ssrc[x], vAnt))

##                 TBL.putcol('Row', int(x), int(x), 1, 1)
##                 TBL.putcol('Ant', vAnt, int(x), 1, 1)
##                 ROWs.append(int(x))
##                 IDs.append(int(ID))
##                 self.Row2ID[vAnt][int(x)] = int(ID)
            ID += 1

        self.vAnt += 1

