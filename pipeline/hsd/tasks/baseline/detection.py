from __future__ import absolute_import

import os
import time
from math import sqrt
import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.jobrequest as jobrequest
#import pipeline.infrastructure.logging as logging
import pipeline.h.heuristics as heuristics
from .. import common
from . import rules

LOG = infrastructure.get_logger(__name__)
#LogLevel='trace'
LogLevel='info'
#logging.set_logging_level(LogLevel)

class DetectLine(object):
    #LineFinder = heuristics.AsapLineFinder
    LineFinder = heuristics.HeuristicsLineFinder
    ThresholdFactor = 3.0

    def __init__(self, datatable):
        self.datatable = datatable
        self.lf = self.LineFinder()

    def execute(self, grid_table, spectra, window=[], edge=(0, 0), broadline=True):
        """
        The process finds emission lines and determines protection regions for baselinefit
        """
        detect_signal = {}

        #(nchan,nrow) = spectra.shape
        (nrow,nchan) = spectra.shape

        # Pre-Defined Spectrum Window
        if len(window) != 0:
            LOG.info('Skip line detection since line window is set.')
            tROW = self.datatable.getcol('RA')
            tDEC = self.datatable.getcol('DEC')
            for row in xrange(nrow):
                detect_signal[row] = [tRA[row], tDEC[row], window]
                for row in range(len(self.datatable)):
                    self.datatable.putcell('MASKLIST', row, window)
            return detect_signal

        LOG.info('Search regions for protection against the background subtraction...')
        LOG.info('Processing %d spectra...' % nrow)

        # Set edge mask region
        (EdgeL, EdgeR) = common.parseEdge(edge)
        Nedge = EdgeR + EdgeL
        if Nedge >= nchan:
            LOG.error('Error: Edge masked region too large...')
            return False

        #2010/6/9 Max FWHM found to be too large!
        #if rules.LineFinderRule['MaxFWHM'] > ((nchan-Nedge)/2):
        #    MaxFWHM = int(0.5 * (nchan-Nedge))
        #    rules.LineFinderRule['MaxFWHM'] = MaxFWHM
        #else:
        #    MaxFWHM = int(rules.LineFinderRule['MaxFWHM'])
        MaxFWHM = int(max(rules.LineFinderRule['MaxFWHM'], 0.5 * (nchan - Nedge)))
        #rules.LineFinderRule['MaxFWHM'] = MaxFWHM
        MinFWHM = int(rules.LineFinderRule['MinFWHM'])
        Threshold = rules.LineFinderRule['Threshold']        

        # 2011/05/17 TN
        # Switch to use either ASAP linefinder or John's linefinder
        if self.lf.__class__.__name__ == 'AsapLineFinder':
            #LF = heuristics.AsapLineFinder()
            ### 2011/05/23 for linefinder2
            Thre = [Threshold, Threshold * sqrt(2)]
            if broadline: (Start, Binning) = (0, 1)
            else: (Start, Binning) = (1, 1)
        elif self.lf.__class__.__name__ == 'HeuristicsLineFinder':
            #LF = heuristics.HeuristicsLineFinder()
            ### 2011/05/23 for linefinder2
            #Thre = [Threshold * self.ThresholdFactor, Threshold * math.sqrt(2) * self.ThresholdFactor]
            ### 2012/02/29 for Broad Line detection by Binning
            ### Only 'Thre' is effective for Heuristics Linefinder. BoxSize, AvgLimit, and MinFWHM are ignored
            #Thre = [Threshold * self.ThresholdFactor, Threshold * self.ThresholdFactor]
            #Thre = [Threshold * self.ThresholdFactor, Threshold * self.ThresholdFactor, Threshold * self.ThresholdFactor]
            Thre = [Threshold * self.ThresholdFactor for i in xrange(3)]
            if broadline: (Start, Binning) = (0, 5)
            else: (Start, Binning) = (1, 3)

        # try 2010/10/27
        #BoxSize = [min(2.0*MaxFWHM/(nchan - Nedge), 0.5, (nchan - Nedge)/float(nchan)*0.9), min(max(MaxFWHM/(nchan - Nedge)/4.0, 0.1), (nchan - Nedge)/float(nchan)/2.0)]
        BoxSize = [min(2.0*MaxFWHM/(nchan - Nedge), 0.5, (nchan - Nedge)/float(nchan)*0.9), min(max(MaxFWHM/(nchan - Nedge)/4.0, 0.1), (nchan - Nedge)/float(nchan)/2.0), min(max(MaxFWHM/(nchan - Nedge)/4.0, 0.1), (nchan - Nedge)/float(nchan)/2.0)]
        ### 2011/05/23 for linefinder2
        #Thre = [Threshold, Threshold * math.sqrt(2)]
        #Thre = [Threshold, Threshold * math.sqrt(2), Threshold * 2]
        #AvgLimit = [MinFWHM * 16, MinFWHM * 4]
        AvgLimit = [MinFWHM * 16, MinFWHM * 4, MinFWHM]

        # Create progress timer
        Timer = common.ProgressTimer(80, nrow, LogLevel)

        for row in xrange(nrow):
            # Countup progress timer
            Timer.count()

            if grid_table[row][6] == 0:
                LOG.debug('Row %d: No spectrum' % row)
                # No spectrum
                protected = [[-1, -1]]
            else:
                ProcStartTime = time.time()
                LOG.debug('Start Row %d' % (row))

                protected = self._detect(spectrum = spectra[row],#spectrum = spectra[:,row], 
                                         start=Start,
                                         end=len(Thre),
                                         binning=Binning,
                                         threshold=Thre,
                                         box_size=BoxSize,
                                         min_nchan=MinFWHM,
                                         avg_limit=AvgLimit,
                                         tweak=True,
                                         edge=(EdgeL,EdgeR))
            detect_signal[row] = [grid_table[row][4], # RA
                                  grid_table[row][5], # DEC
                                  protected]          # Protected Region
            ProcEndTime = time.time()
            LOG.info('Channel ranges of detected lines for Row %s: %s' % (row, detect_signal[row][2]))
            
            LOG.debug('End Row %d: Elapsed Time=%.1f sec' % (row, (ProcEndTime - ProcStartTime)) )
        del Timer

        LOG.debug('DetectSignal = %s'%(detect_signal))
        return detect_signal

    def _detect(self, spectrum, start, end, binning, threshold, box_size, min_nchan, avg_limit, tweak, edge):
        protected = []

        nchan = len(spectrum)
        (EdgeL, EdgeR) = edge
        Nedge = EdgeR + EdgeL
        MaxFWHM = int(max(rules.LineFinderRule['MaxFWHM'], 0.5 * (nchan - Nedge)))
        MinFWHM = int(rules.LineFinderRule['MinFWHM'])

        # Try to detect broader component and narrow component separately
        for y in range(start, end):
        #for y in range(Start, 2):
            ### 2012/02/29 GK for broad line detection
            ### y=0: Bin=25,  y=1: Bin=5 or 3,  y=2: Bin=1
            Bin = binning ** (2-y)
            if Bin != 1 and nchan/Bin < 50: continue
            LOG.debug('line detection parameters: ')
            LOG.debug('threshold (S/N per channel)=%.1f,' % threshold[y] \
                           + 'min_nchan for detection=%d, running mean box size (in fraction of spectrum)=%s, ' % (min_nchan, box_size[y]) \
                           + 'upper limit for averaging=%s channels, edges to be dropped=[%s, %s]' % (avg_limit[y], EdgeL, EdgeR) )
            line_ranges = self.lf(spectrum=spectrum,
                                  threshold=threshold[y], 
                                  box_size=box_size[y], 
                                  min_nchan=min_nchan, 
                                  avg_limit=avg_limit[y], 
                                  tweak=True,
                                  edge=(int((EdgeL+Bin-1)/Bin), int((EdgeR+Bin-1)/Bin)))
            # len(line_ranges) is twice of number of lines detected
            # since line_ranges is a flat list of left/right edges of 
            # detected line regions. 
            # [line0L, line0R, line1L, line1R, ...]
            nlines = len(line_ranges) 

            ### Debug TT
            #LOG.info('NLINES=%s, EdgeL=%s, EdgeR=%s' % (nlines, EdgeL, EdgeR))
            #LOG.debug('ranges=%s'%(line_ranges))
            # No-line is detected
            if (nlines == 0):
                if len(protected) == 0: protected = [[-1, -1]]
            # Single line is detected
            elif (nlines == 2):
                Ranges = getWidenLineList(int(nchan/Bin), 1, 1, line_ranges)
                ### 2012/02/29 GK for broad line detection
                (Ranges[0], Ranges[1]) = (Ranges[0]*Bin+int(Bin/2), Ranges[1]*Bin+int(Bin/2))
                Width = Ranges[1] - Ranges[0] + 1
                ### 2011/05/16 allowance was moved to clustering analysis
                allowance = int(Width/5)
                #allowance = int(Width/10)
                #Debug (TT)
                LOG.debug('Ranges=%s, Width=%s, allowance=%s' % (Ranges, Width, allowance))
                ### 2011/05/16 allowance was moved to clustering analysis
                #if Width >= MinFWHM and Width <= MaxFWHM and \
                #   Ranges[0] > (EdgeL + allowance + 1) and \
                #   Ranges[1] < (nchan - 2 - allowance - EdgeR):
                #    ProtectRegion[2].append([Ranges[0] - allowance, Ranges[1] + allowance])
                if Width >= MinFWHM and Width <= MaxFWHM and \
                   Ranges[0] > EdgeL and \
                   Ranges[1] < (nchan - 1 - EdgeR):
                    if len(protected) != 0 and protected[0] == [-1, -1]:
                        protected[0] = [Ranges[0], Ranges[1]]
                    else: protected.append([Ranges[0], Ranges[1]])
                elif len(protected) == 0: protected = [[-1, -1]]
            # Multipule lines (candidates) are detected
            else:
                linestat = []
                Ranges = getWidenLineList(int(nchan/Bin), 1, 1, line_ranges)
                # y loops over [0,2,4,...] instead of [0,1,2,...]
                #for y in range(int(len(Ranges)/2)):
                for y in xrange(0,len(Ranges),2):
                    ### 2012/02/29 GK for broad line detection
                    (Ranges[y], Ranges[y+1]) = (Ranges[y]*Bin+int(Bin/2), Ranges[y+1]*Bin+int(Bin/2))
                    Width = Ranges[y+1] - Ranges[y] + 1
                    ### 2011/05/16 allowance was moved to clustering analysis
                    allowance = int(Width/5)
                    #allowance = int(Width/10)
                    #Debug (GK)
                    LOG.debug('Ranges=%s, Width=%s, allowance=%s' % (Ranges, Width, allowance))
                    ### 2011/05/16 allowance was moved to clustering analysis
                    if Width > MinFWHM and Width < MaxFWHM and \
                            Ranges[y] > EdgeL and \
                            Ranges[y+1] < (nchan - 1 - EdgeR):
                        #sp = spectra[Ranges[y]:Ranges[y+1], row]
                        linestat.append((Ranges[y], Ranges[y+1], spectrum.max() - spectrum.min()))
                # No candidate lines are left
                if len(linestat) == 0:
                    #if len(ProtectRegion[2]) == 0: ProtectRegion[2].append([-1, -1])
                    if len(protected) == 0: protected = [[-1, -1]]
                # More than or equal to one line are left
                else:
                    if len(protected) == 1 and protected[0] == [-1, -1]:
                        protected = []
                    protected.extend([l[0:2] for l in linestat])
                    # Store line if max intensity exceeds 1/30 of the strongest one
                    #protected.extend([l[0:2] for l in linestat if l[2] > Threshold])

                    # 2007/09/01 Merge lines into one if two lines are close
                    flag = True
                    for y in range(len(linestat) - 1):
                        current0 = linestat[y][0]
                        current1 = linestat[y][1]
                        next0 = linestat[y+1][0]
                        next1 = linestat[y+1][1]
                        if (next0 - current1) < (0.25*min((current1-current0),(next1-next0))):
                            if flag == True:
                                if current1 < next1 and current0 < next0 and (next1 - current0) < MaxFWHM:
                                    protected.append([current0, next1])
                                    Line0 = current0
                                else: continue
                            else:
                                if (next1 - Line0) < MaxFWHM:
                                    protected.pop()
                                    protected.append([Line0, next1])
                                else:
                                    flag = True
                                    continue
                            flag = False
                        else: flag = True
        return protected

def SpBinning(data, Bin):
    if Bin == 1: 
        return data
    else:
        return numpy.array([data[i:i+Bin].mean() for i in xrange(0,len(data),Bin)], dtype=numpy.float)

def getWidenLineList(nChan, mChan, pChan, Lines):
    #print Lines, nChan, mChan, pChan, len(Lines)
    Tmp = [[Lines[i]-mChan, Lines[i+1]+pChan] for i in xrange(0, len(Lines), 2)]
    if len(Tmp) > 1:
        for i in range(len(Tmp)-1, 0, -1):
            if Tmp[i-1][1] >= Tmp[i][0]-1:
                Tmp[i-1][1] = Tmp[i][1]
                Tmp.pop(i)
    Out = [x for t in Tmp for x in t]
    if Out[0] < 0: Out[0] = 0
    if Out[-1] > nChan - 2: Out[-1] = nChan - 1
    return Out
