from __future__ import absolute_import

import os
import numpy
import time
import copy

import asap as sd
from taskinit import gentools

#from SDTool import ProgressTimer

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.sdfilenamer as filenamer
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.utils as utils
from pipeline.domain.datatable import OnlineFlagIndex

from . import SDFlagPlotter as SDP
from .. import common

LOG = infrastructure.get_logger(__name__)


class SDBLFlagSummary(object):
    '''
    A class of single dish flagging task.
    This class defines per spwid flagging operation.
    '''

    def __init__(self, context, datatable, spwid_list, pols_list, file_index, thresholds, flagRule, userFlag=[]):
        '''
        Constructor of worker class
        '''
        self.context = context
        self.datatable = datatable
        self.spwid_list = spwid_list
        self.pols_list = pols_list
        self.file_index = file_index
        self.thres_value = thresholds
        self.flagRule = flagRule
        self.userFlag = userFlag
    
    def execute(self, dry_run=True):
        """
        Summarizes flagging results.
        Iterates over antenna and polarization for a certain spw ID
        """
        start_time = time.time()

        datatable = self.datatable
        spwid_list = self.spwid_list
        pols_list = self.pols_list
        file_index = self.file_index
        thresholds = self.thres_value
        flagRule = self.flagRule
        #userFlag = self.userFlag

        assert len(file_index) == len(spwid_list)
        LOG.debug('Members to be processed:')
        for (a,s,p) in zip(file_index, spwid_list, pols_list):
            LOG.debug('\tAntenna %s Spw %s Pol %s'%(a,s,p))
        
        # output directory
#         if self.context.subtask_counter is 0: 
#             stage_number = self.context.task_counter - 1
#         else:
#             stage_number = self.context.task_counter
        stage_number = self.context.task_counter
        FigFileDir = (self.context.report_dir+"/stage%d" % stage_number)
        ### WORKAROUND to GENERATE stage# dir manually
        if not os.path.exists(FigFileDir):
            os.mkdir(FigFileDir)
        FigFileDir += "/"

        flagSummary = []
        for (idx,spwid,pollist) in zip(file_index, spwid_list, pols_list):
            LOG.debug('Performing flagging for Antenna %s Spw %s'%(idx,spwid))
            st = self.context.observing_run[idx]
            filename_in = st.name
            ant_name = st.antenna.name
            asdm = common.asdm_name(st)
            LOG.info("*** Summarizing table: %s ***" % (os.path.basename(filename_in)))
            for pol in pollist:
                time_table = datatable.get_timetable(idx, spwid, pol)               
                # Select time gap list: 'subscan': large gap; 'raster': small gap
                if flagRule['Flagging']['ApplicableDuration'] == "subscan":
                    TimeTable = time_table[1]
                else:
                    TimeTable = time_table[0]
                flatiter = utils.flatten([ chunks[1] for chunks in TimeTable ])
                dt_idx = [ chunk for chunk in flatiter ]
                # generate summary plot
                st_prefix = st.name.rstrip('/').split('/')[-1].rstrip('\.asap').replace('\.', '_')
                iteration = _get_iteration(self.context.observing_run.reduction_group,
                                           idx, spwid, pol)
                FigFileRoot = ("FlagStat_%s_spw%d_pol%d_iter%d" % (st_prefix, spwid, pol, iteration))
                time_gap = datatable.get_timegap(idx, spwid, pol)
                # time_gap[0]: PosGap, time_gap[1]: TimeGap
                for i in range(len(thresholds)):
                    thres = thresholds[i]
                    if thres['index'] == idx and thres['spw'] == spwid and thres['pol'] == pol:
                        final_thres = thres['result_threshold']
                        is_baselined = thres['baselined'] if thres.has_key('baselined') else False
                        thresholds.pop(i)
                        break
                if (not is_baselined) and not iteration==0:
                    raise Exception, "Internal error: is_baselined flag is set to False for baselined data."
                t0 = time.time()
                htmlName, nflags = self.plot_flag(datatable, dt_idx, time_gap[0], time_gap[1], final_thres, flagRule, FigFileDir, FigFileRoot, is_baselined)
                t1 = time.time()
                LOG.info('Plot flags End: Elapsed time = %.1f sec' % (t1 - t0) )
                flagSummary.append({'html': htmlName, 'name': asdm, 'antenna': ant_name, 'spw': spwid, 'pol': pol,
                                    'nrow': len(dt_idx), 'nflags': nflags, 'baselined': is_baselined})

        end_time = time.time()
        LOG.info('PROFILE execute: elapsed time is %s sec'%(end_time-start_time))

        return flagSummary


    def _get_parmanent_flag_summary(self, pflag, FlagRule):
        # FLAG_PERMANENT[0] --- 'WeatherFlag'
        # FLAG_PERMANENT[1] --- 'TsysFlag'
        # FLAG_PERMANENT[2] --- 'UserFlag'
        # FLAG_PERMANENT[3] --- 'OnlineFlag' (fixed)
        if pflag[OnlineFlagIndex] == 0:
            return 0
        
        types = ['WeatherFlag', 'TsysFlag', 'UserFlag']
        mask = 1
        for idx in xrange(len(types)):
            if FlagRule[types[idx]]['isActive'] and pflag[idx] == 0:
                mask = 0
                break
        return mask

    def _get_stat_flag_summary(self, tflag, FlagRule):
        # FLAG[0] --- 'LowFrRMSFlag' (OBSOLETE)
        # FLAG[1] --- 'RmsPostFitFlag'
        # FLAG[2] --- 'RmsPreFitFlag'
        # FLAG[3] --- 'RunMeanPostFitFlag'
        # FLAG[4] --- 'RunMeanPreFitFlag'
        # FLAG[5] --- 'RmsExpectedPostFitFlag'
        # FLAG[6] --- 'RmsExpectedPreFitFlag'
        types = ['RmsPostFitFlag', 'RmsPreFitFlag', 'RunMeanPostFitFlag', 'RunMeanPreFitFlag',
                 'RmsExpectedPostFitFlag', 'RmsExpectedPreFitFlag']
        mask = 1
        for idx in xrange(len(types)):
            if FlagRule[types[idx]]['isActive'] and tflag[idx+1] == 0:
                mask = 0
                break
        return mask

    def plot_flag(self, DataTable, ids, PosGap, TimeGap, threshold, FlagRule, FigFileDir, FigFileRoot, is_baselined):
        FlagRule_local = copy.deepcopy(FlagRule)
        if not is_baselined:
            FlagRule_local['RmsPostFitFlag']['isActive'] = False
            FlagRule_local['RunMeanPostFitFlag']['isActive'] = False
            FlagRule_local['RmsExpectedPostFitFlag']['isActive'] = False

        # Plot statistics
        NROW = len(ids)
        # Store data for plotting
        NumFlaggedRowsCategory = [0 for i in xrange(10)]
        CategoryFlag = numpy.zeros(NROW, numpy.int16)
        PermanentFlag = numpy.zeros(NROW, numpy.int8)
        NPpdata = numpy.zeros((7,NROW), numpy.float)
        NPpflagBinary = numpy.zeros(NROW, numpy.int8)
        NPprows = numpy.zeros(NROW, numpy.int)

        binaryFlagTsys = 2**0
        binaryFlagWeather = 2**1
        binaryFlagUser = 2**2
        binaryFlagOnline = 2**3
        binaryFlagPreFitRms = 2**5
        binaryFlagPostFitRms = 2**4
        binaryFlagPreFitRunMean = 2**7
        binaryFlagPostFitRunMean = 2**6
        binaryFlagPreFitExpected = 2**9
        binaryFlagPostFitExpected = 2**8
        for N, ID in enumerate(ids):
            row = DataTable.getcell('ROW', ID)
            # Check every flags to create summary flag
            tFLAG = DataTable.getcell('FLAG',ID)
            tPFLAG = DataTable.getcell('FLAG_PERMANENT',ID)
            tTSYS = DataTable.getcell('TSYS',ID)
            tSTAT = DataTable.getcell('STATISTICS',ID)

            # permanent flag
            Flag = self._get_parmanent_flag_summary(tPFLAG, FlagRule_local)
            PermanentFlag[N] = Flag

           # Tsys flag
            NPpdata[0][N] = tTSYS
            NPpflagBinary[N] += tPFLAG[1] * binaryFlagTsys
            NPprows[N] = row
            if tPFLAG[1] == 0:
                NumFlaggedRowsCategory[0] += 1
                CategoryFlag[N] += binaryFlagTsys
            # Weather flag
            if tPFLAG[0] == 0:
                NumFlaggedRowsCategory[1] += 1
                CategoryFlag[N] += binaryFlagWeather
            # User flag
            if tPFLAG[2] == 0:
                NumFlaggedRowsCategory[2] += 1
                CategoryFlag[N] += binaryFlagUser
            # Online flag
            if tPFLAG[3] == 0:
                NumFlaggedRowsCategory[3] += 1
                CategoryFlag[N] += binaryFlagOnline

            #NPprows[1][N] = row
            # RMS flag before baseline fit
            NPpdata[1][N] = tSTAT[2]
            NPpflagBinary[N] += tFLAG[2] * binaryFlagPreFitRms
            if tFLAG[2] == 0:
                NumFlaggedRowsCategory[5] += 1
                CategoryFlag[N] += binaryFlagPreFitRms
            # RMS flag after baseline fit
            NPpdata[2][N] = tSTAT[1]
            NPpflagBinary[N] += tFLAG[1] * binaryFlagPostFitRms
            if tFLAG[1] == 0:
                NumFlaggedRowsCategory[4] += 1
                CategoryFlag[N] += binaryFlagPostFitRms
            # Running mean flag before baseline fit
            NPpdata[3][N] = tSTAT[4]
            NPpflagBinary[N] += tFLAG[4] * binaryFlagPreFitRunMean 
            if tFLAG[4] == 0:
                NumFlaggedRowsCategory[7] += 1
                CategoryFlag[N] += binaryFlagPreFitRunMean
            # Running mean flag after baseline fit
            NPpdata[4][N] = tSTAT[3]
            NPpflagBinary[N] += tFLAG[3] * binaryFlagPostFitRunMean
            if tFLAG[3] == 0:
                NumFlaggedRowsCategory[6] += 1
                CategoryFlag[N] += binaryFlagPostFitRunMean
            # Expected RMS flag before baseline fit
            NPpdata[5][N] = tSTAT[6]
            NPpflagBinary[N] += tFLAG[5] * binaryFlagPreFitExpected
            if tFLAG[6] == 0:
                NumFlaggedRowsCategory[9] += 1
                CategoryFlag[N] += binaryFlagPreFitExpected
            # Expected RMS flag after baseline fit
            NPpdata[6][N] = tSTAT[5]
            NPpflagBinary[N] += tFLAG[5] * binaryFlagPostFitExpected
            if tFLAG[5] == 0:
                NumFlaggedRowsCategory[8] += 1
                CategoryFlag[N] += binaryFlagPostFitExpected
        # data store finished
        
        # summary flag
        NumFlaggedRows = len(CategoryFlag.nonzero()[0])

        ThreExpectedRMSPreFit = FlagRule_local['RmsExpectedPreFitFlag']['Threshold']
        ThreExpectedRMSPostFit = FlagRule_local['RmsExpectedPostFitFlag']['Threshold']
        plots = []
        # Tsys flag
        pflag = (NPpflagBinary / binaryFlagTsys) % 2
        PlotData = {'row': NPprows, 'data': NPpdata[0], 'flag': pflag, \
                    'thre': [threshold[4][1], 0.0], \
                    'gap': [PosGap, TimeGap], \
                            'title': "Tsys (K)\nBlue dots: data points, Red dots: deviator, Cyan H-line: %.1f sigma threshold, Red H-line(s): out of vertical scale limit(s)" % FlagRule_local['TsysFlag']['Threshold'], \
                    'xlabel': "row (spectrum)", \
                    'ylabel': "Tsys (K)", \
                    'permanentflag': PermanentFlag, \
                    'isActive': FlagRule_local['TsysFlag']['isActive'], \
                    'threType': "line"}
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_0')
        plots.append(FigFileRoot+'_0.png')

        # RMS flag before baseline fit
        pflag = (NPpflagBinary / binaryFlagPreFitRms) % 2
        PlotData['data'] = NPpdata[1]
        PlotData['flag'] = pflag
        PlotData['thre'] = [threshold[1][1]]
        PlotData['title'] = "Baseline RMS (K) before baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: %.1f sigma threshold, Red H-line(s): out of vertical scale limit(s)" % FlagRule_local['RmsPreFitFlag']['Threshold']
        PlotData['ylabel'] = "Baseline RMS (K)"
        PlotData['isActive'] = FlagRule_local['RmsPreFitFlag']['isActive']
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_1')
        plots.append(FigFileRoot+'_1.png')

        # RMS flag after baseline fit
        pflag = (NPpflagBinary / binaryFlagPostFitRms) % 2
        PlotData['data'] = NPpdata[2] if is_baselined else None
        PlotData['flag'] = pflag
        PlotData['thre'] = [threshold[0][1]]
        PlotData['title'] = "Baseline RMS (K) after baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: %.1f sigma threshold, Red H-line(s): out of vertical scale limit(s)" % FlagRule_local['RmsPostFitFlag']['Threshold']
        PlotData['isActive'] = FlagRule_local['RmsPostFitFlag']['isActive']
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_2')
        plots.append(FigFileRoot+'_2.png')

        # Running mean flag before baseline fit
        pflag = (NPpflagBinary / binaryFlagPreFitRunMean) % 2
        PlotData['data'] = NPpdata[3]
        PlotData['flag'] = pflag
        PlotData['thre'] = [threshold[3][1]]
        PlotData['title'] = "RMS (K) for Baseline Deviation from the running mean (Nmean=%d) before baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: %.1f sigma threshold, Red H-line(s): out of vertical scale limit(s)" % (FlagRule_local['RunMeanPreFitFlag']['Nmean'], FlagRule_local['RunMeanPreFitFlag']['Threshold'])
        PlotData['isActive'] = FlagRule_local['RunMeanPreFitFlag']['isActive']
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_3')
        plots.append(FigFileRoot+'_3.png')

        # Running mean flag after baseline fit
        pflag = (NPpflagBinary / binaryFlagPostFitRunMean) % 2
        PlotData['data'] = NPpdata[4] if is_baselined else None
        PlotData['flag'] = pflag
        PlotData['thre'] = [threshold[2][1]]
        PlotData['title'] = "RMS (K) for Baseline Deviation from the running mean (Nmean=%d) after baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: %.1f sigma threshold, Red H-line(s): out of vertical scale limit(s)" % (FlagRule_local['RunMeanPostFitFlag']['Nmean'], FlagRule_local['RunMeanPostFitFlag']['Threshold'])
        PlotData['isActive'] = FlagRule_local['RunMeanPostFitFlag']['isActive']
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_4')
        plots.append(FigFileRoot+'_4.png')

        # Expected RMS flag before baseline fit
        pflag = (NPpflagBinary / binaryFlagPreFitExpected) % 2
        PlotData['data'] = NPpdata[1]
        PlotData['flag'] = pflag
        PlotData['thre'] = [pflag]
        PlotData['title'] = "Baseline RMS (K) compared with the expected RMS calculated from Tsys before baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: threshold with the scaling factor of %.1f, Red H-line(s): out of vertical scale limit(s)" % ThreExpectedRMSPreFit
        PlotData['isActive'] = FlagRule_local['RmsExpectedPreFitFlag']['isActive']
        PlotData['threType'] = "plot"
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_5')
        plots.append(FigFileRoot+'_5.png')

        # Expected RMS flag after baseline fit
        pflag = (NPpflagBinary / binaryFlagPostFitExpected) % 2
        PlotData['data'] = NPpdata[2] if is_baselined else None
        PlotData['flag'] = pflag
        PlotData['thre'] = [pflag]
        PlotData['title'] = "Baseline RMS (K) compared with the expected RMS calculated from Tsys after baseline subtraction\nBlue dots: data points, Red dots: deviator, Cyan H-line: threshold with the scaling factor of %.1f" % ThreExpectedRMSPostFit
        PlotData['isActive'] = FlagRule_local['RmsExpectedPostFitFlag']['isActive']
        PlotData['threType'] = "plot"
        SDP.StatisticsPlot(PlotData, FigFileDir, FigFileRoot+'_6')
        plots.append(FigFileRoot+'_6.png')

        # ugly restore for summary table
        if not is_baselined:
            FlagRule_local['RmsPostFitFlag']['isActive'] = FlagRule['RmsPostFitFlag']['isActive']
            FlagRule_local['RmsPostFitFlag']['Threshold'] = "SKIPPED"
            FlagRule_local['RunMeanPostFitFlag']['isActive'] = FlagRule['RunMeanPostFitFlag']['isActive']
            FlagRule_local['RunMeanPostFitFlag']['Threshold'] = "SKIPPED"
            FlagRule_local['RmsExpectedPostFitFlag']['isActive'] = FlagRule['RmsExpectedPostFitFlag']['isActive']
            FlagRule_local['RmsExpectedPostFitFlag']['Threshold'] = "SKIPPED"

        # Create Flagging Summary Page
        if FigFileDir != False:
            Filename = FigFileDir+FigFileRoot+'.html'
            relpath = os.path.basename(FigFileDir.rstrip("/")) ### stage#
            if os.access(Filename, os.F_OK): os.remove(Filename)
            # Assuming single scantable, antenna, spw, and pol
            ID = ids[0]
            ant_id = DataTable.getcell('ANTENNA',ID)
            st_row = DataTable.getcell('ROW',ID)
            #st_name = DataTable.getkeyword('FILENAMES')[ant_id]
            st = self.context.observing_run[ant_id]
            asdm = common.asdm_name(st)
            ant_name = st.antenna.name
            pol = DataTable.getcell('POL',ID)
            spw = DataTable.getcell('IF',ID)
            
            Out = open(Filename, 'w')
            #print >> Out, '<html>\n<head>\n<style>'
            #print >> Out, '.ttl{font-size:20px;font-weight:bold;}'
            #print >> Out, '.stt{font-size:18px;font-weight:bold;color:white;background-color:navy;}'
            #print >> Out, '.stp{font-size:18px;font-weight:bold;color:black;background-color:gray;}'
            #print >> Out, '.stc{font-size:16px;font-weight:normal;}'
            #print >> Out, '</style>\n</head>\n<body>'
            print >> Out, '<body>'
            print >> Out, '<p class="ttl">Data Summary</p>'
            # A table of data summary
            print >> Out, '<table border="0"  cellpadding="3">'
            print >> Out, '<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Name', asdm)
            print >> Out, '<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Antenna', ant_name)
            print >> Out, '<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Spw ID', spw)
            print >> Out, '<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Pol', pol)
            print >> Out, '</table>\n'
            
            print >> Out, '<HR><p class="ttl">Flagging Status</p>'
            # A table of flag statistics summary
            print >> Out, '<table border="1">'
            print >> Out, '<tr align="center" class="stt"><th>&nbsp</th><th>isActive?</th><th>SigmaThreshold<th>Flagged spectra</th><th>Flagged ratio(%)</th></tr>'
            print >> Out, _format_table_row_html('User', FlagRule_local['UserFlag']['isActive'], FlagRule_local['UserFlag']['Threshold'], NumFlaggedRowsCategory[2], NROW)
            print >> Out, _format_table_row_html('Weather', FlagRule_local['WeatherFlag']['isActive'], FlagRule_local['WeatherFlag']['Threshold'], NumFlaggedRowsCategory[1], NROW)
            print >> Out, _format_table_row_html('Tsys', FlagRule_local['TsysFlag']['isActive'], FlagRule_local['TsysFlag']['Threshold'], NumFlaggedRowsCategory[0], NROW)
            print >> Out, _format_table_row_html('Online', True, "-", NumFlaggedRowsCategory[3], NROW)
            print >> Out, _format_table_row_html('RMS baseline (pre-fit)', FlagRule_local['RmsPreFitFlag']['isActive'], FlagRule_local['RmsPreFitFlag']['Threshold'], NumFlaggedRowsCategory[5], NROW)
            print >> Out, _format_table_row_html('RMS baseline (post-fit)', FlagRule_local['RmsPostFitFlag']['isActive'], FlagRule_local['RmsPostFitFlag']['Threshold'], NumFlaggedRowsCategory[4], NROW)
            print >> Out, _format_table_row_html('Running Mean (pre-fit)', FlagRule_local['RunMeanPreFitFlag']['isActive'], FlagRule_local['RunMeanPreFitFlag']['Threshold'], NumFlaggedRowsCategory[7], NROW)
            print >> Out, _format_table_row_html('Running Mean (post-fit)', FlagRule_local['RunMeanPostFitFlag']['isActive'], FlagRule_local['RunMeanPostFitFlag']['Threshold'], NumFlaggedRowsCategory[6], NROW)
            print >> Out, _format_table_row_html('Expected RMS (pre-fit)', FlagRule_local['RmsExpectedPreFitFlag']['isActive'], FlagRule_local['RmsExpectedPreFitFlag']['Threshold'], NumFlaggedRowsCategory[9], NROW)
            print >> Out, _format_table_row_html('Expected RMS (post-fit)', FlagRule_local['RmsExpectedPostFitFlag']['isActive'], FlagRule_local['RmsExpectedPostFitFlag']['Threshold'], NumFlaggedRowsCategory[8], NROW)
            print >> Out, '<tr align="center" class="stt"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th>%.1f</th></tr>' % ('Total Flagged', '-', '-', NumFlaggedRows, NumFlaggedRows*100.0/NROW)
            print >> Out, '<tr><td colspan=4>%s</td></tr>' % ("Note: flags in grey background are permanent, <br> which are not reverted or changed during the iteration cycles.") 
            #print >> Out, '</table>\n</body>\n</html>'
            print >> Out, '</table>\n'
            # NOTE for not is_baselined
            if not is_baselined: print >> Out, 'ATTENTION: flag by post-fit spectra are skipped due to absence of baseline-fitting in previous stages.\n'
            # Plot figures
            print >> Out, '<HR>\nNote to all the plots below: short green vertical lines indicate position gaps; short cyan vertical lines indicate time gaps\n<HR>'
            for name in plots:
                print >> Out, '<img src="%s/%s">\n<HR>' % (relpath, name)
            #print >> Out, '</body>\n</html>'
            print >> Out, '</body>'
            Out.close()

        CalcFlaggedRowsCategory = lambda x: [DataTable.getcell('ROW', ids[i]) for i in xrange(len(ids)) if (CategoryFlag[i] / 2**x) % 2 == 1]
        # User flag
        LOG.info('Number of rows flagged by User = %d /%d' % (NumFlaggedRowsCategory[2], NROW))
        if NumFlaggedRowsCategory[2] > 0:
            LOG.debug('Flagged rows by User =%s ' % CalcFlaggedRowsCategory(2))
        # Weather
        LOG.info('Number of rows flagged by Weather = %d /%d' % (NumFlaggedRowsCategory[1], NROW))
        if NumFlaggedRowsCategory[1] > 0:
            LOG.debug('Flagged rows by Weather =%s ' % CalcFlaggedRowsCategory(1))
        # Tsys
        LOG.info('Number of rows flagged by Tsys = %d /%d' % (NumFlaggedRowsCategory[0], NROW))
        if NumFlaggedRowsCategory[0] > 0:
            LOG.debug('Flagged rows by Tsys =%s ' % CalcFlaggedRowsCategory(0))
        # Tsys
        LOG.info('Number of rows flagged by on-line flag = %d /%d' % (NumFlaggedRowsCategory[3], NROW))
        if NumFlaggedRowsCategory[3] > 0:
            LOG.debug('Flagged rows by on-line flag =%s ' % CalcFlaggedRowsCategory(3))
        # Pre-fit RMS
        LOG.info('Number of rows flagged by the baseline fluctuation (pre-fit) = %d /%d' % (NumFlaggedRowsCategory[5], NROW))
        if NumFlaggedRowsCategory[5] > 0:
            LOG.debug('Flagged rows by the baseline fluctuation (pre-fit) =%s ' % CalcFlaggedRowsCategory(5))
        # Post-fit RMS
        if is_baselined: 
            LOG.info('Number of rows flagged by the baseline fluctuation (post-fit) = %d /%d' % (NumFlaggedRowsCategory[4], NROW))
            if NumFlaggedRowsCategory[4] > 0:
                LOG.debug('Flagged rows by the baseline fluctuation (post-fit) =%s ' % CalcFlaggedRowsCategory(4))
        # Pre-fit running mean
        LOG.info('Number of rows flagged by the difference from running mean (pre-fit) = %d /%d' % (NumFlaggedRowsCategory[7], NROW))
        if NumFlaggedRowsCategory[7] > 0:
            LOG.debug('Flagged rows by the difference from running mean (pre-fit) =%s ' % CalcFlaggedRowsCategory(7))
        # Post-fit running mean
        if is_baselined:
            LOG.info('Number of rows flagged by the difference from running mean (post-fit) = %d /%d' % (NumFlaggedRowsCategory[6], NROW))
            if NumFlaggedRowsCategory[6] > 0:
                LOG.debug('Flagged rows by the difference from running mean (post-fit) =%s ' % CalcFlaggedRowsCategory(6))
        # Pre-fit expected RMS
        LOG.info('Number of rows flagged by the expected RMS (pre-fit) = %d /%d' % (NumFlaggedRowsCategory[9], NROW))
        if NumFlaggedRowsCategory[9] > 0:
            LOG.debug('Flagged rows by the expected RMS (pre-fit) =%s ' % CalcFlaggedRowsCategory(9))
        # Post-fit expected RMS
        if is_baselined:
            LOG.info('Number of rows flagged by the expected RMS (post-fit) = %d /%d' % (NumFlaggedRowsCategory[8], NROW))
            if NumFlaggedRowsCategory[8] > 0:
                LOG.debug('Flagged rows by the expected RMS (post-fit) =%s ' % CalcFlaggedRowsCategory(8))
        # All categories
        LOG.info('Number of rows flagged by all active categories = %d /%d' % (NumFlaggedRows, NROW))
        if NumFlaggedRows > 0:
            LOG.debug('Final Flagged rows by all active categories =%s ' % [DataTable.getcell('ROW', ids[i]) for i in xrange(len(ids)) if CategoryFlag[i] != 0])

        flag_nums = [NumFlaggedRows] + [n for n in NumFlaggedRowsCategory]

        del threshold, NPpdata, NPprows, PlotData
        return os.path.basename(Filename), flag_nums

def _format_table_row_html(label, isactive, threshold, nflag, ntotal):
    valid_flag = isactive and (threshold != 'SKIPPED')
    typestr = "%.1f"
    if not valid_flag: typestr="%s"
    html_str = '<tr align="center" class="stp"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th>'+typestr+'</th></tr>'
    return html_str % (label, isactive, threshold, (nflag if valid_flag else "N/A"), (nflag*100.0/ntotal if valid_flag else "N/A"))

def _get_iteration(reduction_group, antenna, spw, pol):
    for (group_id, group_desc) in reduction_group.items():
        for group_member in group_desc:
            if group_member.antenna == antenna and group_member.spw == spw and pol in group_member.pols:
                return group_member.iteration[pol]
    raise RuntimeError('Given (%s, %s, %s) is not in reduction group.'%(antenna, spw, pol))
