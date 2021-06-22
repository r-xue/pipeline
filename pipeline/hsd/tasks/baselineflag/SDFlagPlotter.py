"""
Created on 2013/07/02

@author: kana
"""
import os

import matplotlib.pyplot as plt

from pipeline.domain import DataTable, MeasurementSet
import pipeline.infrastructure as infrastructure

from .SDFlagRule import INVALID_STAT
from ..common import display as sd_display
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from ..common import utils as sdutils

from typing import Dict, List
import pipeline.infrastructure.utils as utils
from pipeline.domain import DataTable, MeasurementSet


LOG = infrastructure.get_logger(__name__)


## 0:DebugPlot 1:TPlotRADEC 2:TPlotAzEl 3:TPlotCluster 4:TplotFit 5:TPlotMultiSP 6:TPlotSparseSP 7:TPlotChannelMap 8:TPlotFlag 9:TPlotIntermediate
MATPLOTLIB_FIGURE_ID = [8904, 8905, 8906, 8907, 8908, 8909, 8910, 8911, 8912, 8913]
DPIDetail = 130
FIGSIZE_INCHES = (7.0, 2.8)


class SDFlagPlotter(object):
    """
    Class to create Flag Plots for hsd_blflag weblog
    """
    @classmethod
    def create_plots( self, msobj:MeasurementSet, datatable:DataTable, 
                      antid:int, spwid:int, is_baselined:bool, FlagRule_local:Dict, PermanentFlag:List[int], 
                      NPp_dict:Dict, threshold, time_gap, FigFileDir:str, FigFileRoot:str ) -> List[str]:
        """
        Create Summary plots
    
        Vars:
            msobj          : Measurementset object
            datatable      : DataTable
            antid          : antenna ID
            spwid          : SpW ID
            is_baselined   : True if baselined, False if not
            FlagRule_local : flag Rule for local use
            PermanentFlag  : Permanent flag
            NPp_dict:      : flagging data summarized for weblog
            threshold:
            time_gap:
            FigFileDir     : directory to output figure files
            FigFileRoot    : basename of figure files 
        Returns:
            List of Figure filenames (basename only)
        """
        ant_name     = msobj.get_antenna(antid)[0].name
        PosGap = time_gap[0]
        TimeGap = time_gap[1]
        NPpflag = NPp_dict['flag']
        NPprows = NPp_dict['rows']
        NPptime = NPp_dict['time']
        NPpdata = NPp_dict['data']

        ThreExpectedRMSPreFit = FlagRule_local['RmsExpectedPreFitFlag']['Threshold']
        ThreExpectedRMSPostFit = FlagRule_local['RmsExpectedPostFitFlag']['Threshold']
        plots = []

        # Tsys flag
        timeCol = datatable.getcol('TIME')
        PosGapInTime = timeCol.take(PosGap)
        TimeGapInTime = timeCol.take(TimeGap)
        PlotData = {
            'ms_name': msobj.name,
            'ant_name' : ant_name,
            'spw' : spwid,
            'row': NPprows['TsysFlag'],
            'time': NPptime['TsysFlag'],
            'data': NPpdata['TsysFlag'],
            'flag': NPpflag['TsysFlag'],
            'thre': [threshold[4][1], 0.0],
            'gap': [PosGapInTime, TimeGapInTime],
            'title': "Tsys (K)",
            'xlabel': "Time (UTC)",
            'ylabel': "Tsys (K)",
            'permanentflag': PermanentFlag,
            'isActive': FlagRule_local['TsysFlag']['isActive'],
            'threType': "line",
            'threDesc': "{:.1f} sigma threshold".format(FlagRule_local['TsysFlag']['Threshold'])
        }
        figfilename = FigFileRoot+'_0.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename )
        plots.append( figfilename )

        PlotData['row'] = NPprows['BaselineFlag']
        PlotData['time'] = NPptime['BaselineFlag']
        # RMS flag before baseline fit
        bunit = sdutils.get_brightness_unit( msobj.name, defaultunit='Jy/beam' )
        PlotData['data'] = NPpdata['RmsPreFitFlag']
        PlotData['flag'] = NPpflag['RmsPreFitFlag']
        PlotData['thre'] = [threshold[1][1]]
        PlotData['title'] = "Baseline RMS ({}) before baseline subtraction".format(bunit)
        PlotData['ylabel'] = "Baseline RMS ({})".format(bunit)
        PlotData['isActive'] = FlagRule_local['RmsPreFitFlag']['isActive']
        PlotData['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local['RmsPreFitFlag']['Threshold'])
        figfilename = FigFileRoot+'_1.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename )
        plots.append( figfilename )

        # RMS flag after baseline fit
        PlotData['data'] = NPpdata['RmsPostFitFlag'] if is_baselined else None
        PlotData['flag'] = NPpflag['RmsPostFitFlag']
        PlotData['thre'] = [threshold[0][1]]
        PlotData['title'] = "Baseline RMS ({}) after baseline subtraction".format(bunit)
        PlotData['isActive'] = FlagRule_local['RmsPostFitFlag']['isActive']
        PlotData['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local['RmsPostFitFlag']['Threshold'])
        figfilename = FigFileRoot+'_2.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename ) 
        plots.append( figfilename )
    
        # Running mean flag before baseline fit
        PlotData['data'] = NPpdata['RunMeanPreFitFlag']
        PlotData['flag'] = NPpflag['RunMeanPreFitFlag']
        PlotData['thre'] = [threshold[3][1]]
        PlotData['title'] = "RMS ({}) for Baseline Deviation from the running mean (Nmean={:d}) before baseline subtraction".format(bunit, FlagRule_local['RunMeanPreFitFlag']['Nmean'])
        PlotData['isActive'] = FlagRule_local['RunMeanPreFitFlag']['isActive']
        PlotData['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local['RunMeanPreFitFlag']['Threshold'])
        figfilename = FigFileRoot+'_3.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename )
        plots.append( figfilename )

        # Running mean flag after baseline fit
        PlotData['data'] = NPpdata['RunMeanPostFitFlag'] if is_baselined else None
        PlotData['flag'] = NPpflag['RunMeanPostFitFlag']
        PlotData['thre'] = [threshold[2][1]]
        PlotData['title'] = "RMS ({}) for Baseline Deviation from the running mean (Nmean={:d}) after baseline subtraction".format(bunit, FlagRule_local['RunMeanPostFitFlag']['Nmean'])
        PlotData['isActive'] = FlagRule_local['RunMeanPostFitFlag']['isActive']
        PlotData['threDesc'] = "{:.1f} sigma threshold".format(FlagRule_local['RunMeanPostFitFlag']['Threshold'])
        figfilename = FigFileRoot+'_4.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename ) 
        plots.append( figfilename )

        # Expected RMS flag before baseline fit
        PlotData['data'] = NPpdata['RmsPreFitFlag']
        PlotData['flag'] = NPpflag['RmsExpectedPreFitFlag']
        PlotData['thre'] = [NPpdata['RmsExpectedPreFitFlag']]
        PlotData['title'] = "Baseline RMS ({}) compared with the expected RMS calculated from Tsys before baseline subtraction".format(bunit)
        PlotData['isActive'] = FlagRule_local['RmsExpectedPreFitFlag']['isActive']
        PlotData['threType'] = "plot"
        PlotData['threDesc'] = "threshold with the scaling factor of {:.1f}".format(ThreExpectedRMSPreFit)
        figfilename = FigFileRoot+'_5.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename )
        plots.append( figfilename )

        # Expected RMS flag after baseline fit
        PlotData['data'] = NPpdata['RmsPostFitFlag'] if is_baselined else None
        PlotData['flag'] = NPpflag['RmsExpectedPostFitFlag']
        PlotData['thre'] = [NPpdata['RmsExpectedPostFitFlag']]
        PlotData['title'] = "Baseline RMS ({}) compared with the expected RMS calculated from Tsys after baseline subtraction".format(bunit)
        PlotData['isActive'] = FlagRule_local['RmsExpectedPostFitFlag']['isActive']
        PlotData['threType'] = "plot"
        PlotData['threDesc'] = "threshold with the scaling factor of {:.1f}".format(ThreExpectedRMSPostFit)
        figfilename = FigFileRoot+'_6.png'
        self.StatisticsPlot( PlotData, FigFileDir, figfilename )
        plots.append( figfilename )

        del PlotData
        return plots


    @casa5style_plot
    def StatisticsPlot( PlotData, FigFileDir=None, FigFileName=None ):
        # PlotData = {
        #             row:  [row], # row number of the spectrum
        #             time: [time], # timestamp
        #             data: [data],
        #             flag: [flag], # 0: flag out, 1: normal, 2: exclude from the plot
        #             sigma: "Clipping Sigma"
        #             thre: [threshold(s) max, min(if any)], # holizontal line
        #             gap:  [gap(s)], # vertical tick: [[PosGap], [TimeGap]]
        #             title: "title",
        #             xlabel: "xlabel",
        #             ylabel: "ylabel"
        #             permanentflag: [PermanentFlag rows]
        #             isActive: True/False
        #             threType: "line" or "plot" # if "plot" then thre should be a list
        #                           having the equal length of row
        #             threDesc: description of the threshold (for legend)
        #            }

        if FigFileDir is None:
            return

        plt.ioff()
        plt.figure(MATPLOTLIB_FIGURE_ID[8])

        if FigFileDir is not None:
            if os.access(FigFileDir+'listofplots.txt', os.F_OK):
                BrowserFile = open(FigFileDir+'listofplots.txt', 'a')
            else:
                BrowserFile = open(FigFileDir+'listofplots.txt', 'w')
                print('TITLE: BF_Stat', file=BrowserFile)
                print('FIELDS: Stat IF POL Iteration Page', file=BrowserFile)
                print('COMMENT: Statistics of spectra', file=BrowserFile)
            print(FigFileName, file=BrowserFile)
            BrowserFile.close()

        plt.cla()
        plt.clf()
        figsize_org = plt.gcf().get_size_inches()
        plt.gcf().set_size_inches(FIGSIZE_INCHES)
        # plt.subplot(211)
        plt.subplot(111)
        # plt.subplots_adjust(hspace=0.3)
        plt.subplots_adjust(top=0.88, bottom=0.13, left=0.1, right=0.98)
        t = plt.title(PlotData['title'], size=7)
        t.set_position((0.5, 1.05))
        plt.xlabel(PlotData['xlabel'], size=6)
        plt.ylabel(PlotData['ylabel'], size=7)
        plt.xticks(size=6)
        plt.yticks(size=6)
        if PlotData['isActive']:
            plt.figtext(0.01, 0.99, "ACTIVE", horizontalalignment='left', verticalalignment='top', color='green', size=18,
                        style='italic', weight='bold')
        else:
            plt.figtext(0.01, 0.99, "INACTIVE", horizontalalignment='left', verticalalignment='top', color='red', size=18,
                        style='italic', weight='bold')

        # X-scale
        xmin = min(PlotData['time'])
        xmax = max(PlotData['time'])

        axes = plt.gcf().gca()
        axes.xaxis.set_major_locator(sd_display.utc_locator(start_time=xmin, end_time=xmax))
        axes.xaxis.set_major_formatter(sd_display.utc_formatter())

        # For NO DATA
        if PlotData['data'] is None:
            if PlotData['isActive']:
                raise Exception("Got no valid data for active flag type.")
            plt.axis([xmin, xmax, 0.0, 1.0])
            plt.figtext(0.5, 0.5, "NO DATA", horizontalalignment='center', verticalalignment='center', color='Gray', size=24,
                        style='normal', weight='bold')
            plt.ion()
            plt.draw()
            if FigFileDir is not None:
                OldPlot = FigFileDir+FigFileName
                plt.savefig(OldPlot, format='png', dpi=DPIDetail)
            plt.gcf().set_size_inches(figsize_org)
            return

        if len(PlotData['thre']) > 1:
            LowRange = True
        else:
            LowRange = False
            if PlotData['threType'] != "plot":
                PlotData['thre'].append(min(PlotData['data']))
            else:
                PlotData['thre'].append(min(min(PlotData['data']), min(PlotData['thre'][0])))

        # Calculate Y-scale
        if PlotData['threType'] == "plot":
            yrange = max(PlotData['thre'][0]) - PlotData['thre'][1]
            if yrange == 0.0:
                yrange = 1.0
            ymin = max(0.0, PlotData['thre'][1] - yrange * 0.5)
            ymax = (max(PlotData['thre'][0]) - ymin) * 1.3333333333 + ymin
        else:
            yrange = PlotData['thre'][0] - PlotData['thre'][1]
            if yrange == 0.0:
                yrange = 1.0
            ymin = max(0.0, PlotData['thre'][1] - yrange * 0.5)
            ymax = (PlotData['thre'][0] - ymin) * 1.3333333333 + ymin
        yy = ymax - ymin
        ScaleOut = [[ymax - yy * 0.1, ymax - yy * 0.04],
                    [ymin + yy * 0.1, ymin + yy * 0.04]]
        # Make Plot Data
        x = 0
        data = [[], [], [], [], [], []]

        for Pflag in PlotData['permanentflag']:
            if Pflag == 0 or PlotData['data'][x] == INVALID_STAT:  # Flag-out case
                data[4].append(PlotData['time'][x])
                if PlotData['data'][x] > ScaleOut[0][0] or PlotData['data'][x] == INVALID_STAT:
                    data[5].append(ScaleOut[0][1])
                elif LowRange and PlotData['data'][x] < ScaleOut[1][0]:
                    data[5].append(ScaleOut[1][1])
                else:
                    data[5].append(PlotData['data'][x])
            elif PlotData['flag'][x] == 0:  # Flag-out case
                data[2].append(PlotData['time'][x])
                if PlotData['data'][x] > ScaleOut[0][0]:
                    data[3].append(ScaleOut[0][1])
                elif LowRange and PlotData['data'][x] < ScaleOut[1][0]:
                    data[3].append(ScaleOut[1][1])
                else:
                    data[3].append(PlotData['data'][x])
            else:  # Normal case
                data[0].append(PlotData['time'][x])
                data[1].append(PlotData['data'][x])
            x += 1

        # Plot
        plt.plot(sd_display.mjd_to_plotval(data[4]), data[5], 's', markersize=1, markeredgecolor='0.5', markerfacecolor='0.5', label='flagged (online)')
        plt.plot(sd_display.mjd_to_plotval(data[0]), data[1], 'o', markersize=1, markeredgecolor='b', markerfacecolor='b', label='data below threshold')
        plt.plot(sd_display.mjd_to_plotval(data[2]), data[3], 'o', markersize=2, markeredgecolor='r', markerfacecolor='r', label='deviator')
        plt.axhline(y=ScaleOut[0][0], linewidth=1, color='r', label='vertical limit (s)')
        if PlotData['threType'] != "plot":
            plt.axhline(y=PlotData['thre'][0], linewidth=1, color='c', label=PlotData['threDesc'])
            if LowRange:
                plt.axhline(y=PlotData['thre'][1], linewidth=1, color='c')
                plt.axhline(y=ScaleOut[1][0], linewidth=1, color='r')
        else:
            plt.plot(sd_display.mjd_to_plotval(PlotData['time']), PlotData['thre'][0], '-', linewidth=1, color='c', label=PlotData['threDesc'])

        xmin, xmax = sd_display.mjd_to_plotval([xmin, xmax])
        plt.axis([xmin, xmax, ymin, ymax])

        if len(PlotData['gap']) > 0:
            for row in sd_display.mjd_to_plotval(PlotData['gap'][0]):
                plt.axvline(x=row, linewidth=0.5, color='g', ymin=0.95)
        if len(PlotData['gap']) > 1:
            for row in sd_display.mjd_to_plotval(PlotData['gap'][1]):
                plt.axvline(x=row, linewidth=0.5, color='c', ymin=0.9, ymax=0.95)

        plt.axis([xmin, xmax, ymin, ymax])
        plt.legend(loc='lower center', numpoints=1, ncol=5,
                   prop={'size': 7}, frameon=False,
                   bbox_to_anchor=(0.5, 0.99),
                   borderpad=0, handletextpad=0.5,
                   handlelength=1, columnspacing=1,
                   markerscale=2)

        plt.ion()
        plt.draw()
        if FigFileDir is not None:
            OldPlot = FigFileDir+FigFileName
            plt.savefig(OldPlot, format='png', dpi=DPIDetail)
        plt.gcf().set_size_inches(figsize_org)

        plt.close()

        del data, ScaleOut
        return
