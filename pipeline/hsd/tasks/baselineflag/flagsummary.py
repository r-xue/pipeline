import copy
import os
import time
import collections

import numpy as np

from typing import Dict, List, Optional, Tuple

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.domain import DataTable, MeasurementSet

from .SDFlagPlotter import SDFlagPlotter
from .worker import _get_permanent_flag_summary, _get_iteration
from .. import common
from ..common import utils as sdutils

LOG = infrastructure.get_logger(__name__)


class SDBLFlagSummary(object):
    """
    A class of single dish flagging task.
    This class defines per spwid flagging operation.
    """
    def __init__(self, context, ms, antid_list, fieldid_list,
                 spwid_list, pols_list, thresholds, flagRule):
        """
        Constructor of worker class
        """
        self.context = context
        self.ms = ms
        datatable_name = os.path.join(self.context.observing_run.ms_datatable_name, self.ms.basename)
        self.datatable = DataTable(name=datatable_name, readonly=True)
        self.antid_list = antid_list
        self.fieldid_list = fieldid_list
        self.spwid_list = spwid_list
        self.pols_list = pols_list
        self.thres_value = thresholds
        self.flagRule = flagRule
        self.bunit = sdutils.get_brightness_unit(self.ms.name, defaultunit='Jy/beam')

    def execute(self, dry_run:bool=True) -> Tuple[List[Dict],List]:
        """
        Summarizes flagging results.

        Iterates over antenna and polarization for a certain spw ID
        Args:
           dry_run: True if dry_run
        Returns:
           flagSummary : flagsummary
           plot_list   : list of plot objects
        Raises:
            Exception when is_baselined is False for baselined data
        """
        start_time = time.time()

        datatable = self.datatable
        ms = self.ms
        antid_list = self.antid_list
        fieldid_list = self.fieldid_list
        spwid_list = self.spwid_list
        pols_list = self.pols_list
        thresholds = self.thres_value
        flagRule = self.flagRule

        LOG.debug('Members to be processed in worker class:')
        for (a, f, s, p) in zip(antid_list, fieldid_list, spwid_list, pols_list):
            LOG.debug('\t%s: Antenna %s Field %d Spw %d Pol %s'%(ms.basename, a, f, s, p))

        # create output directory, stage#, manually
        stage_number = self.context.task_counter
        FigFileDir = (self.context.report_dir+"/stage%d" % stage_number)
        if not os.path.exists(FigFileDir):
            os.makedirs(FigFileDir, exist_ok=True)  #handle race condition in Tier-0 operation gracefully
        FigFileDir += "/"

        flagSummary = []
        plot_list = []
        # loop over members (practically, per antenna loop in an MS)
        for (antid, fieldid, spwid, pollist) in zip(antid_list, fieldid_list, spwid_list, pols_list):
            LOG.debug('Performing flagging for %s Antenna %d Field %d Spw %d' % (ms.basename, antid, fieldid, spwid))
            filename_in = ms.name
            ant_name = ms.get_antenna(antid)[0].name
            asdm = common.asdm_name_from_ms(ms)
            field_name = ms.get_fields(field_id=fieldid)[0].name
            LOG.info("*** Summarizing table: %s ***" % (os.path.basename(filename_in)))
            time_table = datatable.get_timetable(antid, spwid, None, ms.basename, fieldid)
            # Select time gap list: 'subscan': large gap; 'raster': small gap
            if flagRule['Flagging']['ApplicableDuration'] == "subscan":
                TimeTable = time_table[1]
            else:
                TimeTable = time_table[0]
            flatiter = utils.flatten([chunks[1] for chunks in TimeTable])
            dt_idx = [chunk for chunk in flatiter]
            iteration = _get_iteration(self.context.observing_run.ms_reduction_group,
                                       ms, antid, fieldid, spwid)

            time_gap = datatable.get_timegap(antid, spwid, None, asrow=False,
                                             ms=ms, field_id=fieldid)
            # time_gap[0]: PosGap, time_gap[1]: TimeGap
            
            for pol in pollist:
                ddobj = ms.get_data_description(spw=spwid)
                polid = ddobj.get_polarization_id(pol)
                # generate summary plot
                FigFileRoot = ("FlagStat_%s_ant%d_field%d_spw%d_pol%d_iter%d" %
                               (asdm, antid, fieldid, spwid, polid, iteration))
                # moved outside pol loop
                # time_gap = datatable.get_timegap(antid, spwid, None, asrow=False,
                #                                 ms=ms, field_id=fieldid)
                ## time_gap[0]: PosGap, time_gap[1]: TimeGap
                for i in range(len(thresholds)):
                    thres = thresholds[i]
                    if (thres['msname'] == ms.basename and thres['antenna'] == antid and
                            thres['field'] == fieldid and thres['spw'] == spwid and
                            thres['pol'] == pol):
                        final_thres = thres['result_threshold']
                        is_baselined = thres['baselined'] if 'baselined' in thres else False
                        thresholds.pop(i)
                        break
                if (not is_baselined) and not iteration == 0:
                    raise Exception("Internal error: is_baselined flag is set to False for baselined data.")
                t0 = time.time()

                # create FlagRule_local
                FlagRule_local = copy.deepcopy(flagRule)
                if not is_baselined:
                    FlagRule_local['RmsPostFitFlag']['isActive'] = False
                    FlagRule_local['RunMeanPostFitFlag']['isActive'] = False
                    FlagRule_local['RmsExpectedPostFitFlag']['isActive'] = False
                # pack flag values
                FlaggedRows, FlaggedRowsCategory, PermanentFlag, NPp_dict = self.pack_flags( datatable, polid, dt_idx, FlagRule_local )

                # create plots
                ### instance to be made outside pol loop if overplotting pols
                flagplotter = SDFlagPlotter( self.ms, datatable, antid, spwid, time_gap, FigFileDir )
                flagplotter.register_data( pol, is_baselined, FlagRule_local, PermanentFlag, NPp_dict, final_thres )
                plots = flagplotter.create_plots( FigFileRoot )
                for plot in plots:
                    plot_list.append( { 'FigFileDir' : FigFileDir,
                                        'FigFileRoot' : FigFileRoot,
                                        'plot' : plot['file'],
                                        'vis' : self.ms.name,
                                        'type' : plot['type'],
                                        'ant' : ant_name,
                                        'spw' : spwid, 
                                        'pol' : pol,
                                        'field' : field_name } )

                # delete variables not used after all
                del FlagRule_local, NPp_dict

                # create html file with summary table
                htmlName = self.create_summary_table( self.ms, datatable, polid, is_baselined, plots, 
                                                      dt_idx, flagRule, FlaggedRows, FlaggedRowsCategory, 
                                                      FigFileDir, FigFileRoot )

                # show flags on LOG
                self.show_flags( dt_idx, is_baselined, FlaggedRows, FlaggedRowsCategory )
                # create summary data
                nflags = self.create_summary_data( FlaggedRows, FlaggedRowsCategory )

                t1 = time.time()

                # dict to list conversion (only for compatibility)
                # will be removed once "Flag by Reason" table is removed
                nflags_list = list(nflags.values())

                LOG.info('Plot flags End: Elapsed time = %.1f sec' % (t1 - t0) )
                flagSummary.append({'html': htmlName, 'name': asdm,
                                    'antenna': ant_name, 'field': field_name,
                                    'spw': spwid, 'pol': pol,
                                    'nrow': len(dt_idx), 'nflags': nflags,
                                    'nflags_list': nflags_list,
                                    'baselined': is_baselined})

            flagplotter = None

        end_time = time.time()
        LOG.info('PROFILE execute: elapsed time is %s sec'%(end_time-start_time))

        return flagSummary, plot_list


    def pack_flags( self, datatable:DataTable, polid:int, ids, FlagRule_local:Dict ) -> Tuple[ List[int], Dict, List[int], Dict ]:
        """
        pack flag data into data sets

        Args:
            datatable      : DataTable
            polid          : polarization ID
            ids            : row numbers       
            FlagRule_local : FlagRule modified for local use
        Returns:
            FlaggedRows         : flagged rows
            FlaggedRowsCategory : flagged rows by category
            PermanentFlag       : permanent flag
            NPp_dict            : flagging data summarized for weblog
        """
        FlaggedRows = []
        PermanentFlag = []
        NROW = len(ids)

        NPprows = {}
        NPptime = {}
        for key in [ 'TsysFlag', 'BaselineFlag' ]:
            NPprows[key] = np.zeros( NROW, dtype=np.int )
            NPptime[key] = np.zeros( NROW, dtype=np.float )

        NPpdata = {}
        NPpflag = {}
        for key in [ 'TsysFlag', 'OnlineFlag', 
                     'RmsPostFitFlag', 'RmsPreFitFlag',
                     'RunMeanPostFitFlag', 'RunMeanPreFitFlag',
                     'RmsExpectedPostFitFlag', 'RmsExpectedPreFitFlag' ]:
            NPpdata[key] = np.zeros( NROW, dtype=np.float )
            NPpflag[key] = np.zeros( NROW, dtype=np.int )

        FlaggedRowsCategory = collections.OrderedDict((
            ('TsysFlag', []),              ('OnlineFlag', []),
            ('RmsPostFitFlag', []),        ('RmsPreFitFlag', []),
            ('RunMeanPostFitFlag', []),    ('RunMeanPreFitFlag', []),
            ('RmsExpectedPostFitFlag',[]), ('RmsExpectedPreFitFlag', [])
        ))

        NROW = len( ids )

        # Plot statistics
        # Store data for plotting
        for N, ID in enumerate(ids):
            row = datatable.getcell('ROW', ID)
            time = datatable.getcell('TIME', ID)
            # Check every flags to create summary flag
            tFLAG = datatable.getcell('FLAG', ID)[polid]
            tPFLAG = datatable.getcell('FLAG_PERMANENT', ID)[polid]
            tTSYS = datatable.getcell('TSYS', ID)[polid]
            tSTAT = datatable.getcell('STATISTICS', ID)[polid]

            # FLAG_SUMMARY
            Flag = datatable.getcell('FLAG_SUMMARY', ID)[polid]
            if Flag == 0:
                FlaggedRows.append(row)
            # permanent flag
            PermanentFlag.append( _get_permanent_flag_summary(tPFLAG, FlagRule_local) )
            # Tsys flag
            NPpdata['TsysFlag'][N] = tTSYS
            NPpflag['TsysFlag'][N] = tPFLAG[1]
            NPprows['TsysFlag'][N] = row
            NPptime['TsysFlag'][N] = time
            if FlagRule_local['TsysFlag']['isActive'] and tPFLAG[1] == 0:
                FlaggedRowsCategory['TsysFlag'].append(row)
            # Online flag
            if tPFLAG[3] == 0:
                FlaggedRowsCategory['OnlineFlag'].append(row)

            NPprows['BaselineFlag'][N] = row
            NPptime['BaselineFlag'][N] = time
            # RMS flag before baseline fit
            NPpdata['RmsPreFitFlag'][N] = tSTAT[2]
            NPpflag['RmsPreFitFlag'][N] = tFLAG[2]
            if FlagRule_local['RmsPreFitFlag']['isActive'] and tFLAG[2] == 0:
                FlaggedRowsCategory['RmsPreFitFlag'].append(row)
            #  RMS flag after baseline fit
            NPpdata['RmsPostFitFlag'][N] = tSTAT[1]
            NPpflag['RmsPostFitFlag'][N] = tFLAG[1]
            if FlagRule_local['RmsPostFitFlag']['isActive'] and tFLAG[1] == 0:
                FlaggedRowsCategory['RmsPostFitFlag'].append(row)
            # Running mean flag before baseline fit
            NPpdata['RunMeanPreFitFlag'][N] = tSTAT[4]
            NPpflag['RunMeanPreFitFlag'][N] = tFLAG[4]
            if FlagRule_local['RunMeanPreFitFlag']['isActive'] and tFLAG[4] == 0:
                FlaggedRowsCategory['RunMeanPreFitFlag'].append(row)
            # Running mean flag after baseline fit
            NPpdata['RunMeanPostFitFlag'][N] = tSTAT[3]
            NPpflag['RunMeanPostFitFlag'][N] = tFLAG[3]
            if FlagRule_local['RunMeanPostFitFlag']['isActive'] and tFLAG[3] == 0:
                FlaggedRowsCategory['RunMeanPostFitFlag'].append(row)
            # Expected RMS flag before baseline fit
            NPpdata['RmsExpectedPreFitFlag'][N] = tSTAT[6]
            NPpflag['RmsExpectedPreFitFlag'][N] = tFLAG[6]
            if FlagRule_local['RmsExpectedPreFitFlag']['isActive'] and tFLAG[6] == 0:
                FlaggedRowsCategory['RmsExpectedPreFitFlag'].append(row)
            # Expected RMS flag after baseline fit
            NPpdata['RmsExpectedPostFitFlag'][N] = tSTAT[5]
            NPpflag['RmsExpectedPostFitFlag'][N] = tFLAG[5]
            if FlagRule_local['RmsExpectedPostFitFlag']['isActive'] and tFLAG[5] == 0:
                FlaggedRowsCategory['RmsExpectedPostFitFlag'].append(row)
        # data store finished
        
        NPp_dict = {
            'data' : NPpdata,
            'flag' : NPpflag,
            'rows' : NPprows,
            'time' : NPptime
        }

        return FlaggedRows, FlaggedRowsCategory, PermanentFlag, NPp_dict


    def show_flags( self, ids:List[int], is_baselined:bool, FlaggedRows:List[int], FlaggedRowsCategory:Dict ):
        """
        Output flag statistics to LOG
        
        Args:
            ids                 : row numbers       
            is_baselined        : True if baselined, Fause if not
            FlaggedRows         : flagged rows
            FlaggedRowsCategory : flagged rows by category
        Returns:
            (none)
        """
        NROW = len( ids )

        # Tsys
        LOG.info('Number of rows flagged by Tsys = %d /%d' % (len(FlaggedRowsCategory['TsysFlag']), NROW))
        if len(FlaggedRowsCategory['TsysFlag']) > 0:
            LOG.debug('Flagged rows by Tsys =%s ' % FlaggedRowsCategory['TsysFlag'])
        # on-line flag
        LOG.info('Number of rows flagged by on-line flag = %d /%d' % (len(FlaggedRowsCategory['OnlineFlag']), NROW))
        if len(FlaggedRowsCategory['OnlineFlag']) > 0:
            LOG.debug('Flagged rows by Online-flag =%s ' % FlaggedRowsCategory['OnlineFlag'])
        # Pre-fit RMS
        LOG.info('Number of rows flagged by the baseline fluctuation (pre-fit) = %d /%d' %
                 (len(FlaggedRowsCategory['RmsPreFitFlag']), NROW))
        if len(FlaggedRowsCategory['RmsPreFitFlag']) > 0:
            LOG.debug('Flagged rows by the baseline fluctuation (pre-fit) =%s ' % FlaggedRowsCategory['RmsPreFitFlag'])
        # Post-fit RMS
        if is_baselined:
            LOG.info('Number of rows flagged by the baseline fluctuation (post-fit) = %d /%d' %
                     (len(FlaggedRowsCategory['RmsPostFitFlag']), NROW))
        if len(FlaggedRowsCategory['RmsPostFitFlag']) > 0:
            LOG.debug('Flagged rows by the baseline fluctuation (post-fit) =%s ' % FlaggedRowsCategory['RmsPostFitFlag'])
        # Pre-fit running mean
        LOG.info('Number of rows flagged by the difference from running mean (pre-fit) = %d /%d' %
                 (len(FlaggedRowsCategory['RunMeanPreFitFlag']), NROW))
        if len(FlaggedRowsCategory['RunMeanPreFitFlag']) > 0:
            LOG.debug('Flagged rows by the difference from running mean (pre-fit) =%s ' % FlaggedRowsCategory['RunMeanPreFitFlag'])
        # Post-fit running mean
        if is_baselined:
            LOG.info('Number of rows flagged by the difference from running mean (post-fit) = %d /%d' %
                     (len(FlaggedRowsCategory['RunMeanPostFitFlag']), NROW))
        if len(FlaggedRowsCategory['RunMeanPostFitFlag']) > 0:
            LOG.debug('Flagged rows by the difference from running mean (post-fit) =%s ' % FlaggedRowsCategory['RunMeanPostFitFlag'])
        # Pre-fit expected RMS
        LOG.info('Number of rows flagged by the expected RMS (pre-fit) = %d /%d' % (len(FlaggedRowsCategory['RmsExpectedPreFitFlag']), NROW))
        if len(FlaggedRowsCategory['RmsExpectedPreFitFlag']) > 0:
            LOG.debug('Flagged rows by the expected RMS (pre-fit) =%s ' % FlaggedRowsCategory['RmsExpectedPreFitFlag'])
        # Post-fit expected RMS
        if is_baselined:
            LOG.info('Number of rows flagged by the expected RMS (post-fit) = %d /%d' %
                     (len(FlaggedRowsCategory['RmsExpectedPostFitFlag']), NROW))
        if len(FlaggedRowsCategory['RmsExpectedPostFitFlag']) > 0:
            LOG.debug('Flagged rows by the expected RMS (post-fit) =%s ' % FlaggedRowsCategory['RmsExpectedPostFitFlag'])

        # All categories
        LOG.info('Number of rows flagged by all active categories = %d /%d' % (len(FlaggedRows), NROW))
        if len(FlaggedRows) > 0:
            LOG.debug('Final Flagged rows by all active categories =%s ' % FlaggedRows)


    def create_summary_data( self, FlaggedRows:List[int], FlaggedRowsCategory:Dict ) -> Dict:
        """
        Count flagged rows for each flagging reason
        
        Args:
            FlaggedRows         : flagged rows
            FlaggedRowsCategory : flagged rows by category
        Returns:
            List of flag countes
        """
        # Flag counts
        flag_nums = collections.OrderedDict()
        flag_nums['Flagged']                = len( FlaggedRows )
        flag_nums['TsysFlag']               = len( FlaggedRowsCategory['TsysFlag'] )
        flag_nums['OnlineFlag']             = len( FlaggedRowsCategory['OnlineFlag'] )

        flag_nums['RmsPostFitFlag']         = len( FlaggedRowsCategory['RmsPostFitFlag'] )
        flag_nums['RmsPreFitFlag']          = len( FlaggedRowsCategory['RmsPreFitFlag'] )
        flag_nums['RunMeanPostFitFlag']     = len( FlaggedRowsCategory['RunMeanPostFitFlag'] )
        flag_nums['RunMeanPreFitFlag']      = len( FlaggedRowsCategory['RunMeanPreFitFlag'] )
        flag_nums['RmsExpectedPostFitFlag'] = len( FlaggedRowsCategory['RmsExpectedPostFitFlag'] )
        flag_nums['RmsExpectedPreFitFlag']  = len( FlaggedRowsCategory['RmsExpectedPreFitFlag'] )

        # added following the discussion 
        total_additional = list(set(
            FlaggedRowsCategory['TsysFlag']
            + FlaggedRowsCategory['RmsPostFitFlag'] +FlaggedRowsCategory['RmsPreFitFlag']
            + FlaggedRowsCategory['RunMeanPostFitFlag'] + FlaggedRowsCategory['RunMeanPreFitFlag'] 
            + FlaggedRowsCategory['RmsExpectedPostFitFlag'] + FlaggedRowsCategory['RmsExpectedPreFitFlag'] 
        ))

        return flag_nums


    def create_summary_table( self, msobj:MeasurementSet, datatable:DataTable, polid:int, is_baselined:bool, 
                              plots:List[str], ids:List[int], 
                              FlagRule:Dict, FlaggedRows:List[int], FlaggedRowsCategory:Dict, 
                              FigFileDir:Optional[str], FigFileRoot:str ) -> str:
        """
        Create summary table for detail page

        Args:
            msobj               : Measurement Set Object
            datatable           : DataTable
            polid               : polarization ID
            is_baselined        : True if baselined, Fause if not
            plots               : List of figure filenames
            ids                 : row numbers
            FlagRule            : Flag Rule
            FlaggedRows         : flagged rows
            FlaggedRowsCategory : flagged rows by category
            FigFileDir          : directory to output figure files
            FigFileRoot         : basename of figure files
        Returns:
            html file name
        """
        NROW = len( ids )

        # Create Flagging Summary Page
        if FigFileDir is not None:
            Filename = FigFileDir+FigFileRoot+'.html'
            if os.access(Filename, os.F_OK):
                os.remove(Filename)
            # Assuming single MS, antenna, field, spw, and polid
            ID0 = ids[0]
            antid = datatable.getcell('ANTENNA', ID0)
            fieldid = datatable.getcell('FIELD_ID', ID0)
            spwid = datatable.getcell('IF', ID0)
            asdm = asdm = common.asdm_name_from_ms(msobj)
            ant_name = msobj.get_antenna(antid)[0].name
            field_name = msobj.get_fields(field_id=fieldid)[0].name
            ddobj = msobj.get_data_description(spw=spwid)
            pol_name = ddobj.corr_axis[polid]

            Out = open(Filename, 'w')
            print('<body>', file=Out)
            print('<p class="ttl">Data Summary</p>', file=Out)
            # A table of data summary
            print('<table border="0"  cellpadding="3">', file=Out)
            print('<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Name', asdm), file=Out)
            print('<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Antenna', ant_name), file=Out)
            print('<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Field', field_name), file=Out)
            print('<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Spw ID', spwid), file=Out)
            print('<tr align="left" class="stp"><th>%s</th><th>:</th><th>%s</th></tr>' % ('Pol', pol_name), file=Out)
            print('</table>\n', file=Out)

            print('<HR><p class="ttl">Flagging Status</p>', file=Out)
            # A table of flag statistics summary
            print('<table border="1">', file=Out)
            print('<tr align="center" class="stt"><th>&nbsp</th><th>isActive?</th><th>SigmaThreshold<th>Flagged spectra</th><th>Flagged ratio(%)</th></tr>', file=Out)
            print(self._format_table_row_html('Tsys', FlagRule['TsysFlag']['isActive'], FlagRule['TsysFlag']['Threshold'], len(FlaggedRowsCategory['TsysFlag']), NROW), file=Out)
            print(self._format_table_row_html('Online', True, "-", len(FlaggedRowsCategory['OnlineFlag']), NROW), file=Out)
            print(self._format_table_row_html('RMS baseline (pre-fit)', FlagRule['RmsPreFitFlag']['isActive'], FlagRule['RmsPreFitFlag']['Threshold'], len(FlaggedRowsCategory['RmsPreFitFlag']), NROW), file=Out)
            rmspostfitflag_thres = FlagRule['RmsPostFitFlag']['Threshold'] if is_baselined else "SKIPPED"
            print(self._format_table_row_html('RMS baseline (post-fit)', FlagRule['RmsPostFitFlag']['isActive'], rmspostfitflag_thres, len(FlaggedRowsCategory['RmsPostFitFlag']), NROW), file=Out)
            print(self._format_table_row_html('Running Mean (pre-fit)', FlagRule['RunMeanPreFitFlag']['isActive'], FlagRule['RunMeanPreFitFlag']['Threshold'], len(FlaggedRowsCategory['RunMeanPreFitFlag']), NROW), file=Out)
            runmeanpostfitflag_thres = FlagRule['RunMeanPostFitFlag']['Threshold'] if is_baselined else "SKIPPED"
            print(self._format_table_row_html('Running Mean (post-fit)', FlagRule['RunMeanPostFitFlag']['isActive'], runmeanpostfitflag_thres, len(FlaggedRowsCategory['RunMeanPostFitFlag']), NROW), file=Out)
            print(self._format_table_row_html('Expected RMS (pre-fit)', FlagRule['RmsExpectedPreFitFlag']['isActive'], FlagRule['RmsExpectedPreFitFlag']['Threshold'], len(FlaggedRowsCategory['RmsExpectedPreFitFlag']), NROW), file=Out)
            rmsexpectedpostfitflag_thres = FlagRule['RmsExpectedPostFitFlag']['Threshold'] if is_baselined else "SKIPPED"
            print(self._format_table_row_html('Expected RMS (post-fit)', FlagRule['RmsExpectedPostFitFlag']['isActive'], rmsexpectedpostfitflag_thres, len(FlaggedRowsCategory['RmsExpectedPostFitFlag']), NROW), file=Out)
            print('<tr align="center" class="stt"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th>%.1f</th></tr>' % ('Total Flagged', '-', '-', len(FlaggedRows), len(FlaggedRows)*100.0/NROW), file=Out)
            print('<tr><td colspan=4>%s</td></tr>' % ("Note: flags in grey background are permanent, <br> which are not reverted or changed during the iteration cycles."), file=Out)
            print('</table>\n', file=Out)
            # NOTE for not is_baselined
            if not is_baselined: print('ATTENTION: flag by post-fit spectra are skipped due to absence of baseline-fitting in previous stages.\n', file=Out)
            # Plot figures
            print('<HR>\nNote to all the plots below: short green vertical lines indicate position gaps; short cyan vertical lines indicate time gaps\n<HR>', file=Out)
            for name in plots:
                print('<img src="./%s">\n<HR>' % (name), file=Out)
            print('</body>', file=Out)
            Out.close()

        return os.path.basename(Filename)


    def _format_table_row_html( self, label:str, isactive:bool, threshold:str, nflag:int, ntotal:int ) -> str:
        """
        Format the html string for table row for "Flag by Reason"

        Args:
            label     : label sring
            isactive  : active flag for the criteria
            threshold : threshold value
            nflag     : Number of flagged rows
            ntotal    : Number of total rows
        Returns:
            html string
        """
        valid_flag = isactive and (threshold != 'SKIPPED')

        html_str =  '<tr align="center" class="stp">'
        html_str += '<th>{}</th><th>{}</th><th>{}</th>'.format(label, isactive, threshold)
        html_str += '<th>{}</th>'.format(nflag if valid_flag else "N/A")
        html_str += "<th>{:.1f}</th>".format(100.0*nflag/ntotal if valid_flag else "N/A")
        html_str += '</tr>'

        return html_str 
