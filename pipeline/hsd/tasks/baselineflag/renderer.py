"""Renderer hsd_blflag."""
import os
import collections
import copy
from typing import Dict, List, Optional, Tuple

import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.infrastructure import Context
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.logging as logging
from pipeline.infrastructure.renderer.logger import Plot
from .baselineflag import SDBLFlagResults
from ..common import utils as sdutils
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)

# max number of plots for histogram selector
HIST_PLOTS_MAX = 1000


class T2_4MDetailsBLFlagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """The renderer class for baselineflag."""

    def __init__( self, uri:str = 'hsd_blflag.mako',
                  description:str = 'Flag data by Tsys and statistics of spectra',
                  always_rerender:bool = False):
        """
        Construct T2_4MDetailsBLFlagRenderer instance.

        Args:
            uri             : mako template file
            description     : description string
            always_rerender : True if always rerender, False if not
        Returns:
            (none)
        """
        LOG.info( "###@@ T2_4MDetailsBLFlagRenderer.__init__()" )
        super(T2_4MDetailsBLFlagRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx:dict, context:Context, result:SDBLFlagResults):
        """
        Update mako context.

        Args:
            ctx:     dict for mako context
            context: pipeline context
            result:  SDBLFlag results
        Returns:
            (none)
        """
        # per field, spw table
        accum_flag_field = accumulate_flag_per_source_spw(context, result)
        table_rows_field = make_summary_table_per_field(accum_flag_field)
        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        ctx.update({'per_field_summary_table_rows': table_rows_field,
                    'dovirtual': dovirtual})

        # accumulate plots for statistics page
        plots_list = []
        for r in result:
            plots_list.extend( r.outcome['plots'] )

        # classic style (without histogram selector) with all plots
        title = "Flag Statistics (all) ({} plots)".format(len(plots_list))
        html = self._prepare_subpage( 'hsd_blflag_statistics_nohist.mako', context, result, plots_list,
                                      'hsd_blflag_statistics_full.html', title,
                                      allflags=False, ownflag=False, types=True )
        vis_list = list( set([ p.get('vis') for p in plots_list ]) )
        subpages2 = {}
        for vis in vis_list:
            subpages2[vis] = html

        # reduced plots with histogram selector
        plots_list_hist, title = self._filter_plots( plots_list, HIST_PLOTS_MAX, "Flag Statistics" )
        html = self._prepare_subpage( 'hsd_blflag_statistics.mako', context, result, plots_list_hist,
                                      'hsd_blflag_statistics_historgram.html', title,
                                      allflags=True, ownflag=False, types=True )
        subpages = {}
        for vis in vis_list:
            subpages[vis] = html

        # per_type plots with histogram selector
        type_list = list( set([ p.get('type') for p in plots_list ]) )
        subpages_per_type = {}
        for type in type_list:
            sub_plots_list = [ p for p in plots_list if p.get('type') == type ]

            sub_plots_list_hist, title = self._filter_plots( sub_plots_list, HIST_PLOTS_MAX,
                                                             "Flag Statistics: {}".format(type) )
            filename = 'hsd_blflag_statistics_'+type.replace( ' ', '_' )+'.html'
            html = self._prepare_subpage( 'hsd_blflag_statistics_per_type.mako', context, result, sub_plots_list,
                                          filename, title,
                                          allflags=False, ownflag=True, types=False )
            subpages_per_type[type] = html

        # per EB table
        accum_flag_eb = accumulate_flag_per_eb( context, result )
        table_rows_eb, statistics_subpages = make_summary_table_per_eb( accum_flag_eb, subpages, subpages2 )
        ctx.update( { 'per_eb_summary_table_rows': table_rows_eb,
                      'statistics_subpages'      : statistics_subpages,
                      'subpages_per_type'        : subpages_per_type,
                      'dovirtual'                : dovirtual } )

    def _filter_plots( self, plots_list:List[Plot], max_plots, title_base ) -> Tuple[List[Plot], str]:
        """
        Fliter statistics to reduce the number of plots

        Args:
            plots_list : List of plot objects
            max_plots  : upper limit of number of plots
            title_base : base for the title string
        Returns:
            plots_list_filtered : List of plot objects (filtered)
            title               : title string
        Raises:
            RuntimeError if failed to deterimine a fraction threshold (should not happen)
        """
        # reduce plots_list for histogram selector
        plots_list_filtered = copy.copy(plots_list)
        reduced_by_frac = False
        if len(plots_list_filtered) > max_plots:
            # first: drop zero-flagged cases
            flag_num_threshold = 1
            plots_list_filtered = self._filter_plots_by_num( plots_list_filtered, flag_num_threshold )
            # next:  select with flagged fractions
            flag_frac_threshold = 0.0
            d_thres = 0.01
            while len(plots_list_filtered) > HIST_PLOTS_MAX:
                flag_frac_threshold += d_thres
                plots_list_filtered = self._filter_plots_by_frac( plots_list_filtered, flag_frac_threshold )
                reduced_by_frac = True
                if flag_frac_threshold > 0.095:
                    d_thres = 0.1
                if flag_frac_threshold > 100.0:   # logically should not happen
                    raise RuntimeError( "internal error: could not find suitable flag_frac_threshold" )

        if len(plots_list) == len(plots_list_filtered):
            title = "{} (all) ({} plots)".format(title_base, len(plots_list_filtered))
        elif reduced_by_frac:
            title = "{} (flagged fraction >= {:.2f} %)  ({} plots)".format(
                title_base, flag_frac_threshold, len(plots_list_filtered))
        else:
            title = "{} (N_flagged >= {})  ({} plots)".format(
                title_base, flag_num_threshold, len(plots_list_filtered))

        return plots_list_filtered, title

    def _filter_plots_by_frac( self, plots_list:List[Plot], flag_frac_threshold:float ) -> List[Plot]:
        """
        Apply plot filter by flagged fraction

        Args:
            plots_list : List of plots objects
            flag_frac_threshold :
        Returns:
            List of filtered plot objects
        """
        plots_list_filtered = []
        for p in plots_list:
            if p.get('outlier_Tsys')['frac'] >= flag_frac_threshold \
               or p.get('rms_prefit')['frac'] >= flag_frac_threshold \
               or p.get('rms_postfit')['frac'] >= flag_frac_threshold \
               or p.get('runmean_prefit')['frac'] >= flag_frac_threshold \
               or p.get('runmean_postfit')['frac'] >= flag_frac_threshold \
               or p.get('expected_rms_prefit')['frac'] >= flag_frac_threshold \
               or p.get('expected_rms_postfit')['frac'] >= flag_frac_threshold:
                plots_list_filtered.append(p)
        LOG.info( "###@@ BLFLag plots (filter_by_frac): thres={} remain / total = {} / {}".format(
            flag_frac_threshold, len(plots_list_filtered), len(plots_list)))

        return plots_list_filtered

    def _filter_plots_by_num( self, plots_list:List[Plot], flag_num_threshold:int ) -> List[Plot]:
        """
        Apply plot filter by number of flags

        Args:
            plots_list : List of plots objects
            flag_num_threshold :
        Returns:
            List of filtered plot objects
        """
        plots_list_filtered = []
        for p in plots_list:
            if p.get('outlier_Tsys')['num'] >= flag_num_threshold \
               or p.get('rms_prefit')['num'] >= flag_num_threshold \
               or p.get('rms_postfit')['num'] >= flag_num_threshold \
               or p.get('runmean_prefit')['num'] >= flag_num_threshold \
               or p.get('runmean_postfit')['num'] >= flag_num_threshold \
               or p.get('expected_rms_prefit')['num'] >= flag_num_threshold \
               or p.get('expected_rms_postfit')['num'] >= flag_num_threshold:
                plots_list_filtered.append(p)
        LOG.info( "###@@ BLFLag plots (filter by num): thres={} remain / total = {} / {}".format(
            flag_num_threshold, len(plots_list_filtered), len(plots_list)))

        return plots_list_filtered

    def _prepare_subpage( self, uri:str, context:Context, result:SDBLFlagResults,
                          plots_list:List[Plot], filename:str, title:str,
                          allflags:Optional[bool]=False, ownflag:Optional[bool]=False, 
                          types:Optional[bool]=True ):
        """
        prepare the subpage

        Args:
            context    : Pipeline context
            result     : SDBLFlag results
            plots_list : List of plots
            filename   : output filename
            allflags   : If true, feeds all flagging statistics to the selector
            ownflag    : If true, feeds only the corresponding flag statistics to the selector
            type       : If true, feeds the type information to the selector
        Returns:
            filename of the html file
        """
        wrappers = []
        for plot in plots_list:
            wrapper = logger.Plot( plot['FigFileDir']+plot['plot'],
                                   x_axis="Xaxis",
                                   y_axis="Yaxis",
                                   field=plot['field'],
                                   parameters = {
                                       'vis'                  : plot['vis'],
                                       'type'                 : plot['type'],
                                       'spw'                  : plot['spw'],
                                       'ant'                  : plot['ant'],
                                       'field'                : plot['field'],
                                       'pol'                  : plot['pol'],
                                       'outlier_Tsys'         : plot['outlier_Tsys'],
                                       'rms_prefit'           : plot['rms_prefit'],
                                       'rms_postfit'          : plot['rms_postfit'],
                                       'runmean_prefit'       : plot['runmean_prefit'],
                                       'runmean_postfit'      : plot['runmean_postfit'],
                                       'expected_rms_prefit'  : plot['expected_rms_prefit'],
                                       'expected_rms_postfit' : plot['expected_rms_postfit'],
                                       'ownflag'              : plot['ownflag']
                                   } )
            wrappers.append( wrapper )

        renderer = SDBLFlagStatisticsPlotRenderer( uri, context, result, wrappers,
                                                   filename, title,
                                                   allflags=allflags, ownflag=ownflag, types=types )
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())

        return os.path.basename(renderer.path)


class SDBLFlagStatisticsPlotRenderer( basetemplates.JsonPlotRenderer ):
    """Renderer class for Flag Statistics."""
    def __init__( self, uri:str, context:Context, result:SDBLFlagResults, plots:List[Plot],
                  filename:str, title:str, 
                  allflags:Optional[bool]=False, ownflag:Optional[bool]=False, types:Optional[bool]=True ):
        """
        Construct SDBLFlagStatisticsPlotRenderer instance.

        Args:
            uri       : Mako template to use
            context   : Pipeline Context
            result    : SDBLFlagResults
            plots     : List of plot objects
            title     : Title string for the subpage
            allflags  : If true, feeds all flagging statistics to the selector
            ownflag   : If true, feeds only the corresponding flag statistics to the selector
            types     : If true, feeds the type information to the selector
        Returns:
            (none)
        """
        LOG.info( "###@@ SDBLFlagStatisticsPlotRenderer.__init__()" )
        self.allflags = allflags
        self.ownflag  = ownflag
        self.types    = types

        outfile = filenamer.sanitize( filename.replace( " ", "_") )
        super(SDBLFlagStatisticsPlotRenderer, self ).__init__( uri, context, result, plots, title, outfile )

    def update_json_dict( self, d:Dict, plot:Plot ):
        """
        Update json dict to add new filters.

        Args:
            d    : Json dict for plot
            plot : plot object
        Returns:
            (none)
        """
        if self.ownflag:
            d['ownflag']              = plot.parameters['ownflag']['frac']
        if self.types:
            d['type']                 = plot.parameters['type']
        if self.allflags:
            d['outlier_Tsys']         = plot.parameters['outlier_Tsys']['frac']
            d['rms_prefit']           = plot.parameters['rms_prefit']['frac']
            d['rms_postfit']          = plot.parameters['rms_postfit']['frac']
            d['runmean_prefit']       = plot.parameters['runmean_prefit']['frac']
            d['runmean_postfit']      = plot.parameters['runmean_postfit']['frac']
            d['expected_rms_prefit']  = plot.parameters['expected_rms_prefit']['frac']
            d['expected_rms_postfit'] = plot.parameters['expected_rms_postfit']['frac']


def accumulate_flag_per_eb( context:Context, results:SDBLFlagResults ) -> Dict:
    """
    Accumulate flag per field, spw from the output of flagdata to a dictionary.

    Args:
        context: pipeline context
        results: SDBLFlag Results
    Returns:
        accum_flag: dictionary of accumulated flags
    Raises:
        RuntimeError: if FlagSummary data does not exist for a specific ms.name
    """
    accum_flag = collections.OrderedDict()
    for r in results:
        vis = r.inputs['vis']
        ms = context.observing_run.get_ms(vis)
        accum_flag.setdefault(ms.name, {})

        summaries = r.outcome['summary']
        nrow_tot = 0

        # acquire the keys and set the dictionary
        accum_flag.setdefault(ms.name, {})
        for key in summaries[0]['nflags'].keys():
            accum_flag[ms.name][key] = 0

        # sum up the flag countes
        for summary in summaries:
            nflags = summary['nflags']
            nrow = summary['nrow']
            nrow_tot = nrow_tot + nrow
            for key in nflags.keys():
                accum_flag[ms.name][key] = accum_flag[ms.name][key] + nflags[key]
        accum_flag[ms.name]['total'] = nrow_tot

        # pack flagdata outputs
        accum_flag[ms.name]['flagdata_before'] = 0
        accum_flag[ms.name]['flagdata_after']  = 0
        accum_flag[ms.name]['flagdata_total']  = 0
        before, after = r.outcome['flagdata_summary']
        for fieldobj in ms.get_fields(intent='TARGET'):
            field_candidates = filter(lambda x: x in after,
                                      set([fieldobj.name, fieldobj.name.strip('"'), fieldobj.clean_name]))
            try:
                field = next(field_candidates)
            except StopIteration:
                raise RuntimeError('No flag summary for field "{}"'.format(fieldobj.name))
            accum_flag[ms.name]['flagdata_before'] += before[field]['flagged']
            accum_flag[ms.name]['flagdata_after']  += after[field]['flagged']
            accum_flag[ms.name]['flagdata_total']  += after[field]['total']

    return accum_flag


def make_summary_table_per_eb( accum_flag:Dict, subpages:Dict, subpages2:Dict ) -> Tuple[List[str], List[Dict]]:
    """
    Make summary table data fpr flagsummary per EB.

    Inputs:
        accum_flag : dictionary of acumulated flags
        subpages   : filename of the subpage with histogram selector
        submages2  : filename of the subpage without histogram selctor
    Returns:
        lines for per EB summary table,
        List of dictionaries of subplot info
    """
    FlagSummaryEB_TR = collections.namedtuple(
        'FlagSummaryEB',
        'ms baseline_rms_post baseline_rms_pre running_mean_post running_mean_pre expected_rms_post expected_rms_pre outlier_tsys frac_before frac_additional frac_total' )

    rows = []
    statistics_subpages = []
    for ms_name in accum_flag.keys():
        row_total = accum_flag[ms_name]['total']
        frac_before = accum_flag[ms_name]['flagdata_before']*10.00/accum_flag[ms_name]['flagdata_total']
        frac_after  = accum_flag[ms_name]['flagdata_after']*100.0/accum_flag[ms_name]['flagdata_total']
        tr = FlagSummaryEB_TR( ms_name, 
                               '{:.3f} %'.format(accum_flag[ms_name]['RmsPostFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['RmsPreFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['RunMeanPostFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['RunMeanPreFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['RmsExpectedPostFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['RmsExpectedPreFitFlag']*100.0/row_total), 
                               '{:.3f} %'.format(accum_flag[ms_name]['TsysFlag']*100.0/row_total), 
                               '{:.3f} %'.format( frac_before ),
                               '{:.3f} %'.format( frac_after - frac_before ),
                               '{:.3f} %'.format( frac_after ) )
        rows.append(tr)
        statistics_subpages.append( {'vis': ms_name,
                                     'html' : subpages[ms_name],
                                     'html2' : subpages2[ms_name] } )

    return utils.merge_td_columns(rows, num_to_merge=0), statistics_subpages


def accumulate_flag_per_source_spw(context, results):
    # Accumulate flag per field, spw from the output of flagdata to a dictionary
    # accum_flag[field][spw] = {'additional': # of flagged in task,
    #                           'total': # of total samples,
    #                           'before': # of flagged before task,
    #                           'after': total # of flagged}
    accum_flag = {}
    for r in results:
        vis = r.inputs['vis']
        ms = context.observing_run.get_ms(vis)
        before, after = r.outcome['flagdata_summary']
        assert before['name'] == 'before' and after['name'] == 'after', "Got unexpected flag summary"
        for fieldobj in ms.get_fields(intent='TARGET'):
            field_candidates = filter(lambda x: x in after,
                                      set([fieldobj.name, fieldobj.name.strip('"'), fieldobj.clean_name]))
            try:
                field = next(field_candidates)
            except StopIteration:
                raise RuntimeError('No flag summary for field "{}"'.format(fieldobj.name))
            accum_flag.setdefault(field, {})
            fieldflag = after[field]
            spwflag = fieldflag['spw']
            for spw, flagval in spwflag.items():
                vspw = context.observing_run.real2virtual_spw_id(spw, ms)
                accum_flag[field].setdefault(vspw, dict(before=0, additional=0, after=0, total=0))
                # sum up incremental flags
                accum_flag[field][vspw]['before'] += before[field]['spw'][spw]['flagged']
                accum_flag[field][vspw]['after'] += flagval['flagged']
                accum_flag[field][vspw]['total'] += flagval['total']
                accum_flag[field][vspw]['additional'] += (flagval['flagged']-before[field]['spw'][spw]['flagged'])
    return accum_flag


def make_summary_table_per_field(flagdict):
    # will hold all the flag summary table rows for the results
    FlagSummaryField_TR = collections.namedtuple('FlagSummaryField', 'field spw before additional total')
    rows = []
    for field, flagperspw in flagdict.items():
        for spw, flagval in flagperspw.items():
            frac_before = flagval['before']/flagval['total']
            frac_total = flagval['after']/flagval['total']
            frac_additional = (flagval['after']-flagval['before'])/flagval['total']
            tr = FlagSummaryField_TR(field, spw, '%0.3f%%' % (frac_before*100), '%0.3f%%' % (frac_additional*100),
                               '%0.3f%%' % (frac_total*100))
            rows.append(tr)

    return utils.merge_td_columns(rows, num_to_merge=2)
