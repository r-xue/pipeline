"""
Created on Nov 9, 2016

@author: kana
"""
import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from ..common import utils as sdutils

from typing import Dict, List
from pipeline.infrastructure import Context
from pipeline.infrastructure.basetask import ResultsList
from .baselineflag import SDBLFlagResults

LOG = logging.get_logger(__name__)


class T2_4MDetailsBLFlagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    The renderer class for baselineflag.
    """
    def __init__(self, uri='hsd_blflag.mako',
                 description='Flag data by Tsys and statistics of spectra',
                 always_rerender=False):
        """
        Constructor
        """
        super(T2_4MDetailsBLFlagRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, result):
        # per EB table
        accum_flag_eb = accumulate_flag_per_eb( context, result )
        table_rows_eb = make_summary_table_per_eb( accum_flag_eb )
        ctx.update({'per_eb_summary_table_rows': table_rows_eb})

        # per field, spw table
        accum_flag_field = accumulate_flag_per_source_spw(context, result)
        table_rows_field = make_summary_table_per_field(accum_flag_field)
        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        ctx.update({'per_field_summary_table_rows': table_rows_field,
                    'dovirtual': dovirtual})


def accumulate_flag_per_eb( context:Context, results:SDBLFlagResults ) -> Dict:
    """
    Accumulate flag per field, spw from the output of flagdata to a dictionary
    
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

        # pack detail plot info
        accum_flag[ms.name]['details'] = ""

    return accum_flag

def make_summary_table_per_eb( accum_flag : Dict ) -> str:
    """
    make summary table data fpr flagsummary per EB
    
    Inputs:
        accum_flag : dictionary of acumulated flags
    Returns:
        lines for per EB summary table
    """
    FlagSummaryEB_TR = collections.namedtuple( 
        'FlagSummaryEB', 
        'ms baseline_rms_post baseline_rms_pre running_mean_post running_mean_pre expected_rms_post expected_rms_pre outlier_tsys frac_before frac_additional frac_total details' )

    rows = []
    for ms_name in accum_flag.keys():
        row_total = accum_flag[ms_name]['total']
        frac_before = accum_flag[ms_name]['flagdata_before']*100.0/accum_flag[ms_name]['flagdata_total']
        frac_after  = accum_flag[ms_name]['flagdata_after']*100.0/accum_flag[ms_name]['flagdata_total'] 
        tr = FlagSummaryEB_TR( ms_name, 
                               '{:.1f} %'.format(accum_flag[ms_name]['RmsPostFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['RmsPreFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['RunMeanPostFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['RunMeanPreFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['RmsExpectedPostFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['RmsExpectedPreFitFlag']*100.0/row_total), 
                               '{:.1f} %'.format(accum_flag[ms_name]['TsysFlag']*100.0/row_total), 
                               '{:.1f} %'.format( frac_before ),
                               '{:.1f} %'.format( frac_after - frac_before ),
                               '{:.1f} %'.format( frac_after ),
                               '{}'.format(accum_flag[ms_name]['details']) )
        
        rows.append(tr)
    
    return utils.merge_td_columns(rows, num_to_merge=0)


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
            tr = FlagSummaryField_TR(field, spw, '%0.1f%%' % (frac_before*100), '%0.1f%%' % (frac_additional*100),
                               '%0.1f%%' % (frac_total*100))
            rows.append(tr)

    return utils.merge_td_columns(rows, num_to_merge=2)
