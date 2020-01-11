"""
Created on Nov 9, 2016

@author: kana
"""
import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from ..common import utils as sdutils

LOG = logging.get_logger(__name__)


class T2_4MDetailsBLFlagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    The renderer class for baselineflag.
    """
    def __init__(self, uri='hsd_blflag.mako',
                 description='Flag data by Tsys, weather, and statistics of spectra',
                 always_rerender=False):
        """
        Constructor
        """
        super(T2_4MDetailsBLFlagRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, result):
        accum_flag = accumulate_flag_per_source_spw(context, result)
        table_rows = make_summary_table(accum_flag)
        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)

        ctx.update({'sumary_table_rows': table_rows,
                    'dovirtual': dovirtual})


FlagSummaryTR = collections.namedtuple('FlagSummaryTR', 'field spw before additional total')


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


def make_summary_table(flagdict):
    # will hold all the flag summary table rows for the results
    rows = []
    for field, flagperspw in flagdict.items():
        for spw, flagval in flagperspw.items():
            frac_before = flagval['before']/flagval['total']
            frac_total = flagval['after']/flagval['total']
            frac_additional = (flagval['after']-flagval['before'])/flagval['total']

            tr = FlagSummaryTR(field, spw, '%0.1f%%' % (frac_before*100), '%0.1f%%' % (frac_additional*100),
                               '%0.1f%%' % (frac_total*100))
            rows.append(tr)

    return utils.merge_td_columns(rows, num_to_merge=2)
