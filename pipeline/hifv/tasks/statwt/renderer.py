import collections
import os

import pipeline.infrastructure.exceptions as exceptions
import pipeline.infrastructure.renderer.basetemplates as basetemplates

from . import display as statwtdisplay

class T2_4MDetailsstatwtRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='statwt.mako', description='Statwt summary',
                 always_rerender=False):
        super(T2_4MDetailsstatwtRenderer, self).__init__(uri=uri, description=description,
                                                         always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsstatwtRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        summary_plots = {}
        plotter = None

        for result in results:
            plotter = statwtdisplay.weightboxChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            # Add per-band into here, too.

            summary_plots[ms] = {}
            bands = plotter.band2spw.keys()
            for band in bands: 
                summary_plots[ms][band] = collections.defaultdict(list)

            for plot in plots: 
                print(plot)
                band = plot.parameters.band
                summary_plots[ms][band].append(plot)

            #stats_table_rows = make_stats_table(context, plotter.result.weight_stats)

            if result.inputs['statwtmode'] == 'VLASS-SE':
                is_same = True
                try:
                    weight_stats = plotter.result.weight_stats
                    before_by_ant = weight_stats['before']['per_ant']
                    after_by_ant = weight_stats['after']['per_ant']

                    for idx in range(before_by_ant):
                        is_same &= before_by_ant[idx]['mean'] == after_by_ant[idx]['mean']
                        is_same &= before_by_ant[idx]['med'] == after_by_ant[idx]['med']
                        is_same &= before_by_ant[idx]['stdev'] == after_by_ant[idx]['stdev']
                except:
                    is_same = False

                if is_same:
                    raise exceptions.PipelineException("Statwt failed to recalculate the weights, cannot continue.")

        ctx.update({'summary_plots': summary_plots,
                    'plotter': plotter,
                    'dirname': weblog_dir,
#                   'stats_table_rows': stats_table_rows,
                    'band2spw': plotter.band2spw})

        return ctx

# StatsTR = collections.namedtuple('StatsTR', 'median q1 q2 mean std min max')
# StatsVlassTR = collections.namedtuple('StatsVlassTR', 'median quartiles mean_std')

# consider input: before/after, 'per_spw', etc later
# first, make it work for one table, assuming we've basically input 
# also consider later... multiple results
# call make_stats_table() [per_spw, per_ant, per_scan], [before, after], per band
# this is going to get a lot of calls. How to index result
# stats_table[before/after][band][per_spw, etc]? 
#
# def make_stats_table(context, weight_stats):
#     after_by_spw=weight_stats['after'][band]['per_spw']
#     summary_stats = summarize_stats(after_by_spw) 
#     # will hold all the flux stat table rows for the results
#     rows = []
#     % for i in range(len(after_by_spw)):
#          median = format_weight(summary['med'], after_by_spw[i]['med'])
#          q1 = after_by_spw[i]['q1']
#          q3 = after_by_spw[i]['q3']
#          mean = after_by_spw[i]['mean']
#          std = after_by_spw[i]['std']
#          min = after_by_spw[i]['min']
#          max = after_by_spw[i]['max']
#          tr = StatsTR(median, q1, q3, mean, std, min, max)
#          rows.append(tr)
#       
#     return utils.merge_td_columns(rows) <-- don't actually do this