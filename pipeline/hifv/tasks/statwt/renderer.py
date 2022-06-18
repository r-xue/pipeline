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
            summary_plots[ms] = plots

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
                    'dirname': weblog_dir})

        return ctx
