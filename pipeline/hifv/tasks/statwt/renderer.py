
import os

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

        for result in results:
            plotter = statwtdisplay.weightboxChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

        ctx.update({'summary_plots': summary_plots,
                    'plotter': plotter,
                    'dirname': weblog_dir})

        return ctx
