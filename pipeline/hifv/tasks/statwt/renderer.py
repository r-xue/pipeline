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
            print("Results: {}".format(result))
#            if result.inputs['statwtmode'] == 'VLASS-SE': #My first pass on this is to literally run without the if and see what a "standard vla run looks like"
            plotter = statwtdisplay.weightboxChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

            is_same = True
            after_by_ant = None
            try:
                print("First time")
                weight_stats = plotter.result.weight_stats
                print("Weight stats:")
                print(weight_stats)
                print("Second time")
                before_by_ant = weight_stats['before']['per_ant']
                print("Third time")
                after_by_ant = weight_stats['after']['per_ant']
                print("Fourth time")
                for idx in range(before_by_ant):
                    is_same &= before_by_ant[idx]['mean'] == after_by_ant[idx]['mean']
                    is_same &= before_by_ant[idx]['med'] == after_by_ant[idx]['med']
                    is_same &= before_by_ant[idx]['stdev'] == after_by_ant[idx]['stdev']
            except:
                is_same = False

            if is_same:
                raise exceptions.PipelineException("Statwt failed to recalculate the weights, cannot continue.")

            # else: 
            #     plotter = statwtdisplay.weightboxChart(context, result)
            #     plots = plotter.plot()
            #     ms = os.path.basename(result.inputs['vis'])
            #     summary_plots[ms] = plots

            #     is_same = True
            #     try:
            #         weight_stats = plotter.result.weight_stats
            #         before_by_ant = weight_stats['before']['per_ant']
            #         after_by_ant = weight_stats['after']['per_ant']

            #         for idx in range(before_by_ant):
            #             is_same &= before_by_ant[idx]['mean'] == after_by_ant[idx]['mean']
            #             is_same &= before_by_ant[idx]['med'] == after_by_ant[idx]['med']
            #             is_same &= before_by_ant[idx]['stdev'] == after_by_ant[idx]['stdev']

# Do I actually need to do any of these calculations just to make sure that they change in the before and after? In per-spw or per-scan ways? 

                    #  # by spw
                    # before_by_spw = weight_stats['before']['per_spw']
                    # after_by_spw = weight_stats['after']['per_spw']

                    # # by scan 
                    # before_by_scan = weight_stats['before']['per_spw'] #HERE needs to be updated to 'per_scan' after it is added to the display code
                    # after_by_scan = weight_stats['after']['per_spw']

                # except:
                #     is_same = False

                # if is_same:
                #     raise exceptions.PipelineException("Statwt failed to recalculate the weights, cannot continue.")


        ctx.update({'summary_plots': summary_plots,
                    'plotter': plotter,
                    'dirname': weblog_dir, 
                    'after_by_ant': after_by_ant})

        return ctx
