import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

from . import display

LOG = logging.get_logger(__name__)


class T2_4MDetailsVlassmaskingRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlassmasking.mako',
                 description='Produce a VLASS Mask',
                 always_rerender=False):
        super(T2_4MDetailsVlassmaskingRenderer, self).__init__(uri=uri,
                                                               description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        summary_plots = {}
        # PIPE-1945: even hifv_vlassmasking is converted into a multi-vis task, i.e., its task return is a Result
        # instead of ResultList Object, htmlrenderer.py/T2_4MDetailsRender.render() always tries to wrap
        # the task result into a list. Here again, we need to pick the first element in the list.
        result = results[0]

        plotter = display.MaskSummary(context, result)
        plots = plotter.plot()
        mslist_str = '<br>'.join([os.path.basename(vis) for vis in result.inputs['vis']])
        summary_plots[mslist_str] = plots

        # Number of islands found
        # try:
        #     filelist = glob('*_iter1b.image.smooth5.cat.ds9.reg')
        #     found = subprocess.Popen(['wc', '-l', filelist[0]], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        #     stdout, stderr = found.communicate()
        #     numfound = str(int(stdout.split()[0]) - 3)   # minus three to remove region header
        # except Exception as e:
        #     numfound = ""

        ctx.update({'summary_plots': summary_plots,
                    'dirname': weblog_dir})

        return ctx
