import os

from . import display
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates


LOG = logging.get_logger(__name__)


class T2_4MDetailsVlassmaskingRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlassmasking.mako',
                 description='Produce a VLASS Mask',
                 always_rerender=False):
        super(T2_4MDetailsVlassmaskingRenderer, self).__init__(uri=uri,
                                                               description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsVlassmaskingRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        summary_plots = {}

        for result in results:
            plotter = display.MaskSummary(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

        # Number of islands found
        '''
        try:
            filelist = glob('*_iter1b.image.smooth5.cat.ds9.reg')
            found = subprocess.Popen(['wc', '-l', filelist[0]], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = found.communicate()
            numfound = str(int(stdout.split()[0]) - 3)   # minus three to remove region header
        except Exception as e:
            numfound = ""
        '''

        ctx.update({'summary_plots': summary_plots,
                    'dirname': weblog_dir})

        return ctx
