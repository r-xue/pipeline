
import os

from . import display as analyzestokescube
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

LOG = logging.get_logger(__name__)


class T2_4MDetailsAnalyzestokesCubeRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlasscube_makermsimages.mako',
                 description='Produce rms images',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        # Get results info
        for r in results:

            # Make the plots for Stokes and RMS stats
            stokesplots = {}
            stokesplots['Stokes Summary Plots'] = analyzestokescube.VlassCubeStokesSummary(context, r).plot()
            rmsplots = {}
            rmsplots['Rms Summary Plot'] = [analyzestokescube.VlassCubeRmsSummary(context, r).plot()]

        ctx.update({'rmsplots': rmsplots,
                    'stokesplots': stokesplots,
                    'dirname': weblog_dir})
