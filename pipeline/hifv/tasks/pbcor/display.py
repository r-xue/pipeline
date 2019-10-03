import collections
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools

from pipeline.h.tasks.common.displays import sky as sky

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class PbcorimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        # self.image_stats = image_stats

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG pbcor images for weblog")
        plot_wrappers = []

        for pbcorimagename in self.result.pbcorimagenames:
            if 'residual.pbcor' in pbcorimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, pbcorimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                with casatools.ImageReader(pbcorimagename) as image:
                    self.result.residual_stats = image.statistics(robust=True)
            elif 'image.pbcor' in pbcorimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, pbcorimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                with casatools.ImageReader(pbcorimagename) as image:
                    self.result.pbcor_stats = image.statistics(robust=True)
            else:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, pbcorimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))

        return [p for p in plot_wrappers if p is not None]
