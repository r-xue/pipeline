import collections
import os

import matplotlib.pyplot as plt
import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.h.tasks.common.displays.imhist import ImageHistDisplay
from pipeline.infrastructure import casa_tools

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
        plot_dict = {}
        self.result.residual_stats = {}
        self.result.pbcor_stats = {}

        for basename, pbcor_images in self.result.pbcorimagenames.items():
            plot_wrappers = []
            for pbcor_imagename in pbcor_images:

                if pbcor_imagename.endswith('.pb') or pbcor_imagename.endswith('.pb.tt0'):
                    vmin = 0.0
                    vmax = 1.0
                else:
                    vmin = vmax = None
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, pbcor_imagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean', vmin=vmin, vmax=vmax))

                if 'residual.pbcor' in pbcor_imagename:
                    with casa_tools.ImageReader(pbcor_imagename) as image:
                        self.result.residual_stats[basename] = image.statistics(robust=True)
                elif 'image.pbcor' in pbcor_imagename:
                    with casa_tools.ImageReader(pbcor_imagename) as image:
                        self.result.pbcor_stats[basename] = image.statistics(robust=True)
                else:
                    plot_wrappers.append(ImageHistDisplay(self.context, pbcor_imagename,
                                                          x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                                          reportdir=stage_dir, boxsize=1.0).plot())
            plot_dict[basename] = [p for p in plot_wrappers if p is not None]

        return plot_dict

