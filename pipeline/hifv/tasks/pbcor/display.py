import collections
import os

import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import sky as sky
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

                plot_wrappers.append(sky.SkyDisplay().plot(self.context, pbcor_imagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if 'residual.pbcor' in pbcor_imagename:
                    with casa_tools.ImageReader(pbcor_imagename) as image:
                        self.result.residual_stats[basename] = image.statistics(robust=True)
                elif 'image.pbcor' in pbcor_imagename:
                    with casa_tools.ImageReader(pbcor_imagename) as image:
                        self.result.pbcor_stats[basename] = image.statistics(robust=True)
                else:
                    plot_wrappers.append(ImageHistDisplay(self.context, pbcor_imagename, reportdir=stage_dir).plot())
            plot_dict[basename] = [p for p in plot_wrappers if p is not None]

        return plot_dict


class ImageHistDisplay(object):
    """
    A display class to generate histogram of a CASA image
    # to do: 
    #   make region / chanel selections work
    #   plot stats-ROI-boundary back in the "sky" plot
    """

    def __init__(self, context, imagename, reportdir='./', region='', box='-1,-1', chans=''):
        self.context = context
        self.imagename = imagename
        self.reportdir = reportdir
        self.region = region
        self.box = box
        self.chans = chans
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing image histogram plot')
            return self._get_plot_object()

        LOG.debug('Creating new image histogram plot')
        try:
            with casa_tools.ImageReader(self.imagename) as myia:
                im_val = myia.getchunk()
            fig, ax = plt.subplots()
            ax.hist(im_val.ravel(), bins=20,
                    histtype='barstacked', align='mid', label='')
            ax.set_xlabel('Primary Beam Reponse')
            ax.set_ylabel('Num. of Pixel')
            ax.set_title('PB histogram')
            LOG.debug('Saving new image histogram plot to {}'.format(self.figfile))
            fig.savefig(self.figfile)
        except:
            return None

        return self._get_plot_object()

    def _get_figfile(self):

        return os.path.join(self.reportdir,
                            self.imagename+'.hist.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='Pixel Value',
                           y_axis='Histogram',
                           parameters={'placeholder': 'placeholder'})
