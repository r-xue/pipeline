import collections
import os

import numpy as np
import pipeline.infrastructure as infrastructure
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.h.tasks.common.displays.imhist import ImageHistDisplay
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class CutoutimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG cutout images for weblog")
        plot_wrappers = []

        for subimagename in self.result.subimagenames:
            if '.psf.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-0.1, vmax=0.3))
            elif '.image.' in subimagename and '.pbcor' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.image_stats = image.statistics(robust=True)

            elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.residual_stats = image.statistics(robust=True)

            elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                # CAS-10345/PIPE-1189: use (-5*RMSmedian, 20*RMSmedian) as the colormap scaling range
                # We expect 'subimagename' here to be: X.image.pbcor.ttX.subim or X.image.pbcor.subim
                rms_subimagename = os.path.splitext(subimagename)[0]+'.rms.subim'
                with casa_tools.ImageReader(rms_subimagename) as image:
                    rms_median = image.statistics(robust=True).get('median')[0]
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-5 * rms_median,
                                                           vmax=20 * rms_median))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_stats = image.statistics(robust=True)

            elif '.rms.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.rms_stats = image.statistics(robust=True)
                        arr = image.getchunk()
                        # PIPE-489 changed denominators to unmasked (non-zero) pixels
                        # get fraction of pixels <= 120 micro Jy VLASS technical goal.  ignore 0 (masked) values.
                        self.result.RMSfraction120 = (np.count_nonzero((arr != 0) & (arr <= 120e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 168 micro Jy VLASS SE goal.  ignore 0 (masked) values.
                        self.result.RMSfraction168 = (np.count_nonzero((arr != 0) & (arr <= 168e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 200 micro Jy VLASS technical requirement.  ignore 0 (masked) values.
                        self.result.RMSfraction200 = (np.count_nonzero((arr != 0) & (arr <= 200e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # PIPE-642: include the number and percentage of masked pixels in weblog
                        self.result.n_masked = np.count_nonzero(arr == 0)
                        self.result.pct_masked = (np.count_nonzero(arr == 0) / float(arr.size)) * 100

            elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_residual_stats = image.statistics(robust=True)

            elif '.pb.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean', vmin=0.2, vmax=1.))
                plot_wrappers.append(ImageHistDisplay(self.context, subimagename,
                                                      x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                                      reportdir=stage_dir).plot())
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pb_stats = image.statistics(robust=True)
            else:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))

        return [p for p in plot_wrappers if p is not None]
