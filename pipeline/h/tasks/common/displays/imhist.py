import os

import matplotlib.pyplot as plt
import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class ImageHistDisplay(object):
    """
    A display class to generate histogram of a CASA image
    """

    def __init__(self, context, imagename, reportdir='./', boxsize=None):
        self.context = context
        self.imagename = imagename
        self.reportdir = reportdir
        self.boxsize = boxsize
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing image histogram plot')
            return self._get_plot_object()

        LOG.debug('Creating new image histogram plot')
        try:
            subim = self._get_image_chunk(self.imagename, boxsize=self.boxsize)
            nchanpol = subim.shape[2]*subim.shape[3]
            fig, ax = plt.subplots()
            ax.hist(subim.ravel(), bins=10,
                    histtype='barstacked', align='mid', label='')
            ax.set_xlabel('Primary Beam Response')
            ax.set_ylabel('Num. of Pixel')
            with casa_tools.ImageReader(self.imagename) as myia:
                im_csys = myia.coordsys()
                spaxelarea = np.prod(np.degrees(np.abs(im_csys.increment()['numeric'][0:2])))

            ax_secy = ax.secondary_yaxis('right', functions=(lambda npix: npix/nchanpol*spaxelarea,
                                                             lambda area: area/spaxelarea*nchanpol))
            ax_secy.set_ylabel('Area [deg$^2$]')
            if self.boxsize is None:
                title = 'Histogram (entire image/cube)'
            else:
                title = 'Histogram (inner {:.2f} deg$^2$)'.format(self.boxsize)
            ax.set_title(title)
            LOG.debug('Saving new image histogram plot to {}'.format(self.figfile))
            fig.tight_layout()
            fig.savefig(self.figfile)
            plt.close(fig)
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
                           parameters={'imagename': self.imagename,
                                       'boxsize': self.boxsize})

    def _get_image_chunk(self, imagename, boxsize=None):
        """Return the pixel values from the image file with a box centered at the image reference point.

        imagename
            input casa image name
        boxsize
            box size in degree
        Returns:
            pixel values
        """

        with casa_tools.ImageReader(imagename) as myia:
            if boxsize is None:
                im_val = myia.getchunk()
            else:
                im_csys = myia.coordsys()
                ref_pix = im_csys.referencepixel()['numeric'][0:2]
                box_pix = np.radians(boxsize)/np.abs(im_csys.increment()['numeric'][0:2])
                blc = ref_pix-box_pix/2
                trc = ref_pix+box_pix/2

                # ia.getchunk can take care of out-region query.
                im_val = myia.getchunk(blc, trc)

        return im_val
