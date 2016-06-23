from __future__ import absolute_import

import os
import time
import numpy
import pylab as pl

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from .utils import RADEClabel, RArotation, DECrotation
from .utils import sd_polmap as polmap
from .common import DPISummary, SDImageDisplay, ShowPlot, draw_beam
from . import tpimage

LOG = infrastructure.get_logger(__name__)

NoData = -32767.0
NoDataThreshold = NoData + 10000.0

class RmsMapAxesManager(tpimage.ChannelAveragedAxesManager):
    @property
    def axes_rmsmap(self):
        return self.axes_tpmap


class SDRmsMapDisplay(SDImageDisplay):
    #MATPLOTLIB_FIGURE_ID = 8910

    def plot(self):
        self.init()

        t1 = time.time()
        plot_list = self.__plot_channel_map()
        t2 = time.time()
        LOG.debug('__plot_channel_map: elapsed time %s sec'%(t2-t1))

        return plot_list

    def __get_rms(self):
        # reshape rms to a 3d array in shape, (nx_im, ny_im, npol_data)
        return self.__reshape_grid_table_values(self.inputs.result.outcome['rms'], float)

    def __get_num_valid(self):
        # reshape validsp to a 3d array in shape, (nx_im, ny_im, npol_data)
        return self.__reshape_grid_table_values(self.inputs.result.outcome['validsp'], int)

    def __reshape_grid_table_values(self, array2d, dtype=None):
        # reshape 2d array in shape, (npol, nx*ny), to (nx, ny, npol)
        npol_data = len(array2d)
        # retruned value will be transposed
        array3d = numpy.zeros((npol_data, self.ny, self.nx),dtype=dtype)
        for pol in xrange(npol_data):
            if len(array2d[pol]) == self.nx*self.ny:
                array3d[pol,:,:] = numpy.array(array2d[pol]).reshape((self.ny,self.nx))
        return numpy.flipud(array3d.transpose())
        

    
    def __plot_channel_map(self):
        pl.clf()

        colormap = 'color'
        plot_list = []

        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_min + self.dec_max) / 180.0 * 3.141592653)

        # Draw RMS Map
        TickSize = 6

        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0, self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = RADEClabel(span)

        axes_manager = RmsMapAxesManager(RAformatter, DECformatter,
                                         RAlocator, DEClocator,
                                         RArotation, DECrotation,
                                         TickSize, colormap)
        rms_axes = axes_manager.axes_rmsmap
        rms_colorbar = None
        beam_circle = None

        rms = self.__get_rms()
        nvalid = self.__get_num_valid()
        npol_data = rms.shape[2]
#         for pol in xrange(self.npol):
        for pol in xrange(npol_data):
            rms_map = rms[:,:,pol] * (nvalid[:,:,pol] > 0)
            rms_map = numpy.flipud(rms_map.transpose())
            #LOG.debug('rms_map=%s'%(rms_map))
            # 2008/9/20 DEC Effect
            image = pl.imshow(rms_map, interpolation='nearest', aspect=self.aspect, extent=Extent)
            xlim = rms_axes.get_xlim()
            ylim = rms_axes.get_ylim()
            
            # colorbar
            rmsmin = rms_map.min()
            rmsmax = rms_map.max()
            if not (rmsmin == rmsmax): 
                if not ((self.y_max == self.y_min) and (self.x_max == self.x_min)):
                    if rms_colorbar is None:
                        rms_colorbar = pl.colorbar(shrink=0.8)
                        for t in rms_colorbar.ax.get_yticklabels():
                            newfontsize = t.get_fontsize()*0.5
                            t.set_fontsize(newfontsize)
#                         rms_colorbar.ax.set_title('[%s]' % self.brightnessunit)
#                         lab = rms_colorbar.ax.title
                        rms_colorbar.ax.set_ylabel('[%s]' % self.brightnessunit)
                    else:
                        rms_colorbar.set_clim((rmsmin,rmsmax))
                        rms_colorbar.draw_all()

            # draw beam pattern
            if beam_circle is None:
                beam_circle = draw_beam(rms_axes, self.beam_radius, self.aspect, self.ra_min, self.dec_min)

            rms_axes.set_xlim(xlim)
            rms_axes.set_ylim(ylim)

            if ShowPlot: pl.draw()
            FigFileRoot = self.inputs.imagename + '.pol%s'%(pol)
            plotfile = os.path.join(self.stage_dir, FigFileRoot+'_rmsmap.png')
            pl.savefig(plotfile, format='png', dpi=DPISummary)

            image.remove()

            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.spw
            parameters['pol'] = polmap[pol]
            parameters['ant'] = self.antenna
            parameters['file'] = self.inputs.imagename
            parameters['type'] = 'rms_map'

            plot2 = logger.Plot(plotfile,
                                x_axis='R.A.',
                                y_axis='Dec.',
                                field=self.inputs.source,
                                parameters=parameters)
            plot_list.append(plot2)

        return plot_list
        
