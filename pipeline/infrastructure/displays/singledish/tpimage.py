from __future__ import absolute_import

import os
import numpy
import pylab as pl

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure.jobrequest import casa_tasks
import pipeline.infrastructure.renderer.logger as logger
from .utils import RADEClabel, RArotation, DECrotation
#from .utils import sd_polmap as polmap
from .common import DPIDetail, SDImageDisplay, SDImageDisplayInputs, ShowPlot, draw_beam

LOG = infrastructure.get_logger(__name__)

class ChannelAveragedAxesManager(object):
    def __init__(self, xformatter, yformatter, xlocator, ylocator, xrotation, yrotation, ticksize, colormap):
        self.xformatter = xformatter
        self.yformatter = yformatter
        self.xlocator = xlocator
        self.ylocator = ylocator
        self.xrotation = xrotation
        self.yrotation = yrotation
        self.isgray = (colormap == 'gray')
        
        self.ticksize = ticksize
        
        self._axes_tpmap = None

    @property
    def axes_tpmap(self):
        if self._axes_tpmap is None:
            axes = pl.axes([0.25,0.25,0.5,0.5])
            axes.xaxis.set_major_formatter(self.xformatter)
            axes.yaxis.set_major_formatter(self.yformatter)
            axes.xaxis.set_major_locator(self.xlocator)
            axes.yaxis.set_major_locator(self.ylocator)
            xlabels = axes.get_xticklabels()
            pl.setp(xlabels, 'rotation', self.xrotation, fontsize=self.ticksize)
            ylabels = axes.get_yticklabels()
            pl.setp(ylabels, 'rotation', self.yrotation, fontsize=self.ticksize)
            pl.title('Baseline RMS Map', size=self.ticksize)
            pl.xlabel('RA', size=self.ticksize)
            pl.ylabel('DEC', size=self.ticksize)

            if self.isgray:
                pl.gray()
            else:
                pl.jet()

            self._axes_tpmap = axes
            
        return self._axes_tpmap

class SDChannelAveragedImageDisplay(SDImageDisplay):
    MATPLOTLIB_FIGURE_ID = 8911
    
    def plot(self):
        self.init()
        
        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0, self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = RADEClabel(span)

        # Plotting
        if ShowPlot: pl.ion()
        else: pl.ioff()
        pl.figure(self.MATPLOTLIB_FIGURE_ID)
        if ShowPlot: pl.ioff()
        pl.clf()
        
        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_max + self.dec_min) / 180. * 3.141592653)

        colormap = 'color'
        TickSize = 6

        axes_manager = ChannelAveragedAxesManager(RAformatter, DECformatter,
                                                  RAlocator, DEClocator,
                                                  RArotation, DECrotation,
                                                  TickSize, colormap)
        axes_tpmap = axes_manager.axes_tpmap
        tpmap_colorbar = None
        beam_circle = None

        pl.gcf().sca(axes_tpmap)

        
        plot_list = []
        
#         masked_data = self.data * self.mask
        for pol in xrange(self.npol):
#             Total = masked_data.take([pol], axis=self.id_stokes).squeeze()
            Total = (self.data.take([pol], axis=self.id_stokes) * self.mask.take([pol], axis=self.id_stokes)).squeeze()
            Total = numpy.flipud(Total.transpose())
            tmin = Total.min()
            tmax = Total.max()

            # 2008/9/20 DEC Effect
            im = pl.imshow(Total, interpolation='nearest', aspect=self.aspect, extent=Extent)
            #im = pl.imshow(Total, interpolation='nearest', aspect='equal', extent=Extent)
            del Total

            xlim = axes_tpmap.get_xlim()
            ylim = axes_tpmap.get_ylim()

            # colorbar
            #print "min=%s, max of Total=%s" % (tmin,tmax)
            if not (tmin == tmax): 
                #if not ((Ymax == Ymin) and (Xmax == Xmin)): 
                #if not all(image_shape[id_direction] <= 1):
                if self.nx > 1 or self.ny > 1:
                    if tpmap_colorbar is None:
                        tpmap_colorbar = pl.colorbar(shrink=0.8)
                        for t in tpmap_colorbar.ax.get_yticklabels():
                            newfontsize = t.get_fontsize()*0.5
                            t.set_fontsize(newfontsize)
#                         #tpmap_colorbar.ax.set_title('[K km/s]')
#                         tpmap_colorbar.ax.set_title('[%s]'%(self.image.brightnessunit))
#                         lab = tpmap_colorbar.ax.title
#                         lab.set_fontsize(newfontsize)
                        tpmap_colorbar.ax.set_ylabel('[%s]'%(self.image.brightnessunit), fontsize=newfontsize)
                    else:
                        tpmap_colorbar.set_clim((tmin,tmax))
                        tpmap_colorbar.draw_all()
                        
            # draw beam pattern
            if beam_circle is None:
                beam_circle = draw_beam(axes_tpmap, 0.5 * self.beam_size, self.aspect, self.ra_min, self.dec_min)

            pl.title('Total Power', size=TickSize)
            axes_tpmap.set_xlim(xlim)
            axes_tpmap.set_ylim(ylim)

            if ShowPlot: pl.draw()
            FigFileRoot = self.inputs.imagename+'.pol%s'%(pol)
            plotfile = os.path.join(self.stage_dir,FigFileRoot+'_TP.png')
            pl.savefig(plotfile, format='png', dpi=DPIDetail)

            im.remove()
            
            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.inputs.spw
            parameters['pol'] = self.image.coordsys.stokes()[pol]#polmap[pol]
            parameters['ant'] = self.inputs.antenna
            #parameters['type'] = 'sd_channel-averaged'
            parameters['type'] = 'sd_integrated_map'
            parameters['file'] = self.inputs.imagename
            if self.inputs.vis is not None:
                parameters['vis'] = self.inputs.vis

            plot = logger.Plot(plotfile,
                               x_axis='R.A.',
                               y_axis='Dec.',
                               field=self.inputs.source,
                               parameters=parameters)
            plot_list.append(plot)

        return plot_list

class SDIntegratedImageDisplayInputs(SDImageDisplayInputs):
    def __init__(self, context, result):
        super(SDIntegratedImageDisplayInputs,self).__init__(context, result)
        # obtain integrated image using immoments task
        print self.imagename
        #job = casa_tasks.immoments(imagename=self.imagename, moments=[0], outfile=self.integrated_imagename)

    @property
    def integrated_imagename(self):
        return self.result.outcome['image'].imagename.rstrip('/') + '.integ'
    
class SDIntegratedImageDisplay(SDImageDisplay):
    MATPLOTLIB_FIGURE_ID = 8911

    def __init__(self, inputs):
        super(self.__class__, self).__init__(inputs)
        if hasattr(self.inputs, 'integrated_imagename'):
            self.imagename = self.inputs.integrated_imagename
        else:
            self.imagename = self.inputs.result.outcome['image'].imagename.rstrip('/') + '.integ'

    def init(self):
        if os.path.exists(self.imagename):
            os.system('rm -rf %s'%(self.imagename))
        job = casa_tasks.immoments(imagename=self.inputs.imagename, moments=[0], outfile=self.imagename)
        job.execute(dry_run=False)
        assert os.path.exists(self.imagename)
        super(self.__class__, self).init()
    
    def plot(self):
        self.init()
        
        Extent = (self.ra_max+self.grid_size/2.0, self.ra_min-self.grid_size/2.0, self.dec_min-self.grid_size/2.0, self.dec_max+self.grid_size/2.0)
        span = max(self.ra_max - self.ra_min + self.grid_size, self.dec_max - self.dec_min + self.grid_size)
        (RAlocator, DEClocator, RAformatter, DECformatter) = RADEClabel(span)

        # Plotting
        if ShowPlot: pl.ion()
        else: pl.ioff()
        pl.figure(self.MATPLOTLIB_FIGURE_ID)
        if ShowPlot: pl.ioff()
        pl.clf()
        
        # 2008/9/20 Dec Effect has been taken into account
        #Aspect = 1.0 / math.cos(0.5 * (self.dec_max + self.dec_min) / 180. * 3.141592653)

        colormap = 'color'
        TickSize = 6

        axes_manager = ChannelAveragedAxesManager(RAformatter, DECformatter,
                                                  RAlocator, DEClocator,
                                                  RArotation, DECrotation,
                                                  TickSize, colormap)
        axes_tpmap = axes_manager.axes_tpmap
        tpmap_colorbar = None
        beam_circle = None

        pl.gcf().sca(axes_tpmap)

        
        plot_list = []
        
        for pol in xrange(self.npol):
            masked_data = (self.data.take([pol], axis=self.id_stokes) * self.mask.take([pol], axis=self.id_stokes)).squeeze()
            Total = numpy.flipud(masked_data.transpose())
            del masked_data

            # 2008/9/20 DEC Effect
            im = pl.imshow(Total, interpolation='nearest', aspect=self.aspect, extent=Extent)
            #im = pl.imshow(Total, interpolation='nearest', aspect='equal', extent=Extent)
            tmin = Total.min()
            tmax = Total.max()
            del Total

            xlim = axes_tpmap.get_xlim()
            ylim = axes_tpmap.get_ylim()

            # colorbar
            #print "min=%s, max of Total=%s" % (tmin,tmax)
            if not (tmin == tmax): 
                #if not ((Ymax == Ymin) and (Xmax == Xmin)): 
                #if not all(image_shape[id_direction] <= 1):
                if self.nx > 1 or self.ny > 1:
                    if tpmap_colorbar is None:
                        tpmap_colorbar = pl.colorbar(shrink=0.8)
                        newfontsize = None
                        for t in tpmap_colorbar.ax.get_yticklabels():
                            newfontsize = t.get_fontsize()*0.5
                            t.set_fontsize(newfontsize)
                        #if newfontsize is None: # no ticks in colorbar likely invalid TP map
                        tpmap_colorbar.ax.set_ylabel('[%s]'%(self.brightnessunit), fontsize=newfontsize)
                    else:
                        tpmap_colorbar.set_clim((tmin,tmax))
                        tpmap_colorbar.draw_all()
                        
            # draw beam pattern
            if beam_circle is None:
                beam_circle = draw_beam(axes_tpmap, 0.5 * self.beam_size, self.aspect, self.ra_min, self.dec_min)

            pl.title('Total Power', size=TickSize)
            axes_tpmap.set_xlim(xlim)
            axes_tpmap.set_ylim(ylim)

            if ShowPlot: pl.draw()
            FigFileRoot = self.inputs.imagename+'.pol%s'%(pol)
            plotfile = os.path.join(self.stage_dir,FigFileRoot+'_TP.png')
            pl.savefig(plotfile, format='png', dpi=DPIDetail)

            im.remove()
            
            parameters = {}
            parameters['intent'] = 'TARGET'
            parameters['spw'] = self.inputs.spw
            parameters['pol'] = self.image.coordsys.stokes()[pol]#polmap[pol]
            parameters['ant'] = self.inputs.antenna
            parameters['type'] = 'sd_integrated_map'
            parameters['file'] = self.inputs.imagename

            plot = logger.Plot(plotfile,
                               x_axis='R.A.',
                               y_axis='Dec.',
                               field=self.inputs.source,
                               parameters=parameters)
            
            plot_list.append(plot)

        return plot_list
 
