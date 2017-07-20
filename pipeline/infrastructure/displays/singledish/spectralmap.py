from __future__ import absolute_import

import os
import numpy
import math
import pylab as pl
from matplotlib.ticker import MultipleLocator

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from .utils import DDMMSSs, HHMMSSss
from .common import DPIDetail, SDImageDisplay, ShowPlot
import pipeline.infrastructure.casatools as casatools

LOG = infrastructure.get_logger(__name__)

class SpectralMapAxesManager(object):
    def __init__(self, nh, nv, brightnessunit, formatter, locator, ticksize):
        self.nh = nh
        self.nv = nv
        self.brightnessunit = brightnessunit
        self.formatter = formatter
        self.locator = locator
        self.ticksize = ticksize

        self._axes = None
        
    @property
    def axes_list(self):
        if self._axes is None:
            self._axes = list(self.__axes_list())

        return self._axes

    def __axes_list(self):
        npanel = self.nh * self.nv
        for ipanel in xrange(npanel):
            x = ipanel % self.nh
            y = int(ipanel / self.nh)
            #x0 = 1.0 / float(self.nh) * (x + 0.1)
            x0 = 1.0 / float(self.nh) * (x + 0.22)
            #x1 = 1.0 / float(self.nh) * 0.8
            x1 = 1.0 / float(self.nh) * 0.75
            y0 = 1.0 / float(self.nv) * (y + 0.15)
            #y1 = 1.0 / float(self.nv) * 0.7
            y1 = 1.0 / float(self.nv) * 0.65
            a = pl.axes([x0, y0, x1, y1])
            a.xaxis.set_major_formatter(self.formatter)
            a.xaxis.set_major_locator(self.locator)
            a.yaxis.set_label_coords(-0.22,0.5)
            a.title.set_y(0.95)
            a.title.set_size(self.ticksize)
            pl.ylabel('Intensity (%s)'%(self.brightnessunit), size=self.ticksize)
            pl.xticks(size=self.ticksize)
            pl.yticks(size=self.ticksize)

            yield a

        
class SDSpectralMapDisplay(SDImageDisplay):
    #MATPLOTLIB_FIGURE_ID = 8910
    MaxNhPanel = 5
    MaxNvPanel = 5
    
    def plot(self):
        self.init()
        return self.__plot_spectral_map()

    def __get_strides(self):
        qa = casatools.quanta
        increment = self.image.coordsys.increment()
        units = self.image.coordsys.units()
        factors = []
        for idx in self.image.id_direction:
            cell = qa.convert(qa.quantity(increment['numeric'][idx],units[idx]),'deg')['value']
            factors.append( int( numpy.round( abs(self.grid_size / cell) ) ) )
        return factors

    def __plot_spectral_map(self):
        pl.clf()

        # read data to storage
        masked_data = self.data * self.mask
        (STEPX, STEPY) = self.__get_strides()

        # Raster Case: re-arrange spectra to match RA-DEC orientation
        mode = 'raster'
        if mode.upper() == 'RASTER':
            # the number of panels in each page
            NhPanel = min( max((self.x_max - self.x_min + 1)/STEPX, (self.y_max - self.y_min + 1)/STEPY), self.MaxNhPanel )
            NvPanel = min( max((self.x_max - self.x_min + 1)/STEPX, (self.y_max - self.y_min + 1)/STEPY), self.MaxNvPanel )
            # total number of pages in horizontal and vertical directions
            NH = int((self.x_max - self.x_min) / STEPX / NhPanel + 1)
            NV = int((self.y_max - self.y_min) / STEPY / NvPanel + 1)
            # an array with length of total number of spectra to be plotted (initialized by -1)
            ROWS = numpy.zeros(NH * NV * NhPanel * NvPanel, dtype=numpy.int) - 1
            # 2010/6/15 GK Change the plotting direction: UpperLeft->UpperRight->OneLineDown repeat...
            for x in xrange(0,self.nx,STEPX):
                posx = (self.x_max - x)/STEPX / NhPanel
                offsetx = ( (self.x_max - x)/STEPX ) % NhPanel
                for y in xrange(0,self.ny,STEPY):
                    posy = (self.y_max - y)/STEPY / NvPanel
                    offsety = NvPanel - 1 - (self.y_max - y)/STEPY % NvPanel
                    row = (self.nx - x - 1) * self.ny + y
                    ROWS[(posy*NH+posx)*NvPanel*NhPanel + offsety*NhPanel + offsetx] = row
        else: ### This block is currently broken (2016/06/23 KS)
            ROWS = rows[:]
            NROW = len(rows)
            Npanel = (NROW - 1) / (self.MaxNhPanel * self.MaxNvPanel) + 1
            if Npanel > 1:  (NhPanel, NvPanel) = (self.MaxNhPanel, self.MaxNvPanel)
            else: (NhPanel, NvPanel) = (int((NROW - 0.1) ** 0.5) + 1, int((NROW - 0.1) ** 0.5) + 1)

        LOG.debug("Generating spectral map")
        LOG.debug("- Stride: [%d, %d]" % (STEPX, STEPY))
        LOG.debug("- Number of panels: [%d, %d]" % (NhPanel, NvPanel))
        LOG.debug("- Number of pages: [%d, %d]" % (NH, NV))
        LOG.debug("- Number of spcetra to be plotted: %d" % (len(ROWS)))
        
        Npanel = 0
        TickSize = 11 - NhPanel

        # Plotting routine
        connect = True
        if connect is True: Mark = '-b'
        else: Mark = 'bo'
        chan0 = 0
        chan1 = -1
        if chan1 == -1:
            chan0 = 0
            chan1 = self.nchan - 1
        xmin = min(self.frequency[chan0], self.frequency[chan1])
        xmax = max(self.frequency[chan0], self.frequency[chan1])


        NSp = 0
        xtick = abs(self.frequency[-1] - self.frequency[0]) / 4.0
        Order = int(math.floor(math.log10(xtick)))
        NewTick = int(xtick / (10**Order) + 1) * (10**Order)
        FreqLocator = MultipleLocator(NewTick)
        if Order < 0: FMT = '%%.%dfG' % (-Order)
        else: FMT = '%.2fG'
        Format = pl.FormatStrFormatter(FMT)


        (xrp,xrv,xic) = self.image.direction_axis(0)
        (yrp,yrv,yic) = self.image.direction_axis(1)

        plot_list = []

        axes_manager = SpectralMapAxesManager(NhPanel, NvPanel, self.brightnessunit,
                                              Format, FreqLocator,
                                              TickSize)
        axes_list = axes_manager.axes_list
        plot_objects = []

        # MS-based procedure
        reference_data = self.context.observing_run.measurement_sets[self.inputs.msid_list[0]]
        is_baselined = reference_data.work_data != reference_data.name
        
        for pol in xrange(self.npol):
            data = masked_data.take([pol], axis=self.id_stokes).squeeze()
            Npanel = 0

            # to eliminate max/min value due to bad pixel or bad fitting,
            #  1/10-th value from max and min are used instead
#             valid_index = numpy.where(self.num_valid_spectrum[:,:,pol] > 0)
            mask2d = numpy.any(self.mask.take([pol], axis=self.id_stokes).squeeze(), axis=2)
            valid_index = mask2d.nonzero()
            valid_data = data[valid_index[0],valid_index[1],chan0:chan1]
            ListMax = valid_data.max(axis=1)
            ListMin = valid_data.min(axis=1)
            del valid_index, valid_data
            if len(ListMax) == 0: continue 
            if is_baselined:
                ymax = numpy.sort(ListMax)[len(ListMax) - len(ListMax)/10 - 1]
                ymin = numpy.sort(ListMin)[len(ListMin)/10]
            else:
                ymax = numpy.sort(ListMax)[-1]
                ymin = numpy.sort(ListMin)[1]
            ymax = ymax + (ymax - ymin) * 0.2
            ymin = ymin - (ymax - ymin) * 0.1
            LOG.debug('ymin=%s, ymax=%s'%(ymin,ymax))
            del ListMax, ListMin

            for irow in xrange(len(ROWS)):
                row = ROWS[irow]
                    
                _x = row / self.ny
                _y = row % self.ny
                
                prefix = self.inputs.imagename+'.pol%s_Result'%(pol)
                plotfile = os.path.join(self.stage_dir, prefix+'_%s.png'%(Npanel))

                if not os.path.exists(plotfile):
                    if 0 <= _x < self.nx and 0 <= _y < self.ny:
                        a = axes_list[NSp]
                        a.set_axis_on()
                        pl.gcf().sca(a)
                        world_x = xrv + (_x - xrp) * xic
                        world_y = yrv + (_y - yrp) * yic
                        title = '(IF, POL, X, Y) = (%s, %s, %s, %s)\n%s %s' % (self.spw, pol, _x, _y, HHMMSSss(world_x, 0), DDMMSSs(world_y, 0))
#                         if self.num_valid_spectrum[_x][_y][pol] > 0:
                        if mask2d[_x][_y]:
                            plot_objects.extend(
                                pl.plot(self.frequency, data[_x][_y], Mark, markersize=2, markeredgecolor='b', markerfacecolor='b')
                                )
                        else:
                            plot_objects.append(
                                pl.text((xmin+xmax)/2.0, (ymin+ymax)/2.0, 'NO DATA', horizontalalignment='center', verticalalignment='center', size=TickSize)
                                )
                        a.title.set_text(title)
                        pl.axis([xmin, xmax, ymin, ymax])
                    else:
                        a = axes_list[NSp]
                        a.set_axis_off()
                        a.title.set_text('')
                        
                NSp += 1
                if NSp >= (NhPanel * NvPanel) or (irow == len(ROWS)-1 and mode.upper() != 'RASTER'):
                    NSp = 0
                    if ShowPlot:
                        pl.draw()

                    prefix = self.inputs.imagename+'.pol%s_Result'%(pol)
                    plotfile = os.path.join(self.stage_dir, prefix+'_%s.png'%(Npanel))
                    if not os.path.exists(plotfile):
                        LOG.debug('Regenerate plot: %s'%(plotfile))
                        pl.savefig(plotfile, format='png', dpi=DPIDetail)
                    else:
                        LOG.debug('Use existing plot: %s'%(plotfile))

                    for obj in plot_objects:
                        obj.remove()
                    plot_objects = []

                    parameters = {}
                    parameters['intent'] = 'TARGET'
                    parameters['spw'] = self.inputs.spw
                    parameters['pol'] = self.image.coordsys.stokes()[pol]#polmap[pol]
                    parameters['ant'] = self.inputs.antenna
                    parameters['type'] = 'sd_spectral_map'
                    parameters['file'] = self.inputs.imagename

                    plot = logger.Plot(plotfile,
                                       x_axis='Frequency',
                                       y_axis='Intensity',
                                       field=self.inputs.source,
                                       parameters=parameters)
                    plot_list.append(plot)
            

                    Npanel += 1
            del data, mask2d
        del ROWS, masked_data
        print("Returning %d plots from spectralmap" % len(plot_list))
        return plot_list
