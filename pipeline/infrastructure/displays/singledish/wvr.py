from __future__ import absolute_import

import os
import pylab as pl
import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.renderer.logger as logger
from . import common
from . import utils as utils

LOG = infrastructure.get_logger(__name__)

# Scantable-based tasks are gone so input data should always be in MS
def is_ms(filename):
    return True

class WvrAxesManager(common.TimeAxesManager):
    Colors = ['r', 'g', 'b', 'c']
    Markers = ['o', '^', 's', 'D']
    
    def __init__(self):
        super(WvrAxesManager,self).__init__()
        self._axes = None
        self._xlabel = None

    @property
    def axes(self):
        if self._axes is None:
            self._axes = self.__axes()
        return self._axes

    def __axes(self):
        a = pl.subplot(111)
        a.set_xlabel('Time (UT)')
        a.set_ylabel('WVR reading')
        a.set_title('WVR reading versus UTC')
        a.xaxis.set_major_locator(self.locator)
        a.xaxis.set_major_formatter(utils.utc_formatter())

        # shift axes upward
        pos = a.get_position()
        left = pos.x0
        bottom = pos.y0 + 0.02
        width = pos.x1 - pos.x0
        height = pos.y1 - pos.y0
        a.set_position([left, bottom, width, height])
        
        return a
        
class SDWvrDisplay(common.SDInspectionDisplay):
    MATPLOTLIB_FIGURE_ID = 8907
    AxesManager = WvrAxesManager

    def doplot(self, idx, stage_dir):
        st = self.context.observing_run[idx]
        parent_ms = st.ms
        vis = parent_ms.basename
        spws = self.context.observing_run.get_spw_for_wvr(st.basename)
        plotfile = os.path.join(stage_dir, 'wvr_%s.png'%(st.basename))
        wvr_data = self.get_wvr_data(st.name, spws)
        wvr_flag = self.get_wvr_flag(st.name, spws) # [Time, FlagRow, FlagChan0, FlagChan1, FlagChan2, FlagChan3]
        wvr_frequency = self.get_wvr_frequency(st, spws)
        if len(wvr_data) == 0:
            return 
        self.draw_wvr(wvr_data, wvr_frequency, wvr_flag, plotfile, plotpolicy='greyed')
        parameters = {}
        parameters['intent'] = 'TARGET'
        parameters['spw'] = spws[0]
        parameters['pol'] = 'I'
        parameters['ant'] = st.antenna.name
        parameters['type'] = 'sd'
        parameters['file'] = st.basename
        parameters['vis'] = vis
        plot = logger.Plot(plotfile,
          x_axis='Time', y_axis='WVR Reading',
          field=parent_ms.fields[0].name,
          parameters=parameters)
        return plot

    def draw_wvr(self, wvr_data, wvr_frequency, wvr_flag, plotfile=None, plotpolicy='ignore'):
        # Plotting routine
        Fig = pl.gcf()

        # Convert MJD sec to MJD date for wvr_data
        # Furthermore, MJD date is converted to matplotlib
        # specific time value
        mjd = wvr_data[0]/3600.0/24.0
        time_for_plot = utils.mjd_to_plotval(mjd)

        # Convert wvr_frequency in Hz to GHz
        wvr_frequency = wvr_frequency * 1.0e-9

        wvr = wvr_data[1:,:]
        wvr_flagrow = wvr_flag[1,:]
        wvr_flagchan = wvr_flag[2:,:]

        xmin = time_for_plot.min()
        xmax = time_for_plot.max()
        dx = (xmax - xmin) * 0.1
        xmin -= dx
        xmax += dx
        ymin = wvr.min()
        ymax = wvr.max()
        if ymin == ymax:
            dy = 0.06
        else:
            dy = 0.1 * (ymax - ymin)
        ymin -= dy
        ymax += dy
        
        # Plot WVR data
        plot_objects = []
        self.axes_manager.init(xmin, xmax)
        Ax1 = self.axes_manager.axes
        colors = self.axes_manager.Colors
        markers = self.axes_manager.Markers
        lines = Ax1.get_lines()
        if len(wvr_data[0]) == 1:
            for i in xrange(4):
                plot_objects.append(
                    Ax1.axhline(wvr[0], markeredgecolor=colors[i],
                                markersize=3, markerfacecolor=colors[i])
                )
        else:
            for i in xrange(4):
                
                if plotpolicy == 'plot':
                    plot_objects.extend(
                        Ax1.plot(time_for_plot, wvr[i], '%s%s'%(colors[i],markers[i]),
                                 markersize=3, markeredgecolor=colors[i],
                                 markerfacecolor=colors[i], label='%.2fGHz'%(wvr_frequency[i]))
                        )
                elif plotpolicy == 'ignore':
                    filter = numpy.logical_and(wvr_flagrow == 0, wvr_flagchan[i,:] == 0)
                    plot_objects.extend(
                        Ax1.plot(time_for_plot[filter], wvr[i][filter], '%s%s'%(colors[i],markers[i]),
                                 markersize=3, markeredgecolor=colors[i],
                                 markerfacecolor=colors[i], label='%.2fGHz'%(wvr_frequency[i]))
                        )
                elif plotpolicy == 'greyed':
                    filter = numpy.logical_and(wvr_flagrow == 0, wvr_flagchan[i,:] == 0)
                    plot_objects.extend(
                        Ax1.plot(time_for_plot[filter], wvr[i][filter], '%s%s'%(colors[i],markers[i]),
                                 markersize=3, markeredgecolor=colors[i],
                                 markerfacecolor=colors[i], label='%.2fGHz'%(wvr_frequency[i]))
                        )
                    filter = numpy.logical_or(wvr_flagrow > 0, wvr_flagchan[i,:] > 0)
                    if numpy.any(filter == True):
                        plot_objects.extend(
                            Ax1.plot(time_for_plot[filter], wvr[i][filter], markers[i],
                                     markersize=3, markeredgecolor='grey',
                                     markerfacecolor='grey')
                            )
                    
                    
        Ax1.legend(loc=0, numpoints=1, prop={'size': 'smaller'})
        Ax1.set_xlim(xmin, xmax)
        Ax1.set_ylim(ymin, ymax)

        if common.ShowPlot != False: pl.draw()
        pl.savefig(plotfile, format='png', dpi=common.DPISummary)

        for obj in plot_objects:
            obj.remove()
        
        return


    def get_wvr_frequency(self, st, spwids):
        if len(spwids) == 0:
            return []

        spwid = spwids[0]

        if is_ms(st.ms.name):
            # take frequency from MS
            table_name = os.path.join(st.ms.name, 'SPECTRAL_WINDOW')
            with casatools.TableReader(table_name) as tb:
                chanfreq = tb.getcell('CHAN_FREQ', spwid)
        else:
            # take frequency from Scantable
            table_name = os.path.join(st.name, 'FREQUENCIES')
            with casatools.TableReader(table_name) as tb:
                refpix = tb.getcell('REFPIX', spwid)
                refval = tb.getcell('REFVAL', spwid)
                increm = tb.getcell('INCREMENT', spwid)
            chanfreq = [refval + (i - refpix) * increm for i in xrange(4)]

        return chanfreq
                

    def get_wvr_data(self, name, spwids):
        if len(spwids) == 0:
            return []
        
        with casatools.TableReader(name) as tb:
            tsel = tb.query('IFNO IN %s'%(list(spwids)))
            timecol = tsel.getcol('TIME') * 86400.0 # Day -> Sec
            wvrdata = tsel.getcol('SPECTRA')
            tsel.close()
            timecol = timecol.reshape(1, timecol.shape[0])
            data = numpy.concatenate([timecol,wvrdata])

        return data
    
    def get_wvr_flag(self, name, spwids):
        if len(spwids) == 0:
            return []
        
        with casatools.TableReader(name) as tb:
            tsel = tb.query('IFNO IN %s'%(list(spwids)))
            timecol = tsel.getcol('TIME') * 86400.0 # Day -> Sec
            wvrflagchan = tsel.getcol('FLAGTRA')
            wvrflagrow= tsel.getcol('FLAGROW')
            tsel.close()
            timecol = timecol.reshape(1, timecol.shape[0])
            wvrflagrow = wvrflagrow.reshape(1, wvrflagrow.shape[0])
            flag = numpy.concatenate([timecol,wvrflagrow, wvrflagchan])

        return flag
        return data