from __future__ import absolute_import

import pipeline.h.tasks.common.displays.common as common
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


class GaincalSummaryChart(object):
    """
    Base class for executing plotms per spw
    """
    def __init__(self, context, result, calapps, intent, xaxis, yaxis, plotrange=None, coloraxis=''):
        if plotrange is None:
            plotrange = []
        if yaxis == 'amp':
            calmode = 'a'
        elif yaxis == 'phase':
            calmode = 'p'
        else:
            raise ValueError('Unmapped calmode for y-axis: ' % yaxis)    

        # identify the phase-only solution for the target
        selected = [c for c in calapps
                    if (intent in c.intent or c.intent == '') 
                    and calmode == utils.get_origin_input_arg(c, 'calmode')]

        plotters = []
        # PIPE-390: Need to handle cases when more than one caltables should be
        # plotted to accomplish calmode and intent selection (e.g., multiple SpectralSpec)
        for calapp in selected:
            # Take ant from calapp.
            ant = calapp.antenna
            # request plots per spw, overlaying all antennas
            plot_cls = common.PlotmsCalSpwComposite(context, result, calapp,
                                                    xaxis=xaxis, yaxis=yaxis, ant=ant,
                                                    plotrange=plotrange, coloraxis=coloraxis)
            plotters.append(plot_cls)

        self.plotters = plotters
        
    def plot(self):
        plot_wrappers = []
        for plot_cls in self.plotters:
            plot_wrappers.extend(plot_cls.plot())
        return plot_wrappers

class GaincalDetailChart(object):
    """
    Base class for executing plotms per spw and antenna
    """
    def __init__(self, context, result, calapps, intent, xaxis, yaxis, plotrange=None, coloraxis=''):
        if plotrange is None:
            plotrange = []
        if yaxis == 'amp':
            calmode = 'a'
        elif yaxis == 'phase':
            calmode = 'p'
        else:
            raise ValueError('Unmapped calmode for y-axis: ' % yaxis)    

        # identify the phase-only solution for the target
        selected = [c for c in calapps
                    if (intent in c.intent or c.intent == '') 
                    and calmode == utils.get_origin_input_arg(c, 'calmode')]

        plotters = []
        # PIPE-390: Need to handle cases when more than one caltables should be
        # plotted to accomplish calmode and intent selection (e.g., multiple SpectralSpec)
        for calapp in selected:
            # request plots per spw, overlaying all antennas, and setting same
            # y-range for each spw.
            plot_cls = common.PlotmsCalSpwAntComposite(context, result, calapp,
                                                       xaxis=xaxis, yaxis=yaxis, 
                                                       plotrange=plotrange, coloraxis=coloraxis,
                                                       ysamescale=True)
            plotters.append(plot_cls)
            
        self.plotters = plotters

    def plot(self):
        plot_wrappers = []
        for plot_cls in self.plotters:
            plot_wrappers.extend(plot_cls.plot())
        return plot_wrappers

class GaincalAmpVsTimeSummaryChart(GaincalSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """
    def __init__(self, context, result, calapps, intent):
        super(GaincalAmpVsTimeSummaryChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='antenna1')


class GaincalAmpVsTimeDetailChart(GaincalDetailChart):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalAmpVsTimeDetailChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='corr')


class GaincalPhaseVsTimeSummaryChart(GaincalSummaryChart):
    """
    Create a phase vs time plot for each spw, overplotting by antenna.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalPhaseVsTimeSummaryChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='phase', plotrange=[0, 0, -180, 180],
            coloraxis='antenna1')


class GaincalPhaseVsTimeDetailChart(GaincalDetailChart):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalPhaseVsTimeDetailChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='phase', plotrange=[0, 0, -180, 180],
            coloraxis='corr')
