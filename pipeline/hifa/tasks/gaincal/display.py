from __future__ import absolute_import

import pipeline.h.tasks.common.displays.common as common
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


class GaincalSummaryChart2(common.PlotmsCalSpwComposite):
    """
    Base class for executing plotms per spw
    """
    def __init__(self, context, result, calapps, intent, xaxis, yaxis, 
                 plotrange=[], coloraxis=''):
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

        assert len(selected) is 1, '%s %s solutions != 1' % (intent, yaxis)
        calapp = selected[0]

        # request plots per spw, overlaying all antennas
        super(GaincalSummaryChart2, self).__init__(
                context, result, calapp, xaxis=xaxis, yaxis=yaxis, ant='', 
                plotrange=plotrange, coloraxis=coloraxis)


class GaincalDetailChart2(common.PlotmsCalSpwAntComposite):
    """
    Base class for executing plotcal per spw and antenna
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

        assert len(selected) is 1, '%s %s solutions != 1' % (intent, yaxis)
        calapp = selected[0]

        # request plots per spw, overlaying all antennas, and setting same
        # y-range for each spw.
        super(GaincalDetailChart2, self).__init__(context, result, calapp, xaxis=xaxis, yaxis=yaxis,
                                                  plotrange=plotrange, coloraxis=coloraxis, ysamescale=True)


class GaincalAmpVsTimeSummaryChart2(GaincalSummaryChart2):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna.
    """
    def __init__(self, context, result, calapps, intent):
        super(GaincalAmpVsTimeSummaryChart2, self).__init__(
                context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='antenna1')


class GaincalAmpVsTimeDetailChart2(GaincalDetailChart2):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalAmpVsTimeDetailChart2, self).__init__(
                context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='corr')


class GaincalPhaseVsTimeSummaryChart2(GaincalSummaryChart2):
    """
    Create a phase vs time plot for each spw, overplotting by antenna.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalPhaseVsTimeSummaryChart2, self).__init__(
                context, result, calapps, intent, xaxis='time', yaxis='phase',
                plotrange=[0, 0, -180, 180], coloraxis='antenna1')


class GaincalPhaseVsTimeDetailChart2(GaincalDetailChart2):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super(GaincalPhaseVsTimeDetailChart2, self).__init__(
                context, result, calapps, intent, xaxis='time', yaxis='phase', 
                plotrange=[0, 0, -180, 180], coloraxis='corr')
