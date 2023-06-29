from typing import List

import pipeline.infrastructure.callibrary as callibrary
from . import applycal
from . import common


class AmpVsAntennaChart(object):
    """
    Plotting class that creates a gain amplitude (ratio) vs. antenna plot
    per SpW for a caltable.
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication], correlation=''):
        plot_args = {
            'xaxis': 'antenna1',
            'yaxis': 'amp',
            'correlation': correlation,
            'coloraxis': 'antenna1',
        }
        self.plotters = common.PlotmsCalSpwComposite(context, result, calapps, **plot_args)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


# TODO: move parent chart to common module?
class AmpVsParangSummaryChart(applycal.SpwSummaryChart):
    """
    Plotting class that creates an amplitude vs. parallactic angle plot for
    each spw.
    """
    def __init__(self, context, output_dir, calto, **overrides):
        plot_args = {
            'ydatacolumn': 'data',
            'correlation': 'XX,YY',
            'averagedata': True,
            'avgchannel': '9999',
            'avgbaseline': True,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, xaxis='parang', yaxis='amp',
                         intent='POLARIZATION,POLANGLE,POLLEAKAGE', **plot_args)


class AmpVsScanChart(object):
    """
    Plotting class that creates a polarisation ratio amplitude vs. scan plot
    for a caltable.
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication]):
        plot_args = {
            'xaxis': 'scan',
            'yaxis': 'amp',
            'correlation': '/',
            'coloraxis': 'spw',
        }
        self.plotters = common.PlotmsCalLeaf(context, result, calapps, **plot_args)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class PhaseVsChannelChart(object):
    """
    Plotting class that creates a polarisation ratio amplitude vs. scan plot
    for a caltable.
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication]):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': 'phase',
            'coloraxis': 'spw',
        }
        self.plotters = common.PlotmsCalLeaf(context, result, calapps, **plot_args)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers
