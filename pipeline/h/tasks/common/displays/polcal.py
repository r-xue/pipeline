import os
from typing import List

import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from . import applycal
from . import common

LOG = infrastructure.get_logger(__name__)


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


class GainRatioRMSVsScanChart(object):
    def __init__(self, context, output_dir, result):
        self.context = context
        self.output_dir = output_dir
        self.result = result

    def plot(self):
        # Set plot file name.
        figfile = self._get_figfile()

        # Create plot wrapper.
        wrapper = logger.Plot(figfile, x_axis='Scan', y_axis='Gain Ratio RMS',
                              parameters={'vis': os.path.basename(self.result.vis)})

        # Create plot if still missing.
        if not os.path.exists(figfile):
            LOG.trace(f"Gain ratio rms vs scan plot for {self.result.vis} not found. Creating new plot:"
                      f" {figfile}.")

            try:
                self._create_plot(figfile)
            except Exception as e:
                LOG.error(f"Could not create Gain ratio rms vs scan plot for {self.result.vis}.")
                LOG.exception(e)
                # Ensure nothing is kept for next figure.
                plt.clf()
                return None

        return [wrapper]

    def _get_figfile(self):
        png = f"{self.result.vis}.gain_ratio_rms_vs_scan.png"
        return os.path.join(self.output_dir, png)

    def _create_plot(self, figfile):
        # Compute the gain ratio RMS per scan before and after polarization
        # calibration.
        scans_before, rrms_before = self._compute_gain_ratio_rms(self.result.init_gcal_result.final[0].gaintable)
        scans_after, rrms_after = self._compute_gain_ratio_rms(self.result.final_gcal_result.final[0].gaintable)

        # Create plot with Matplotlib.
        fig = plt.gcf()
        sub = fig.add_subplot(111)
        sub.plot(scans_before, rrms_before, 'or', label='Before polcal')
        sub.plot(scans_after, rrms_after, 'ob', label='After polcal')
        sub.set_xlabel('Scan Number')
        sub.set_ylabel('Gain Ratio RMS')
        plt.legend(numpoints=1)
        plt.savefig(figfile)
        plt.close()

    @staticmethod
    def _compute_gain_ratio_rms(caltable):
        # Retrieve gains and scan from caltable.
        with casa_tools.TableReader(caltable) as table:
            scans = table.getcol('SCAN_NUMBER')
            gains = np.squeeze(table.getcol('CPARAM'))

        # Compute the gain ratio RMS for each scan.
        uniq_scans = sorted(set(scans))
        ratio_rms = np.zeros(len(uniq_scans))
        for ind, scanid in enumerate(uniq_scans):
            filt = np.where(scans == scanid)[0]
            ratio_rms[ind] = np.sqrt(np.average(np.power(np.abs(gains[0, filt]) / np.abs(gains[1, filt]) - 1.0, 2.)))

        return uniq_scans, ratio_rms


class PhaseVsChannelChart(object):
    """
    Plotting class that creates a polarisation ratio phase vs. channel plot
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


class RealVsImagChart(applycal.PlotmsLeaf):
    """
    Plotting class that creates a real vs. imag plot.
    """
    def __init__(self, context, output_dir, calto, **overrides):
        plot_args = {
            'xaxis': 'real',
            'xdatacolumn': 'corrected',
            'yaxis': 'imag',
            'ydatacolumn': 'corrected',
            'intent': 'POLARIZATION,POLANGLE,POLLEAKAGE',
            'correlation': 'XX,YY',
            'averagedata': True,
            'avgchannel': '4000',
            'avgtime': '1000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, **plot_args)

    def plot(self):
        jobs_and_wrappers = super().plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers)
        return successful_wrappers


class XVsChannelSummaryChart(object):
    """
    Plotting class that creates an "X" vs. channel summary chart per SpW for
    all antennas, for a caltable.
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication], yaxis):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': yaxis,
            'coloraxis': 'antenna1',
        }
        self.plotters = common.PlotmsCalSpwComposite(context, result, calapps, **plot_args)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class XVsChannelDetailChart(object):
    """
    Plotting class that creates an "X" vs. channel detail chart per SpW and per
    antenna for a caltable.
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication], yaxis):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': yaxis,
        }
        self.plotters = common.PlotmsCalSpwAntComposite(context, result, calapps, **plot_args)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers
