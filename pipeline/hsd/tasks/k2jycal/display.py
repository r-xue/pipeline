"""Plotting class for k2jycal stage."""
import collections
import os

from typing import Any, Dict, Generator, List, Sequence, Tuple, Union

import numpy
import matplotlib.pyplot as plt
import matplotlib.cm as cm

import pipeline.infrastructure.renderer.logger as logger
from ..common.display import DPISummary


class K2JyHistDisplay(object):
    """A display class to generate histogram of Jy/K factors."""

    def __init__(
        self,
        stage: str,
        spw: int,
        valid_factors: Dict[int, Dict[str, List[Union[float, str]]]],
        bandname: str = ''
    ) -> None:
        """Initialize K2JyHistDisplay instance.

        Args:
            stage: Stage directory to which plots are exported
            spw: Virtual spw ID of valid_factors
            valid_factors: A dictionary or an array of Jy/K valid_factors to generate histogram
            bandname: Name of the observing receiver band

        Raises:
            ValueError: unexpected type of valid_factors
        """
        self.stage_dir = stage
        self.spw = spw
        self.band = bandname
        if isinstance(valid_factors, dict) or numpy.iterable(valid_factors) == 1:
            self.factors = valid_factors
        else:
            raise ValueError("valid_factors should be dictionary or an iterable")

    def plot(self) -> List[logger.Plot]:
        """Generate histogram plots.

        Returns:
            List of histogram plots.
        """
        plt.ioff()
        plt.clf()

        return list(self._plot())

    def _create_plot(self, plotfile: str, x_axis: str, y_axis: str) -> logger.Plot:
        """Create Plot instance from plotfile.

        Args:
            plotfile: Name of the plot file
            x_axis: X-axis label
            y_axis: Y-axis label

        Returns:
            Plot instance
        """
        parameters = {}
        parameters['spw'] = self.spw
        parameters['receiver'] = self.band
        plot_obj = logger.Plot(plotfile,
                               x_axis=x_axis,
                               y_axis=y_axis,
                               parameters=parameters)
        return plot_obj

    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Create histogram plot.

        Yields:
            Plot instance
        """
        if type(self.factors) in [dict, collections.defaultdict]:
            labels = []
            factors = []
            for lab, spw_factors in self.factors.items():
                dummy, f = collect_dict_values(spw_factors)
                factors.append(f)
                labels.append(lab)
        elif numpy.iterable(self.factors):
            labels = 'all data'
            factors = list(self.factors)
        # define binning
        factors1d = []
        for f in factors:
            factors1d += f
        data = numpy.array(factors1d)
        medval = numpy.median(data)
        bin_width = medval*0.05
        nbin_min = 6   # minimum number of bins on each side of the center bin
        nbin_neg = max( nbin_min, numpy.ceil( ( medval - data.min() ) / bin_width - 0.5 ) )
        nbin_pos = max( nbin_min, numpy.ceil( ( data.max() - medval ) / bin_width - 0.5 ) )
        minval = medval - bin_width * ( nbin_neg + 0.5 )
        maxval = medval + bin_width * ( nbin_pos + 0.5 )
        num_bin = int(nbin_pos + nbin_neg + 1)

        plt.hist(factors, range=[minval, maxval], bins=num_bin,
                 histtype='barstacked', align='mid', label=labels, ec='black')
        plt.xlabel('Jy/K factor', fontsize=11)
        plt.ylabel('Numbers', fontsize=11)
        plt.title('Jy/K factors (SPW %d)' % self.spw, fontsize=11)
        plt.legend(loc=1)

        plotfile = os.path.join(self.stage_dir, 'jyperk_spw%s.png' % self.spw)
        plt.savefig(plotfile, format='png', dpi=DPISummary)
        plot = self._create_plot(plotfile, 'Jy/K factor', 'Number of MS, ant, and pol combinations')
        yield plot


class K2JySingleScatterDisplay(object):
    """A display class to generate a scatter plot of K/Jy factors across all SPWs."""

    def __init__(
        self,
        stage: str,
        valid_factors: Dict[str, Dict[int, List[float]]],
        spw_frequencies: Dict[int, float],
        spw_bands: Dict[int, str],
    ) -> None:
        """Initialize K2JySingleHistDisplay instance.

        Args:
            stage: Stage directory to which plots are exported
            valid_factors: A dictionary mapping MS labels to SPW IDs and their Jy/K factors
            spw_frequencies: A dictionary mapping SPW IDs to their centre frequencies
            spw_band: A dictionary mapping SPW IDs to their observing bands

        Raises:
            ValueError: unexpected type of valid_factors
        """
        self.stage_dir = stage
        self.valid_factors = valid_factors
        self.spw_frequencies = spw_frequencies
        self.spw_bands = spw_bands
        
    def plot(self) -> List[logger.Plot]:
        """Generate scatter plot.

        Returns:
            List of plots.
        """
        plt.ioff()
        plt.clf()

        return list(self._plot())
    
    def _create_plot(self, plotfile: str, x_axis: str, y_axis: str) -> logger.Plot:
        """Create Plot instance from plotfile.

        Args:
            plotfile: Name of the plot file
            x_axis: X-axis label
            y_axis: Y-axis label

        Returns:
            Plot instance
        """
        parameters = {}
        # Collect SPW IDs and Receiver bands
        parameters['spws'] = list(self.spw_bands.keys())
        parameters['receivers'] = list(set(self.spw_bands.values()))
        plot_obj = logger.Plot(plotfile,
                            x_axis=x_axis,
                            y_axis=y_axis,
                            parameters=parameters)
        return plot_obj
    
    
    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Create scatter plot with Frequencies as the main X-axis."""

        fig, ax = plt.subplots()
        ax.set_xlabel('Frequency (GHz)', fontsize=11)
        ax.set_ylabel('K/Jy factor', fontsize=11)
        ax.set_title('K/Jy Factors across Frequencies', fontsize=11, fontweight='bold')

        # Prepare labels for plotting
        ms_labels = list(self.valid_factors.keys())
        spw_ids = sorted({spw_id for ms_data in self.valid_factors.values() for spw_id in ms_data.keys()})
        frequencies = [self.spw_frequencies[spw_id].value for spw_id in spw_ids]  # Extract frequency decimal values
        # freq_to_spw = {self.spw_frequencies[spw_id].value: spw_id for spw_id in spw_ids}  # Map frequencies to SPW IDs

        # Plot data points for each MS
        for ms_label in ms_labels:
            ms_data = self.valid_factors[ms_label]
            x = []
            y = []
            for spw_id in spw_ids:
                factors = ms_data.get(spw_id, [])
                x.extend([self.spw_frequencies[spw_id].value] * len(factors))  # Use frequencies as x-values
                y.extend(factors)
            ax.scatter(x, y, label=ms_label)

        # Set frequency axis ticks and labels
        ax.set_xticks(frequencies)
        ax.set_xticklabels(['{:.2f} GHz'.format(freq) for freq in frequencies], rotation=45, ha="right")  

        # Add secondary x-axis for SPW IDs
        ax_top = ax.twiny()
        ax_top.set_xlim(ax.get_xlim())  # Match the range of the main x-axis
        ax_top.set_xticks(frequencies)  # Use the same tick positions as the main x-axis
        ax_top.set_xticklabels(spw_ids)  # Map frequencies back to SPW IDs
        ax_top.set_xlabel('SPW ID', fontsize=11)

        # Add grids and legend
        ax.legend(title='Measurement Sets', loc='best')
        ax.grid(True)

        # Save the plot
        plotfile = os.path.join(self.stage_dir, 'kjy_factors_across_frequencies.png')
        plt.tight_layout()
        plt.savefig(plotfile, format='png', dpi=DPISummary)
        plt.close(fig)

        # Create Plot object
        plot = self._create_plot(plotfile, 'Frequency (GHz)', 'K/Jy factor')
        yield plot
        
        
    # def _plot(self) -> Generator[logger.Plot, None, None]:
    #     """Create scatter plot.

    #     Yields:
    #         Plot instance
    #     """
    #     fig, ax = plt.subplots()
    #     plt.xlabel('SPW ID', fontsize=11)
    #     plt.ylabel('K/Jy factor', fontsize=11)
    #     plt.title('K/Jy Factors across SPWs', fontsize=11, fontweight='bold')
    #     # Prepare lables for plotting
    #     ms_labels = list(self.valid_factors.keys())
    #     spw_ids = sorted({spw_id for ms_data in self.valid_factors.values() for spw_id in ms_data.keys()})
    #     frequencies = [self.spw_frequencies[spw_id] for spw_id in spw_ids]
    #     # Plot data points for each MS     
    #     for ms_label in ms_labels:
    #         ms_data = self.valid_factors[ms_label]
    #         x = []
    #         y = []
    #         for spw_id in spw_ids:
    #             factors = ms_data.get(spw_id, [])
    #             x.extend([spw_id] * len(factors))
    #             y.extend(factors)
    #         ax.scatter(x, y, label=ms_label)
    #     # Set x-axis ticks and labels
    #     ax.set_xticks(spw_ids)
    #     ax.set_xticklabels(spw_ids)
    #     # Add secondary x-axis for frequencies
    #     ax_top = ax.twiny()
    #     ax_top.set_xlim(ax.get_xlim())
    #     ax_top.set_xticks(spw_ids)
    #     ax_top.set_xticklabels([str(freq) for freq in frequencies])
    #     ax_top.set_xlabel('Frequency (GHz)', fontsize=11)
    #     # Add grids and legend
    #     ax.legend(title='Measurement Sets', loc='best')
    #     ax.grid(True)
    #     # Save the plot
    #     plotfile = os.path.join(self.stage_dir, 'kjy_factors_across_spws.png')
    #     plt.tight_layout()
    #     plt.savefig(plotfile, format='png', dpi=DPISummary)
    #     plt.close(fig)
    #     # Create Plot object
    #     plot = self._create_plot(plotfile, 'SPW ID', 'K/Jy factor')
    #     yield plot     
    

def collect_dict_values(in_value: Union[dict, Sequence[Any], Any]) -> Tuple[bool, List[Any]]:
    """Return a list of values in in_value.

    When in_value = dict(a=1, b=dict(c=2, d=4)), the method collects
    all values in tips of branches and returns, [1, 2, 4].
    When in_value is a simple number or an array, it returns a list
    of the number or the array.

    Args:
        in_value: A dictionary, number or array to collect values and construct a list

    Returns:
        Tuple of True or False and the flat list of values contained in in_value.
    """
    if type(in_value) not in [dict, collections.defaultdict]:
        if numpy.iterable(in_value) == 0:
            in_value = [in_value]
        return True, list(in_value)
    out_factor = []
    for value in in_value.values():
        done = False
        while not done:
            done, value = collect_dict_values(value)
        out_factor += value
    return done, out_factor

