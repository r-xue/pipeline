"""Plotting class for k2jycal stage."""
import collections
import decimal
import os

from typing import Any, Dict, Generator, List, Sequence, Tuple, Union

import numpy
import itertools
     
from matplotlib.figure import Figure
import matplotlib.cm as cm
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

import pipeline.infrastructure.renderer.logger as logger
from ..common.display import DPISummary
from pipeline.domain.spectralwindow import SpectralWindow
from pipeline.domain.measures import FrequencyUnits

class K2JySingleScatterDisplay(object):
    """A display class to generate a scatter plot of K/Jy factors across all SPWs."""
    
    def __init__(
        self,
        stage: str,
        valid_factors: Dict[str, Dict[int, List[float]]],
        spws: Dict[int, SpectralWindow],
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
        self.spws = spws
        
    def plot(self) -> List[logger.Plot]:
        """Generate scatter plot.

        Returns:
            List of plots.
        """
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
        parameters['spws'] = list(self.spws.keys())
        parameters['receivers'] = list(set([spw.band for spw in self.spws.values()]))
        plot_obj = logger.Plot(plotfile,
                            x_axis=x_axis,
                            y_axis=y_axis,
                            parameters=parameters)
        return plot_obj
    
    def _plot(self) -> Generator[logger.Plot, None, None]:
        """Create scatter plot with Frequencies as the main X-axis."""
        fig = Figure(figsize=(8, 6))  # Customize size as needed
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)
        ax.set_xlabel('Frequency (GHz)', fontsize=11)
        ax.set_ylabel('K/Jy factor', fontsize=11)
        ax.set_title('K/Jy Factors across Frequencies', fontsize=11, fontweight='bold')
        # Corrected symbols_and_colours definition
        symbols_and_colours = zip(
            itertools.cycle('osDv^<>'),  # Cycle through marker symbols
            itertools.cycle(cm.get_cmap('tab10').colors)  # Get colors from the 'tab10' colormap
        )  # standard Tableau colormap
        # Prepare labels for plotting
        ms_labels = list(self.valid_factors.keys())
        spw_ids = sorted({spw_id for ms_data in self.valid_factors.values() for spw_id in ms_data.keys()})
        frequencies = [self.spws[spw_id].centre_frequency.to_units(FrequencyUnits.GIGAHERTZ) for spw_id in spw_ids]  # Extract frequency decimal values
        # Plot data points for each MS
        for ms_label in ms_labels:
            symbol, colour = next(symbols_and_colours)
            ms_data = self.valid_factors[ms_label]
            for idx, spw_id in enumerate(spw_ids):
                y = ms_data.get(spw_id, [])
                x = [self.spws[spw_id].centre_frequency.to_units(FrequencyUnits.GIGAHERTZ)] * len(y)
                x_unc = [decimal.Decimal('0.5') * self.spws[spw_id].bandwidth.to_units(FrequencyUnits.GIGAHERTZ)] * len(y)
                # Plot ranges for SPWs
                ax.errorbar(
                        x, y, xerr=x_unc,
                        marker=symbol, color=colour, alpha = 1.0, linestyle="-", 
                        label=ms_label if idx == 0 else ""
                    )
        # Add secondary x-axis for SPW IDs
        ax_top = ax.twiny()
        ax_top.set_xlim(ax.get_xlim())  # Match the range of the main x-axis
        ax_top.set_xticks(frequencies)  # Use the same tick positions as the main x-axis
        ax_top.set_xticklabels(spw_ids)  # Map frequencies back to SPW IDs
        ax_top.set_xlabel('SPW ID', fontsize=11)
        for freq in frequencies:
            ax.axvline(freq, linestyle = '--', color = 'lightgray', alpha = 0.5)
        y_min, y_max = ax.get_ylim()
        if y_max - y_min < 0.2:  
            ax.set_ylim(y_min - 0.1, y_max + 0.1)
        ax.legend(title='EBs', loc='best')
        ax.grid(True)
        
        # Save the plot
        plotfile = os.path.join(self.stage_dir, 'kjy_factors_across_frequencies.png')
        canvas.print_figure(plotfile, format='png', dpi=DPISummary)
        # Create Plot object
        plot = self._create_plot(plotfile, 'Frequency (GHz)', 'K/Jy factor')
        yield plot
        

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

