"""Renderer for hsd_atmcor stage."""
import collections
import glob
import itertools
import os
import re
from typing import TYPE_CHECKING, List

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.basetask import ResultsList
from pipeline.infrastructure.launcher import Context

from .display import PlotmsRealVsFreqPlotter

if TYPE_CHECKING:
    from .atmcor import SDATMCorrectionResults

LOG = logging.get_logger(__name__)


ATMHeuristicsTR = collections.namedtuple('ATMHeuristicsTR', 'msname apply plot atmtype h0 dtem_dh')


def construct_heuristics_table_row(results: 'SDATMCorrectionResults', detail_page: str) -> ATMHeuristicsTR:
    vis = os.path.basename(results.inputs['vis'])
    if results.atm_heuristics == 'Y':
        plot = ' '.join([
            f'<a href="{detail_page}"',
            'class="replace"',
            f'data-vis="{vis}">View</a>',
        ])
        atm_model = results.model_list[results.best_model_index]
    elif results.atm_heuristics == 'N':
        # no heuristics, fixed parameter
        plot = 'N/A'
        atm_model = (results.inputs['atmtype'], results.inputs['maxalt'], results.inputs['dtem_dh'], results.inputs['h0'])
    else:
        # ATM heuristics failed: results.atm_heuristics should be 'Default'
        plot = 'N/A'
        if len(atm_model) == 1:
            atm_model = results.model_list[0]
        else:
            # something went wrong
            raise RuntimeError('model_list should store default model.')

    row = ATMHeuristicsTR(msname=vis,
                          apply=results.atm_heuristics,
                          plot=plot,
                          atmtype=atm_model[0],
                          h0=atm_model[3],
                          dtem_dh=atm_model[2])
    return row


def identify_heuristics_plots(stage_dir: str, results: 'SDATMCorrectionResults') -> List[logger.Plot]:
    if results.atm_heuristics != 'Y' or results.best_model_index == -1:
        # no useful heuristics plots exist, return empty list
        return []

    basename = os.path.basename(results.inputs['vis'])
    p = fr'{basename}\.field([0-9]+)\.spw([0-9]+)\.model\.([0-9]+)\.png$'
    LOG.info(p)
    heuristics_plots = []
    best_model_index = results.best_model_index
    model_list = results.model_list
    for png_file in glob.iglob(os.path.join(stage_dir, '*.png')):
        match = re.search(p, png_file)
        if match:
            field_id = match.group(1)
            spw_id = match.group(2)
            model_id = int(match.group(3))
            LOG.info(f'Match: field {field_id} spw {spw_id} model {model_id}')
            status = 'Applied' if model_id == best_model_index else 'Discarded'
            atmtype, _, lapse_rate, scale_height = model_list[model_id]
            model = f'atmtype={atmtype:d}, h0={scale_height:.1f}km, dTem_dh={lapse_rate:.1f}K/km'
            heuristics_plots.append(
                logger.Plot(
                    png_file,
                    x_axis='Frequency',
                    y_axis='Amplitude',
                    field=field_id,
                    parameters={
                        'vis': basename,
                        'spw': spw_id,
                        'ant': 'all',
                        'pol': 'XXYY',
                        'model': model,
                        'status': status,
                    }
                )
            )
    LOG.info(f'{heuristics_plots}')
    return heuristics_plots


class SDATMCorrHeuristicsDetailPlotRenderer(basetemplates.JsonPlotRenderer):
    """Renderer class for ATM heuristics detail plots."""

    def __init__(self, context: Context, result: 'SDATMCorrectionResults', plots: List[logger.Plot]) -> None:
        """
        Construct SDATMCorrHeuristicsDetailPlotRenderer instance.

        Args:
            context: pipeline Context
            result: SDATMCorrectionResults instance
            plots: list of plot objects
        """
        uri = 'hsd_atmcor_heuristics_detail_plots.mako'
        title = 'ATM Heuristics Plots'
        outfile = filenamer.sanitize(f'{title.lower()}.html')
        super().__init__(uri, context, result, plots, title, outfile)

    def update_json_dict(self, d: dict, plot: logger.Plot) -> None:
        """
        Update json dict to add new filters.

        Args:
            d: Json dict for plot
            plot: plot object
        """
        d['status'] = plot.parameters['status']
        d['model'] = plot.parameters['model']


class T2_4MDetailsSingleDishATMCorRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """Renderer class for hsd_atmcor stage."""

    def __init__(self, always_rerender=False):
        """Initialize renderer.

        Args:
            always_rerender: Set True to always render the page. Defaults to False.
        """
        uri = 'hsd_atmcor.mako'
        description = 'Apply correction for atmospheric effects'
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self,
                            mako_context: dict,
                            pipeline_context: Context,
                            result: ResultsList):
        """Update Mako context.

        Args:
            mako_context (): original Mako context
            pipeline_context (): pipeline context
            result (): ResultsList containing SDATMCorrectionResults

        Raises:
            RuntimeError: given results object is not valid
        """
        super().update_mako_context(mako_context, pipeline_context, result)
        stage_dir = os.path.join(
            pipeline_context.report_dir,
            'stage{}'.format(result.stage_number)
        )
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        summary_plots = {}
        detail_plots = []
        heuristics_plots = []
        for r in result:
            LOG.info('Rendering result for "%s"', r.inputs['vis'])
            if not hasattr(r, 'atmcor_ms_name'):
                raise RuntimeError('Wrong result object is given.')

            vis = r.inputs['vis']
            atmvis = r.atmcor_ms_name
            ms = pipeline_context.observing_run.get_ms(os.path.basename(vis))
            antenna_ids = [int(a.id) for a in ms.get_antenna()]
            field_ids = [int(f.id) for f in ms.get_fields(intent='TARGET')]
            science_spws = [int(s.id) for s in ms.get_spectral_windows(science_windows_only=True)]
            spw_selection = r.inputs['spw']
            if len(spw_selection) > 0:
                selected_spws = set(map(int, spw_selection.split(','))).intersection(science_spws)
            else:
                selected_spws = science_spws

            plotter = PlotmsRealVsFreqPlotter(
                ms=ms, atmvis=atmvis,
                atmtype=r.inputs['atmtype'], output_dir=stage_dir
            )
            summaries = {}
            for field_id, spw_id in itertools.product(field_ids, selected_spws):
                LOG.info(f'field {field_id} spw {spw_id}')
                spw = str(spw_id)
                plotter.set_field(field_id)
                field_name = plotter.original_field_name
                summaries.setdefault(field_name, {})
                plotter.set_spw(spw)
                # reset antenna selection
                plotter.set_antenna()
                p = plotter.plot()
                summaries[field_name][spw] = p
                for antenna_id in antenna_ids:
                    plotter.set_antenna(antenna_id)
                    p = plotter.plot()
                    detail_plots.append(p)
            summary_plots[os.path.basename(vis)] = summaries

            # PIPE-1443 ATM heuristics plots
            heuristics_plots.extend(identify_heuristics_plots(stage_dir, r))

        detail_page_title = 'ATM corrected amplitude vs frequency'
        detail_renderer = basetemplates.JsonPlotRenderer(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako',
            pipeline_context,
            result,
            detail_plots,
            detail_page_title,
            filenamer.sanitize(f'{detail_page_title.lower()}.html')
        )

        with detail_renderer.get_file() as fileobj:
            fileobj.write(detail_renderer.render())

        heuristics_renderer = SDATMCorrHeuristicsDetailPlotRenderer(
            pipeline_context,
            result,
            heuristics_plots,
        )

        with heuristics_renderer.get_file() as fileobj:
            fileobj.write(heuristics_renderer.render())

        # PIPE-1443 ATM heuristics table
        heuristics_path = os.path.relpath(heuristics_renderer.path, pipeline_context.report_dir)
        LOG.info(heuristics_path)
        heuristics_summary = [
            construct_heuristics_table_row(r, heuristics_path) for r in result
        ]
        heuristics_table = utils.merge_td_columns(heuristics_summary, num_to_merge=0)

        mako_context.update({
            'summary_plots': summary_plots,
            'detail_page': detail_renderer.path,
            'heuristics_table': heuristics_table,
        })
