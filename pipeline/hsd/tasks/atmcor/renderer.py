"""Renderer for hsd_atmcor stage."""
import collections
import itertools
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from .display import PlotmsRealVsFreqPlotter
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)


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
        for r in result:
            LOG.info('Rendering result for "%s"', r.inputs['vis'])
            if not hasattr(r, 'atmcor_ms_name'):
                raise RuntimeError('Wrong result object is given.')

            vis = r.inputs['vis']
            atmvis = r.atmcor_ms_name
            ms = pipeline_context.observing_run.get_ms(os.path.basename(vis))
            antenna_ids = sorted([int(a.id) for a in ms.get_antenna()])
            field_ids = sorted([int(f.id) for f in ms.get_fields(intent='TARGET')])
            science_spws = sorted([int(s.id) for s in ms.get_spectral_windows(science_windows_only=True)])
            spw_selection = r.inputs['spw']
            if len(spw_selection) > 0:
                selected_spws = set(map(int, spw_selection.split(','))).intersection(science_spws)
            else:
                selected_spws = science_spws
            selected_spws = sorted(selected_spws)

            plotter = PlotmsRealVsFreqPlotter(
                ms=ms, atmvis=atmvis,
                atmtype=r.inputs['atmtype'], output_dir=stage_dir
            )
            summaries = collections.OrderedDict()
            for field_id, spw_id in itertools.product(field_ids, selected_spws):
                LOG.info(f'field {field_id} spw {spw_id}')
                spw = str(spw_id)
                plotter.set_field(field_id)
                field_name = plotter.original_field_name
                summaries.setdefault(field_name, collections.OrderedDict())
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

        detail_page_title = 'ATM corrected amplitude vs frequency'
        renderer = basetemplates.JsonPlotRenderer('generic_x_vs_y_field_spw_ant_detail_plots.mako',
                                                  pipeline_context,
                                                  result,
                                                  detail_plots,
                                                  detail_page_title,
                                                  filenamer.sanitize('%s.html' % (detail_page_title.lower())))
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())

        mako_context.update({
            'summary_plots': summary_plots,
            'detail_page': renderer.path,
        })
