import itertools
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from .display import PlotmsRealVsFreqPlotter

LOG = logging.get_logger(__name__)


class T2_4MDetailsSingleDishATMCorRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='hsd_atmcor.mako',
                 description='Apply correction for atmospheric effects',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        super().update_mako_context(mako_context, pipeline_context, result)

        LOG.info('This is ATMCorRenderer')

        stage_dir = os.path.join(
            pipeline_context.report_dir,
            'stage{}'.format(result.stage_number)
        )
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        summary_plots = {}
        detail_plots = []
        for r in result:
            LOG.info('result for "%s"', r.inputs['vis'])
            if not hasattr(r, 'atmcorr_ms_name'):
                raise RuntimeError('Wrong result object is given.')

            vis = r.inputs['vis']
            atmvis = r.atmcorr_ms_name
            ms = pipeline_context.observing_run.get_ms(os.path.basename(vis))
            antennas = [a.name for a in ms.get_antenna()]
            field_names = [f.clean_name for f in ms.get_fields(intent='TARGET')]
            science_spws = [s.id for s in ms.get_spectral_windows(science_windows_only=True)]
            spw_selection = r.inputs['spw']
            if len(spw_selection) > 0:
                selected_spws = set(map(int, spw_selection.split(','))).intersection(science_spws)
            else:
                selected_spws = science_spws

            plotter = PlotmsRealVsFreqPlotter(
                vis=ms.basename, atmvis=atmvis,
                atmtype=r.inputs['atmtype'], output_dir=stage_dir
            )
            summaries = {}
            for field_name, spw_id in itertools.product(field_names, selected_spws):
                LOG.info(f'field {field_name} spw {spw_id}')
                spw = str(spw_id)
                summaries.setdefault(field_name, {})
                plotter.set_field(field_name)
                plotter.set_spw(spw)
                p = plotter.plot()
                summaries[field_name][spw] = p
                for antenna_name in antennas:
                    plotter.set_antenna(antenna_name)
                    p = plotter.plot()
                    detail_plots.append(p)
            LOG.info('spw list %s', list(summaries.keys()))
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
