import os

import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.h.tasks.common.displays import polcal

LOG = logging.get_logger(__name__)


class T2_4MDetailsPolcalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the Polcal task.
    """
    def __init__(self, uri='polcal.mako',
                 description='Polarisation Calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # As a multi-vis task, there is only 1 Result for Polcal.
        result = results[0]
        output_dir = os.path.join(pipeline_context.report_dir, 'stage%s' % result.stage_number)

        # Initialize required output for weblog.
        session_names = []
        vislists = {}
        refants = {}
        polfields = {}

        # Retrieve info for each session.
        for session_name, session_results in result.session.items():
            # Store session name and corresponding vislist.
            session_names.append(session_name)
            vislists[session_name] = session_results['vislist']

            # Store pol cal field name and refant.
            refants[session_name] = session_results['refant']
            polfields[session_name] = session_results['polcal_field_name']

        # Render summary plots for main task page.
        # TODO: disable for now: session MS not registered in context.
        #  Merge session MS, or create these during stage?
        # amp_vs_parang = self.create_amp_parang_plots(pipeline_context, output_dir, result)

        # Update the mako context.
        mako_context.update({
            'session_names': session_names,
            'vislists': vislists,
            'refants': refants,
            'polfields': polfields,
            # 'amp_vs_parang': amp_vs_parang,
        })

    @staticmethod
    def create_amp_parang_plots(context, output_dir, result):
        plots = {}

        for session_name, session_results in result.session.items():
            vis = session_results['session_vis']
            calto = callibrary.CalTo(vis=vis)
            plots[session_name] = polcal.AmpVsParangSummaryChart(context, output_dir, calto).plot()

        return plots
