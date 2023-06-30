import os

import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.utils as utils
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

        # Create local copy of pipeline context and register the polarisation
        # session MSes, to enable creation of session related plots that rely
        # on the MS being registered.
        pipeline_context = self.create_copy_pcontext_with_session_mses(pipeline_context, result)

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

        # Create amp vs. parallactic angle plots.
        amp_vs_parang = self.create_amp_parang_plots(pipeline_context, output_dir, result)

        # Create gain amp polarisation ratio vs. scan plots.
        amp_vs_scan_before, amp_vs_scan_after = self.create_amp_scan_plots(pipeline_context, result)

        # Create cross-hand phase vs. channel plots.
        phase_vs_channel = self.create_phase_channel_plots(pipeline_context, result)

        # Create X-Y gain amplitude vs. antenna plots.
        amp_vs_ant, ampratio_vs_ant = self.create_xy_amp_ant_plots(pipeline_context, result)

        # Render real vs. imaginary corrected XX,XY and XY,YX plots for the session.
        real_vs_imag = self.create_real_imag_plots(pipeline_context, output_dir, result)

        # Update the mako context.
        mako_context.update({
            'session_names': session_names,
            'vislists': vislists,
            'refants': refants,
            'polfields': polfields,
            'amp_vs_parang': amp_vs_parang,
            'amp_vs_scan_before': amp_vs_scan_before,
            'amp_vs_scan_after': amp_vs_scan_after,
            'phase_vs_channel': phase_vs_channel,
            'amp_vs_ant': amp_vs_ant,
            'ampratio_vs_ant': ampratio_vs_ant,
            'real_vs_imag': real_vs_imag,
        })

    @staticmethod
    def create_copy_pcontext_with_session_mses(context, result):
        # Create a copy of the pipeline context.
        LOG.debug("Creating local copy of pipeline context for weblog rendering.")
        context = utils.pickle_copy(context)

        # Register each session MS in this new copy of the pipeline context.
        for session_results in result.session.values():
            LOG.debug(f"Registering session MS {session_results['session_vis']} to local copy of pipeline context for"
                      f" weblog rendering.")
            session_ms = tablereader.MeasurementSetReader.get_measurement_set(session_results['session_vis'])
            context.observing_run.add_measurement_set(session_ms)

        return context

    @staticmethod
    def create_amp_parang_plots(context, output_dir, result):
        plots = {}
        for session_name, session_results in result.session.items():
            vis = session_results['session_vis']
            calto = callibrary.CalTo(vis=vis)
            plots[session_name] = polcal.AmpVsParangSummaryChart(context, output_dir, calto).plot()

        return plots

    @staticmethod
    def create_amp_scan_plots(context, result):
        plots_before, plots_after = {}, {}
        for session_name, session_results in result.session.items():
            plots_before[session_name] = polcal.AmpVsScanChart(
                context, result, session_results['init_gcal_result'].final).plot()
            plots_after[session_name] = polcal.AmpVsScanChart(
                context, result, session_results['final_gcal_result'].final).plot()

        return plots_before, plots_after

    @staticmethod
    def create_phase_channel_plots(context, result):
        plots = {}
        for session_name, session_results in result.session.items():
            plots[session_name] = polcal.PhaseVsChannelChart(
                context, result, session_results['polcal_phase_result'].final).plot()

        return plots

    @staticmethod
    def create_xy_amp_ant_plots(context, result):
        plots_amp, plots_ampratio = {}, {}
        for session_name, session_results in result.session.items():
            plots_amp[session_name] = polcal.AmpVsAntennaChart(
                context, result, session_results['xyratio_gcal_result'].final).plot()
            plots_ampratio[session_name] = polcal.AmpVsAntennaChart(
                context, result, session_results['xyratio_gcal_result'].final, correlation='/').plot()

        return plots_amp, plots_ampratio

    @staticmethod
    def create_real_imag_plots(context, output_dir, result):
        plots = {}
        for session_name, session_results in result.session.items():
            vis = session_results['session_vis']
            calto = callibrary.CalTo(vis=vis)
            plots[session_name] = polcal.RealVsImagChart(context, output_dir, calto, correlation='XX,YY').plot()
            plots[session_name].extend(polcal.RealVsImagChart(context, output_dir, calto, correlation='XY,YX').plot())

        return plots
