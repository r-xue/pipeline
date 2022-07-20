import collections
import glob
import os
import shutil
from typing import Dict, List

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)

PhaseTR = collections.namedtuple('PhaseTR', 'ms phase_field field_names')
SnrTR = collections.namedtuple('SnrTR', 'ms threshold field intent spw snr')
SpwMapInfo = collections.namedtuple('SpwMapInfo', 'ms intent field fieldid combine spwmap scanids scispws')
SpwPhaseupApplication = collections.namedtuple('SpwPhaseupApplication', 'ms gaintable calmode solint intent spw')
PhaseRmsTR = collections.namedtuple('PhaseRmsTR', 'ms type time median_phase_rms noisy_ant')


class T2_4MDetailsSpwPhaseupRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='spwphaseup.mako',
                 description='Spw phase offsets calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        # Get info on spectral window mappings.
        spwmaps = get_spwmaps(context, results)

        # Generate rows for phase SNR table.
        snr_table_rows = get_snr_table_rows(context, results)

        # Generate rows for phase calibrator mapping table.
        pcal_table_rows = get_pcal_table_rows(context, results)

        # Get info on phase caltable.
        applications = get_gaincal_applications(context, results)

        # Get info on the RMS plots and tables
        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results[0].stage_number)
        rmsplots = make_rms_plots(results, weblog_dir)
        phaserms_table_rows = get_phaserms_table_rows(context, results)

        # Update mako context.
        ctx.update({
            'applications': applications,
            'pcal_table_rows': pcal_table_rows,
            'snr_table_rows': snr_table_rows,
            'phaserms_table_rows': phaserms_table_rows,
            'spwmaps': spwmaps,
            'rmsplots' : rmsplots,
            'results' : results
        })


def get_gaincal_applications(context: Context, results: ResultsList) -> List[SpwPhaseupApplication]:
    """
    Return list of SpwPhaseupApplication entries that contain all the necessary
    information to show in a Phase-up caltable application table in the task
    weblog page.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of SpwPhaseupApplication instances.
    """
    applications = []

    calmode_map = {
        'p': 'Phase only',
        'a': 'Amplitude only',
        'ap': 'Phase and amplitude'
    }

    for result in results:
        ms = context.observing_run.get_ms(result.vis)

        for calapp in result.phaseup_result.final:
            solint = utils.get_origin_input_arg(calapp, 'solint')

            if solint == 'inf':
                solint = 'Infinite'

            # Convert solint=int to a real integration time.
            # solint is spw dependent; science windows usually have the same
            # integration time, though that's not guaranteed.
            if solint == 'int':
                in_secs = ['%0.2fs' % (dt.seconds + dt.microseconds * 1e-6)
                           for dt in utils.get_intervals(context, calapp)]
                solint = 'Per integration (%s)' % utils.commafy(in_secs, quotes=False, conjunction='or')

            gaintable = os.path.basename(calapp.gaintable)
            spw = ', '.join(calapp.spw.split(','))

            to_intent = ', '.join(calapp.intent.split(','))
            if to_intent == '':
                to_intent = 'ALL'

            calmode = utils.get_origin_input_arg(calapp, 'calmode')
            calmode = calmode_map.get(calmode, calmode)

            applications.append(SpwPhaseupApplication(ms.basename, gaintable, solint, calmode, to_intent, spw))

    return applications


def get_spwmaps(context: Context, results: ResultsList) -> List[SpwMapInfo]:
    """
    Return list of SpwMapInfo entries that contain all the necessary
    information to be shown in a Spectral Window Mapping table in the task
    weblog page.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of SpwMapInfo instances.
    """
    spwmaps = []

    for result in results:
        ms = context.observing_run.get_ms(result.vis)

        # Get science spws
        science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        if result.spwmaps:
            for (intent, field), spwmapping in result.spwmaps.items():
                # Get ID of field and scans.
                fieldid = ms.get_fields(name=[field])[0].id
                scanids = ", ".join(str(scan.id) for scan in ms.get_scans(scan_intent=intent, field=field))

                # Append info on spwmap to list.
                spwmaps.append(SpwMapInfo(ms.basename, intent, field, fieldid, spwmapping.combine, spwmapping.spwmap,
                                          scanids, science_spw_ids))
        else:
            spwmaps.append(SpwMapInfo(ms.basename, '', '', '', '', '', '', ''))

    return spwmaps


def get_pcal_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the phase calibrator mapping table.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase calibrator mapping table.
    """
    rows = []
    for result in results:
        if result.phasecal_mapping:
            ms = context.observing_run.get_ms(result.vis)
            for pfield, tcfields in result.phasecal_mapping.items():
                # Compose phase field string.
                pfieldid = ms.get_fields(name=[pfield])[0].id
                field_str = f"{pfield} (#{pfieldid})"

                rows.append(PhaseTR(ms.basename, field_str, ", ".join(sorted(tcfields))))

    return utils.merge_td_columns(rows)


def get_snr_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the phase SNR table.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase SNR table.
    """
    rows = []
    for result in results:
        ms = context.observing_run.get_ms(result.vis)
        if result.spwmaps:
            # Get phase SNR threshold, and present this in the table if the phase
            # SNR test was run during task.
            threshold = result.inputs['phasesnr']
            spwmapmode = result.inputs['hm_spwmapmode']
            if spwmapmode == 'auto':
                thr_str = str(threshold)
            else:
                thr_str = f"N/A <p>(hm_spwmapmode='{spwmapmode}')"

            for (intent, field), spwmapping in result.spwmaps.items():
                # Compose field string.
                fieldid = ms.get_fields(name=[field])[0].id
                field_str = f"{field} (#{fieldid})"

                # For each SpW in SNR info, create a row, and highlight when
                # the SNR was missing or below the phase SNR threshold.
                for row in spwmapping.snr_info:
                    spwid = row[0]
                    if row[1] is None:
                        snr = '<strong class="alert-danger">N/A</strong>'
                    elif row[1] < threshold:
                        snr = f'<strong class="alert-danger">{row[1]:.1f}</strong>'
                    else:
                        snr = f'{row[1]:.1f}'

                    rows.append(SnrTR(ms.basename, thr_str, field_str, intent, spwid, snr))
        else:
            rows.append(SnrTR(ms.basename, '', '', '', '', ''))

    return utils.merge_td_columns(rows)

def get_phaserms_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the phase rms results table.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase rms table.
    """
    rows = []
    for result in results:
        ms = context.observing_run.get_ms(result.vis)
        noisier_antennas = ''.join(result.phaserms_antout)
        if noisier_antennas == '':
            noisier_antennas = "None"
        total_time = f'{result.phaserms_totaltime:.1f}'
        cycle_time = f'{result.phaserms_cycletime:.1f}'
        phasermsp80 = result.phaserms_results['phasermsP80']
        phasermscyclep80 = result.phaserms_results['phasermscycleP80']
        phaserms_totaltime = f'{phasermsp80:.2f}'
        phaserms_cycletime = f'{phasermscyclep80:.2f}'
        
        rows.append(PhaseRmsTR(ms.basename, 'Total Time', total_time, \
                    phaserms_totaltime, noisier_antennas))
        rows.append(PhaseRmsTR(ms.basename, 'Cycle Time', cycle_time, \
                    phaserms_cycletime, noisier_antennas))
    return utils.merge_td_columns(rows)

#TODO: This is a copied verison of the function from PIPE-1264 lightly edited.
def make_rms_plots(results, weblog_dir: str) -> Dict[str, List[logger.Plot]]:
    """
    Create and return a list of renorm plots. 

    Args:
        results: the renormalization results. 
        weblog_dir: the weblog directory
    Returns:
        summary_plots: dictionary with MS with some additional html 
                    as the keys and lists of plot objects as the values
    """
    summary_plots = collections.defaultdict(list)
    for result in results:
        vis = os.path.basename(result.inputs['vis'])
#        specplot = spw_stats.get('spec_plot')
        specplot = glob.glob('uid*PIPE-692_SSF.png')
        if specplot:
            specplot=specplot[0]
#        specplot = 'uid___A002_Xc845c0_X2fea.ms_PIPE-692_SSF.png' #uid___A002_Xef72bb_X9d29.ms_PIPE-692_SSF.png'
        #specplot_path = f"RN_plots/{specplot}"
        specplot_path = f"{specplot}"
        if os.path.exists(specplot_path):
            LOG.trace(f"Copying {specplot_path} to {weblog_dir}")
            shutil.copy(specplot_path, weblog_dir)
            specplot_path = f'{weblog_dir}/{specplot_path}'
            plot = {vis : [logger.Plot(specplot_path,
                    x_axis='Fill in',
                    y_axis='Fill in',
                    parameters={'vis': vis, 
                                'desc': 'Plot Description'})]}
        else:
            LOG.debug(f"Failed to copy {specplot_path} to {weblog_dir}")
            plot = {}
    return plot