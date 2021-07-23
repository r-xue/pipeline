import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)

SnrTR = collections.namedtuple('SnrTR', 'vis threshold spw snr')
SpwMaps = collections.namedtuple('SpwMaps', 'ms spwmap scispws')
SpwPhaseupApplication = collections.namedtuple('SpwPhaseupApplication', 'ms gaintable calmode solint intent spw')


class T2_4MDetailsSpwPhaseupRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='spwphaseup.mako',
                 description='Spw phase offsets calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        spwmaps = []
        applications = []

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            if result.combine_spwmap:
                spwmap = result.combine_spwmap
            else:
                spwmap = result.phaseup_spwmap

            # Get science spws
            science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

            spwmaps.append(SpwMaps(ms.basename, spwmap, science_spw_ids))

            applications.extend(self.get_gaincal_applications(context, result.phaseup_result, ms))

        # Generate rows for phase SNR table.
        snr_table_rows = get_snr_table_rows(context, results)

        ctx.update({
            'snr_table_rows': snr_table_rows,
            'spwmaps': spwmaps,
            'applications': applications
        })

    def get_gaincal_applications(self, context, result, ms):
        applications = []

        calmode_map = {
            'p': 'Phase only',
            'a': 'Amplitude only',
            'ap': 'Phase and amplitude'
        }

        for calapp in result.final:
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
            a = SpwPhaseupApplication(ms.basename, gaintable, solint, calmode, to_intent, spw)
            applications.append(a)

        return applications


def get_snr_table_rows(context, results):
    rows = []

    for result in results:
        ms = context.observing_run.get_ms(result.vis)
        threshold = result.inputs['phasesnr']

        for row in result.snr_info:
            spwid = row[0]
            if row[1] is None:
                snr = '<strong class="text-danger">N/A</strong>'
            elif row[1] < threshold:
                snr = f'<strong class="text-danger">{row[1]:.1f}</strong>'
            else:
                snr = f'{row[1]:.1f}'

            rows.append(SnrTR(ms.basename, threshold, spwid, snr))

    return utils.merge_td_columns(rows)
