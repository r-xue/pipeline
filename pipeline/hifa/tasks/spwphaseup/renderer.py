import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)

SpwMapInfo = collections.namedtuple('SpwMapInfo', 'ms intent field fieldid combine spwmap scanids scispws')
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

            # Get info on spectral window mappings.
            spwmaps.extend(self.get_spwmaps(result, ms))

            # Get info on phase caltable.
            applications.extend(self.get_gaincal_applications(context, result.phaseup_result, ms))

        ctx.update({
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

    def get_spwmaps(self, result, ms):
        spwmaps = []

        # Get science spws
        science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        if result.spwmaps:
            for (intent, field), spwmapping in result.spwmaps.items():
                # Get field ID.
                fieldid = ms.get_fields(name=[field])[0].id

                # Get scan IDs
                scanids = ", ".join(str(scan.id) for scan in ms.get_scans(scan_intent=intent, field=field))

                # Append info on spwmap to list.
                spwmaps.append(SpwMapInfo(ms.basename, intent, field, fieldid, spwmapping.combine, spwmapping.spwmap,
                                          scanids, science_spw_ids))

        return spwmaps
