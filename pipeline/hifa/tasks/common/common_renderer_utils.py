import collections

import pipeline.infrastructure.logging as logging
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)

SpwMapInfo = collections.namedtuple('SpwMapInfo', 'ms intent field fieldid combine spwmap scanids'
                                                  ' scispws solint gaintype')


def get_spwmaps(context: Context, results: ResultsList) -> list[SpwMapInfo]:
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
                                          scanids, science_spw_ids, spwmapping.solint, spwmapping.gaintype))
        else:
            spwmaps.append(SpwMapInfo(ms.basename, '', '', '', '', '', '', '', '', ''))

    return spwmaps
