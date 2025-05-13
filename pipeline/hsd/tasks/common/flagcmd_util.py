from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure.casa_tools import quanta as qa

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pipeline.domain.datatable import DataTableImpl

LOG = infrastructure.logging.get_logger(__name__)


def mjdsec_to_time_selection(t: float) -> str:
    """Convert MJD seconds to time selection string.

    Args:
        t: MJD second to convert
    Returns:
        str: formatted datetime
    """
    time_quantity = qa.quantity(t, "s")
    return qa.time(time_quantity, form="ymd", prec=12)[0]


def merge_timerange(timerange_list: list[list[float]]) -> list[list[float]]:
    """Merge time ranges.

    Args:
        timerange_list: list of timeranges
    Returns:
        List: merged time ranges
    """
    if len(timerange_list) < 2:
        return timerange_list

    timegap_list = np.asarray([l1[0] - l0[1] for l0, l1 in zip(timerange_list, timerange_list[1:])])
    LOG.debug(f'timegap_list is {timegap_list}')

    # regard timegap <= 0.1msec as continuous
    time_gap_threshold = 1e-4
    gap_index = [-1] + np.where(timegap_list > time_gap_threshold)[0].tolist() + [len(timegap_list)]

    timerange_merged = [[timerange_list[i + 1][0], timerange_list[j][1]] for i, j in zip(gap_index, gap_index[1:])]
    LOG.debug(f'timerange_merged is {timerange_merged}')

    return timerange_merged


def datatable_rowid_to_timerange(
        datatable: DataTableImpl,
        datatable_rows: Sequence[int]
) -> list[str]:
    """Convert datatable row ids to time range.

    This function reads TIME and EXPOSURE columns from
    the datatable for given list of row ids, and
    generate time range strings from them.
    The time range strings are compatible with the
    one used in the flagcmd.

    Args:
        datatable: DataTable object.
        datatable_rows: List of row ids.

    Returns:
        List of time range strings.
    """
    cmdlist = []
    time_list, sort_index = np.unique(
        [datatable.getcell('TIME', row) for row in datatable_rows],
        return_index=True
    )

    # day -> sec
    time_list *= 86400.0

    interval_list = [datatable.getcell('EXPOSURE', row) for row in np.asarray(datatable_rows)[sort_index]]

    timerange_list = [[t - i / 2, t + i / 2] for t, i in zip(time_list, interval_list)]

    timerange_merged = merge_timerange(timerange_list)
    cmdlist = [
        "~".join(map(mjdsec_to_time_selection, t)) for t in timerange_merged
    ]

    return cmdlist
