import argparse
import collections
import glob
import itertools
import math
from matplotlib.animation import FuncAnimation, ImageMagickWriter
import matplotlib.pyplot as plt
import numpy as np
from operator import sub
import os
import sys

import pipeline.domain.datatable as datatable
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.logging as logging


LOG = logging.get_logger(__name__)


MetaDataSet = collections.namedtuple(
    'MetaDataSet',
    ['timestamp', 'dtrow', 'field', 'antenna', 'ra', 'dec', 'srctype', 'pflag'])


def distance(x0, y0, x1, y1):
    """
    Compute distance between two points (x0, y0) and (x1, y1).

    :param x0: x-coordinate value for point 0
    :type x0: float
    :param y0: y-coordinate value for point 0
    :type y0: float
    :param x1: x-coordinate value for point 1
    :type x1: float
    :param y1: y-coordinate value for point 1
    :type y1: float
    :return: distance between two points
    :rtype: float
    """
    _dx = x1 - x0
    _dy = y1 - y0
    return np.hypot(_dx, _dy)


def read_readonly_data(table):
    timestamp = table.getcol('TIME')
    dtrow = np.arange(len(timestamp))
    ra = table.getcol('OFS_RA')
    dec = table.getcol('OFS_DEC')
    srctype = table.getcol('SRCTYPE')
    antenna = table.getcol('ANTENNA')
    field = table.getcol('FIELD_ID')
    return timestamp, dtrow, ra, dec, srctype, antenna, field


def read_readwrite_data(table):
    pflags = table.getcol('FLAG_PERMANENT')
    pflag = pflags[0, datatable.OnlineFlagIndex, :]
    return pflag


def read_datatable(datatable):
    """
    extract necessary data from datatable instance.

    :param datatable: datatable instance
    :type datatable: DataTableImpl
    :return: metadata
    :rtype: MetaDataSet
    """
    timestamp, dtrow, ra, dec, srctype, antenna, field = read_readonly_data(datatable)
    pflag = read_readwrite_data(datatable)
    metadata = MetaDataSet(
        timestamp=timestamp,
        dtrow=dtrow,
        field=field,
        antenna=antenna,
        ra=ra, dec=dec,
        srctype=srctype,
        pflag=pflag)

    return metadata


def from_context(context_dir):
    """
    read DataTable located in the context directory.

    NOTE: only one DataTable will be loaded for multi-EB run

    :param context_dir: path to the pipeline context directory
    :type context_dir: str
    :return: metadata
    :rtype: MetaDataSet
    """
    datatable_dir = os.path.join(context_dir, 'MSDataTable.tbl')
    rotable = glob.glob(f'{datatable_dir}/*.ms/RO')[0]
    rwtable = glob.glob(f'{datatable_dir}/*.ms/RW')[0]

    tb = casatools.table

    tb.open(rotable)
    try:
        timestamp, dtrow, ra, dec, srctype, antenna, field = read_readonly_data(tb)
    finally:
        tb.close()

    tb.open(rwtable)
    try:
        pflag = read_readwrite_data(tb)
    finally:
        tb.close()

    metadata = MetaDataSet(
        timestamp=timestamp,
        dtrow=dtrow,
        field=field,
        antenna=antenna,
        ra=ra, dec=dec,
        srctype=srctype,
        pflag=pflag)

    return metadata


def get_science_target_fields(metadata):
    """
    Get list of field ids for science targets.

    :param metadata: metadata
    :type metadata: MetaDataSet
    :return: list of field ids for science targets
    :rtype: ndarray
    """
    return np.unique(metadata.field[metadata.srctype == 0])


def filter_data(metadata, field_id, antenna_id, onsource=True):
    """
    Filter metadata.

    :param metadata: input metadata
    :type metadata: MetaDataSet
    :param field_id: field id
    :type field_id: int
    :param antenna_id: antenna id
    :type antenna_id: int
    :param onsource: take ON_SOURCE data only, defaults to True
    :type onsource: bool, optional
    :raises RuntimeError: filter causes empty result
    :return: filtered metadata
    :rtype: MetaDataSet
    """
    mask = np.logical_and(
        metadata.antenna == antenna_id,
        metadata.field == field_id
    )
    if onsource == True:
        mask = np.logical_and(mask, metadata.srctype == 0)
        srctype = 0
    else:
        srctype = None

    metadata2 = MetaDataSet(
        timestamp=metadata.timestamp[mask],
        dtrow=metadata.dtrow[mask],
        field=field_id,
        antenna=antenna_id,
        ra=metadata.ra[mask],
        dec=metadata.dec[mask],
        srctype=srctype,
        pflag=metadata.pflag[mask]
    )

    if len(metadata2.timestamp) == 0:
        raise RuntimeError('No data available for field ID {} antenna ID {} {}'.format(
            field_id,
            antenna_id,
            '(ON_SOURCE)' if onsource else ''
        ))

    return metadata2


def squeeze_data(metadata):
    """
    Make timestamp in input metadata unique.

    :param metadata: input metadata
    :type metadata: MetaDataSet
    :return: metadata without duplication of timestamp
    :rtype: MetaDataSet
    """
    utime, idx = np.unique(metadata.timestamp, return_index=True)

    urow = metadata.dtrow[idx]
    ura = metadata.ra[idx]
    udec = metadata.dec[idx]
    uflag = metadata.pflag[idx]
    if isinstance(metadata.field, (int, np.int32, np.int64)):
        ufield = metadata.field
    else:
        ufield = metadata.field[idx]
    if isinstance(metadata.antenna, (int, np.int32, np.int64)):
        uant = metadata.antenna
    else:
        uant = metadata.antenna[idx]
    if isinstance(metadata.srctype, (int, np.int32, np.int64)) or metadata.srctype is None:
        usrctype = metadata.srctype
    else:
        usrctype = metadata.srctype[idx]
    metadata2 = MetaDataSet(
        timestamp=utime,
        dtrow=urow,
        field=ufield,
        antenna=uant,
        ra=ura, dec=udec,
        srctype=usrctype,
        pflag=uflag
    )
    return metadata2


def find_time_gap(timestamp):
    """
    Find time gap. Condition for gap is

      - time interval > 3 * median(time interval) for small gap
      - time gap > 3 * median(time gap) for large gap

    :param timestamp: list of timestamp. no duplication. must be sorted in ascending order.
    :type timestamp: ndarray
    :return: list of small and large time gaps
    :rtype: two-tuple of lists
    """
    dt = timestamp[1:] - timestamp[:-1]
    med = np.median(dt)
    gsmall = np.where(dt > 3 * med)[0]
    med2 = np.median(dt[gsmall])
    glarge = np.where(dt > 3 * med2)[0]
    return gsmall, glarge


def gap_gen(gaplist, length=None):
    """
    Generate range of data (start and end indices) from
    given gap list. Return values, s and e, can be used to
    arr[s:e] to extract the data from the original array, arr.

    :param gaplist: list of indices indicating gap
    :type gaplist: list
    :param length: total number of data, defaults to None
    :type length: int, optional
    :yield: start and end indices
    :rtype: two tuple of integers
    """
    n = -1 if length is None else length
    if len(gaplist) == 0:
        yield 0, n
    else:
        yield 0, gaplist[0] + 1
        for i, j in zip(gaplist[:-1], gaplist[1:]):
            yield i + 1, j + 1
        yield gaplist[-1] + 1, n


def get_raster_distance(ra, dec, gaplist):
    """
    Compute list of distances between raster rows.
    Origin of the distance is the first raster row.
    Representative position of each raster row is
    its midpoint (mean position).

    :param ra: list of RA
    :type ra: ndarray
    :param dec: list of Dec
    :type dec: ndarray
    :param gaplist: list of indices indicating gaps between raster rows
    :type gaplist: list
    :return: list of distances between raster rows
    :rtype: ndarray
    """
    x1 = ra[:gaplist[0] + 1].mean()
    y1 = dec[:gaplist[0] + 1].mean()

    distance_list = np.fromiter(
        (distance(ra[s:e].mean(), dec[s:e].mean(), x1, y1) for s, e in gap_gen(gaplist)),
        dtype=float)

    return distance_list


def find_raster_gap(timestamp, ra, dec, time_gap=None):
    """
    Find gaps between individual raster map. Returned list should be
    used in combination with gap_gen. Here is an example to plot
    RA/DEC data per raster map:

        import maplotlib.pyplot as plt
        gap = find_raster_gap(timestamp, ra, dec)
        for s, e in gap_gen(gap):
            plt.plot(ra[s:e], dec[s:e], '.')

    :param timestamp: list of time stamp
    :type timestamp: ndarray
    :param ra: list of RA
    :type ra: ndarray
    :param dec: list of Dec
    :type dec: ndarray
    :param time_gap: list of index of time gaps, defaults to None
    :type time_gap: ndarray, optional
    :return: list of index indicating boundary between raster maps
    :rtype: ndarray
    """
    if time_gap is None:
        timegap_small, _ = find_time_gap(timestamp)
    else:
        timegap_small = time_gap

    distance_list = get_raster_distance(ra, dec, timegap_small)
    delta_distance = distance_list[1:] - distance_list[:-1]
    idx = np.where(delta_distance < 0)
    raster_gap = timegap_small[idx]
    return raster_gap


def flag_incomplete_raster(meta, raster_gap, nd_raster, nd_row):
    """
    flag incomplete raster map
    N: number of data per raster map
    M: number of data per raster row
    MN: median of N => typical number of data per raster map
    MM: median of M => typical number of data per raster row
    logic:
      - if N[x] < MN + MM then flag whole data in raster map x
      - if N[x] > MN + MM then flag whole data in raster map x and later

    :param meta: metadata object
    :type meta: MetaDataSet
    :param raster_gap: gap list
    :type raster_gap: list
    :param nd_raster: typical number of data per raster map (MN)
    :type nd_raster: int
    :param nd_row: typical number of data per raster row (MM)
    :type nd_row: int
    :return: list of index for raster map
    :rtype: list
    """
    gap = gap_gen(raster_gap, len(meta.timestamp))
    nd = np.asarray([e - s for s, e in gap])
    assert nd_raster >= nd_row
    upper_threshold = nd_raster + nd_row
    lower_threshold = nd_raster - nd_row

    # nd exceeds upper_threshold
    test_upper = nd >= upper_threshold
    idx = np.where(test_upper)[0]
    if len(idx) > 0:
        test_upper[idx[-1]:] = True
    LOG.debug(f'test_upper={test_upper}')

    # nd is less than lower_threshold
    test_lower = nd <= lower_threshold
    LOG.debug(f'test_lower={test_lower}')

    idx = np.where(np.logical_or(test_upper, test_lower))[0]

    return idx


def flag_worm_eaten_raster(meta, raster_gap, nd_row):
    """
    flag raster map if number of continuous flagged data
    exceeds upper limit given by nd_row
    M: number of data per raster row
    MM: median of M => typical number of data per raster row
    L: maximum length of continuous flagged data
    logic:
      - if L[x] > MM then flag whole data in raster map x

    :param meta: metadata object
    :type meta: MetaDataSet
    :param raster_gap: gap list
    :type raster_gap: list
    :param nd_row: typical number of data per raster row (MM)
    :type nd_row: int
    :return: list of index for raster map
    :rtype: list
    """
    gap = gap_gen(raster_gap, len(meta.timestamp))
    # flag
    # 1: valid, 0: invalid
    flag_raster = [meta.pflag[s:e] for s, e in gap]
    LOG.debug(f'Typical number of data per raster row: {nd_row}')
    flag_continuous = [
        np.fromiter(
            map(sum, (f[i:i + nd_row] for i in range(len(f) - nd_row + 1))),
            dtype=int
        )
        for f in flag_raster
    ]
    min_count = np.fromiter(
        (x.min() for x in flag_continuous),
        dtype=int
    )
    LOG.debug(f'Minimum number of continuous valid data: {min_count}')
    test = min_count == 0
    LOG.debug(f'test={test}')

    idx = np.where(test)[0]

    return idx


def get_raster_flag_list(flagged1, flagged2, raster_gap, ndata):
    """
    Merge flag result and convert raster id to list of data index.

    :param flagged1: list of flagged raster id
    :type flagged1: list
    :param flagged2: list of flagged raster id
    :type flagged2: list
    :param raster_gap: list of gaps between raster maps
    :type raster_gap: list
    :param ndata: total number of data points
    :type ndata: int
    :return: list of data ids to be flagged
    :rtype: ndarray
    """
    flagged = set(flagged1).union(set(flagged2))
    gap = list(gap_gen(raster_gap, ndata))
    g = (range(*gap[i]) for i in flagged)
    data_ids = np.fromiter(itertools.chain(*g), dtype=int)
    return data_ids


def flag_raster_map(metadata):
    """
    Return list of index to be flagged by flagging heuristics
    for raster scan

    :param meta: metadata
    :type meta: MetaDataSet
    :return: per-antenna list of index to be flagged
    :rtype: list
    """
    field_list = get_science_target_fields(metadata)

    rows_per_field = [flag_raster_map_per_field(metadata, f) for f in field_list]
    rows_per_antenna = zip(*rows_per_field)
    rows_merged = list(map(np.concatenate, rows_per_antenna))

    return rows_merged


def find_most_frequent(v):
    """
    Return the most frequent number (mode) in v.

    :param v: data
    :type v: ndarray
    :return: most frequent number (mode)
    :rtype: int
    """
    values, counts = np.unique(v, return_counts=True)
    max_count = counts.max()
    LOG.trace(f'max count: {max_count}')
    modes = values[counts == max_count]
    LOG.trace(f'modes: {modes}')
    if len(modes) > 1:
        mode = modes.max()
    else:
        mode = modes[0]
    LOG.trace(f'mode = {mode}')

    return mode


def flag_raster_map_per_field(metadata, field_id):
    """
    Flag raster map based on two flagging heuristics for
    given field id.

    :param metadata: metadata
    :type metadata: MetaDataSet
    :param field_id: field id to process
    :type field_id: int
    :return: per-antenna list of data ids to be flagged
    :rtype: list of ndarray
    """
    # metadata per antenna (with duplication)
    antenna_list = np.unique(metadata.antenna)
    meta_per_ant_dup = [filter_data(metadata, field_id, a) for a in antenna_list]

    # metadata per antenna (without duplication)
    meta_per_ant = [squeeze_data(meta) for meta in meta_per_ant_dup]
    ndata_per_ant = list(map(lambda x: len(x.timestamp), meta_per_ant))

    # get time gap
    time_gap_per_ant = [find_time_gap(m.timestamp)[0] for m in meta_per_ant]
    LOG.trace('{} {}'.format(len(meta_per_ant), len(time_gap_per_ant)))

    # get raster gap
    raster_gap_per_ant = [
        find_raster_gap(m.timestamp, m.ra, m.dec, gap)
        for m, gap in zip(meta_per_ant, time_gap_per_ant)
    ]

    # compute number of data per raster row
    num_data_per_raster_row = [
        [len(m.ra[s:e]) for s, e in gap_gen(gap)]
        for m, gap in zip(meta_per_ant, time_gap_per_ant)
    ]
    LOG.trace(num_data_per_raster_row)
    nd_per_row_rep = find_most_frequent(
        np.fromiter(itertools.chain(*num_data_per_raster_row), dtype=int)
    )
    LOG.debug('number of raster row: {}'.format(list(map(len, num_data_per_raster_row))))
    LOG.debug(f'most frequent # of data per raster row: {nd_per_row_rep}')

    # compute number of data per raster map
    num_data_per_raster_map = [
        [len(m.ra[s:e]) for s, e in gap_gen(gap)]
        for m, gap in zip(meta_per_ant, raster_gap_per_ant)
    ]
    LOG.trace(num_data_per_raster_map)
    nd_per_raster_rep = find_most_frequent(
        np.fromiter(itertools.chain(*num_data_per_raster_map), dtype=int)
    )
    LOG.debug('number of raster map: {}'.format(list(map(len, num_data_per_raster_map))))
    LOG.debug(f'most frequent # of data per raster map: {nd_per_raster_rep}')
    LOG.debug('nominal number of row per raster map: {}'.format(nd_per_raster_rep // nd_per_row_rep))

    # flag incomplete raster map
    flag_raster1 = [
        flag_incomplete_raster(m, gap, nd_per_raster_rep, nd_per_row_rep)
        for m, gap in zip(meta_per_ant, raster_gap_per_ant)
    ]

    # flag raster map if it contains continuous flagged data
    # whose length is larger than the number of data per raster row
    flag_raster2 = [
        flag_worm_eaten_raster(m, gap, nd_per_row_rep)
        for m, gap in zip(meta_per_ant, raster_gap_per_ant)
    ]

    # merge flag result and convert raster id to list of data index
    flag_list = [
        get_raster_flag_list(f1, f2, gap, n)
        for f1, f2, gap, n in zip(flag_raster1, flag_raster2, raster_gap_per_ant, ndata_per_ant)
    ]
    LOG.trace(flag_list)

    # get timestamp list
    time_list = [
        set((m.timestamp[i] for i in f))
        for f, m in zip(flag_list, meta_per_ant)
    ]

    # convert timestamp list into row list
    row_list = [
        m.dtrow[[x in t for x in m.timestamp]]
        for t, m in zip(time_list, meta_per_ant_dup)
    ]

    return row_list


def get_aspect(ax):
    """
    Compute aspect ratio of matplotlib figure.

    :param ax: Axes object to examine
    :type ax: Axes
    :return: aspect ratio
    :rtype: float
    """
    # Total figure size
    figW, figH = ax.get_figure().get_size_inches()
    # Axis size on figure
    _, _, w, h = ax.get_position().bounds
    # Ratio of display units
    disp_ratio = (figH * h) / (figW * w)
    # Ratio of data units
    # Negative over negative because of the order of subtraction
    data_ratio = sub(*ax.get_ylim()) / sub(*ax.get_xlim())

    return disp_ratio / data_ratio


def get_angle(dx, dy, aspect_ratio=1):
    """
    Compute tangential angle taking into account aspect ratio.

    :param dx: length along x-axis
    :type dx: float
    :param dy: length along y-axis
    :type dy: float
    :param aspect_ratio: aspect_ratio, defaults to 1
    :type aspect_ratio: float, optional
    :return: tangential angle
    :rtype: float
    """
    offset = 30
    theta = math.degrees(math.atan2(dy * aspect_ratio, dx))
    return offset + theta


def anim_gen(ra, dec, idx_generator, dist_list, cmap):
    """
    Generator for generate_animation.

    :param ra: list of RA
    :type ra: ndarray
    :param dec: list of Dec
    :type dec: ndarray
    :param idx_generator: generator yielding start and end indices indicating raster row
    :type idx_generator: generator
    :param dist_list: distance list
    :type dist_list: ndarray
    :param cmap: color map
    :type cmap: ListedColorMap
    :yield: position, color, and boolean flag to clear existing plot
    :rtype: tuple
    """
    dist_prev = 0
    cidx = 0
    raster_flag = False
    for idx, dist in zip(idx_generator, dist_list):
        print('{} - {} = {}'.format(dist, dist_prev, dist - dist_prev))
        if dist - dist_prev < 0:
            print('updating cidx {}'.format(cidx))
            cidx = (cidx + 1) % cmap.N
            raster_flag = True
        color = cmap(cidx)
        yield ra[idx[0]:idx[1]], dec[idx[0]:idx[1]], color, raster_flag
        dist_prev = dist
        raster_flag = False

    raster_flag = True
    cidx = 0
    color = cmap(cidx)
    yield None, None, color, raster_flag


def animate(i):
    """
    Generate plot corresponding to single frame

    :param i: position, color, and boolean flag to clear existing plot
    :type i: tuple
    :return: lines for this frame
    :rtype: Lines2D
    """
    ra, dec, c, flag = i
    print(c)
    if flag is True:
        # clear existing raster scan
        for l in plt.gca().get_lines()[1:]:
            l.remove()
    if ra is None:
        return []

    dx = np.median(ra[1:] - ra[:-1])
    dy = np.median(dec[1:] - dec[:-1])
    aspect = get_aspect(plt.gca())
    angle = get_angle(dx, dy, aspect)
    lines = plt.plot(ra, dec, marker=(3, 0, angle), color=c, linewidth=1, markersize=6)
    return lines


def generate_animation(ra, dec, gaplist, figfile='movie.gif'):
    """
    Generate animation GIF file to illustrate observing pattern.

    :param ra: list of RA
    :type ra: ndarray
    :param dec: list of Dec
    :type dec: ndarray
    :param gaplist: list of gaps between raster rows
    :type gaplist: list
    :param figfile: output file name, defaults to 'movie.gif'
    :type figfile: str, optional
    """
    row_distance = get_raster_distance(ra, dec, gaplist)
    cmap = plt.get_cmap('tab10')

    fig = plt.figure()
    plt.clf()
    anim = FuncAnimation(
        fig, animate,
        anim_gen(ra, dec, gap_gen(gaplist), row_distance, cmap),
        init_func=lambda: plt.plot(ra, dec, '.', color='gray', markersize=2),
        blit=True)
    anim.save(figfile, writer=ImageMagickWriter(fps=4))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Generate gif animation of raster pattern'
    )
    parser.add_argument('context_dir', type=str, help='context directory')
    parser.add_argument('-a', '--antenna', action='store', dest='antenna', type=int, default=0)
    parser.add_argument('-f', '--field', action='store', dest='field', type=int, default=-1)
    args = parser.parse_args()
    print('DEBUG: antenna={}'.format(args.antenna))
    print('DEBUG: field={}'.format(args.field))
    print('DEBUG: context_dir="{}"'.format(args.context_dir))

    metadata = from_context(args.context_dir)

    # Field ID to process
    science_targets = get_science_target_fields(metadata)
    print(f'DEBUG: science target list: {science_targets}')
    if args.field == -1:
        field = science_targets[0]
    else:
        field = args.field

    if field not in science_targets:
        print(f'ERROR: science target field {field} does not exist')
        sys.exit(1)

    # ON-SOURCE with Antenna selection
    metaon = filter_data(metadata, field, args.antenna, True)

    utime = metaon.timestamp
    ura = metaon.ra
    udec = metaon.dec
    if len(utime) == 0:
        print('ERROR: antenna {} for field {} does not exist'.format(args.antenna, field))
        sys.exit(1)

    gsmall, glarge = find_time_gap(utime)

    figfile = 'pointing.field{}ant{}.gif'.format(field, args.antenna)
    generate_animation(ura, udec, gsmall, figfile=figfile)
