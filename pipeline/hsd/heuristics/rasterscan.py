"""
Heuristics to detect raster rows and maps

Examine spatial observing pattern in celestial coordinate
(R.A./Dec. etc.) to distinguish each raster row and each
raster map, which is a set of raster rows to cover whole
observing region.

Note that it does not check if observing pattern is raster.
"""
# import standard modules

# import 3rd party modules
import numpy as np
import scipy

# import pipeline submodules
import pipeline.infrastructure.api as api
import pipeline.infrastructure.logging as logging

LOG = logging.get_logger(__name__)


def get_func_compute_mad():
    # assuming X.Y.Z style version string
    scipy_version = scipy.version.full_version
    versioning = map(int, scipy_version.split('.'))
    major = next(versioning)
    minor = next(versioning)
    if major > 1 or (major == 1 and minor >= 5):
        return lambda x: scipy.stats.median_abs_deviation(x, scale='normal')
    elif major == 1 and minor >= 3:
        return scipy.stats.median_absolute_deviation
    else:
        raise NotImplementedError('No MAD function available in scipy. Use scipy 1.3 or higher.')


compute_mad = get_func_compute_mad()


def generate_histogram(arr, bin_width, left_bin, right_bin):
    nbin = int(right_bin - left_bin / bin_width) + 1
    left_bin_edge = left_bin - bin_width / 2
    right_bin_edge = right_bin + bin_width / 2
    return np.histogram(arr, bins=nbin, range=(left_bin_edge, right_bin_edge))


def find_histogram_peak(hist):
    h_threshold = hist.max() * 3 / 7
    idx = np.where(hist > h_threshold)[0]
    LOG.info('hist = %s', hist.tolist())
    LOG.info('threshold = %s, idx = %s', h_threshold, idx)
    if len(idx) == 1:
        num_peak = 1
        peak_indices = [idx]
    else:
        hist_gaps = []
        peak_ranges = []
        left = idx[0]
        for i in range(len(idx) - 1):
            l = idx[i]
            r = idx[i + 1]
            if np.any(hist[l:r] == 0):
                hist_gaps.append(i)
                peak_ranges.append((left, l))
                left = r
        num_peak = len(hist_gaps) + 1
        peak_ranges.append((left, idx[-1]))
        LOG.info('peak_ranges = %s', peak_ranges)
        peak_indices = []
        for l, r in peak_ranges:
            h = hist[l:r + 1]
            LOG.info('hist[%s,%s] = %s', l, r + 1, h)
            peak_indices.append(np.argmax(h) + l)
    LOG.info('peak_indices = %s', peak_indices)

    return peak_indices


def shift_angle(angle, delta):
    return (angle + 360 + delta) % 360


def find_distance_gap(delta_ra, delta_dec):
    distance = np.hypot(delta_ra, delta_dec).flatten()
    distance_median = np.median(distance)
    distance_mad = compute_mad(distance)
    factor = 10
    #
    # ASCII illustration for distance gap
    #
    #                      (gap)
    #  gap idx |   0   1     2     3
    #          | *---*---*-------*---*
    # data idx | 0   1   2       3   4
    #
    distance_gap = np.where(np.abs(distance - distance_median) > factor * distance_mad)[0] + 1
    return distance, distance_gap


def find_angle_gap(angle_deg: np.ndarray):
    num_angle = len(angle_deg)
    bin_width = 0.5
    hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width, left_bin=-180, right_bin=180)

    peak_indices = find_histogram_peak(hist)
    num_peak = len(peak_indices)
    LOG.info('original angle: found %s peaks', num_peak)
    if num_peak == 3:
        # should be raster scan, but one of the raster direction is along 180deg
        # need angle shifting
        delta = 70
        angle_deg = shift_angle(angle_deg, delta)
        hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width, left_bin=0, right_bin=360 + delta)
        peak_indices = find_histogram_peak(hist)
        num_peak = len(peak_indices)
        LOG.info('shifted angle: found %s peaks', num_peak)

    peak_values = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in peak_indices]
    LOG.info('peak_values = %s', peak_values)

    # warn if scan pattern is not likely to be raster
    # 1. check if number of peaks in angle histogram is 2 (assuming round-trip scan)
    # 2. difference of peak angles is around 180deg
    if num_peak != 2 or abs(180 - abs(peak_values[1] - peak_values[0])) > 5:
        LOG.warn('Scan pattern is not likely to be round-trip raster scan.')

    # acceptable angle deviation from peak angle in degree
    acceptable_deviation = 45
    acceptable_ranges = []
    angle_min = bin_edges.min()
    angle_max = bin_edges.max()
    for p in peak_values:
        upper = p + acceptable_deviation
        lower = p - acceptable_deviation
        if angle_min <= lower and upper <= angle_max:
            acceptable_ranges.append((lower, upper))
        elif angle_max < upper:
            upper = angle_min + (upper - angle_max)
            acceptable_ranges.append((lower, angle_max))
            acceptable_ranges.append((angle_min, upper))
        elif lower < angle_min:
            lower = angle_max - (angle_min - lower)
            acceptable_ranges.append((angle_min, upper))
            acceptable_ranges.append((lower, angle_max))
        else:
            msg = 'Inconsistent angle range. Aborting.'
            raise RuntimeError(msg)

    mask = np.empty(num_angle, dtype=bool)
    mask[:] = False
    for l, r in acceptable_ranges:
        in_range = np.logical_and(l <= angle_deg, angle_deg <= r)
        mask = np.logical_or(mask, in_range)

    # ASCII illustration for angle gap index
    #
    #         |                 *
    #  gap idx|   0   1   2   3 | 4 (gap)
    #         | *---*---*---*---*
    # data idx| 0   1   2   3   4
    #
    angle_gap = np.where(mask == False)[0] + 1
    num_data_between_gap = np.diff(angle_gap)
    num_data_median = np.median(num_data_between_gap)
    if not np.all(num_data_between_gap == num_data_median):
        # remove gaps that cause isolated data points
        isolated_indices = np.where(num_data_between_gap == 1)[0]
        if len(isolated_indices) > 0:
            angle_gap[isolated_indices + 1] -= 1
        angle_gap = np.unique(angle_gap)

    return angle_gap, hist, bin_edges, peak_indices


def find_raster_row(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    delta_ra = ra[1:] - ra[:-1]
    delta_dec = dec[1:] - dec[:-1]

    # heuristics based on distances between integrations
    distance, distance_gap = find_distance_gap(delta_ra, delta_dec)

    # heuristics based on scan direction
    angle_rad = np.arctan2(delta_dec, delta_ra).flatten()
    angle_deg = angle_rad * 180 / np.pi
    angle_gap, hist, bin_edges, peak_indices = find_angle_gap(angle_deg)
    merged_gap = np.union1d(angle_gap, distance_gap)
    num_data = len(ra)
    merged_gap = np.concatenate(([0], merged_gap, [num_data]))

    return angle_deg, distance, merged_gap, angle_gap, distance_gap, (hist, bin_edges), peak_indices


class RasterScanHeuristic(api.Heuristic):
    def calculate(self, ra, dec):
        return None
