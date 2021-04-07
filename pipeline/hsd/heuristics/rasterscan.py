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


def find_position_gap(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    delta_ra = ra[1:] - ra[:-1]
    delta_dec = dec[1:] - dec[:-1]
    angle_rad = np.arctan2(delta_dec, delta_ra).flatten()
    angle_deg = angle_rad * 180 / np.pi
    num_angle = len(angle_deg)
    bin_width = 0.5
    hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width, left_bin=-180, right_bin=180)

    peak_indices = find_histogram_peak(hist)
    peak_values = [(bin_edges[i] + bin_edges[i]) / 2 for i in peak_indices]
    LOG.info('peak_values = %s', peak_values)
    num_peak = len(peak_indices)
    LOG.info('original angle: found %s peaks', num_peak)
    if num_peak == 3:
        # should be raster scan, but one of the raster direction is along 180deg
        # need angle shifting
        delta = 70
        angle_deg = (angle_deg + 360 + delta) % 360
        hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width, left_bin=0, right_bin=360 + delta)
        peak_indices = find_histogram_peak(hist)
        peak_values = [(bin_edges[i] + bin_edges[i]) / 2 for i in peak_indices]
        LOG.info('peak_values = %s', peak_values)
        num_peak = len(peak_indices)
        LOG.info('shifted angle: found %s peaks', num_peak)
    if num_peak != 2 or abs(180 - abs(peak_values[1] - peak_values[0])) > 5:
        #raise RuntimeError('Unexpected angle sequence.')
        LOG.warn('Unexpected angle sequence.')

    # angle_median = np.median(angle_abs)
    # angle_mad = compute_mad(angle_abs)
    distance = np.hypot(delta_ra, delta_dec).flatten()
    distance_median = np.median(distance)
    distance_mad = compute_mad(distance)
    angle_threshold = np.pi / 4  # 45deg
    factor = 10
    angle_gap = 0
    # angle_gap = np.where(np.abs(angle_abs - angle_median) > angle_threshold)[0]
    #angle_gap = np.where(np.abs(angle_abs - angle_median) > factor * angle_mad)[0]
    distance_gap = np.where(np.abs(distance - distance_median) > factor * distance_mad)[0]

    return angle_deg, distance, angle_gap, distance_gap, (hist, bin_edges), peak_values


def union_position_gap(gaps_list):
    num_gaps = len(gaps_list)
    assert num_gaps > 0
    assert np.all([isinstance(g, (list, np.ndarray)) for g in gaps_list])

    LOG.info(gaps_list)
    gaps_union, counts = np.unique(np.concatenate(gaps_list), return_counts=True)

    return gaps_union, counts


def merge_position_gap(gaps_list):
    num_gaps = len(gaps_list)
    listed_gaps, counts = union_position_gap(gaps_list)
    majority = num_gaps // 2 + 1
    LOG.info('majority %s, counts=%s', majority, counts)
    merged_gaps = listed_gaps[counts >= majority]

    return merged_gaps


class RasterScanHeuristic(api.Heuristic):
    def calculate(self, ra, dec):
        return None
