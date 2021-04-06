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


def find_position_gap(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    delta_ra = ra[1:] - ra[:-1]
    delta_dec = dec[1:] - dec[:-1]
    angle_abs = np.abs(np.arctan2(delta_dec, delta_ra)).flatten()
    angle_median = np.median(angle_abs)
    angle_mad = compute_mad(angle_abs)
    distance = np.hypot(delta_ra, delta_dec).flatten()
    distance_median = np.median(distance)
    distance_mad = compute_mad(distance)
    angle_threshold = np.pi / 4  # 45deg
    factor = 10
    angle_gap = np.where(np.abs(angle_abs - angle_median) > angle_threshold)[0]
    #angle_gap = np.where(np.abs(angle_abs - angle_median) > factor * angle_mad)[0]
    distance_gap = np.where(np.abs(distance - distance_median) > factor * distance_mad)[0]

    return angle_gap, distance_gap


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
