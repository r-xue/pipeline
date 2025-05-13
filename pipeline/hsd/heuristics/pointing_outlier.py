import collections
import copy

import numpy as np

import pipeline.infrastructure.api as api
import pipeline.infrastructure.casa_tools as casa_tools

PointingOutlierHeuristicsResult = collections.namedtuple(
    "PointingOutlierHeuristicsResult",
    ["cx", "cy", "med_dist", "factor", "mask", "dist"]
)

# Variables below are the threshold factors for the heuristics.
# Which factor to use depends on the situation of the scan,
# especially, whether the last scan can be interrupted or not,
# and the number of raster rows per map.

# (1) Interrupted scan is not allowed by the system
#     The factor is 2.0 in this case.
FACTOR_NO_INTERRUPT = 2

# (2) Raster scan can be interrupted in the middle, and
#     number of raster rows per map is always greater than 2.
#     The factor is sqrt(13 * pi / 8) in this case.
FACTOR_INTERRUPT = 2.26

# (3) Raster scan can be interrupted in the middle, and
#     number of raster rows per map can be 2.
#     The factor is 4 * sqrt(5) / 3 in this case.
FACTOR_INTERRUPT_LESS_RASTER_ROWS = 2.982


def compute_distance(dir_frame: str,
                     ra: np.ndarray,
                     dec: np.ndarray,
                     ref_ra: float,
                     ref_dec: float) -> np.ndarray:
    """Compute distance from a reference position.

    Args:
        dir_frame: direction reference frame.
        ra: List of RA values in degrees.
        dec: List of DEC values in degrees.
        ref_ra: RA value of reference position in degrees.
        ref_dec: DEC value of reference position in degrees.

    Returns:
        List of distance values from the reference position in degrees.
    """
    me = casa_tools.measures
    qa = casa_tools.quanta
    ref_dir = me.direction(
        dir_frame,
        qa.quantity(ref_ra, "deg"),
        qa.quantity(ref_dec, "deg")
    )
    _dir = copy.deepcopy(ref_dir)

    def _dist(_ra, _dec):
        _dir['m0'] = qa.quantity(_ra, 'deg')
        _dir['m1'] = qa.quantity(_dec, 'deg')
        qdist = me.separation(_dir, ref_dir)
        return qa.convert(qdist, 'deg')['value']

    dist = np.asarray(
        [_dist(_ra, _dec) for _ra, _dec in zip(ra, dec)]
    )
    return dist


class PointingOutlierHeuristics(api.Heuristic):
    def calculate(self,
                  dir_frame: str,
                  x: np.ndarray,
                  y: np.ndarray,
                  iterate: bool = False) -> PointingOutlierHeuristicsResult:
        """Perform pointing outlier heuristics.

        Algorithm is as follows:

            1. take median of x and y coordinates -> med_x, med_y
            2. compute distance from (med_x, med_y) for each data -> dist
            3. take median of distance array, dist -> med_dist
            4. mask data if dist > threshold * med_dist
               (mask is False if dist > threshold * med_dist)
               threshold depends on the observing strategy
               - if interrupted scan can happen,
                 and there is a guarantee that number of raster rows > 2,
                 then threshold will be 2.26
               - if interrupted scan can happen, and number of raster
                 rows can be 2, then threshold will be 2.982
               - if interrupted scan should not happen, threshold will be 2.0
            5. if iterate is True, do the following:
                5-1. re-compute med_x and med_y using a subset of
                     the data where mask is True
                5-2. re-compute distance from updated (med_x, med_y)
                     to update dist
                5-3. take median of updated dist
                5-4. update mask using updated med_dist

        Args:
            dir_frame: direction reference frame
            x: List of RA values in degrees. For ephemeris source,
               RA values must be shifted in advance (use SHIFT_RA column
               in datatable).
            y: List of DEC values in degrees. For ephemeris source,
               DEC values must be shifted in advance (use SHIFT_DEC column
               in datatable).
            iterate: whether or not perform iteration. Default is False.

        Returns:
            PointingOutlierHeuristicsResult object, which includes:

                - cx: x coordinate of nominal center position
                - cy: y coordinate of nominal center position
                - med_dist: median distance from the nominal center
                - threshold: factor to derive the threshold from
                    median distance
                - mask: validity mask (False means flagged by this heuristic)
                - dist: computed distance array with respect to
                    the nominal center
        """
        med_x = np.median(x)
        med_y = np.median(y)
        distance = compute_distance(dir_frame, x, y, med_x, med_y)
        median_distance = np.median(distance)
        # raster scan can be interrupted, and number of raster rows > 2
        threshold = FACTOR_INTERRUPT
        if iterate:
            unflagged = distance <= threshold * median_distance
            med_x = np.median(x[unflagged])
            med_y = np.median(y[unflagged])
            distance = np.sqrt(np.square(x - med_x) + np.square(y - med_y))
            median_distance = np.median(distance)
        mask = distance <= threshold * median_distance

        return PointingOutlierHeuristicsResult(
            med_x,
            med_y,
            median_distance,
            threshold,
            mask,
            distance
        )
