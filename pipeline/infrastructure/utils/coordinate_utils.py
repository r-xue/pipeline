""" Ulititiy methods related to coordinate calculations."""
from __future__ import annotations

import numpy as np
from astropy.coordinates import SkyCoord

import pipeline.infrastructure.utils.conversion as conversion


def angular_distances(dir_frame: str,
                      ra: np.ndarray,
                      dec: np.ndarray,
                      ref_ra: float,
                      ref_dec: float) -> np.ndarray:
    """Compute angular distances from a reference position.

    Args:
        dir_frame: direction reference frame.
        ra: List of RA values in degrees.
        dec: List of DEC values in degrees.
        ref_ra: RA value of reference position in degrees.
        ref_dec: DEC value of reference position in degrees.

    Returns:
        List of distance values from the reference position in degrees.
    """
    sky_frame = conversion.refcode_to_skyframe(dir_frame)
    ref_dir = SkyCoord(ra=ref_ra, dec=ref_dec, unit='deg', frame=sky_frame)

    _dir = SkyCoord(ra=ra, dec=dec, unit='deg', frame=sky_frame)
    dist = ref_dir.separation(_dir).degree

    return dist
