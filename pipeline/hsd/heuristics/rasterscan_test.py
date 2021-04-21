"""
Test for heuristics defined in rasterscan.py.

Configuration of Raster scan:
  - raster scan consists of two raster maps
  - each raster map consists of two raster rows
  - configuration options:
    - row scan direction: round-trip or one-way
    - map scan direction: round-trip or one-way
    - scan angle: horizontal, vertical, arbitrary angle
    - ratio of raster row interval to pointing interval: <1, 1, >1
    - pointing error: 0 to 10% of pointing interval

Failure Cases:
  - non-raster pattern (PSW)
  - large pointing error: pointing error comparable to pointing interval
"""
import math
from typing import Tuple

import numpy as np
import pytest

from .rasterscan import RasterScanHeuristic, RasterScanHeuristicsFailure


def random_noise(n: int, mean: int = 0, amp: int = 1, rs: np.random.mtrand.RandomState = None) -> np.ndarray:
    """Generate random noise.

    Generate random noise with given mean and maximum amplitude.
    Seed for random noise can be specified.

    Args:
        n (int): number of random noise
        mean (int, optional): mean value of random noise. Defaults to 0.
        amp (int, optional): maximum amplitude of random noise. Defaults to 1.
        rs (np.random.mtrand.RandomState, optional): seed for random noise. Defaults to None.

    Returns:
        np.ndarray: random noise
    """
    if rs is None:
        r = np.random.rand(n)
    else:
        r = rs.rand(n)
    return (r - (0.5 - mean)) * amp / 0.5


def generate_position_data_psw() -> Tuple[np.ndarray, np.ndarray]:
    """Generate position data for simulated position-switch observation.

    Generate position data for simulated position-switch observatin.
    The observation consists of four positions in 2x2 grids, (0,0),
    (0,1), (1,0), and (1,1). Each position has ten data that contains
    random noise around commanded position.

      y

      |  (0,1)   (1,1)
      |  +       +
      |
      |  (0,0)   (1,0)
      |  +       +
      |
      -----------------  x

    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    xlist = [0, 1]
    ylist = [0, 1]
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = 0.02
    num_pointings_per_grid = 10
    ra_list = []
    dec_list = []
    for x in xlist:
        for y in ylist:
            xnoise = random_noise(num_pointings_per_grid, noise_mean, noise_amp, rs)
            ynoise = random_noise(num_pointings_per_grid, noise_mean, noise_amp, rs)
            for _ in range(3):
                ra_list.append(xnoise + x)
                dec_list.append(ynoise + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    return ra_list, dec_list


def generate_position_data_raster(
    oneway_row: bool = False,
    oneway_map: bool = True,
    scan_angle: float = 0.0,
    interval_factor: float = 1.0,
    pointing_error: float = 0.1
) -> Tuple[np.ndarray, np.ndarray]:
    """Generate position data for simulated OTF raster observation.

    Generate position data for simulated OTF raster observation
    along scan direction specified by scan_angle. Default scan
    angle is along the x-direction (R.A.). The observation consists
    of two raster rows. Each row has twenty continuously taken data
    that contains random noise around commanded position. Scanning
    directions are opposite in these two rows if oneway_row is False
    (default).

        y

        |
        |    <--------------------------------
      1 -  + + + + + + + + + + + + + + + + + + + +
        |
        |
      0 -  + + + + + + + + + + + + + + + + + + + +
        |    -------------------------------->
        |
        -----------------------------------------------  x

    Args:
        oneway_row: all raster rows goes the same direction specified
                    by scan angle if True. Otherwise, next raster row
                    goes opposite direction to the current row.
                    defaults to False.
        oneway_map: all raster maps start from the same point if True.
                    Otherwise, next raster map starts from the point
                    diagonal to the starting point of the current map.
                    defaults to True.
        scan_angle: scan angle in degree with respect to x-direction.
                    defaults to 0.
        interval_factor: spacing between raster rows as a fraction of
                         pointing interval. Larger value corresponds
                         to coarse spacing between rows. defaults to 1.0.
        pointing_error: pointing error factor as a fraction of pointing
                        interval. Larger value corresponds to larger error.
                        defaults to 0.1.
    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    x_interval = 0.1
    y_interval = x_interval * interval_factor
    print(f'pointing interval = {x_interval}, row interval = {y_interval}')
    xlist = np.arange(0, 1, x_interval)
    xlist2 = xlist if oneway_row is True else xlist[::-1]
    xlist = [xlist, xlist2] * 8
    ylist = np.arange(0, (len(xlist) + 0.1) * y_interval, y_interval)
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = x_interval * pointing_error
    print(f'Noise Amplitude = {noise_amp}')
    ra_list = []
    dec_list = []
    for x, y in zip(xlist, ylist):
        ndata = len(x)
        ra_list.append(x)
        dec_list.append(np.zeros(ndata, dtype=float) + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    if oneway_map is True:
        ra_list2 = ra_list
        dec_list2 = dec_list
    else:
        ra_list2 = ra_list[::-1]
        dec_list2 = dec_list[::-1]
    map_ra = np.concatenate((ra_list, ra_list2))
    map_dec = np.concatenate((dec_list, dec_list2))

    ndata = len(map_ra)
    xnoise = random_noise(ndata, noise_mean, noise_amp, rs)
    ynoise = random_noise(ndata, noise_mean, noise_amp, rs)
    map_ra += xnoise
    map_dec += ynoise

    angle_rad = math.radians(scan_angle)
    cost = math.cos(angle_rad)
    sint = math.sin(angle_rad)
    rot_map_ra = map_ra * cost - map_dec * sint
    rot_map_dec = map_ra * sint + map_dec * cost

    return rot_map_ra, rot_map_dec


@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor',
    [
        (False, True, 0.0, 1.0),
        (False, True, 30.0, 1.0),
        (False, True, 90.0, 1.0),
        (True, True, 0.0, 1.0),
        (True, True, 30.0, 1.0),
        (True, True, 90.0, 1.0),
        (True, False, 0.0, 1.0),
        (True, False, 30.0, 1.0),
        (True, False, 90.0, 1.0),
        (False, False, 0.0, 1.0),
        (False, False, 30.0, 1.0),
        (False, False, 90.0, 1.0),
        (True, True, 0.0, 0.5),
        (False, True, 0.0, 0.5),
    ]
)
def test_raster_gap_detection(oneway_row, oneway_map, scan_angle, interval_factor):
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    pointing_error = 0.1
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    # if oneway_map is False:
    #     pytest.skip('Round-trip raster scan is not supported')

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    h = RasterScanHeuristic()

    if oneway_map is False:
        # should raise RasterScanHeuristicsFailure
        with pytest.raises(RasterScanHeuristicsFailure):
            gaptable, gaplist = h(ra, dec)
    else:
        gaptable, gaplist = h(ra, dec)

        # total number of data is 320
        ndata = 320
        # total number of data per row is 10
        nrow = 10
        # each raster map consists of 16 raster rows
        nmap = nrow * 16
        gaptable_expected_small = [
            np.arange(s, s + nrow) for s in range(0, ndata, nrow)
        ]
        gaptable_expected_large = [
            np.arange(s, s + nmap) for s in range(0, ndata, nmap)
        ]
        gaptable_small = gaptable[0]
        gaptable_large = gaptable[1]
        assert len(gaptable_small) == len(gaptable_expected_small)
        assert len(gaptable_large) == len(gaptable_expected_large)
        for result, ref in zip(gaptable_small, gaptable_expected_small):
            assert np.array_equal(result, ref)
        for result, ref in zip(gaptable_large, gaptable_expected_large):
            assert np.array_equal(result, ref)

        gaplist_expected_small = np.arange(nrow, ndata, nrow, dtype=int)
        gaplist_expected_large = np.arange(nmap, ndata, nmap, dtype=int)
        gaplist_small = gaplist[0]
        gaplist_large = gaplist[1]
        assert np.array_equal(gaplist_small, gaplist_expected_small)
        assert np.array_equal(gaplist_large, gaplist_expected_large)