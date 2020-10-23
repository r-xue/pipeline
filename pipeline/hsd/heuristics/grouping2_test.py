import pytest

import functools
import numpy as np
import pipeline.infrastructure.casatools as casatools

from .grouping2 import GroupByPosition2
from .grouping2 import GroupByTime2
from .grouping2 import MergeGapTables2
from .grouping2 import ThresholdForGroupByTime

qa = casatools.quanta


def random_noise(n, mean=0, amp=1, rs=None):
    if rs is None:
        r = np.random.rand(n)
    else:
        r = rs.rand(n)
    return (r - (0.5 - mean)) * amp / 0.5


def generate_position_data_psw():
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
            ra_list.append(xnoise + x)
            dec_list.append(ynoise + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    return ra_list, dec_list


def generate_time_data_psw():
    time_list = np.arange(40, dtype=float)
    for gap, incr in [(10, 9), (20, 59), (30, 9)]:
        time_list[gap:] += incr
    return time_list


def generate_position_data_raster():
    xlist = np.arange(0, 1, 0.05)
    xlist = [xlist, xlist[::-1]]
    ylist = [0, 1]
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = 0.00001
    ra_list = []
    dec_list = []
    for x, y in zip(xlist, ylist):
        ndata = len(x)
        xnoise = random_noise(ndata, noise_mean, noise_amp, rs)
        ynoise = random_noise(ndata, noise_mean, noise_amp, rs)
        ra_list.append(xnoise + x)
        dec_list.append(ynoise + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    return ra_list, dec_list


def generate_time_data_raster():
    time_list = np.arange(40, dtype=float)
    time_list[20:] += 9
    return time_list


@pytest.fixture(name='position_psw')
@functools.lru_cache(1)
def fixture_position_psw():
    return generate_position_data_psw()


@pytest.fixture(name='position_raster')
@functools.lru_cache(1)
def fixture_position_raster():
    return generate_position_data_raster()


@functools.lru_cache(1)
def posdict_normal():
    posdict = dict((i, [-1, (i // 10) * 10]) for i in range(40))
    for i in range(0, 40, 10):
        posdict[i] = list(range(i, i + 10))
    return posdict


@functools.lru_cache(1)
def posdict_one():
    posdict = dict((i, [-1, 0]) for i in range(40))
    posdict[0] = list(range(40))
    return posdict


def posgap_psw():
    return [10, 20, 30]


def posgap_raster():
    return [20]


@functools.lru_cache(1)
def group_one():
    return [list(range(40))]


@functools.lru_cache(1)
def group_two():
    return [list(range(i, i + 20)) for i in (0, 20)]


@functools.lru_cache(1)
def group_four():
    return [list(range(i, i + 10)) for i in (0, 10, 20, 30)]


def time_table_psw():
    tt_small = group_four()
    tt_large = group_two()
    return [tt_small, tt_large]


def time_gap_psw():
    return [[10, 20, 30], [20]]


def time_table_raster():
    tt_small = group_two()
    tt_large = group_one()
    return [tt_small, tt_large]


def time_gap_raster():
    return [[20], []]


def merge_table_psw():
    return time_table_psw()


def merge_table_raster():
    tt_small = group_four()
    tt_large = group_one()
    return [tt_small, tt_large]


@pytest.mark.parametrize(
    "combine_radius, allowance_radius, expected_posdict, expected_posgap",
    [
        (0.4, 0.05, posdict_normal(), posgap_psw()),
        (qa.quantity(0.4, 'deg'), 0.05, posdict_normal(), posgap_psw()),
        (0.4, qa.quantity(0.05, 'deg'), posdict_normal(), posgap_psw()),
        (qa.quantity(0.4, 'deg'), qa.quantity(0.05, 'deg'), posdict_normal(), posgap_psw()),
        # unit conversion: rad -> deg
        (qa.quantity(0.4 * np.pi / 180, 'rad'), 0.05, posdict_normal(), posgap_psw()),
        # unit conversion: arcsec -> deg
        (qa.quantity(0.4 * 3600, 'arcsec'), 0.05, posdict_normal(), posgap_psw()),
    ]
)
def test_group_by_position_psw(combine_radius, allowance_radius, expected_posdict, expected_posgap, position_psw):
    '''test grouping by position on position switch pattern'''
    ra_list, dec_list = position_psw
    h = GroupByPosition2()
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    assert posdict == expected_posdict
    assert posgap == expected_posgap


@pytest.mark.parametrize(
    "combine_radius, allowance_radius, expected",
    [
        (0.249, 0.01, (posdict_normal(), posgap_raster())),
        (qa.quantity(0.249, 'deg'), 0.01, (posdict_normal(), posgap_raster())),
        (0.249, qa.quantity(0.01, 'deg'), (posdict_normal(), posgap_raster())),
        (qa.quantity(0.249, 'deg'), qa.quantity(0.01, 'deg'), (posdict_normal(), posgap_raster())),
        # too large allowance radius -> all gaps are detected
        (0.249, 10, (posdict_normal(), list(range(1, 40)))),
        # moderate allowance radius -> no gap is detected
        (0.249, 1, (posdict_normal(), [])),
        # too large combine radius -> only one group
        (10, 0.01, (posdict_one(), posgap_raster())),
        # too small combine radius -> all data are separated
        (1e-5, 0.01, (dict((i, [i]) for i in range(40)), posgap_raster())),
    ]
)
def test_group_by_position_raster(combine_radius, allowance_radius, expected, position_raster):
    '''test grouping by position on raster pattern including some edge cases'''
    ra_list, dec_list = position_raster
    h = GroupByPosition2()
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    assert posdict == expected[0]
    assert posgap == expected[1]


def test_group_by_posiition_error(position_psw):
    ra_list, dec_list = position_psw
    h = GroupByPosition2()

    # GroupByPosition2 only accepts numpy.ndarray for RA/DEC data
    with pytest.raises(AttributeError):
        posdict, posgap = h.calculate(ra_list.tolist(), dec_list, 0.4, 0.05)

    with pytest.raises(AttributeError):
        posdict, posgap = h.calculate(ra_list, dec_list.tolist(), 0.4, 0.05)


@pytest.mark.parametrize(
    'time_list, expected_gaps',
    [
        (generate_time_data_psw(), (5.0, 50.0)),
        (generate_time_data_raster(), (5.0, 50.0)),
    ]
)
def test_threshold_for_time(time_list, expected_gaps):
    '''test evaluation of threshold for time grouping'''
    h = ThresholdForGroupByTime()
    gaps = h.calculate(time_list)
    assert gaps == expected_gaps


@pytest.mark.parametrize(
    'time_list, expected',
    [
        (generate_time_data_psw(), (time_table_psw(), time_gap_psw())),
        (generate_time_data_raster(), (time_table_raster(), time_gap_raster())),
        # list input is allowed
        (generate_time_data_psw().tolist(), (time_table_psw(), time_gap_psw())),
    ]
)
def test_group_by_time(time_list, expected):
    '''test grouping by time'''
    h = GroupByTime2()
    time_list_np = np.asarray(time_list)
    delta_np = time_list_np[1:] - time_list_np[:-1]

    # delta can be either np.ndarray or list
    for delta in [delta_np, delta_np.tolist()]:
        time_table, time_gap = h.calculate(time_list, delta)
        assert time_table == expected[0]
        assert time_gap == expected[1]


@pytest.mark.parametrize(
    'time_gap, time_table, expected',
    [
        (time_gap_psw(), time_table_psw(), merge_table_psw()),
        (time_gap_raster(), time_table_raster(), merge_table_raster()),
    ]
)
def test_merge_gap_tables(time_gap, time_table, expected):
    '''test merging gap tables'''
    position_gaps = [10, 20, 30]
    beam_np = np.zeros(40, dtype=int)
    h = MergeGapTables2()

    # beam can be either np.ndarray or list
    for beam in (beam_np, beam_np.tolist()):
        merge_table, merge_gap = h.calculate(time_gap, time_table, position_gaps, beam)
        assert merge_table == expected
        assert merge_gap == [position_gaps, time_gap[1]]
