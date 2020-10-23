import datetime

import pytest

from .conversion import commafy, dequote, flatten, format_datetime, format_timedelta, mjd_seconds_to_datetime,\
    range_to_list, safe_split, unix_seconds_to_datetime


@pytest.mark.parametrize("inp, kwargs, expected", [
    ([], {}, ""),
    (['a'], {}, "'a'"),
    (['a', 'b'], {}, "'a' and 'b'"),
    (['a', 'b', 'c'], {}, "'a', 'b' and 'c'"),
    (["a", "b", "c"], {}, "'a', 'b' and 'c'"),
    (['a', 'b'], {'quotes': False}, "a and b"),
    (['25', '27', '29'], {'multi_prefix': 's'}, "s '25', '27' and '29'"),
    (['a', 'b', 'c'], {'separator': '; '}, "'a'; 'b' and 'c'"),
    (['a', 'b', 'c'], {'conjunction': '&'}, "'a', 'b' & 'c'"),
])
def test_commafy(inp, kwargs, expected):
    """Test commafy()"""
    assert commafy(inp, **kwargs) == expected


@pytest.mark.parametrize("inp, expected", [
    ('Field \'1\'', "Field 1"),
    ("Field \"1\"", "Field 1"),
    ('"Field 1", "Field 2"', "Field 1, Field 2"),
    ("'Field 1', 'Field 2'", "Field 1, Field 2"),
])
def test_dequote(inp, expected):
    """Test dequote()"""
    assert dequote(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ([1, 2], [1, 2]),
    ([[1], [2]], [1, 2]),
    ([[[1], [2]], [3]], [1, 2, 3]),
    ('abcd', ['a', 'b', 'c', 'd']),
    (['ab', 'cd'], ['ab', 'cd']),
])
def test_flatten(inp, expected):
    """Test flatten()"""
    it = flatten(inp)
    for el in expected:
        assert next(it) == el


def test_flatten_empty():
    """Test flatten() with empty input"""
    with pytest.raises(StopIteration):
        next(flatten([]))
    with pytest.raises(StopIteration):
        next(flatten(''))


@pytest.mark.parametrize("inp, kwargs, expected", [
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {}, '2020-01-01 12:34:56'),
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {'dp': 5}, '2020-01-01 12:34:56.00001'),
    (datetime.datetime(2020, 1, 1, 12, 34, 56, 7), {'dp': 6}, '2020-01-01 12:34:56.000007'),
])
def test_format_datetime(inp, kwargs, expected):
    """Test format_datetime()"""
    assert format_datetime(inp, **kwargs) == expected


@pytest.mark.parametrize("inp, kwargs, expected", [
    (datetime.timedelta(1), {}, '1 day, 0:00:00'),
    (datetime.timedelta(9, 8, 7, 6), {'dp': 5}, '9 days, 0:00:08.00601'),
    (datetime.timedelta(9, 8, 7, 6), {'dp': 6}, '9 days, 0:00:08.006007'),
])
def test_format_timedelta(inp, kwargs, expected):
    """Test format_timedelta()"""
    assert format_timedelta(inp, **kwargs) == expected


@pytest.mark.parametrize("inp, expected", [
    ([1, 2], [datetime.datetime(1858, 11, 17, 0, 0, 1), datetime.datetime(1858, 11, 17, 0, 0, 2)]),
    ([1, 1.5], [datetime.datetime(1858, 11, 17, 0, 0, 1), datetime.datetime(1858, 11, 17, 0, 0, 1, 500000)]),
])
def test_mjd_seconds_to_datetime(inp, expected):
    """Test mjd_seconds_to_datetime()"""
    assert mjd_seconds_to_datetime(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ('', []),
    ('1', [1]),
    ('1,2', [1, 2]),
    ('1~3,5,7~9', [1, 2, 3, 5, 7, 8, 9]),
])
def test_range_to_list(inp, expected):
    """Test range_to_list()"""
    assert range_to_list(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ("", ['']),
    ("Field 1", ["Field 1"]),
    ("Field 1, 'Field 2,3'", ['Field 1', "'Field 2,3'"]),
    ('"Field 1,2" , Field 3', ['"Field 1,2"', 'Field 3']),
])
def test_safe_split(inp, expected):
    """Test safe_split()"""
    assert safe_split(inp) == expected


@pytest.mark.parametrize("inp, expected", [
    ([1, 1.5], [datetime.datetime(1970, 1, 1, 0, 0, 1), datetime.datetime(1970, 1, 1, 0, 0, 1, 500000)]),
])
def test_unix_seconds_to_datetime(inp, expected):
    """Test unix_seconds_to_datetime()"""
    assert unix_seconds_to_datetime(inp) == expected
