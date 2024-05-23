"""
utils_test.py : Unit tests for "hsd/tasks/common/utils.py".

Unit tests for "hsd/tasks/common/utils.py"
"""
import datetime

import pytest

from .utils import mjd_to_datetime, mjd_to_datestring

test_cases = [
    (56839.91646527777, datetime.datetime(2014, 7, 1, 21, 59, 42, 599999)),
    (56839.91647013888, datetime.datetime(2014, 7, 1, 21, 59, 43, 19999)),
    (60414, datetime.datetime(2024, 4, 14, 0, 0, 0)),
    (60414.9999999999, datetime.datetime(2024, 4, 14, 23, 59, 59, 999991)),
    (60414.999999999985, datetime.datetime(2024, 4, 14, 23, 59, 59, 999999))
]


@pytest.mark.parametrize('mjd, expected', test_cases)
def test_mjd_to_datetime(mjd: float, expected: datetime.datetime):
    result = mjd_to_datetime(mjd)
    assert result.year == expected.year
    assert result.month == expected.month
    assert result.day == expected.day
    assert result.hour == expected.hour
    assert result.minute == expected.minute
    assert result.second == expected.second
    assert result.microsecond == expected.microsecond


test_cases = [
    (56839.91646527777, 'Tue Jul  1 21:59:42 2014 UTC'),
    (56839.91647013888, 'Tue Jul  1 21:59:43 2014 UTC'),
    (60414, 'Sun Apr 14 00:00:00 2024 UTC'),
    (60414.9999999999, 'Sun Apr 14 23:59:59 2024 UTC'),
    (60414.999999999985, 'Sun Apr 14 23:59:59 2024 UTC')
]


@pytest.mark.parametrize('mjd, expected', test_cases)
def test_mjd_to_datestring(mjd: float, expected: str):
    result = mjd_to_datestring(mjd, 'd')
    assert result == expected

    mjd_sec = mjd * 86400
    result = mjd_to_datestring(mjd_sec, 's')
    assert result == expected

    # default unit is 's'
    result = mjd_to_datestring(mjd_sec)
    assert result == expected
