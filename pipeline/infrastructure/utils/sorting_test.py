"""
Tests for the sorting.py module.
"""
import pytest

from . sorting import natural_sort, numeric_sort


test_params_natural_sort = (
    ("session9", ["session", 9, ""]),
    ("session10", ["session", 10, ""]),
    ("session10a", ["session", 10, "a"]),
    ("spw10", ["spw", 10, ""]),
    ("spw100", ["spw", 100, ""]),
    ("spw1", ["spw", 1, ""]),
    ("Spw10", ["spw", 10, ""]),
)

@pytest.mark.parametrize("input_list, expected", test_params_natural_sort)
def test_natural_sort(input_list, expected):
    """Test natural_sort()

    Natural sort splits a string into a list of elements defined by being sets 
    of digits or other types of characters. The digits are converted to 
    integers and the strings to lower case to allow a natural sorting when 
    using this list as the sort key. 
    >>> sorted((), key=utils.natural_sort)
    In its current implementation natural_sort returns an empty string element 
    if the input string ends with a number. 
    """
    assert natural_sort(input_list) == expected


test_params_numeric_sort = (
    (['9,11,13,15', '11,13', '9'], ['9', '9,11,13,15', '11,13']),
    (['9,11,13,15', '11,13', '10'], ['9,11,13,15', '10', '11,13'])
)

@pytest.mark.parametrize("input_list, expected", test_params_numeric_sort)
def test_numeric_sort(input_list, expected):
    """Test numeric_sort()

    An input list should be order numerically.
    """
    assert numeric_sort(input_list) == expected