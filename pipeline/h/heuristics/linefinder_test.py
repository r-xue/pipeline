"""Unit test for h/heuristics/linefinder.py module."""
import pytest
from . linefinder import HeuristicsLineFinder
from typing import NoReturn

spectrum = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 
            30.0, 15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 
            10.0, 20.0, 40.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0]
edge = [9, 4]
ranges = [8, 12]

def test_simple() -> NoReturn:
    """
    Args:
        none
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.calculate(spectrum) == [8, 12, 20, 24]

def test_linefinder() -> NoReturn:
    """
    Args:
        none
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.calculate(spectrum, threshold=7.0, min_nchan=3, avg_limit=2, 
                       box_size=2, tweak=False, mask=[], edge=edge) == []

def test_tweak_lines() -> NoReturn:
    """
    Args:
        none
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.tweak_lines(spectrum, ranges, edge, n_ignore=1) == [8, 12]
