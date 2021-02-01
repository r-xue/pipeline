"""Unit test for h/heuristics/linefinder.py module."""
import pytest
from . linefinder import HeuristicsLineFinder
from typing import NoReturn

spectrum_0 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 
            30.0, 15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
spectrum_1 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 
            30.0, 15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 
            10.0, 20.0, 40.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0]

@pytest.mark.parametrize("spectrum, threshold, min_nchan, avg_limit, box_size, tweak, mask, edge, expected", 
                         [
                             (spectrum_0, 7.0, 3, 2, 2, False, [], None, [8, 12]),
                             (spectrum_1, 7.0, 3, 2, 2, False, [], None, [8, 12, 20, 24]),
                             (spectrum_1, 7.0, 3, 2, 2, True, [], None, [8, 21, 11, 24]),
                             (spectrum_1, 7.0, 3, 2, 2, False, [7, 5], []),
                         ])

def test_linefinder(spectrum, threshold, min_nchan, avg_limit, box_size, tweak, mask, edge, expected) -> NoReturn:
    """
    Unit test for calculate method.

    Args:
        spectrum: list of spectrum data
        threshold: threshold value to detect lines
        min_nchan: minimum value of nchan
        avg_limit: average value of limit
        box_size: value of box size
        tweak: True or False of tweak
        mask: list of indice of mask
        edge: list of indice of edge
        expected  : expected result
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.calculate(spectrum, threshold, min_nchan, avg_limit, box_size, tweak, mask, edge) == expected

@pytest.mark.parametrize("spectrum, ranges, edge, n_ignore, expected", 
                         [
                             (spectrum_0, [8, 12], [0, 0], 1, [8, 12]),
                             (spectrum_1, [8, 12], [0, 0], 1, [8, 12]),
                             (spectrum_1, [20, 24], [0, 0], 1, [11, 24]),
                             (spectrum_1, [12, 20], [0, 0], 1, [11, 20]),                              
                         ])
def test_tweak_lines(spectrum, ranges, edge, n_ignore, expected) -> NoReturn:
    """
    Unit test for tweak_lines method.

    Args:
        spectrum: list of spectrum data
        ranges: list of indice of line range
        edge: list of indice of edge
        n_ignore: number of ignore
        expected  : expected result
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.tweak_lines(spectrum, ranges, edge, n_ignore) == expected
