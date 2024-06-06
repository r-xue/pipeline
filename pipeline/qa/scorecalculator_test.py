"""
scorecalculator_test.py : Unit tests for "qa/scorecalculator.py".

Unit tests for "qa/scorecalculator.py"
"""
from typing import List, Optional, Tuple

import pytest

import pipeline.qa.scorecalculator as qacalc


@pytest.mark.parametrize(
    'lines, expected',
    [
        ([(10, 5, True)], [(7, 13)]),
        ([(10, 5, False)], []),
    ]
)
def test_get_line_ranges(lines: List[Tuple[int, int, bool]], expected: List[Tuple[int, int]]):
    line_ranges = qacalc.get_line_ranges(lines)
    assert line_ranges == expected


@pytest.mark.parametrize(
    'lines, nchan, fraction, edge, expected',
    [
        ([(0, 100)], 1024, 1 / 3, False, False),
        ([(0, 100)], 1024, 1 / 3, True, False),
        ([(0, 400)], 1024, 1 / 3, False, True),
        ([(0, 400)], 1024, 1 / 3, True, True),
        ([(0, 115), (200, 315), (400, 515)], 1024, 1 / 3, False, True),
        ([(0, 115), (200, 315), (400, 515)], 1024, 1 / 3, True, False),
        ([(0, 100)], 1024, 1 / 20, False, True),
        ([(0, 100)], 1024, 1 / 20, True, True),
        ([(0, 200), (100, 250)], 1024, 1 / 4, False, False),
        ([(0, 200), (100, 250)], 1024, 1 / 4, True, False),
        ([(1, 500)], 1024, 1/3, False, True),
        ([(1, 500)], 1024, 1/3, True, False),
    ]
)
def test_line_wider_than(lines: List[Tuple[int, int]], nchan: int, fraction: float, edge: bool, expected: bool):
    is_wider = qacalc.line_wider_than(lines, nchan, fraction, edge)
    assert is_wider == expected


@pytest.mark.parametrize(
    'lines, nchan, expected',
    [
        # line not extend to the edge
        ([(1020, 1030)], 2048, False),
        ([(1, 11)], 1024, False),
        ([(800, 1022)], 1024, False),
        # narrow edge lines
        ([(0, 10)], 1024, True),
        ([(0, 203)], 1024, True),
        ([(990, 1023)], 1024, True),
        ([(-10, 200)], 1024, True),
        ([(900, 2047)], 1024, True),
        # wide edge lines
        ([(0, 204)], 1024, True),
        ([(800, 1023)], 1024, True),
        ([(800, 1024)], 1024, True),
        ([(0, 1023)], 1024, True),
        ([(-10, 210)], 1024, True),
        ([(800, 2047)], 1024, True)
    ]
)
def test_test_sd_edge_lines(lines: List[Tuple[int, int]], nchan: int, expected: float):
    score = qacalc.test_sd_edge_lines(lines, nchan)
    assert score == expected


@pytest.mark.parametrize(
    'lines, nchan, expected',
    [
        # single narrow line
        ([(1020, 1030)], 2048, False),
        ([(500, 840)], 1024, False),
        ([(1000, 1681)], 2048, False),
        # single wide line that lowers QA score
        ([(0, 1023)], 1024, True),
        ([(0, 2047)], 1024, True),
        ([(500, 841)], 1024, True),
        ([(1000, 1682)], 2048, True),
        # multiple lines, less coverage than threshold
        ([(100, 199), (300, 399), (500, 599), (700, 740)], 1024, False),
        # multiple lines, enough coverage to lower QA score
        ([(100, 199), (300, 399), (500, 599), (700, 741)], 1024, True)
    ]
)
def test_test_sd_wide_lines(lines: List[Tuple[int, int]], nchan: int, expected: float):
    score = qacalc.test_sd_wide_lines(lines, nchan)
    assert score == expected
