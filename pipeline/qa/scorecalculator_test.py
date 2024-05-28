"""
scorecalculator_test.py : Unit tests for "qa/scorecalculator.py".

Unit tests for "qa/scorecalculator.py"
"""
from typing import List, Optional, Tuple, TYPE_CHECKING

import pytest

import pipeline.qa.scorecalculator as qacalc

test_cases_edge = [
    # line not extend to the edge
    ([(1020, 1030)], 2048, 1.0),
    ([(1, 11)], 1024, 1.0),
    ([(800, 1022)], 1024, 1.0),
    # too narrow edge lines
    ([(0, 10)], 1024, 1.0),
    ([(0, 203)], 1024, 1.0),
    ([(990, 1023)], 1024, 1.0),
    ([(-10, 200)], 1024, 1.0),
    ([(900, 2047)], 1024, 1.0),
    # wide edge line that lowers QA score
    ([(0, 204)], 1024, 0.65),
    ([(800, 1023)], 1024, 0.65),
    ([(800, 1024)], 1024, 0.65),
    ([(0, 1023)], 1024, 0.65),
    ([(-10, 210)], 1024, 0.65),
    ([(800, 2047)], 1024, 0.65)
]


@pytest.mark.parametrize('lines, nchan, expected', test_cases_edge)
def test_score_sd_edge_lines(lines: List[Tuple[int, int]], nchan: int, expected: float):
    score = qacalc.score_sd_edge_lines(lines, nchan)
    assert score == expected


test_cases_wide = [
    # single narrow line
    ([(1020, 1030)], 2048, 1.0),
    ([(500, 840)], 1024, 1.0),
    ([(1000, 1681)], 2048, 1.0),
    # single wide line that lowers QA score
    ([(0, 1023)], 1024, 0.65),
    ([(0, 2047)], 1024, 0.65),
    ([(500, 841)], 1024, 0.65),
    ([(1000, 1682)], 2048, 0.65),
    # multiple lines, less coverage than threshold
    ([(100, 199), (300, 399), (500, 599), (700, 740)], 1024, 1.0),
    # multiple lines, enough coverage to lower QA score
    ([(100, 199), (300, 399), (500, 599), (700, 741)], 1024, 0.65)
]


@pytest.mark.parametrize('lines, nchan, expected', test_cases_wide)
def test_score_sd_wide_lines(lines: List[Tuple[int, int]], nchan: int, expected: float):
    score = qacalc.score_sd_wide_lines(lines, nchan)
    assert score == expected


test_cases_qa_msg = [
    # narrow line at the middle
    ([(1024, 10, True)], 2048, 1.0, 'Successfully detected spectral lines'),
    # narrow edge line
    ([(5, 10, True)], 1024, 1.0, 'Successfully detected spectral lines'),
    # edge line with enough coverage to lower QA score
    ([(105, 210, True)], 1024, 0.65, 'An edge line is detected.'),
    # wide line with enough coverage to lower QA score
    ([(521, 342, True)], 1024, 0.65, 'A wide line is detected.'),
    # multiple narrow lines with enough coverage *in total* to lower QA score
    ([(150, 100, True), (350, 100, True), (550, 100, True), (720, 42, True)], 1024, 0.65, 'A wide line is detected.'),
    # wide edge lines that can violate both criteria
    ([(512, 1024, True)], 1024, 0.65, 'An edge line is detected. A wide line is detected.'),
    # no valid line, no QA score
    ([(1024, 6, False)], 2048, None, 'N/A'),

]


@pytest.mark.parametrize('lines, nchan, expected_score, expected_msg', test_cases_qa_msg)
def test_score_sd_lines_overall(lines: List[Tuple[int, int]], nchan: int, expected_score: Optional[float], expected_msg: str):
    field_name = 'M100'
    spw_id = [17]
    score = qacalc.score_sd_line_detection(field_name, spw_id, nchan, lines)
    if expected_score:
        assert expected_score == score.score
        assert expected_msg == score.shortmsg
        assert expected_msg in score.longmsg
    else:
        assert score is None
