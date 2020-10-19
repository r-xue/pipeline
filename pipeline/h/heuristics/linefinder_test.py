"""
Tests for the h/heuristics/linefinder.py module.
"""
import pytest
from . linefinder import HeuristicsLineFinder

spectrum = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 30.0, 
            15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

def test_simple():
    s = HeuristicsLineFinder()
    assert s.calculate(spectrum) == [8, 12]
