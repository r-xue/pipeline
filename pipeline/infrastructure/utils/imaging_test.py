import pytest

from .imaging import spw_intersect

spw_intersect_test_params = (([4, 12], [[7, 9]], [[4, 7], [9, 12]]),
                             ([4, 12], [[4, 5]], [[5, 12]]),
                             ([4, 12], [[11, 12]], [[4, 11]]),
                             ([4, 12], [[4, 5], [11, 12]], [[5, 11]]),
                             ([4, 12], [[5, 6], [10, 11]], [[4, 5], [6, 10], [11, 12]]),
                             ([228.0, 232.0], [[229.7, 229.9], [230.4, 230.6], [231.0, 231.5]], [[228.0, 229.7], [229.9, 230.4], [230.6, 231.0], [231.5, 232.0]]))


@pytest.mark.parametrize("spw_range, line_regions, expected", spw_intersect_test_params)
def test_spw_intersect(spw_range, line_regions, expected):
    """
    Test spw_intersect()

    This utility function takes a frequency range (as doubles in arbitrary
    units) and computes the intersection with a list of frequency ranges
    denoting the regions of spectral lines. It returns the remaining
    ranges excluding the line frequency ranges.

    """
    assert spw_intersect(spw_range, line_regions) == expected
