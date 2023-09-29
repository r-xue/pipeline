import pytest

from .math import round_half_up

def test_simple():
    assert round_half_up(-1.055, precision=2) == -1.06


test_params = [(-2.5, 0, -3), ("-3.5", 0, -4), (4.5, 0, 5), ("5.5", 0, 6),
               (-6.034543211, 4, -6.03450), (-7.034443211, 4, -7.03440)]


@pytest.mark.parametrize("unrounded, precision, expected", test_params)
def test_round_half_up(unrounded, precision, expected):
    """Test round_half_up()

    This utility function takes an un-rounded scalar value and rounds
    it to the nearest value away from zero, using 'precision' to signify
    the decimal place to round.

    """
    assert round_half_up(unrounded, precision=precision) == expected
