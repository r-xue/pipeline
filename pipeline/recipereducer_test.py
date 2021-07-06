import pytest

from .recipereducer import string_to_val

test_params = [('True', True),
               ('false', 'false'),
               ('1,2,3', '1,2,3'),
               ('(1,2,3)', (1, 2, 3)),
               ('[1,2,3]', [1, 2, 3]),
               ('None', None),
               ('none', 'none'),
               ('>12klambda', '>12klambda'),
               ('&lt;12km', '&lt;12km')]


@pytest.mark.parametrize("input_str, expected_value", test_params)
def test_string_to_val(input_str, expected_value):
    """Test string_to_val() from pipeline.recipereducer."""
    assert string_to_val(input_str) == expected_value
