from typing import Union, List, Dict, Tuple
from unittest.mock import MagicMock
import operator
import pytest

import numpy as np


from pipeline import domain
from .. import casatools
from .utils import find_ranges, dict_merge, are_equal, approx_equal, flagged_intervals, \
    get_casa_quantity, get_num_caltable_polarizations, fieldname_for_casa, fieldname_clean, \
    get_field_accessor, get_field_identifiers, get_receiver_type_for_spws


# Create mock MeasurementSet instance
def create_mock_field(name: str = 'Mars', field_id: int = 1, source_id: int = 1):
    """
    Create a mock Field object.

    The name argument is used to determine the direction Field object attribute,
    therefore use field names only that are known to the casatools.measure.direction()
    tool.

    Returns:
        Instance of Field class.
    """
    return domain.Field(field_id=field_id, name=name, source_id=source_id, time=np.zeros(0),
                        direction=casatools.measures.direction(name))


def create_mock_ms(name: str, field_name_list: List[str] = ['Mars'],
                   spw_return: Union[None, str] = None):
    """
    Create a mock MeasurementSet object.

    Args:
        name: arbitrary MeasurementSet name
        field_name_list: list of fields to be stored in MeasurementSet
        spw_return: mocked return value for MeasurementSet.get_spectral_windows() method.

    Returns:
        Instance of MeasurementSet class.
    """
    # Mock MeasurementSet
    mock_ms = domain.MeasurementSet(name=name)

    # Mock fields
    for field_name in field_name_list:
        mock_ms.fields.append(create_mock_field(field_name, field_id=len(mock_ms.fields)+1,
                              source_id=len(mock_ms.fields)+1))

    # Mock a spectral window
    if type(spw_return) is str:
        class SpwReceiver:
            receiver = spw_return
        spw_return = [SpwReceiver()]
    mock_ms.get_spectral_windows = MagicMock(return_value=spw_return)

    return mock_ms


params_find_ranges = [('', ''), ([], ''), ('1:2', '1:2'), ([1, 2, 3], '1~3'),
                      (['5~12', '14', '16:17'], '5~12,14,16:17'),
                      ([1, 2, 3, 6, 7], '1~3,6~7'),
                      ([1, 2, 3, '6', '7'], '1~3,6~7')]


@pytest.mark.parametrize('data, expected', params_find_ranges)
def test_find_ranges(data: Union[str, list], expected: str):
    """Test find_ranges()

    This utility function takes a string or a list of integers (e.g. spectral
    window lists) and returns a string containing identified ranges.
    E.g. [1,2,3] -> '1~3'
    """
    assert find_ranges(data) == expected


params_dict_merge = [({}, {}, {}), ({}, 1, 1), ({'a': 1}, {}, {'a': 1}),
                     ({'a': {'b': 1}}, {'c': 2}, {'a': {'b': 1}, 'c': 2}),
                     ({'a': {'b': 1}}, {'a': {'b': 2}}, {'a': {'b': 2}}),
                     ({'a': {'b': 1}}, {'a': 2}, {'a': 2}),
                     ({'a': {'b': {'c': 1}}}, {'a': {'b': {'c': 2}}},
                      {'a': {'b': {'c': 2}}})]


@pytest.mark.parametrize('a, b, expected', params_dict_merge)
def test_dict_merge(a: Dict, b: Dict, expected: Dict):
    """Test dict_merge()

    This utility function recursively merges dictionaries. If second argument
    (b) is a dictionary, then a copy of first argument (dictionary a) is created
    and the elements of b are merged into the new dictionary. Otherwise return
    argument b.

    In case of matching non-dictionary value keywords, content of dictionary b
    overwrites that of dictionary a. If the matching keyword value is a dictionary
    then continue merging recursively.
    """
    assert dict_merge(a, b) == expected


params_are_equal = [([1, 2, 3], [1, 2, 3], True), ([1, 2.5, 3], [1, 2, 3], False),
                    (np.ones(2), np.zeros(2), False), (np.ones(2), np.ones(2), True),
                    (np.ones(2), np.ones(3), False)]


@pytest.mark.parametrize('a, b, expected', params_are_equal)
def test_are_equal(a: Union[List, np.ndarray], b: Union[List, np.ndarray], expected: bool):
    """Test are_equal()

    This utility function check the equivalence of array like objects. Two arrays
    are equal if they have the same number of elements and elements of the same
    index are equal.
    """
    assert are_equal(a, b) == expected


params_approx_eqaul = [(1.0e-2, 1.2e-2, 1e-2, True),
                       (1.0e-2, 1.2e-2, 1e-3, False),
                       (1.0, 2.0, 0.1, False), (1, 2, 10, True)]


@pytest.mark.parametrize('x, y, tol, expected', params_approx_eqaul)
def test_approx_equal(x: float, y: float, tol: float, expected: bool):
    """Test approx_equal()

    This utility function returns True if two numbers are equal within the
    given tolerance.
    """
    assert approx_equal(x, y, tol=tol) == expected


params_test_get_num_calltable_pol = [('uid___A002_Xc46ab2_X15ae_spw16_17_small.ms.hifa_'
                                      'timegaincal.s17_7.spw0.solintinf.gacal.tbl', 1),
                                     ('uid___A002_Xc46ab2_X15ae_spw16_17_small.ms.hifa_'
                                      'timegaincal.s17_2.spw0.solintinf.gpcal.tbl', 2)]


@pytest.mark.skip(reason="Currently no general online pipeline date storage is available for test datasets.")
@pytest.mark.parametrize('caltable, expected', params_test_get_num_calltable_pol)
def test_get_num_caltable_polarizations(caltable: str, expected: int):
    """Test get_num_caltable_polarizations()
    """
    assert get_num_caltable_polarizations(caltable=caltable) == expected


params_flagged_intervals = [([], []), ([1, 2], [(0, 0)]),
                            ([0, 1, 0, 1, 1], [(1, 1), (3, 4)]),
                            ([0, 1, 0, 1, 2], [(1, 1), (3, 3)])]


@pytest.mark.parametrize('vec, expected', params_flagged_intervals)
def test_flagged_intervals(vec: Union[List[int], np.ndarray], expected: List[Tuple[int]]):
    """Test flagged_intervals()

    This utility function finds islands of ones in vector provided in argument.
    Used to find contiguous flagged channels in a given spw.  Returns a list of
    tuples with the start and end channels.
    """
    assert flagged_intervals(vec=vec) == expected


params_fieldname_for_casa = [('', ''), ('helm30', 'helm30'),
                             ('helm=30', '"helm=30"'), ('1', '"1"')]


@pytest.mark.parametrize('field, expected', params_fieldname_for_casa)
def test_fieldname_for_casa(field: str, expected: str):
    """Test fieldname_for_casa()

    This utility function ensures that field string can be used as CASA argument.

    If field contains special characters, then return field string enclose in
    quotation marks, otherwise return unchanged string.
    """
    assert fieldname_for_casa(field=field) == expected


params_fieldname_clean = [('', ''), ('helm30', 'helm30'), ('helm=30', 'helm_30'),
                          ('1', '1')]


@pytest.mark.parametrize('field, expected', params_fieldname_clean)
def test_fieldname_clean(field: str, expected: str):
    """Test fieldname_clean()

    This utility function replaces special characters in string with underscore.
    """
    assert fieldname_clean(field=field) == expected


params_get_field_accessor = [(create_mock_ms('mock', ['Jupiter']), create_mock_field('Jupiter'),
                              'Jupiter')]


@pytest.mark.parametrize('ms, field, expected', params_get_field_accessor)
def test_get_field_accessor(ms, field, expected):
    """Test get_field_accessor()
    """
    assert get_field_accessor(ms, field)(field) == expected


params_get_field_ids = [(create_mock_ms('mock', ['Mars', 'Jupiter']), {1: 'Mars', 2: 'Jupiter'})]


@pytest.mark.parametrize('ms, expected', params_get_field_ids)
def test_get_field_identifiers(ms, expected):
    """Test get_field_identifiers()
    """
    assert get_field_identifiers(ms=ms) == expected


params_get_receiver_type_for_spws = [(create_mock_ms('mock'), [1], {1: "N/A"}),
                                     (create_mock_ms('mock', spw_return='fake'),
                                      [1], {1: 'fake'})]


@pytest.mark.parametrize('ms, spwids, expected', params_get_receiver_type_for_spws)
def test_get_receiver_type_for_spws(ms, spwids, expected):
    """Test get_receiver_type_for_spws()"""
    assert get_receiver_type_for_spws(ms=ms, spwids=spwids) == expected


params_get_casa_quantity = [(None, {'unit': '', 'value': 0.0}),
                            ('10klambda', {'unit': 'klambda', 'value': 10.0}),
                            (10.0, {'unit': '', 'value': 10.0})]


@pytest.mark.parametrize('value, expected', params_get_casa_quantity)
def test_get_casa_quantity(value: Union[str, float, Dict, None], expected: Dict):
    """Test get_casa_quantity()

    This utility function handles None values when calling CASA quanta.quantity()
    tool method.
    """
    assert get_casa_quantity(value) == expected
