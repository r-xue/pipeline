import numpy as np
import pytest

from pipeline.domain.datatype import DataType
from .importdata import get_ms_data_types_from_history

def test_get_ms_data_types_with_valid_history():
    """Test retrieval of DataType info from MS HISTORY entries."""
    msgs = np.array([
        "some irrelevant log",
        "data_type_per_column = {'RAW': 'DATA'}",
        "data_types_per_source_and_spw = {('2039540-005735', 0): ['RAW'], ('2039540-005735', 1): ['RAW']}",        
        "some irrelevant log",
        "data_type_per_column = {'REGCAL_CONTLINE_SCIENCE': 'DATA', 'SELFCAL_CONTLINE_SCIENCE': 'CORRECTED_DATA'}",
        "data_types_per_source_and_spw = {('2039540-005735', 0): ['REGCAL_CONTLINE_SCIENCE', 'REGCAL_CONTLINE_SCIENCE'], ('2039540-005735', 1): ['REGCAL_CONTLINE_SCIENCE', 'SELFCAL_CONTLINE_SCIENCE']}",
        "some irrelevant log",
    ])

    dt_per_col, dt_per_src_spw = get_ms_data_types_from_history(msgs)

    assert dt_per_col == {
        DataType.REGCAL_CONTLINE_SCIENCE: 'DATA',
        DataType.SELFCAL_CONTLINE_SCIENCE: 'CORRECTED_DATA'
    }

    assert dt_per_src_spw == {
        ('2039540-005735', 0): [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE],
        ('2039540-005735', 1): [DataType.REGCAL_CONTLINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE],
    }
