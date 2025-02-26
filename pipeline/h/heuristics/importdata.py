import numpy
from pipeline.domain.datatype import DataType

def get_ms_datatypes_from_history(msgs: numpy.ndarray) -> (dict, dict):
    """
    Retrieve the original datatype lookup dictionaries from MS HISTORY table entries.

    Args:
        msgs: HISTORY table message as numpy array

    Returns:
        datatype per column dictionary
        datatype per source and spw dictionary
    """

    # Define some dictionaries representing the datatypes as strings
    datatype_per_column_strtypes = {}
    found_datatype_per_column = False
    datatypes_per_source_and_spw_strtypes = {}
    found_datatypes_per_source_and_spw = False

    # Search backwards from latest messages since the datatype information is
    # written multiple times as new history entries throughout the pipeline processing.
    for item in msgs[-1::-1]:
        if not found_datatype_per_column and 'datatype_per_column' in item:
            datatype_per_column_strtypes = eval(item.split('=')[1])
            found_datatype_per_column = True
        if not found_datatypes_per_source_and_spw and 'datatypes_per_source_and_spw' in item:
            datatypes_per_source_and_spw_strtypes = eval(item.split('=')[1])
            found_datatypes_per_source_and_spw = True
        if found_datatype_per_column and found_datatypes_per_source_and_spw:
            break

    # Make actual dictionaries with datatype enums
    if datatype_per_column_strtypes != {}:
        datatype_per_column = dict((eval(f'DataType.{k}'), v) for k, v in datatype_per_column_strtypes.items())
    else:
        datatype_per_column  = dict()

    if datatypes_per_source_and_spw_strtypes != {}:
        datatypes_per_source_and_spw = dict((k, [eval(f'DataType.{item}') for item in v]) for k, v in datatypes_per_source_and_spw_strtypes.items())
    else:
        datatypes_per_source_and_spw  = dict()

    return datatype_per_column, datatypes_per_source_and_spw
