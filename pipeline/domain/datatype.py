'''
datatype module is 

Classes:
    DataType: An Enum class to define data type.
'''

from enum import Enum, auto

class DataType(Enum):
    '''
    A class to define DataType enum.
    
    Attributes:
        RAW: raw data
        CALIBRATED: calibrated data
        BASELINED: baseline subtracted data
        ATMCORR: data corrected for residual ATM
        TARGET: data in target MS
    '''
    
    RAW = auto()
    CALIBRATED = auto()
    BASELINED = auto()
    ATMCORR = auto()
    TARGET = auto()
    LINE = auto()

        