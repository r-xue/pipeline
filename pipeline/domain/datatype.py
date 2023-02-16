"""
Module to define data type.

Classes:
    DataType: An Enum class to define data type.
"""

from enum import Enum, auto, unique

@unique
class DataType(Enum):
    """
    A class to define DataType enum.

    Attributes:
        RAW: raw data
        REGCAL_CONTLINE_ALL: calibrated data
        BASELINED: data after spectral baseline subtraction
        ATMCORR: data corrected for residual ATM
        REGCAL_CONTLINE_SCIENCE: calibrated data of target scans
        SELFCAL_CONTLINE_SCIENCE: self-calibrated data of target scans
        REGCAL_LINE_SCIENCE: calibrated spectral line data
        SELFCAL_LINE_SCIENCE self-calibrated spectral line data
    """

    RAW = auto()
    REGCAL_CONTLINE_ALL = auto()
    BASELINED = auto()
    ATMCORR = auto()
    REGCAL_CONTLINE_SCIENCE = auto()
    SELFCAL_CONTLINE_SCIENCE = auto()
    REGCAL_LINE_SCIENCE = auto()
    SELFCAL_LINE_SCIENCE = auto()
