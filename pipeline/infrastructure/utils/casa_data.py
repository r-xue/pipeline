"""
Utilities to work with the CASA data files
"""
from datetime import datetime
from glob import glob
import hashlib
import json
import os
from typing import List, Dict

from .. import casa_tools
from .conversion import get_epoch_as_datetime

__all__ = [
    "SOLAR_SYSTEM_MODELS_PATH",
    "IERS_TABLES_PATH",
    "get_file_md5",
    "get_iso_mtime",
    "get_solar_system_model_files",
    "get_filename_info",
    "get_object_info_string",
    "IERSInfo",
    "from_mjd_to_datetime"
]


SOLAR_SYSTEM_MODELS_PATH = casa_tools.utils.resolve("alma/SolarSystemModels")
IERS_TABLES_PATH = casa_tools.utils.resolve("geodetic")


def get_file_md5(filename: str) -> str:
    """Return a readable hex string of the MD5 hash of a given file"""
    return hashlib.md5(open(filename, 'rb').read()).hexdigest()


def get_iso_mtime(filename: str) -> str:
    """Return the ISO 8601 datetime string corresponding to the
    modification time of a given file"""
    return datetime.fromtimestamp(os.path.getmtime(filename)).isoformat()


def get_solar_system_model_files(ss_object: str, ss_path: str = SOLAR_SYSTEM_MODELS_PATH) -> List[str]:
    """Return the data files corresponding to a Solar System object"""
    models = glob(os.path.join(ss_path, "*.dat"))
    # NOTE: The filter function may fail in the unlikely case that an object name is
    #  contained in other object name or in the path. This may be refined later.
    object_models = filter(lambda x: ss_object in x, models)
    return sorted(object_models)


def get_filename_info(filename: str) -> Dict[str, str]:
    """Get a string with information about the modification date and MD5 hash of a file"""
    md5_hex = get_file_md5(filename)
    mtime = get_iso_mtime(filename)
    return {"MD5": md5_hex, "mtime": mtime}


def get_object_info_string(ss_object: str, ss_path: str = SOLAR_SYSTEM_MODELS_PATH) -> str:
    """Get the file information (MD5 hash and modification date) for a given
    Solar System object.

    At the moment the function returns all the matching model files corresponding
    to an object.
    """
    object_models = get_solar_system_model_files(ss_object, ss_path=ss_path)
    object_model_filenames = [os.path.split(o)[-1] for o in object_models]
    info_dict = {object_model_filenames[i]: get_filename_info(om) for i, om in enumerate(object_models)}
    info_string = json.dumps(info_dict)
    return f"Solar System models used for {ss_object} => " + info_string


# Get IERSpredict version
def from_mjd_to_datetime(mjd: float) -> datetime:
    """Convert a MJD float into a datetime"""
    mt = casa_tools.measures
    epoch = mt.epoch('UTC', '{}d'.format(mjd))
    return get_epoch_as_datetime(epoch)


class IERSInfo():
    """Class to store, retrieve and process the information from the IERS geodetic tables

    Attributes
    ----------
    IERS_tables : tuple
        Class attribute with the name of the relevant tables
    iers_path : str
        Path to the location of the geodetic tables
    info : dict
        Dictionary with the information retrieved
    """
    IERS_tables = ("IERSpredict", "IERSeop2000")

    def __init__(self, iers_path: str = IERS_TABLES_PATH, load_on_creation: bool = True):
        """Create instance of IERS Tables info.

        In the option load_on_creation is set to False the information has to be manually
        loaded with the method load_info().

        Parameters
        ----------
        iers_path : str, optional
            Path to the location of the IERS geodetic tables
        load_on_creation : bool, optional
            Do not load the IERS tables information when creating the instance
        """
        self.iers_path = iers_path
        if load_on_creation:
            self.load_info()
        else:
            self.info = None

    def get_IERS_version(self, IERS_tablename: str) -> str:
        """Get the VS_VERSION header of the IERSpredict table

        Parameters
        ----------
        IERS_tablename : str
            Name of the table to be loaded ("IERSpredict" or "IERSeop2000")
        """
        assert IERS_tablename in self.IERS_tables
        table_name = os.path.join(self.iers_path, IERS_tablename)
        try:
            with casa_tools.TableReader(table_name) as table:
                vs_version = table.getkeyword('VS_VERSION')
        except (IOError, RuntimeError):
            vs_version = "NOT FOUND"
        return vs_version

    def get_IERSeop2000_last_entry(self) -> float:
        """Get the last entry in the MJD column of the table IERSeop2000
        """
        table_name = os.path.join(self.iers_path, "IERSeop2000")
        try:
            with casa_tools.TableReader(table_name) as table:
                last_mjd = table.getcol('MJD')[-1]
        except (IOError, IndexError):
            last_mjd = "NOT FOUND"
        return last_mjd

    def load_info(self):
        """Get the following data from the casa geodetic tables:
            * IERSpredict version
            * IERSeop2000 version
            * IERSeop2000 last MJD entry
        """
        versions = {table: self.get_IERS_version(table) for table in self.IERS_tables}
        last_mjd = self.get_IERSeop2000_last_entry()
        if last_mjd != "NOT FOUND":
            last_dt = from_mjd_to_datetime(last_mjd)
        else:
            last_dt = None
        self.info = {"versions": versions, "IERSeop2000_last_MJD": last_mjd, "IERSeop2000_last": last_dt}

    def validate_date(self, date: datetime) -> bool:
        """Check if a date is lower or equal than the last entry of the IERSeop2000 table.
        The end date of the MS should be lower (see PIPE-734).
        If the geodetic tables could not be loaded correctly it always return False.
        """
        if self.info["IERSeop2000_last"] is not None:
            return date <= self.info["IERSeop2000_last"]
        else:
            return False

    def __call__(self):
        return self.info

    def __str__(self):
        if self.info is not None:
            info_string = json.dumps(self.info, default=str)
            return "IERS table information => " + info_string
        else:
            return "IERS table information not loaded"