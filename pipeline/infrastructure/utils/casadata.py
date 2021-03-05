"""
Utilities to work with the CASA data files
"""
from datetime import datetime
from glob import glob
import hashlib
import os
from typing import List

from .. import casa_tools

__all__ = [
    "SOLAR_SYSTEM_MODELS_PATH",
    "IERS_TABLES_PATH",
    "get_file_md5",
    "get_iso_mtime",
    "get_solar_system_model_files",
    "get_filename_info",
    "get_object_info",
    "get_IERS_versions",
    "get_IERS_versions",
    "get_IERSeop2000_last_entry"
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


def get_solar_system_model_files(ss_object: str) -> List[str]:
    """Return the data files corresponding to a Solar System object"""
    models = glob(os.path.join(SOLAR_SYSTEM_MODELS_PATH, "*.dat"))
    # NOTE: The filter function may fail in the unlikely case that an object name is
    #  contained in other object name or in the path. This may be refined later.
    object_models = filter(lambda x: ss_object in x, models)
    return sorted(object_models)


def get_filename_info(filename: str) -> str:
    """Get a string with information about the modification date and MD5 hash of a file"""
    md5_hex = get_file_md5(filename)
    mtime = get_iso_mtime(filename)
    return f"MD5: {md5_hex}, mod. time: {mtime}"


def get_object_info(ss_object: str) -> str:
    """Get the file information (MD5 hash and modification date) for a given
    Solar System object.

    At the moment the function returns all the matching model files corresponding
    to an object.
    """
    object_models = get_solar_system_model_files(ss_object)
    object_model_filenames = [os.path.split(o)[-1] for o in object_models]
    info_list = []
    for i, object_model in enumerate(object_models):
        info_string = get_filename_info(object_model)
        filename = object_model_filenames[i]
        info_list.append(f"{filename} -> {info_string}")
    return f"Solar System models used for {ss_object} => " + "; ".join(info_list)


# Get IERSpredict version
def get_IERS_version(IERS_tablename: str) -> str:
    """Get the VS_VERSION header of the IERSpredict table"""
    assert IERS_tablename in ["IERSpredict", "IERSeop2000"]
    tablename = os.path.join(IERS_TABLES_PATH, IERS_tablename)
    with casa_tools.TableReader(tablename) as table:
        vs_version = table.getkeyword('VS_VERSION')
    return vs_version


def get_IERS_versions():
    IERS_tables = ["IERSpredict", "IERSeop2000"]
    return {i: get_IERS_version(i) for i in IERS_tables}


def get_IERSeop2000_last_entry() -> str:
    """Get the last entry in the MJD column of the table IERSeop2000"""
    tablename = os.path.join(IERS_TABLES_PATH, "IERSeop2000")
    with casa_tools.TableReader(tablename) as table:
        last_mjd = table.getcol('MJD')[-1]
    return last_mjd


def get_IERS_data():
    """Get the following data from the casa geodetic tables:
    * IERSpredict version
    * IERSeop2000 version
    * IERSeop2000 last MJD entry
    """
    pass

