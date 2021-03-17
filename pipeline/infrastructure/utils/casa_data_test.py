import pytest
from .. import casa_tools
from .casa_data import (
    get_file_md5,
    get_iso_mtime,
    get_solar_system_model_files,
    get_filename_info,
    get_object_info_string
)


# # Tests that depend on the pipeline-testdata repository
TEST_DATA_PATH = casa_tools.utils.resolve('pl-unittest/casa_data')
# Skip tests if CASA cannot resolve to an absolute path
skip_data_tests = not TEST_DATA_PATH.startswith('/')
# Create decorator with reason to skip tests
skip_if_no_data_repo = pytest.mark.skipif(
    skip_data_tests,
    reason="The repo pipeline-testdata is not set up for the tests"
)


SOLAR_SYSTEM_MODELS_PATH = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels')
SS_CALLISTO = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Callisto_Tb.dat')
SS_CERES = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Ceres_Tb.dat')
SS_CERES_FD = casa_tools.utils.resolve('pl-unittest/casa_data/alma/SolarSystemModels/Ceres_fd_time.dat')


@skip_if_no_data_repo
def test_md5_hash_string_solar_system_models():
    assert get_file_md5(SS_CALLISTO) == "d943f9e40d13c433213de24a01a488d8"
    assert get_file_md5(SS_CERES) == "91a16285f38c9ed05e18cbf3d3573463"
    assert get_file_md5(SS_CERES_FD) == "a6200c8a75195bb43f80c3576907a12e"


@skip_if_no_data_repo
def test_get_iso_mtime():
    # At the moment we only check that the format is roughly an ISO date
    iso_mtime = get_iso_mtime(SS_CALLISTO)
    assert iso_mtime[4] == "-"
    assert iso_mtime[7] == "-"
    assert iso_mtime[10] == "T"
    assert iso_mtime[13] == ":"
    assert iso_mtime[16] == ":"


@skip_if_no_data_repo
def test_retrieve_corresponding_solar_system_models_with_one_file():
    callisto_ss = get_solar_system_model_files("Callisto", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert len(callisto_ss) == 1


@skip_if_no_data_repo
def test_retrieve_corresponding_solar_system_models_with_two_files():
    ceres_ss = get_solar_system_model_files("Ceres", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert len(ceres_ss) == 2


@skip_if_no_data_repo
def test_get_filename_info():
    ceres_info = get_filename_info(SS_CERES)
    ceres_fd_info = get_filename_info(SS_CERES_FD)
    callisto_info = get_filename_info(SS_CALLISTO)
    assert callisto_info["MD5"] == "d943f9e40d13c433213de24a01a488d8"
    assert ceres_info["MD5"] == "91a16285f38c9ed05e18cbf3d3573463"
    assert ceres_fd_info["MD5"] == "a6200c8a75195bb43f80c3576907a12e"
    assert 19 <= len(callisto_info["mtime"]) <= 26


@skip_if_no_data_repo
def test_info_string_solar_system_models_with_one_file():
    info_string = get_object_info_string("Callisto", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert info_string.startswith('Solar System models used for Callisto => {"Callisto')
    assert "d943f9e40d13c433213de24a01a488d8" in info_string
    assert "mtime" in info_string
    assert len(info_string.split("MD5")) == 2
    assert info_string.endswith('}}')


@skip_if_no_data_repo
def test_info_string_solar_system_models_with_two_files():
    info_string = get_object_info_string("Ceres", ss_path=SOLAR_SYSTEM_MODELS_PATH)
    assert info_string.startswith('Solar System models used for Ceres => {"Ceres')
    assert "91a16285f38c9ed05e18cbf3d3573463" in info_string
    assert "a6200c8a75195bb43f80c3576907a12e" in info_string
    assert "mtime" in info_string
    assert len(info_string.split("MD5")) == 3
    assert info_string.endswith('}}')
