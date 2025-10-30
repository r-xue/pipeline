"""Pipeline Regression Testing Framework.

This module provides the `RegressionTester` class and associated pytest test functions
for automated regression testing of the NRAO pipeline. It supports both ALMA and VLA
pipelines, using either Pipeline Processing Request (PPR) files or recipe XML files.

The individual test functions have been moved and split according to its type of regression
test, first split by either fast or slow tests, then by datatype (ALMA-IF, ALMA-SD, VLA, VLASS).
They are also further separated according to the marks each function is decorated with.
"""
from __future__ import annotations

import os
import shutil
from typing import Literal

import pytest

from pipeline import infrastructure
from pipeline.infrastructure import casa_tools
from tests.testing_utils import PipelineTester

LOG = infrastructure.logging.get_logger(__name__)


@pytest.fixture(autouse=True)
def data_directory(scope="module") -> str:
    if hasattr(pytest, 'pytestconfig'):
        big_data_dir = pytest.pytestconfig.getoption('--data-directory')
    else:
        big_data_dir = "/lustre/cv/projects/pipeline-test-data/regression-test-data/"

    if not os.path.exists(big_data_dir):
        print(f"Warning! The large dataset directory {big_data_dir} does not exist, so any long-running tests will fail.")
    else:
        print(f"Using: {big_data_dir} for data directory")
    return big_data_dir


def setup_flux_antennapos(test_directory, output_dir):
    # Copy flux.csv and antennapos.csv into the working directory
    flux_file = casa_tools.utils.resolve(f'{test_directory}/flux.csv')
    anteannapos_file = casa_tools.utils.resolve(f'{test_directory}/antennapos.csv')

    try:
        os.mkdir(f'{output_dir}/working/')
    except FileExistsError:
        pass

    shutil.copyfile(flux_file, casa_tools.utils.resolve(f'{output_dir}/working/flux.csv'))
    shutil.copyfile(anteannapos_file, casa_tools.utils.resolve(f'{output_dir}/working/antennapos.csv'))


class RegressionTester(PipelineTester):
    def __init__(
            self,
            visname: list[str],
            mode: Literal['regression', 'component'] = 'regression',
            ppr: str | None = None,
            recipe: str | None = None,
            project_id: str | None = None,
            input_dir: str | None = None,
            output_dir: str | None = None,
            expectedoutput_file: str | None = None,
            expectedoutput_dir: str | None = None,
            ):
        super().__init__(
            visname,
            mode=mode,
            ppr=ppr,
            recipe=recipe,
            project_id=project_id,
            input_dir=input_dir,
            output_dir=output_dir,
            expectedoutput_file=expectedoutput_file,
            expectedoutput_dir=expectedoutput_dir,
            )
        """Initializes a RegressionTester instance.

        A list of MeasurementSet names (`visname`) is required. Either `ppr` or `recipe` must be provided; 
        if both are given, `ppr` takes precedence.

        Args:
            visname: List of MeasurementSets used for testing.
            mode: Signifies testing workflow to follow. Options are 'regression' and 'component'. Default is 'regression'.
            ppr: Path to the PPR file. Takes precedence over `recipe` if both are provided.
            recipe: Path to the recipe XML file.
            project_id: Project ID. If provided, it is prefixed to the `output_dir` name.
            input_dir: Path to the directory containing input files.
            output_dir: Path to the output directory. If `None`, it is derived using `visname` and/or `project_id`.
            expectedoutput_file: Path to a file defining the expected test output. Overrides `expectedoutput_dir` if set.
            expectedoutput_dir: Path to a directory containing expected output files. Ignored if `expectedoutput_file` is set.

        Raises:
            ValueError: If neither `ppr` nor `recipe` is provided.
        """

        if not recipe and not ppr:
            raise ValueError("At least one of recipe or ppr must be provided.")
