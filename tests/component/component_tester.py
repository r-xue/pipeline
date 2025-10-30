"""Pipeline Component Testing Framework.

This module provides the `ComponentTester` class and associated pytest test functions
for automated regression testing of the NRAO pipeline. It supports both ALMA and VLA
pipelines, using either an OrderedDict of pipeline stages and optional parameters or recipe XML files.
"""
from __future__ import annotations

from typing import Any, Literal

from tests.testing_utils import PipelineTester


class ComponentTester(PipelineTester):
    def __init__(
            self,
            visname: list[str],
            mode: Literal['regression', 'component'] = 'component',
            tasks: list[tuple[str, dict[str, Any]] | None] | None = None,
            project_id: str | None = None,
            input_dir: str | None = None,
            output_dir: str | None = None,
            expectedoutput_file: str | None = None,
            expectedoutput_dir: str | None = None,
            ):
        super().__init__(
            visname,
            mode=mode,
            tasks=tasks,
            project_id=project_id,
            input_dir=input_dir,
            output_dir=output_dir,
            expectedoutput_file=expectedoutput_file,
            expectedoutput_dir=expectedoutput_dir,
            )
        """Initializes a ComponentTester instance.

        A list of MeasurementSet names (`visname`) is required. Either `recipe` or `tasks` must be provided; 
        if both are given, `recipe` takes precedence.

        Args:
            visname: List of MeasurementSets used for testing.
            mode: Signifies testing workflow to follow. Options are 'regression' and 'component'. Default is 'component'.
            tasks: List of tuples with pipeline stage strings first and optional parameters second, which only used in
                component tests.
            project_id: Project ID. If provided, it is prefixed to the `output_dir` name.
            input_dir: Path to the directory containing input files.
            output_dir: Path to the output directory. If `None`, it is derived using `visname` and/or `project_id`.
            expectedoutput_file: Path to a file defining the expected test output. Overrides `expectedoutput_dir` if set.
            expectedoutput_dir: Path to a directory containing expected output files. Ignored if `expectedoutput_file` is set.

        Raises:
            ValueError: If neither `recipe` nor `tasks` is provided.
        """

        if not tasks:
            raise ValueError("A list of tasks must be provided.")
