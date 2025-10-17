"""
Tests for the regression_tester.py module.
"""
from __future__ import annotations

import packaging
import pytest
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from pipeline.infrastructure.utils import regression_tester

if TYPE_CHECKING:
    from pytest import Config, Parser


def pytest_addoption(parser: Parser) -> None:
    """Adds command-line options to pytest."""
    parser.addoption("--compare-only", action="store_true", help="Run tests with compare-only mode")
    parser.addoption("--remove-workdir", action="store_true", help="Enable workdir removal")


class TestPipelineRegression(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def setup_pipeline(self, pytestconfig: Config) -> None:
        """Sets up a PipelineRegression instance with mock environment values."""
        self.compare_only = pytestconfig.getoption("--compare-only", default=False)
        self.remove_workdir = pytestconfig.getoption("--remove-workdir", default=False)
        self.pipeline = regression_tester.PipelineRegression(visname=["test_results"], ppr="test.xml")

    @mock.patch("pipeline.environment.casa_version_string", "6.5.1.15")  # casa_version_string already replaces dash
    @mock.patch("pipeline.environment.pipeline_revision", "2023.1.0.8")
    def test_pick_results_file_valid_cases(self) -> None:
        """Tests that _pick_results_file correctly extracts and selects the best matching file."""
        reference_files = [
            "test_results_casa-6.4.0-20-pipeline-2022.4.0.1",
            "test_results_casa-6.6.0.8-pipeline-2023.2.0.140",  # Exceeds both current versions
            "test_results_casa-6.6.0.8-pipeline-2023.1.0.8",  # Exceeds current CASA version
            "test_results_casa-6.5.1-15-pipeline-2023.2.0.140",  # Exceeds current pipeline version
            "test_results_casa-6.5.1-15-pipeline-2023.1.0.8",  # Exact match
        ]

        best_match = self.pipeline._pick_results_file(reference_files)
        self.assertEqual(best_match, "test_results_casa-6.5.1-15-pipeline-2023.1.0.8")

    @mock.patch("pipeline.environment.casa_version_string", "6.5.1.15")
    @mock.patch("pipeline.environment.pipeline_revision", "2023.1.0.8")
    def test_pick_results_file_exceeding_versions(self) -> None:
        """Tests that files with exceeding versions are ignored."""
        reference_files = [
            "test_results_casa-6.7.0.0-pipeline-2023.1.0.8",  # CASA version too high
            "test_results_casa-6.5.1-15-pipeline-2023.3.0.1",  # Pipeline version too high
        ]

        best_match = self.pipeline._pick_results_file(reference_files)
        self.assertIsNone(best_match)

    @mock.patch("pipeline.environment.casa_version_string", "6.5.1.15")
    @mock.patch("pipeline.environment.pipeline_revision", "2023.1.0.8")
    def test_results_file_heuristics_selects_best_match(self) -> None:
        """Tests that _results_file_heuristics picks the closest match."""
        reference_dict = {
            "test_results_casa-6.5.0-12-pipeline-2023.1.0.1": {
                "CASA version": packaging.version.parse("6.5.0.12"),
                "Pipeline version": packaging.version.parse("2023.1.0.1"),
            },
            "test_results_casa-6.4.0.33-pipeline-2022.4.0.125": {
                "CASA version": packaging.version.parse("6.4.0.33"),
                "Pipeline version": packaging.version.parse("2022.4.0.125"),
            },
            "test_results_casa-6.5.1-15-pipeline-2023.1.0.4": {
                "CASA version": packaging.version.parse("6.5.1.15"),
                "Pipeline version": packaging.version.parse("2023.1.0.4"),
            },
        }

        best_match = self.pipeline._results_file_heuristics(reference_dict)
        self.assertEqual(best_match, "test_results_casa-6.5.1-15-pipeline-2023.1.0.4")

    def test_compare_only_option(self) -> None:
        """Ensure compare-only option is set correctly."""
        self.assertEqual(self.pipeline.compare_only, self.compare_only)

    def test_regex_matching(self) -> None:
        """Tests that the regex correctly extracts CASA and Pipeline versions."""
        test_cases = [
            ("test_results_casa-6.5.0.5-pipeline-2023.1.0.20", "6.5.0.5", "2023.1.0.20"),
            ("test_results_casa-6.4.3-17-pipeline-2022.3.0.40", "6.4.3-17", "2022.3.0.40"),
            ("invalid_casa_pipeline-2021.1.0.12", None, "2021.1.0.12"),  # Missing CASA version
            ("invalid_casa-6.7.0.1-pipeline", "6.7.0.1", None),  # Missing pipeline version
            ("random_file.txt", None, None),  # No match
        ]

        for filename, expected_casa, expected_pipeline in test_cases:
            casa_match = self.pipeline.regex_casa_pattern.match(filename)
            pipeline_match = self.pipeline.regex_pipeline_pattern.match(filename)

            casa_version = casa_match.group(1) if casa_match else None
            pipeline_version = pipeline_match.group(1) if pipeline_match else None

            self.assertEqual(casa_version, expected_casa)
            self.assertEqual(pipeline_version, expected_pipeline)
