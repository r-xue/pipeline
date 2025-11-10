"""This file contains some pipeline-specific pytest configuration settings."""
from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING

import pytest

from casatasks import casalog

if TYPE_CHECKING:
    from _pytest.config import Config
    from _pytest.nodes import Item, Session
    from pytest import FixtureRequest, Parser


def pytest_addoption(parser: Parser) -> None:
    """Add custom pytest command-line options."""

    nologfile_help = r"""
        Do not create CASA log files, equivalent to 'casa --nologfile'.
        Please note that if you're running regression tests, casa logfiles 
        will still be generated within individual test "working/" directories and 
        appear in test weblogs. In general, this option is only recommended when 
        manually/frequently running unit tests, to keep your local repo clean.
        """
    parser.addoption("--collect-tests", action="store_true", default=False,
                     help="Collect tests and export test node ids to a plain text file `collected_tests.txt`")
    parser.addoption("--nologfile", action="store_true", default=False,
                     help=nologfile_help)
    parser.addoption("--pyclean", action="store_true", default=False,
                     help="Clean up .pyc to reproduce certain warnings only issued when the bytecode is compiled.")
    parser.addoption("--remove-workdir", action="store_true", default=False,
                     help="Remove individual working directories from regression tests.")
    parser.addoption("--longtests", action="store_true", default=False, help="Run longer tests.")
    parser.addoption("--compare-only", action="store_true", default=False, help="Skip running the recipe and do the comparison using the working directories from a previous test run.")
    parser.addoption("--data-directory", action="store", default="/lustre/cv/projects/pipeline-test-data/regression-test-data/", help="Specify directory where larger test data files are stored.")


def pytest_sessionstart(session: Session) -> None:
    """Prepare pytest session."""

    # redirect casalog for the master process, if requested
    if session.config.getoption('--nologfile') and not hasattr(session.config, 'workerinput'):
        redirect_casalog()

    # clean up .pyc to reproduce certain warnings only when the bytecode is compiled, e.g,
    #   invalid escape sequence warnings
    if session.config.getoption('--pyclean'):
        for p in pathlib.Path('.').rglob('*.py[co]'):
            p.unlink()
        for p in pathlib.Path('.').rglob('__pycache__'):
            p.rmdir()


def pytest_configure(config: Config) -> None:
    # pytest.config (global) is deprecated from pytest ver>5.0.
    # we save a copy of its content under `pytest.pytestconfig` for an easy access from helper classes.
    pytest.pytestconfig = config


def pytest_collection_finish(session: Session) -> None:
    """Exit after collection if only collect test."""

    if session.config.getoption('--collect-tests'):
        with open('collected_tests.txt', 'w') as f:
            for item in session.items:
                node_id=item.nodeid.split("::")
                node_id[0]=str(item.fspath)
                node_id="::".join(node_id)
                print(node_id)
                f.write(node_id+'\n')
        pytest.exit('Tests collection completed.')


@pytest.fixture(scope="session", autouse=True)
def redirect_casalog_for_workers(request: FixtureRequest) -> None:
    """Remove casalog for each worker, if requested."""
    
    if request.config.getoption("--nologfile") and hasattr(request.config, 'workerinput'):
        redirect_casalog()


def redirect_casalog() -> None:
    """Redirect casalog to /dev/null before executeting tests.

    We clean up the default logfile potentially created from the CASA session initialization,
    and then redirect logging to `/dev/null`.
    """
    last_casalog = casalog.logfile()
    if last_casalog != '/dev/null':
        try:
            os.remove(last_casalog)
        except OSError as e:
            pass
        casalog.setlogfile('/dev/null')


def _auto_mark(item: Item) -> None:
    """
    Apply marks based on test location (path segments) and node id.
    Adjust the paths to match your repo layout.
    """
    # Robust path-based matching
    parts = tuple(pathlib.Path(getattr(item, "path", "")).parts)

    # High-level buckets by directory
    if "tests" in parts and "regression" in parts:
        item.add_marker(pytest.mark.regression)
        if "slow" in parts:
            item.add_marker(pytest.mark.slow)
        if "fast" in parts:
            item.add_marker(pytest.mark.fast)
        if "alma" in parts[-1]:
            item.add_marker(pytest.mark.alma)
        if "nobeyama" in parts[-1]:
            item.add_marker(pytest.mark.nobeyama)
        elif "vlass" in parts[-1]:
            item.add_marker(pytest.mark.vlass)
        elif "vla" in parts[-1]:
            item.add_marker(pytest.mark.vla)
        if "sd" in parts[-1]:
            item.add_marker(pytest.mark.sd)
        elif "if" in parts[-1]:
            item.add_marker(pytest.mark.interferometry)

    if "tests" in parts and "component" in parts:
        item.add_marker(pytest.mark.component)

    # Fallback: if none of the main buckets matched, default to unit
    if not any(k in item.keywords for k in ("component", "regression")):
        item.add_marker(pytest.mark.unit)


def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    # 1) Auto-mark everything
    for item in items:
        _auto_mark(item)

    # 2) Optionally skip slow unless --longtests
    if not config.getoption("--longtests"):
        skip_slow = pytest.mark.skip(reason="need --longtests option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)
