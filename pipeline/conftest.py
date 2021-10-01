"""This file contains some pipeline-specific pytest configuration settings."""

import os
import pytest

from casatasks import casalog


def redirect_casalog():
    """Redirect casalog to /dev/null before executeting tests.

    We clean up the default logfile potentially created from the CASA session initialization, 
    and then redirect logging to `dev/null`.
    """
    last_casalog = casalog.logfile()
    if last_casalog != '/dev/null':
        try:
            os.remove(last_casalog)
        except OSError as e:
            pass
        casalog.setlogfile('/dev/null')


def pytest_sessionstart(session):
    """Redirect casalog for the master process (if needed)."""
    workerinput = getattr(session.config, 'workerinput', None)
    if workerinput is None:
        redirect_casalog()


@pytest.fixture(scope="session", autouse=True)
def redirect_casalog_for_workers():
    """Remove casalog for each session/worker."""
    return redirect_casalog()
