"""This file contains some pipeline-specific pytest configuration settings."""

import os
import pytest


def redirect_casalog():
    """Redirect casalog to /dev/null before executeting tests.

    We first clean up the default logfile created when casatasks is imported,
    and then redirect casa-*.log to dev/null
    """
    from casatasks import casalog
    current_casalog = casalog.logfile()
    if os.path.exists(current_casalog) and current_casalog != '/dev/null':
        os.remove(current_casalog)
    casalog.setlogfile('/dev/null')


redirect_casalog()


@pytest.fixture(scope="session", autouse=True)
def redirect_casalog_for_workers():
    """Remove casalog for worker threads."""
    return redirect_casalog()
