## Unit tests

The principles of writing pipeline unit tests are described in [PIPE-862](https://open-jira.nrao.edu/browse/PIPE-862). Some tips for running local unit tests are below:

To invoke unit tests under the repo root directory,

```console
$ <casadir>/bin/python3 -m pytest -v pipeline/
```

Alternatively, use
```console
$ <casadir>/bin/casa --nogui --nologger --logfile /dev/null -c "import pytest; pytest.main(['-v', '.'])" 
```
However, this will use `casashell` (i.e., `python3 -m casashell`), which slightly slows things down.

If `pytest-xdist` and `pytest-cov` are avilable, you can also try:
```console
$ <casadir>/bin/python3 -m pytest -n auto -v --cov=pipeline/ --cov-report=html
```
On a multi-core system, `pytest-xdist` can speed things up significantly, e.g.,

    walltime   0m21.744s # with pytest-xdist
    walltime   2m13.076s # without pytest-xdist

A summary of various test framworks and tools is available in [PIPE-806](https://open-jira.nrao.edu/browse/PIPE-806).

## Regression tests

see https://open-confluence.nrao.edu/display/PL/Regression+Testing