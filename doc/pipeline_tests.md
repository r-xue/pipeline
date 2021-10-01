## Unit tests

The instruction of writing/running pipeline unit tests is described in [PIPE-862](https://open-jira.nrao.edu/browse/PIPE-862). Some tips for running local tests are below:

To invoke unit tests within the pipeline repo directory,

```console
<casadir>/bin/python3 -m pytest -v .
```

Alternatively, use

```console
<casadir>/bin/casa --nogui --nologger --agg --nologfile -c "import pytest; pytest.main(['-v', '.'])"
```

The CASA call version will use `casashell` (i.e., `python3 -m casashell`), which might show slightly different results.
The `--nologfile` (equivalent to `--logfile /dev/null`) here can prevent generating CASA log files in your pipeline repository directory.

If `pytest-xdist` and `pytest-cov` are avilable, you can also try:

```console
<casadir>/bin/python3 -m pytest -n auto -v --cov=pipeline/ --cov-report=html
```
or

```console
<casadir>/bin/casa --nogui --nologger --agg --nologfile -c "import pytest; pytest.main(['-v', '.', '-n', 'auto'])"
```

On a multi-core system, `pytest-xdist` can speed things up significantly, e.g.,

    walltime   0m21.744s # with pytest-xdist
    walltime   2m13.076s # without pytest-xdist

A summary of various test frameworks and tools is available in [PIPE-806](https://open-jira.nrao.edu/browse/PIPE-806).

## Regression tests

see https://open-confluence.nrao.edu/display/PL/Regression+Testing for details

To add the *xdist* flavor to your local regression run, one can try:

```console
$  xvfb-run -d <casadir>/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', '-n', '4', '<pl_repodir>/pipeline/infrastructure/utils/regression-tester.py'])"
```

Here are some additional examples to run selected tests using their [node IDs or custom makers](https://docs.pytest.org/en/latest/example/markers.html):

```console
# select a single test using its node ID
xvfb-run -d <casadir>/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', '<pl_repodir>/pipeline/infrastructure/utils/regression-tester.py::test_uid___A002_X85c183_X36f_SPW15_23__PPR__regression'])"
```

```console
# select a group of tests using the test marker `hifa`
xvfb-run -d <casadir>/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', '-m', 'hifa', '<pl_repodir>/pipeline/infrastructure/utils/regression-tester.py'])"
```

```console
# select all tests with `mg2_20170525142607_180419` in the test function names
xvfb-run -d <casadir>/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', '-k', 'mg2_20170525142607_180419', '<pl_repodir>/pipeline/infrastructure/utils/regression-tester.py'])"
```