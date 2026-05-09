# Automated tests

The pipeline uses the `PipelineTester` framework (`tests/testing_utils.py`) for both
component and regression tests.

## Test types

### Component tests

Component tests validate individual pipeline tasks or small task sequences in isolation.
They focus on specific functionality and run faster than full regression tests.

- Located in `tests/component/`
- Execute specific tasks with controlled inputs
- Test edge cases without running the full pipeline recipe

### Regression tests

Regression tests validate complete pipeline recipes or PPR (Pipeline Processing Request)
executions end-to-end, comparing comprehensive output metrics against versioned reference
results.

- Located in `tests/regression/fast/` and `tests/regression/slow/`
- Run complete recipes using PPR files or recipe XML files
- Slow tests (marked `slow`) require the `--longtests` flag and are not actively maintained
  due to computing resource constraints

Tests are organized under `tests/`:

```text
tests/
├── component/
│   └── component_test.py
├── regression/
│   ├── fast/
│   │   ├── alma_if_fast_test.py
│   │   ├── alma_sd_fast_test.py
│   │   ├── vla_fast_test.py
│   │   ├── vlass_fast_test.py
│   │   └── nobeyama_sd_fast_test.py
│   └── slow/
│       ├── alma_if_slow_test.py
│       ├── alma_sd_slow_test.py
│       ├── vla_slow_test.py
│       └── vlass_slow_test.py
├── testing_utils.py                        # PipelineTester framework
└── test_pipeline_testing_framework.py
```

## Running tests

Before running locally, configure CASA to find your test data in `~/.casa/config.py`
(see {doc}`test_environment` for the full `casa-data` setup):

```python
datapath = ['/path/to/pipeline-testdata']
```

### Component tests

```console
PYTHONNOUSERSITE=1 ${casa_dir}/bin/python3 -m pytest -vv --junitxml=component-results.xml \
    -n 4 <pipeline_dir>/tests/component/.
```

Alternatively via CASA's shell:

```console
xvfb-run -d ${casa_dir}/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=component-results.xml', '-n', '4', \
     '<pipeline_dir>/tests/component'])"
```

### Regression tests

Run all fast tests (slow tests excluded by default):

```console
PYTHONNOUSERSITE=1 ${casa_dir}/bin/python3 -m pytest -vv --junitxml=regression-results.xml \
    <pipeline_dir>/tests/regression/fast/.
```

Include slow tests:

```console
PYTHONNOUSERSITE=1 ${casa_dir}/bin/python3 -m pytest -vv --longtests \
    --junitxml=regression-results.xml <pipeline_dir>/tests/regression/.
```

:::{warning}
Slow tests (marked `slow`, enabled by `--longtests`) are not actively maintained due to
computing resource constraints. They may have stale expected results or fail on current
pipeline versions. Use them with caution and verify results independently.
:::

Select by node ID:

```console
xvfb-run -d ${casa_dir}/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', \
     '<pipeline_dir>/tests/regression/fast/alma_if_fast_test.py::test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__PPR__regression'])"
```

Select by marker or name substring:

```console
# all tests with 'hifa' in their name (keyword match)
xvfb-run -d ${casa_dir}/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', \
     '-k', 'hifa', '<pipeline_dir>/tests/regression'])"

# tests with a specific dataset name
xvfb-run -d ${casa_dir}/bin/casa --nogui --nologger --agg -c \
    "import pytest; pytest.main(['-vv', '--junitxml=regression-results.xml', \
     '-k', 'mg2_20170525142607_180419', '<pipeline_dir>/tests/regression'])"
```

### Coverage

For a single regression test, using CASA's Python directly (skips the casashell layer):

```console
PYTHONNOUSERSITE=1 xvfb-run -d ${casa_dir}/bin/python3 -m pytest -v --pyclean \
    --cov=pipeline --cov-report=html \
    ${pipeline_dir}/tests/regression/fast/alma_if_fast_test.py::test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__PPR__regression
```

The coverage report is saved to `htmlcov/index.html`. To merge `.coverage.*` data files
from parallel or separate runs into a single report:

```console
# merge all .coverage* files from subdirectories
coverage combine --keep $(find ./* -name ".coverage*")

# generate the final HTML report
coverage html
```

### Running in mpicasa

Five-process example (1 client + 4 workers):

```console
PYTHONNOUSERSITE=1 xvfb-run -d ${casa_dir}/bin/mpicasa \
    -display-allocation -display-map --report-bindings -oversubscribe -n 5 \
    ${casa_dir}/bin/casa --cachedir ./rcdir --configfile ./rcdir/config.py \
    --startupfile ./rcdir/startup.py --nologger --log2term --nogui --agg -c \
    "import pytest; pytest.main(['--junitxml=./regression.xml', \
     '<pipeline_dir>/tests/regression/fast/alma_if_fast_test.py::test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__PPR__regression'])"
```

### Compare-only mode

Re-evaluate results against reference values without re-running the pipeline. Useful for
tweaking tolerances, updating expected results files, or debugging comparison logic:

```console
xvfb-run casa --nogui --nologger --log2term --agg -c \
    "import pytest; pytest.main(['-vv', '-m alma and fast', '--compare-only', '<pipeline_dir>'])"
```

## Adding new tests

### Test inputs

Writing a new test requires the following inputs in the
[pipeline-testdata](https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline-testdata/browse)
repository. Review the
[README](https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline-testdata/browse/readme.md)
before adding new data.

#### Input SDM/MS

Use the smallest, fastest dataset available. Check whether the dataset already exists in
`pl-unittest/` or elsewhere in the repository before adding a new one. To add a new dataset,
follow the testdata repository README instructions.

#### Expected output

Expected output follows the format generated by `infrastructure/renderer/regression.py`,
with `:::` and an optional relative tolerance appended to each line:

```text
s15.hifa_gfluxscaleflag.uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.num_rows_flagged.after=268048:::
s16.hifa_gfluxscale.uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.field_0.spw_0.I=2.2542400978094923:::1e-6
```

If no tolerance is supplied, the default tolerance in `PipelineTester.run()` applies
(currently `1e-7`). To add new values to check, add to or modify a class in
`infrastructure/renderer/regression.py` (e.g. `FluxcalflagRegressionExtractor`).

#### PPR (optional)

If a PPR is supplied, the framework uses `executeppr` (ALMA) or `executevlappr` (VLA). Without
a PPR, tests use `recipereducer` with a recipe XML file.

#### Versioned results files

Results files carry the CASA and Pipeline versions in their names:

```text
<dataset_name>.casa-<CASA_version>-pipeline-<Pipeline_version>.results.txt
```

For example:

```text
uid___A002_Xc46ab2_X15ae.casa-6.5.1-15-pipeline-2023.1.0.8.results.txt
uid___A002_Xc46ab2_X15ae.casa-6.6.0-21-pipeline-2024.1.0.12.results.txt
```

When `expectedoutput_dir` is used instead of a specific `expectedoutput_file`, the framework
scans for `*.results.txt` files, parses their versions, and automatically selects the file
with versions closest to (but not exceeding) the current running versions. Store all inputs
under `pl-regressiontest/`:

```text
pl-regressiontest/
└── uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/
    ├── PPR.xml
    ├── uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.casa-6.1.1-15-pipeline-2020.1.0.40.results.txt
    └── uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.casa-6.1.3-2-pipeline-2021.1.1.6.results.txt
```

### Writing component tests

Add new functions to `tests/component/component_test.py` using `mode='component'`. The
`tasks` parameter is a list of `(task_name, kwargs)` tuples:

```python
@pytest.mark.importdata
@pytest.mark.selfcal
def test_dataset__task_sequence__component():
    """Run test of specific task sequence.

    Dataset(s):  dataset_name.ms
    Task(s):     hifa_importdata, hif_selfcal
    """
    data_dir = 'pl-unittest'
    visname = 'dataset_name.ms'
    tasks = [
        ('hifa_importdata', {'vis': casa_tools.utils.resolve(os.path.join(data_dir, visname)),
                             'datacolumns': {'data': 'regcal_contline'}}),
        ('hif_selfcal', {}),
        ('hif_selfcal', {'restore_only': True}),
    ]

    pt = PipelineTester(
        visname=[visname],
        mode='component',
        tasks=tasks,
        output_dir='test_output_dir',
        expectedoutput_dir='pl-componenttest/test_name',
    )
    pt.run()
```

### Writing regression tests

Add new functions to `tests/regression/fast/` or `tests/regression/slow/`. The `mode`
parameter defaults to `'regression'`.

Using a PPR:

```python
@pytest.mark.seven
def test_uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small__PPR__regression():
    """Run ALMA cal+image regression on a small test dataset with a PPR file.

    PPR:      pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml
    Dataset:  uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
    """
    ref_directory = 'pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small'

    pt = PipelineTester(
        visname=['uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
        ppr=f'{ref_directory}/PPR.xml',
        input_dir='pl-unittest',
        expectedoutput_dir=ref_directory,
    )
    pt.run()
```

Using a recipe XML file:

```python
def test_dataset__recipe_name__regression():
    """Run test with recipe XML file.

    Recipe:   procedure_hifa_image
    Dataset:  uid___A002_Xef72bb_X9d29
    """
    ref_directory = 'pl-regressiontest/uid___A002_Xef72bb_X9d29'

    pt = PipelineTester(
        visname=['uid___A002_Xef72bb_X9d29'],
        recipe='procedure_hifa_image.xml',
        input_dir=ref_directory,
        expectedoutput_dir=ref_directory,
    )
    pt.run()
```

:::{note}
Build success and failure is reported in Bamboo with email notification. Failure does not
prevent a pre-release tarball from being published.
:::
