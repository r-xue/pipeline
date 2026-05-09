# Comparing pipeline executions

When refactoring pipeline code, it is common to want to restructure and
modify the code but leave the overall behaviour unchanged. It falls to the
developer to manually verify that the pipeline behaviour has not changed.

There are three general ways to compare pipeline executions and determine
whether pipeline behaviour has changed:

1. Check whether the same CASA tasks are called in the same sequence and with
   the same arguments;
1. In addition to the above, check whether the same sequence of CASA tool
   commands is invoked;
1. Check whether the output data are identical (within limits).

This note describes ways the pipeline can assist in scenarios #1 and #2. The
comparison of output caltables and images is considered out of scope for this
document.

## Ordered pipeline execution logs

In short, to compare two pipeline runs, it is simplest to diff the log files
for each run. However, the standard CASA command log (casa_commands.log) is of
limited use due to the number of false positives raised with diff; there are
several unordered task arguments (spw, flag commands, output directories,
etc.) which can vary from run to run but actually result in identical data on
disk.

The pipeline has a debug mode to facilitate the comparison of pipeline runs.
Lowering the log threshold from INFO results enables a debug mode which
processes all CASA task calls and CASA tool calls and logs them to a file. The
log records are processed to give a stable output; arguments are sorted and
variable arguments replaced (UUIDs, directory paths, etc.) so that no/minimal
false positives are raised.

The filename of the processed execution log is of the format
`casacalls-<HOSTNAME>.txt`. The hostname is included so that the output of
each MPI client can be determined for HPC executions.

### Log all CASA task calls

To log all CASA tasks to a file suitable for diff, use a log level of DEBUG, e.g.,

```python
h_init(loglevel='debug')
```

or if you are using recipereducer,

```python
pipeline.recipereducer.reduce(vis=['uid___A002_X30a93d_X43e'], loglevel='debug')
```

### Log all CASA tool calls

To record all CASA task and CASA tool invocations, use a log level of TRACE,
e.g.,

```python
h_init(loglevel='trace')
```

or if you are using recipereducer,

```python
pipeline.recipereducer.reduce(vis=['uid___A002_X30a93d_X43e'], loglevel='trace')
```

#### Restricting log output

Lowering the log level to TRACE will log all tool calls — including tools such
as CASA quanta and measures. Records from these tools may be considered noise;
you may prefer to leave the log level at DEBUG and selectively enable output
for certain tools by editing `pipeline/infrastructure/casa_tools.py`. For
example, to enable logging of calls to the imager tool alone, edit `casa_tools.py`
and change the private class definition from:

```python
_logging_imager_cls = create_logging_class(casatools.imager,
                                           level=logging.INFO, to_log=(...))
```

to log all method calls at DEBUG level:

```python
_logging_imager_cls = create_logging_class(casatools.imager, level=logging.DEBUG)
```

To omit log records entirely for a tool, assign the unwrapped CASA tool class directly:

```python
imager = casatools.imager()
```

## Example

Scenario: two branches have different code but should result in identical CASA
calls. This can be tested by running an execution for both branches, keeping the:

1. same CASA version
1. same data
1. same pipeline recipe

From an initial root directory, run the pipeline procedure for the main branch with

```console
mkdir main && cd main
pixi run -e default casa
```

```python
import pipeline
pipeline.recipereducer.reduce(vis=['/rawdata/uid___A002_X30a93d_X43e'],
                              procedure='hifacal.xml',
                              loglevel='debug')
```

From the same initial root directory, run the pipeline procedure for the feature branch:

```console
mkdir branch && cd branch
pixi run -e default casa
```

```python
import pipeline
pipeline.recipereducer.reduce(vis=['/rawdata/uid___A002_X30a93d_X43e'],
                              procedure='hifacal.xml',
                              loglevel='debug')
```

You can now diff the output with:

```console
diff main/casacalls-<hostname>.txt branch/casacalls-<hostname>.txt
```
