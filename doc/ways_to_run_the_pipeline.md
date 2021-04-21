# Ways to run the Pipeline

## PPR

At the highest level of abstraction, we can execute a pipeline processing request (ppr)
This can be done at the command line, or at a CASA command prompt.

Execute PPR from the command line:
```console
$ casa --nologger --nogui -c $SCIPIPE_HEURISTICS/pipeline/runvlapipeline.py PPRnew_VLAT003.xml

$ casa --nogui --log2term -c $SCIPIPE_HEURISTICS/pipeline/runvlapipeline.py PPRnew_VLAT003.xml
```
At a CASA command prompt:

```python
$ casa
# execute a pipeline processing request (PPR)
CASA <1>: import pipeline.infrastructure.executevlappr as eppr
CASA <2>: eppr.executeppr('PPR_VLAT003.xml', importonly=False)
```

## Series of steps invoking CASA Pipeline tasks

A little lower level of abstraction would be to run the pipeline as a series of
steps as described in the VLA pipeline [casaguide](
https://casaguides.nrao.edu/index.php/VLA_CASA_Pipeline-CASA4.5.3)

At the lowest level of abstraction, we can run the pipeline as a series of steps
like the following

A pipeline run will generate a file like the following:
  ```
  pipeline_test_data/VLAT003/working/pipeline-20161014T172229/html/casa_pipescript.py
  ```
It contains a series of steps like those described in the casaguide.

We can execute this script from CASA
```python
CASA <1>: execfile('casa_pipescript.py')
```

Or we can run it from the command line:
```console
casa --nogui --log2term -c casa_pipescript.py
```

We can edit the script and turn on memory usage for each task:
```python
CASA <1>: h_init()
CASA <2>: import pipeline
CASA <3>: pipeline.infrastructure.utils.enable_memstats()
```

We can also turn weblog and plotting off:
```python
CASA <1>: h_init(pipelinemode="automatic",loglevel="info",plotlevel="summary",output_dir="./",weblog=False,overwrite=True,dryrun=False,acceptresults=True)
```

Or we can turn debug mode on, weblog off:
```python
CASA <1>: h_init(pipelinemode="automatic",loglevel="debug",plotlevel="summary",output_dir="./",weblog=True,overwrite=True,dryrun=False,acceptresults=True)
```

Full example of running Pipeline importdata task on CASA prompt:

```python
CASA <1>: h_init()
CASA <2>: h_save()
CASA <3>: import pipeline
CASA <4>: hifv_importdata(vis=['../rawdata/13A-537.sb24066356.eb24324502.56514.05971091435'], session=['session_1'], overwrite=False)
CASA <5>: h_save()
CASA <6>: exit
```

```python
casa
CASA <1>: context = h_resume(filename='last')
```

## Creating and running Pipeline tasks, bypassing CASA task interface
At the lowest level of abstraction, we can bypass the CASA Pipeline Task interface, and work directly within 
CASA / Python, by instantiating a Pipeline InputsContainer object for the Pipeline Task, using it to instantiate a Pipeline Task object,
and then running its 'execute' method to get the task result, as shown in this example (assumed to run in a 
directory where the Pipeline has already been partly run, i.e. a context already exists):
```python
CASA <1>: context = pipeline.Pipeline(context='last').context

CASA <1>: vis='13A-537.sb24066356.eb24324502.56514.05971091435.ms'
CASA <2>: m = context.observing_run.get_ms(vis)
CASA <3>: spws=m.get_spectral_windows()

CASA <4>: inputs = pipeline.infrastructure.vdp.InputsContainer(pipeline.hifv.tasks.hanning.Hanning, context)
CASA <5>: task = pipeline.hifv.tasks.hanning.Hanning(inputs)
CASA <6>: result = task.execute(dry_run=False)
CASA <7>: result.accept(context)
CASA <8>: context.save() 
```

If we don't have a PPR or an executable script available.

```python
$ casa
import pipeline
import pipeline.recipes.hifv as hifv
# the next line will only importevla and save a context, b/c importonly=True
hifv.hifv(['../rawdata/13A-537.foofoof.eb.barbar.2378.2934723984397'], importonly=True)

context = pipeline.Pipeline(context='last').context
vis = '13A-537.foofoof.eb.barbar.2378.2934723984397.ms'
# get the domain object
m = context.observering_run.get_ms(vis)
type(m)
# study this m object for INTENTS
# <class 'pipeline.domain.measurementset.MeasurementSet>
m.intents  # shows a python set of the MS intents
m.polarization  # show a list of polarization objects
```

## Running Pipeline with the "recipereducer"

To run one of the standard recipes we can use a recipereducer:
```python
import pipeline.recipereducer
pipeline.recipereducer.reduce(vis=['../rawdata/yourasdm'], procedure='procedure_hifv.xml')
```

To run a standard recipe until the end of a specified stage number (dependent on recipe) and running it with
 a different log level:

```python
import pipeline.recipereducer
pipeline.recipereducer.reduce(vis=['../rawdata/yourasdm'], procedure='procedure_hifa.xml', exitstage=6, loglevel='trace')
```
This can be useful to run the Pipeline just up to the stage that you want to debug / develop. Once the PL run has exited
 after e.g. stage 6, you could tarball the "working" directory (to be able to restore the run up to this point), 
 then create a short script in "../debug.script" with:
  
```python
task_to_run = 'hifa_tsysflag'
import pipeline
from pipeline.infrastructure import task_registry
context = pipeline.Pipeline(context='last', loglevel='info', plotlevel='default').context
taskclass = task_registry.get_pipeline_class_for_task(task_to_run)
inputs = pipeline.infrastructure.vdp.InputsContainer(taskclass, context)
task = taskclass(inputs)
result = task.execute(dry_run=False)
result.accept(context)
context.save()
``` 
and then run this with:
```console
$ casa -c ../debug.script
```

## Workflow Break/Resume

### Context-by-Stage

The context content at individual stages can be pickled after the completion of each PL task.
The [implementation](https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/commits/bf904d167c09c2f7a9e648ce3e30122185887586) is in `infrastructure.basetask` and will only be switched on if the pipeline is in the `DEBUG` (or lower) logging level. This feature works for both PPR and receipereducer runs.

The path of pickled context files is: `output_dir`/`context_name`/`saved_state`/`context-stage*.pickle`, saved along with `result-stage*.pickle` which is always present in the same directory.

### Break/Resume

* `executeppr` offers a "break/resume" feature at the workflow level (see the optional keywords [`bpaction` and `breakpoint`](https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline/browse/pipeline/infrastructure/executeppr.py)). One practical example is below, which is based on the test data available in the [pipeline-testdata](https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline-testdata) repository:

  * pl-unittest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms
  * pl-regressiontest/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small/PPR.xml

  We first put the above MeasurementSet and PPR in the `rawdata` directory of your workspace. This specific PPR file already has a breakpoint set after the first `hifa_exportdata` call:
  
  To run the PPR up to the breakpoint, at a CASA command prompt,

  ```python
  import os
  import pipeline.infrastructure.executeppr as eppr
  os.environ['SCIPIPE_ROOTDIR'] = os.getcwd()
  eppr.executeppr('../rawdata/PPR.xml', importonly=False, loglevel='debug', bpaction='break')

  ```
  
  From the current or a new CASA session, the PPR can be resumed,
  
  ```python
  import os
  import pipeline.infrastructure.executeppr as eppr
  os.environ['SCIPIPE_ROOTDIR'] = os.getcwd()
  eppr.executeppr('../rawdata/PPR.xml', importonly=False, loglevel='debug', bpaction='resume')
  ```

  **Note**: if you try to run the PPR with bpaction='resume' again, the subsequent call(s) will likely fail: `executeppr` is hardcoded to resume from the "last" context (i.e. the `.context` file in the working directory with latest timestamps), but we need the context content at the "breakpoint" stage to resume. Also, the file states (e.g. MS,s caltables) have changed. The only safe option is making a copy of the working directory before trying resume in case you might tweak the PPR in another attempt.

  For development/test purposes, one workaround to avoid copying the entire working directory is to create a fresh copy of the context pickled from the "breakpoint" stage (where your loglevel='debug' setting is crucial) in the existing working directory:
  
  First, you back up the context pickle files from differnet stages:
  ```console
  $ cp -rf pipeline-20210421T172403/saved_state pipeline-20210421T172403/saved_state_backup
  ```
  Then, at a CASA command prompt,
  ```python
  import os
  os.environ['SCIPIPE_ROOTDIR'] = os.getcwd()
  os.system('cp -rf pipeline-20210421T172403/saved_state_backup/context-stage26.pickle current.context')
  # we need some cleanup as this MS below is blocking the executetion of the stage27 hif_mstransform() call.
  os.system('rm -rf uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small_target.ms*') 
  import pipeline.infrastructure.executeppr as eppr
  eppr.executeppr('../rawdata/PPR.xml', importonly=False, loglevel='debug', bpaction='resume')
  ```
  Again, please note that the above workaround might yield scientifically meanless results due to the changing file status and is only useful for testing under certain scenarios.
  All break/resume approaches use the **current** files (e.g. MSs/caltables) in your working directory. So be aware of the existence of files/versions that might be unexpected to the resumed PL workflow task call(s)!

* With `recipereducer`, you can load context saved at a specific stage from the working directory and run/rerun the next PL task designed in the workflow (also see the demonstration in the last section). 
  
  A full recipe run based on the above test data example can be achieved with:
  
  ```python
  import pipeline.recipereducer, os
  pipeline.recipereducer.reduce(vis=['../rawdata/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
                              procedure='procedure_hifa_calimage.xml', loglevel='debug')
  ``` 

  ```python
  task_to_run='hif_checkproductsize'
  task_keywords={'maxcubesize':40.0,'maxcubelimit':60,'maxproductsize':500.0}
  import pipeline
  from pipeline.infrastructure import task_registry
  context = pipeline.Pipeline(context='pipeline-procedure_hifa_calimage/saved_state/context-stage24.pickle', loglevel='debug', plotlevel='default').context
  taskclass = task_registry.get_pipeline_class_for_task(task_to_run)
  inputs = pipeline.infrastructure.vdp.InputsContainer(taskclass, context, **task_keywords)
  task = taskclass(inputs)
  result = task.execute(dry_run=False)
  result.accept(context)
  context.save('test-context-stage25.pickle')
  ```

  `recipereducer` doesn't offer the "breakpoint" feature built in `executeppr`. However, a calculated usage of `starttage`/ `exitstage`/`context` keywords may achieve the same workflow-level "break/resume":

  ```python
  import pipeline
  context = pipeline.Pipeline(context='pipeline-procedure_hifa_calimage/saved_state/context-stage3.pickle', loglevel='debug', plotlevel='default').context
  import pipeline.recipereducer
  pipeline.recipereducer.reduce(vis=['../rawdata/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
                                procedure='procedure_hifa_calimage.xml', loglevel='debug',startstage=4,exitstage=20,
                                context=context)
  ```
## Re-render Weblog

A common development task is improving weblog. Without re-running a time-consuming pipeline task itself, you can only re-render the weblog using the existing context/result to test a small weblog-related change (e.g., minor tweaks in mako templates). Note: this will only rerun the weblog rendering portion of a pipeline stage (therefore limits your testing scope).

```python
import pipeline, os
from pipeline.infrastructure.renderer import htmlrenderer as hr
context = pipeline.Pipeline(context='last', loglevel='debug', plotlevel='default').context
os.environ['WEBLOG_RERENDER_STAGES']='16'
hr.WebLogGenerator.render(context)
```

## Known issues

Don't worry about the following intermittent message at the end of a pipeline run. It's a bug
  but it doesn't mean the pipeline was unsuccessful.

```console
invalid command name "102990944filter_destroy"
    while executing
"102990944filter_destroy 3657 ?? ?? ?? ?? ?? ?? ?? ?? ?? ?? 0 ?? ?? .86583632 17 ?? ?? ??"
    invoked from within
"if {"[102990944filter_destroy 3657 ?? ?? ?? ?? ?? ?? ?? ?? ?? ?? 0 ?? ?? .86583632 17 ?? ?? ??]" == "break"} break"
    (command bound to event)
```