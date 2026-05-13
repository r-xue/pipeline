# Pipeline Context Use Cases

## Overview

The pipeline `Context` is the central state object used for a pipeline execution. It carries observation data, calibration state, imaging state, execution history and state, project metadata, and serves as the primary communication channel between pipeline stages.

This document catalogues the use cases of the current pipeline context as determined by examination of the codebase. The goal is to describe the current pipeline context implementation and document observed limitations.

---

## 1. Context Lifecycle

The canonical flow through the context is:

1. **Create session** — `h_init()` constructs a `launcher.Pipeline(...)` and returns a new `Context`. In PPR-driven execution, `executeppr()` or `executevlappr()` also populates project metadata at this point.
2. **Populate data** — Import tasks (`h*_importdata`) attach datasets to the context's domain model (`context.observing_run`, measurement sets, scans, SPWs, etc.).
3. **Execute tasks** — Tasks execute against the in-memory context and return a `Results` object. After each task, `Results.accept(context)` records the outcome and mutates shared state.
4. **Accept results** — Inside `accept()`, results are merged via `Results.merge_with_context(context)`. A `ResultsProxy` is pickled to disk per-stage to keep the in-memory context bounded. The weblog is typically rendered after each top-level stage.
5. **Save / resume** — `h_save()` pickles the context; `h_resume(filename='last')` restores it. Driver-managed breakpoints and developer debugging workflows rely on this cycle.

### Key implementation references

- `Context` / `Pipeline`: `pipeline/infrastructure/launcher.py`
- CLI lifecycle tasks: `pipeline/h/cli/h_init.py`, `pipeline/h/cli/h_save.py`, `pipeline/h/cli/h_resume.py`
- Task dispatch & result acceptance: `pipeline/h/cli/utils.py`, `pipeline/infrastructure/basetask.py`
- PPR-driven execution loops:
  - ALMA: `pipeline/infrastructure/executeppr.py` (used by `pipeline/runpipeline.py`)
  - VLA: `pipeline/infrastructure/executevlappr.py` (used by `pipeline/runvlapipeline.py`)
- Direct XML procedure execution: `pipeline/recipereducer.py`
- MPI distribution: `pipeline/infrastructure/mpihelpers.py`
- QA framework: `pipeline/infrastructure/pipelineqa.py`, `pipeline/qa/`
- Weblog renderer: `pipeline/infrastructure/renderer/htmlrenderer.py`

---

## 2. Use Cases

Each use case below is mapped directly to current code paths and concrete runtime behavior found in this repository. The goal is to describe what the context must do and where that capability is implemented today (modules, classes, or functions). Implementation notes follow each use case and reference the exact symbols and files that implement or exercise the behavior.

The following fields are used in each use case:

- **Actor(s):** The human or system role that directly creates, updates, consumes, or inspects the context state described by the use case. Actors are role categories, not specific task names or current implementations.
- **Summary:** What the context must do to satisfy the use case.
- **Invariant:** A condition that must always be true while the system is operating. Present only where a meaningful invariant exists.
- **Postcondition:** A condition that must be true after a specific operation completes. Present only where a meaningful postcondition exists.

---

### UC-01 — Populate, Access, and Provide Observation Metadata

| | |
|-------|---------|
| **Actor(s)** | Data import task, any downstream task, heuristics, renderers, QA handlers |
| **Summary** | Import tasks under `pipeline/h/*_importdata` populate observation metadata into `context.observing_run` (MS objects, SPWs, fields, scans). Downstream code queries this metadata via `context.observing_run.get_ms(...)`, `context.observing_run.measurement_sets`, and related helpers. Derived datasets (calibrated/averaged subsets) are registered on the domain objects rather than mutating original MS objects. |
| **Invariant** | All populated datasets and derived dataset records remain queryable for the lifetime of the session without repeating the import process. |

**Implementation notes** — `context.observing_run` holds the observation metadata and is the most frequently queried attribute of the context:

- `context.observing_run.get_ms(name=vis)` — resolve an MS by filename
- `context.observing_run.measurement_sets` — all registered MS objects
- `context.observing_run.ms_reduction_group` — per-group reduction metadata (single-dish)
- Provenance attributes: `start_datetime`, `end_datetime`, `project_ids`, `schedblock_ids`, `execblock_ids`, `observers`

The MS objects stored by `context.observing_run` carry information about scans, fields, SPWs, antennas, reference antenna ordering, etc. Tasks read per-MS state like `ms.reference_antenna`, `ms.session`, `ms.start_time`, `ms.origin_ms`.

For the single-dish pipeline, this use case also includes per-MS `DataTable` products referenced through `context.observing_run.ms_datatable_name`. These are not just raw imported metadata tables: they persist row-level metadata and derived quantities used by downstream SD tasks. During SD import, the reader populates `DataTable` columns such as `RA`, `DEC`, `AZ`, `EL`, `SHIFT_RA`, `SHIFT_DEC`, `OFS_RA`, and `OFS_DEC`, including coordinate conversions into the pipeline's chosen celestial frame (for example ICRS) so later imaging, gridding, plotting, and QA code can reuse those values efficiently.

---

### UC-02 — Cross-MS Metadata Matching and Lookup
| | |
|-------|---------|
| **Actor(s)** | Calibration tasks, imaging tasks, heuristics
| **Summary** | The code implements cross-MS lookup via `context.observing_run` utilities: virtual SPW mapping (`virtual2real_spw_id` / `real2virtual_spw_id`) and type-aware filters (`get_measurement_sets_of_type(...)`). Downstream calibration and imaging tasks call these helpers directly to resolve SPWs, fields, and MS-level selections across heterogeneous datasets. |
| **Postcondition** | Downstream tasks can resolve applicable metadata across registered MSes using a unified identifier scheme, and can look up MSes and data columns by data type.

**Implementation notes** - `context.observing_run` handles the virtual spw mapping, as well as filtering MSes by type:

- `context.observing_run.get_measurement_sets_of_type(dtypes)` — filter by data type (RAW, REGCAL_CONTLINE_ALL, BASELINED, etc.)
- `context.observing_run.virtual2real_spw_id(vspw, ms)` / `real2virtual_spw_id(...)` — translate between abstract pipeline SPW IDs and CASA-native IDs
- `context.observing_run.virtual_science_spw_ids` — virtual SPW catalog

---

### UC-03 — Store and Provide Project-Level Metadata

| | |
|-------|---------|
| **Actor(s)** | Initialization, any task, report generators |
| **Summary** | Project-level metadata is populated by initialization and PPR executors (`executeppr()` / `executevlappr()`), is stored in `context.project_summary`, `context.project_structure`, and `context.project_performance_parameters`, and is read by heuristics and reporters (see `pipeline/infrastructure/executeppr.py`, `pipeline/infrastructure/executevlappr.py`, and `pipeline/infrastructure/launcher.py`). |
| **Invariant** | Project metadata is available for the lifetime of the processing session. |

**Implementation notes** — project metadata (properties of the observation program such as PI, targeted sensitivities, beam requirements) is set during initialization or import, is not modified after import, and is read many times:

- `context.project_summary = project.ProjectSummary(...)` — set by `executeppr()` / `executevlappr()`
- `context.project_structure = project.ProjectStructure(...)` — set by PPR executors
- `context.project_performance_parameters` — performance parameters from the PPR
- `context.set_state('ProjectStructure', 'recipe_name', value)` — used by `recipereducer.reduce()` and SD heuristics
- `context.processing_intents` — set by `Pipeline` during initialization

---

### UC-04 — Register, Query, and Update Calibration State

| | |
|-------|---------|
| **Actor(s)** | Calibration tasks, export tasks, restore tasks, report generators |
| **Summary** | Calibration workflows register and query cal tables through `context.callibrary` (see `context.callibrary.add()`, `context.callibrary.active.get_caltable()`, `context.callibrary.unregister_calibrations()`), which records application scope and provenance; tasks query the callibrary to find applicable calibrations for selections. Atomic registration/de-registration is handled by callibrary primitives and `CalApplication` objects. |
| **Invariant** | Calibration state is queryable and correctly scoped to data selections, and can be updated as processing progresses. |

**Implementation notes** — `context.callibrary` is the primary cross-stage communication channel for calibration workflows:

- **Write:** `context.callibrary.add(calto, calfrom)` — register a calibration application (cal table + target selection); `context.callibrary.unregister_calibrations(matcher)` — remove by predicate
- **Read:** `context.callibrary.active.get_caltable(caltypes=...)` — list active cal tables; `context.callibrary.get_calstate(calto)` — get full application state for a target selection
- Backed by `CalApplication` → `CalTo` / `CalFrom` objects with interval trees for efficient matching.
 - The callibrary also supports de-registration of trial or reverted calibrations via predicate-based removal.
 - The current implementation bundles project metadata (scientific intent and constraints: PI, targeted sensitivities, beam requirements) and workflow/recipe metadata (describe how the data should be processed: recipes, execution instructions, heuristic parameters) in the ProjectStructure class. These have different origins and lifecycles.

---

### UC-05 — Manage Imaging State

| | |
|-------|---------|
| **Actor(s)** | Imaging tasks, downstream heuristics, and export tasks |
| **Summary** | Imaging/planning tasks (`makeimlist`, `editimlist`) and execution tasks (`makeimages`, `tclean`) populate and consume imaging state via context attributes (for example `clean_list_pending`, `imaging_parameters`, `synthesized_beams`, `size_mitigation_parameters`). These attributes are written/read by imaging helpers and heuristics across `pipeline/h*` tasks and imaging utilities. |
| **Invariant** | Imaging state reflects contributions from all completed imaging-related stages, and is available for reading or refinement by subsequent stages. |

Imaging workflows consist of two separate phases: a lightweight planning phase (for example, `makeimlist`, `editimlist`) that assembles imaging instructions, target lists, and imaging-mode heuristics and a computationally intensive execution phase (for example, `makeimages`) that performs the heavy imaging work.

**Implementation notes** — imaging state is stored as ad-hoc attributes on the context object with no formal schema. Defensive `hasattr()` checks appear throughout the code to guard against attributes that may not yet exist:

| Attribute | Written by | Read by |
|---|---|---|
| `clean_list_pending` | `editimlist`, `makeimlist`, `findcont`, `makeimages` | `findcont`, `transformimagedata`, `makeimages`, `vlassmasking` |
| `clean_list_info` | `makeimlist`, `makeimages` | `makeimages` |
| `imaging_mode` | `editimlist` | `makermsimages`, `makecutoutimages`, `makeimages`, VLASS export/display code |
| `imaging_parameters` | `imageprecheck` | `tclean`, `checkproductsize`, `makeimlist`, heuristics |
| `synthesized_beams` | `imageprecheck`, `tclean`, `checkproductsize`, `makeimlist`, `makeimages` | `imageprecheck`, `editimlist`, `tclean`, `uvcontsub`, `checkproductsize`, heuristics |
| `size_mitigation_parameters` | `checkproductsize` | downstream stages |
| `selfcal_targets` | `selfcal` | `makeimlist` |
| `selfcal_resources` | `selfcal` | `exportdata` |

---

### UC-06 — Register and Query Produced Image Products

| | |
|-------|---------|
| **Actor(s)** | Imaging tasks, export tasks, report generators |
| **Summary** | Image registries are explicit lists on the context (`context.sciimlist`, `context.calimlist`, `context.rmsimlist`, `context.subimlist`) populated by imaging and export tasks (e.g., `makeimages`, `exportdata`) so downstream tasks and reporters can discover produced image products by type. |
| **Invariant** | Produced image products are registered by type and remain queryable for downstream processing, reporting, and export. |

**Implementation notes** — image libraries provide typed registries:

- `context.sciimlist` — science images
- `context.calimlist` — calibrator images
- `context.rmsimlist` — RMS images
- `context.subimlist` — sub-product images (cutouts, cubes)

---

### UC-07 — Track Current Execution Progress

| | |
|-------|---------|
| **Actor(s)** | Workflow orchestration layer, tasks, pipeline operators |
| **Summary** | Execution progress is tracked by fields set during task execution (`context.stage`, `context.task_counter`, `context.subtask_counter`) by the `Executor` and `StandardTaskTemplate` in `pipeline/infrastructure/basetask.py` and by driver entrypoints in `pipeline/h/cli/`. These fields, together with `context.results`, provide an ordered record that is preserved on save/resume. |
| **Invariant** | The currently executing stage is identifiable and completed stages are recorded in stable order. |

**Implementation notes:**

- `context.stage`, `context.task_counter`, `context.subtask_counter` track progress

---

### UC-08 — Preserve Per-Stage Execution Record

| | |
|-------|---------|
| **Actor(s)** | Report generators, pipeline operators, workflow orchestration layer |
| **Summary** | Per-stage execution records are captured as `ResultsProxy` objects (see `pipeline/infrastructure/basetask.py`) and appended to `context.results` via `Results.accept()` / `Results.merge_with_context()`. Results proxies are pickled to disk to bound memory and carry timing, traceback, invocation arguments, and outcome metadata for reporting and resume. |
| **Invariant** | Each completed stage retains its full execution record — identity, outcome, timing, traceback, and invocation arguments — for the lifetime of the session. |

**Implementation notes:**

- `context.results` holds an ordered list of `ResultsProxy` objects which are proxied to disk to bound memory
- Timetracker integration provides per-stage timing data
- Results proxies store basenames for portability

---

### UC-09 — Propagate Task Outputs to Downstream Tasks

| | |
|-------|---------|
| **Actor(s)** | Any task producing output, downstream tasks |
| **Summary** | Tasks publish outputs by returning `Results` objects which are merged into the shared `Context` via `Results.accept()` / `Results.merge_with_context()`; this path updates `context.callibrary`, image registries, and context attributes so downstream stages can consume published outputs without inspecting raw result objects. |
| **Postcondition** | Downstream tasks can access the propagated processing state they need. |

**Implementation notes** — the intended primary mechanism in the current pipeline is immediate propagation through context state updated during result acceptance. Over time, some workflows also came to inspect recorded results directly. Both patterns exist in the codebase, but the second should be understood as an accreted pattern rather than the original design intent.

This use case is also a concrete example of context creep caused by weakly enforced contracts: the intended contract was that downstream tasks would consume explicitly merged shared state, but later code sometimes reached into `context.results` directly when that contract was not maintained consistently.

1. **Immediate state propagation** — `Results.merge_with_context(context)` updates calibration library, image libraries, and dedicated context attributes such as `clean_list_pending`, `clean_list_info`, `synthesized_beams`, `size_mitigation_parameters`, `selfcal_targets`, and `selfcal_resources` so later tasks can access the current processing state directly without parsing another task's results object.
2. **Recorded-result inspection** — some tasks read `context.results` to find outputs from earlier stages when those outputs are needed from the recorded results rather than from merged shared state. This pattern introduces coupling to recipe order or to another task's result class structure. For example:
   - VLA tasks compute `stage_number` from `context.results[-1].read().stage_number + 1`
   - `vlassmasking` iterates `context.results[::-1]` to find the latest `MakeImagesResult`
   - Export/AQUA code reads `context.results[0]` and `context.results[-1]` for timestamps

---

### UC-10 — Provide a Transient Intra-Stage Workspace

| | |
|-------|---------|
| **Actor(s)** | Aggregate tasks, child tasks, task execution framework |
| **Summary** | The `StandardTaskTemplate` / `Executor` pattern provides a transient workspace: `StandardTaskTemplate.execute()` executes child tasks against a pickled copy of the inputs/context and `Executor.execute(job, merge=True)` commits a child's `Results` via `result.accept(context)`. Temporary mutations are discarded unless explicitly merged. |
| **Invariant** | State changes made while executing against a temporary child-task context do not escape that workspace unless they are explicitly accepted and merged. |
| **Postcondition** | When a child task finishes, the enclosing task retains only the accepted state changes; unaccepted mutations to the temporary workspace are discarded. |

**Implementation notes** — the current framework implements this behavior in `pipeline/infrastructure/basetask.py`:

- `StandardTaskTemplate.execute()` replaces `self.inputs` with a pickled copy of the original inputs, including the context, before task logic runs, and restores the original inputs in `finally`
- Child tasks therefore execute against a duplicated context that may be mutated freely during `prepare()` / `analyse()`
- `Executor.execute(job, merge=True)` commits a child result by calling `result.accept(self._context)`; with `merge=False`, the child task may still be run and inspected without committing its state
- This makes it possible for aggregate tasks to try tentative calibration paths or other destructive edits inside a stage and keep only the results they explicitly accept
- The rollback mechanism is in-memory copy/restore of task inputs and context; it is distinct from explicit session save/resume workflows

---

### UC-11 — Support Multiple Orchestration Drivers

| | |
|-------|---------|
| **Actor(s)** | Operations / automated processing (PPR-driven batch), pipeline developer / power user (interactive), recipe executor |
| **Summary** | Multiple drivers converge on the same `Pipeline`/`Context` objects: interactive CLI wrappers under `pipeline/h/cli/`, PPR executors (`pipeline/infrastructure/executeppr.py`, `executevlappr.py`), and `recipereducer.py`. The context is populated and resumed by these drivers and must retain driver-injected metadata (e.g., `context.project_structure`) so downstream tasks behave consistently. |
| **Invariant** | Processing state is consistent and usable regardless of which orchestration driver created or resumed it, and success/failure signals are produced when appropriate. |

**Implementation notes** — multiple entry points converge on the same task execution path:

- **Task-driven**: direct task calls via CLI wrappers in `pipeline/h/cli/`
- **Command-list-driven**: PPR and XML procedure commands via `executeppr.py` / `executevlappr.py` and `recipereducer.py`

They differ in how inputs are specified, how session paths are selected, and how resume is initiated, but the persisted context is the same.

---

### UC-12 — Save and Restore a Processing Session

| | |
|-------|---------|
| **Actor(s)** | Pipeline operator, workflow orchestration layer, pipeline developer |
| **Summary** | Serialization is performed by `h_save()` / `h_resume()` (CLI wrappers) which pickle the `Context` object to `<context.name>.context` and later restore it. Per-stage results are proxied to disk (result-stageX.pickle) to reduce in-memory size. The restore path is implemented in `pipeline/h/cli/h_resume.py` and `launcher.Pipeline` initialization logic. |
| **Postcondition** | After restore, the processing state is operationally equivalent to the saved state for supported resume workflows, and processing can continue from the specified point, assuming the data files are in a state consistent with the snapshot used to create the saved context. |

**Implementation notes:**

- `h_save()` pickles the whole context to `<context.name>.context`
- `h_resume(filename)` loads a `.context` file, defaulting to the most recent context file available if `filename` is `None` or `last` is used.
- Per-stage results are proxied to disk (`saved_state/result-stageX.pickle`) to keep the in-memory context smaller
- Used by driver-managed breakpoint/resume (`executeppr(..., bpaction='resume')`) and developer debugging workflows

Note: The current implementation does not handle restoring data state to a past processing state and as such does not have a built-in, robust "true resume". If the data directory needs to be restored to match a saved context snapshot, this must be done manually (e.g. via having created a snapshot of the data at an earlier stage, and then restoring this state) before resuming.

---

### UC-13 — Provide State to Parallel Workers

| | |
|-------|---------|
| **Actor(s)** | Workflow orchestration layer, parallel worker processes |
| **Summary** | Parallel workers obtain context snapshots via `pipeline/infrastructure/mpihelpers.py` which pickles the context and task args to share with workers. Worker-side startup loads the pickled context and adjusts local paths (e.g., `context.logs['casa_commands']`) before executing tasks. This read-only snapshot model is the current distribution mechanism. |
| **Postcondition** | After distribution, each worker has a consistent view of the processing state for the duration of its work. |

**Implementation notes** — `pipeline/infrastructure/mpihelpers.py`, class `Tier0PipelineTask`:

1. The MPI client saves the context to disk as a pickle: `context.save(path)`.
2. Task arguments are also pickled to disk alongside the context.
3. On the server, `get_executable()` loads the context, modifies `context.logs['casa_commands']` to a server-local temp path, creates the task's `Inputs(context, **task_args)`, then executes the task.
4. For `Tier0JobRequest` (lower-level distribution), the executor is shallow-copied *excluding* the context reference to stay within the pipeline-enforced MPI buffer limit (100 MiB). Comments in the code note CASA's higher native limit (~150 MiB; see PIPE-1337 / CAS-13656).

- The current implementation uses a read-only worker model: workers do not modify shared processing state directly, and results are committed through a result-accept/merge flow.

---

### UC-14 — Aggregate Results from Parallel Workers

| | |
|-------|---------|
| **Actor(s)** | Workflow orchestration layer |
| **Summary** | Aggregation is implemented by the orchestration layer calling `result.accept(context)` for worker results; `Results.merge_with_context()` applies aggregated outputs (cal tables, image registries) into the shared `Context` in a single commit step to avoid interleaved conflicting writes. |
| **Postcondition** | The processing state reflects the combined outcomes of all parallel workers. |

---

### UC-15 — Provide Read-Only State for Reporting

| | |
|-------|---------|
| **Actor(s)** | Report generators (weblog, quality reports, reproducibility scripts, pipeline statistics) |
| **Summary** | Reporting uses `WebLogGenerator.render(context)` in `pipeline/infrastructure/renderer/htmlrenderer.py`, which unpickles `context.results` proxies and reads `context.observing_run`, `context.project_summary`, `context.report_dir`, and `context.output_dir` to assemble pages and report artifacts. |
| **Postcondition** | Reports accurately reflect the processing state at the time of generation. |

**Implementation notes** — `WebLogGenerator.render(context)` in `pipeline/infrastructure/renderer/htmlrenderer.py`:

- `WebLogGenerator.render(context)` explicitly does `context.results = [proxy.read() for proxy in context.results]` once before the renderer loop, so individual renderers iterate fully unpickled result objects rather than calling `read()` themselves
- Reads `context.report_dir`, `context.output_dir` — filesystem layout
- Reads `context.observing_run.*` — MS metadata, scheduling blocks, execution blocks, observers, project IDs, start/end times
- Reads `context.project_summary.telescope` — to determine telescope-specific page layouts (ALMA vs VLA vs NRO)
- Reads `context.project_structure.*` — OUS IDs, PPR file, recipe name
- The larger renderer stack, including the Mako templates under `pipeline/infrastructure/renderer/templates/`, reads `context.logs['casa_commands']` and related log references when generating weblog links

---

### UC-16 — Support QA Evaluation and Store Quality Assessments

| | |
|-------|---------|
| **Actor(s)** | QA scoring framework, report generators |
| **Summary** | After `Results.accept()` the QA framework (`pipeline/infrastructure/pipelineqa.py`) invokes `pipelineqa.qa_registry.do_qa(context, result)`. Handlers call `context.observing_run.get_ms(vis)` and inspect `context.imaging_mode` / `context.project_structure` to compute `QAScore` objects appended to `result.qa.pool`. |
| **Postcondition** | Quality scores are associated with the relevant processing step and accessible to reports. |

**Implementation notes** — after `merge_with_context()`, `accept()` triggers `pipelineqa.qa_registry.do_qa(context, result)`:

- QA handlers implement `QAPlugin.handle(context, result)`
- The context provides inputs to QA evaluation:
  - Most handlers call `context.observing_run.get_ms(vis)` to look up metadata for scoring (antenna count, channel count, SPW properties, field intents)
  - Some handlers check `context.imaging_mode` to branch on VLASS-specific scoring
  - Others check things in `context.observing_run`, `context.project_structure`, or the callibrary (`context.callibrary`)
- Scores are appended to `result.qa.pool`, so the scores are stored on the results rather than directly on the context. This also keeps detailed QA collections scoped to the stage result that produced them; in current code, a `QAScorePool` can hold many `QAScore` objects, and each score may carry fine-grained `applies_to` selections (e.g. vis, field, SPW, antenna, polarization), so the per-result pool can become fairly large for detailed assessments.

QA handlers write scores to `result.qa.pool` and do not modify the shared context directly.

---

### UC-17 — Support Inspection and Debugging

| | |
|-------|---------|
| **Actor(s)** | Pipeline developer, pipeline operator, CI harnesses |
| **Summary** | Inspection and debugging are supported by `h_save()`/`h_resume()` (pickled `Context` files), `context.results` (ordered `ResultsProxy` snapshots), and developer utilities used in tests and CI (`tests/testing_utils.py`). Weblog rendering (`pipeline/infrastructure/renderer/htmlrenderer.py`) and diagnostic helpers in `pipeline/infrastructure/basetask.py` expose per-stage timing, tracebacks, and log references so operators can inspect registered datasets (`context.observing_run`), calibrations (`context.callibrary`), and produced images (`context.sciimlist`). |
| **Invariant** | The current processing state is inspectable at any point during execution, and sufficient information is retained to diagnose failures after the fact. |

---

### UC-18 — Manage Telescope- and Array-Specific State

| | |
|-------|---------|
| **Actor(s)** | Telescope-specific tasks and heuristics, array-specific tasks and heuristics |
| **Summary** | Telescope- and array-specific state is implemented in a mixture of dynamic sidecars and domain-model extensions: `context.evla` (VLA-specific sidecar created by `hifv_importdata` and consumed by VLA calibration tasks), ALMA single-dish extensions under `context.observing_run` (e.g., `ms_datatable_name`, `ms_reduction_group`, per-MS `DataTable` objects), and task-specific heuristics in the `pipeline/h*` task implementations. Generic components read these extensions via their documented helpers rather than assuming their presence. |
| **Invariant** | Telescope- and array-specific extensions are present only for runs that require them, available to the tasks that need them, and are never assumed by shared pipeline code. |

**Implementation notes** — the current codebase shows at least two different forms of telescope-/array-specific state.

One is a VLA-specific sub-context (`context.evla`) which is created during `hifv_importdata` and is updated by several subsequent tasks. Functionally, it provides a way to store observation metadata and pass state between tasks under `context.evla` rather than using the top-level context directly or other context objects (e.g. the domain objects). `context.evla` is an untyped, dictionary-of-dictionaries sidecar dynamically attached to the top-level context with no schema, no type annotations, and no declaration in `Context.__init__`.

`context.evla` is a `collections.defaultdict(dict)`, keyed as `context.evla['msinfo'][ms_name].<property>`:

- **Written by:** `hifv_importdata` (creates + initializes), `testBPdcals` (gain intervals, ignorerefant), `fluxscale/solint`, `fluxboot`
- **Read by:** Most VLA calibration tasks and heuristics
- Accessed fields include: `gain_solint1`, `gain_solint2`, `setjy_results`, `ignorerefant`, various `*_field_select_string` / `*_scan_select_string` values, `fluxscale_sources`, `spindex_results`, and many more

Another is ALMA TP / single-dish state, which is array-specific rather than telescope-wide and is carried mainly through SD-specific structures under `context.observing_run`, such as `ms_datatable_name`, `ms_reduction_group`, and `org_directions`, plus the per-MS `DataTable` products referenced from that state. This is a useful reminder that array-specific extensions do not always appear as a single sidecar object like `context.evla`; they may instead live in domain-model extensions and array-specific cached metadata products.

---

### UC-19 — Provide State for Product Export

| | |
|-------|---------|
| **Actor(s)** | Export tasks |
| **Summary** | Export tasks (for example `hifa_exportdata`, `hifv_exportdata`, and the `hsdn/tasks/exportdata` implementations) consume `context.sciimlist`, `context.callibrary`, `context.project_summary`, `context.report_dir`, and `context.output_dir` to assemble deliverable bundles. The `pipeline/infrastructure/imagelibrary.py` utilities and export task code are the canonical places where exportable product lists are assembled and serialized. |
| **Invariant** | The information needed to assemble a deliverable product package is accessible through the processing state. |

---

## 3. Architectural Observations

### The context is a "big ball of state", by design

The current approach is extremely flexible for a long-running, stateful CASA session, but there is no explicit schema boundary between persisted state, ephemeral caches, runtime-only services, and large artifacts. Tasks can (and do) add new fields in an ad-hoc way over time.

UC-08 shows the same problem in behavioral form rather than structural form: once the contract for cross-stage state exchange stopped being enforced strictly, some code continued to use dedicated merged context state while other code began reaching into `context.results` directly. That drift increased coupling to task ordering and result-object internals, which is a useful concrete example of how context creep develops in practice.

### Persistence is pickle-based

Pickle works for short-lived resume/debug use cases, but it is fragile across version changes, risky as a long-term archive format, and not friendly to multi-writer or multi-process updates. The codebase mitigates size by proxying stage results to disk, but the context itself remains a potentially large and unstable object graph.

### Two orchestration planes converge on the same context

Task-driven (interactive CLI) and command-list-driven (PPR / XML procedures) execution both produce and consume the same context. They differ in how inputs are marshalled, how paths are selected, and how resume is initiated, but the persisted context is the same object.

### Asynchronous execution

The executor and `StandardTaskTemplate` flow (see `pipeline/infrastructure/basetask.py` and `pipeline/h/cli/utils.py`) run child tasks in-process or by MPI worker snapshots. There is no explicit support in the current codebase for independently scheduled subtasks with safe, later-state merges; the pipeline relies on synchronous execution or a snapshot/accept pattern rather than asynchronous commits.

### Distributed execution without a shared filesystem

The MPI helper flow (`pipeline/infrastructure/mpihelpers.py`) serializes the `Context` and task arguments to local files and assumes a shared filesystem for CASA command logs and artifact paths. This creates coupling to POSIX-shared storage and limits execution on nodes that do not share a common filesystem namespace.

### Provenance and metadata gaps

Per-stage snapshots are stored via `ResultsProxy` pickles, but the repository lacks a consistent, structured capture of software/package versions, exact input hashes, executor/node metadata, and other reproducibility records. The current evidence is dispersed in `pipeline/infrastructure/basetask.py` and the `ResultsProxy` implementation, with no single machine-readable provenance record attached to accepted results.

### Partial re-execution / dependency visibility

There is no first-class dependency graph or deterministic invalidation API in the codebase. Several code paths derive stage ordering or affected downstream stages from `context.results` ordering rather than from explicit declared dependencies, which complicates selective re-execution and deterministic invalidation.

### External integration and language-neutral access

Renderers and external tools currently access artifacts and state via filesystem artifacts and pickled context blobs. The repository does not provide a stable, language-neutral query or event interface for external systems (dashboards, archives, schedulers) to consume processing state without parsing files.

### Streaming / incremental ingest

Import code registers measurement sets and execution blocks atomically using the domain model (`pipeline/domain/observingrun.py` and `h*_importdata` tasks). There are no well-adopted incremental registration APIs in the domain objects for appending new scans or execution blocks to a live session.

### Heterogeneous-dataset matching

The domain mapping utilities (for example `observing_run.virtual2real_spw_id()`) implement a single mapping strategy for SPWs. The codebase does not include a pluggable policy mechanism for alternate matching semantics (frequency overlap, channel ranges, column-layout-aware matching) that different calibration or imaging tasks might require.

---

## 4. Context Contract Summary

The following capabilities are essential requirements for the pipeline context, derived from current behavior and internal usage patterns:

**System-level requirements:**

- Run identity: `Context` object, recipe/procedure name, inputs, operator/mode
- Path layout: working/report/products directories with ability to relocate
- Dataset inventory: execution blocks / measurement sets with per-MS metadata
- Stage results timeline: ordered stages, durations, QA outcomes, tracebacks
- Export products: weblog tar, manifest, AQUA report, scripts
- Resume: restart from last known good stage (or after a breakpoint)

**Internal usage requirements:**

- Fast MS lookup: random-access by name, filtering by data type, virtual-to-real SPW translation
- Calibration library: append-oriented, ordered, with transactional multi-entry updates and predicate-based queries
- Image library: four typed registries (science, calibrator, RMS, sub-product) with add/query semantics
- Imaging state: typed, versioned configuration for the imaging sub-pipeline
- QA scoring: read-only context snapshot for parallel-safe QA handler execution
- Weblog rendering: read-only traversal of full results timeline + MS metadata + project metadata
- MPI/distributed: efficient context snapshot broadcast + results write-back
- Cross-stage data flow: explicit named outputs rather than results-list walking
- Contract enforcement: downstream tasks may depend only on declared published context outputs; `context.results` is execution history, not a general integration interface
- Project metadata: immutable-after-init sub-record
- Telescope-specific state: typed, composable extension rather than untyped dict

---

## Appendix A. Immutability Assessment of the Current Context

The current pipeline context is best described as **partially immutable at a few important boundaries, but not internally immutable overall**. In other words, the codebase already uses snapshots and copy-based isolation in several places, but the shared top-level `Context` object remains an open, mutable state container.

That distinction matters because it means the current implementation already has some useful immutability patterns worth preserving, while also carrying several risks that come from unconstrained in-place mutation.

### Where immutability is respected today

1. **Child-task execution uses a copied context rather than the live shared context.** In `pipeline/infrastructure/basetask.py`, `StandardTaskTemplate.execute()` replaces `self.inputs` with `utils.pickle_copy(original_inputs)` before `prepare()` / `analyse()` runs, and restores the original inputs in `finally`. This gives subtask execution a temporary workspace: child tasks may mutate that copied context freely, but those mutations do not escape unless a result is explicitly accepted into the parent context. This is the strongest immutability boundary in the current framework.

2. **Accepted stage results are treated as snapshots.** `Results.accept()` writes each accepted top-level result through `ResultsProxy`, which pickles a per-stage snapshot to disk and appends the proxy to `context.results`. The UUID check in `_check_for_remerge()` also prevents the same result from being merged twice. This is not full immutability of the context, but it is an append-only history model for stage outcomes.

3. **Some consumers already defend themselves against accidental aliasing.** A good example is `FindContInputs.target_list` in `pipeline/hif/tasks/findcont/findcont.py`, which deep-copies `context.clean_list_pending` specifically to avoid mutating the context's imaging list while preparing task inputs.

4. **Some domain objects are genuinely immutable after construction.** `pipeline/domain/scan.py` stores `antennas`, `fields`, `intents`, `states`, and `data_descriptions` as `frozenset`s. These are good examples of value-like domain records that are safer to share because callers cannot casually mutate them in place.

5. **Parallel workers operate on snapshots rather than shared live state.** The MPI/distribution flow serializes context state for workers and applies worker-side changes only through the normal result acceptance path. This is a pragmatic form of immutability at the process boundary and is important for correctness.

6. **Project metadata is often used as if it were immutable after initialization.** In practice, `project_summary`, `project_structure`, and `project_performance_parameters` are usually initialized during startup/import and then read throughout the rest of the run. The code treats these records as stable inputs even though that stability is conventional rather than enforced.

### Where immutability is not respected

1. **The top-level `Context` is an open mutable bag of attributes.** `pipeline/infrastructure/launcher.py` initializes many public lists, dicts, and registries directly on the context (`results`, `processing_intents`, `clean_list_pending`, `imaging_parameters`, `selfcal_targets`, image libraries, and more), and other code can assign to them freely. The design intentionally favors convenience over encapsulation.

2. **`merge_with_context()` mutates shared state in place and is not transactional.** Result acceptance calls `self.merge_with_context(context)` directly on the live shared context. If a merge fails part-way through, `accept()` can wrap the failure in `FailedTaskResults`, but it does not roll back partial mutations that may already have been applied. This is one of the clearest places where immutability and atomicity are both weak.

3. **Imaging state is carried as ad-hoc mutable sidecars.** Attributes such as `clean_list_pending`, `clean_list_info`, `imaging_mode`, `imaging_parameters`, `synthesized_beams`, and `size_mitigation_parameters` are updated directly by tasks. For example, `pipeline/hif/tasks/editimlist/resultobjects.py` clears and appends to `context.clean_list_pending`, and `pipeline/hif/tasks/findcont/resultobjects.py` rewrites target dictionaries and then replaces the full list. This works, but it gives callers many opportunities to mutate nested structures accidentally.

4. **The observation registry is mutable and directly exposed.** `pipeline/domain/observingrun.py` updates `measurement_sets`, `virtual_science_spw_ids`, and `virtual_science_spw_names` in place during registration. Callers then read those same live lists and dicts directly through the context. This is reasonable during import, but after import the structure is effectively shared mutable state.

5. **Image registries are mutable and leak their backing collection.** `pipeline/infrastructure/imagelibrary.py` stores images in a mutable list of mutable dictionaries. `get_imlist()` returns the backing list itself, not a copy or read-only view, so any caller can mutate the registry without going through `add_item()` or `delete_item()`.

6. **Even some read-only consumers temporarily mutate the context.** `WebLogGenerator.render()` in `pipeline/infrastructure/renderer/htmlrenderer.py` temporarily replaces `context.results` with fully unpickled results and then restores the proxies in `finally`. The net effect is restored correctly, but it is still a read-path that mutates shared state.

7. **Project metadata is not actually enforced as immutable.** `executeppr.py` and `executevlappr.py` replace the project metadata objects outright, and `Context.set_state()` ultimately uses `setattr()` on those records. The current code relies on discipline rather than on a frozen type or a validated update API.

8. **Execution history is sometimes used as a mutable integration surface rather than as history.** A number of tasks and renderers walk `context.results` directly, and some logic depends on the latest result ordering. That is not an immutability violation by itself, but it does weaken the boundary between append-only execution history and current published state.

### How to improve this, and where mutability is acceptable

The highest-value improvements are the ones that strengthen boundaries without forcing the entire pipeline into a fully immutable style.

1. **Make accepted context updates transactional.** The most important improvement would be to stop merging directly into the live shared context. A result merge should either build a patch first and commit it on success, or merge into a scratch copy and swap it in only after the merge completes. That would eliminate the current "partially mutated context after failure" risk.

2. **Seal or freeze state after its construction phase.** `ObservingRun` is naturally mutable during import, and project metadata is naturally mutable during initialization. After those phases finish, both should ideally become read-only to ordinary task code. A freeze-after-init model would preserve the current workflow while tightening contracts.

3. **Replace ad-hoc mutable dictionaries/lists with typed state records.** Imaging state is the clearest candidate. A typed `ImagingState` object would make it easier to validate updates, replace the whole record intentionally, and document which fields are shared across stages.

4. **Stop exposing backing collections directly.** `ImageLibrary.get_imlist()` should return a copy, tuple, or read-only view. Similar accessor patterns would help for `ObservingRun.measurement_sets` and other registries that are currently exposed as raw mutable containers.

5. **Give read-only consumers a snapshot/view API.** Reporting and statistics code should not need to rewrite `context.results` temporarily. A dedicated snapshot helper or iterator over resolved stage results would preserve current behavior without mutating the context on the read path.

6. **Keep execution cursors mutable.** `task_counter` and `subtask_counter` are inherently mutable run-state cursors. Making them immutable would add ceremony without delivering meaningful safety.

7. **Keep the calibration library mutable, but keep it encapsulated.** `context.callibrary` is fundamentally a working state accumulator. Full immutability here would likely be expensive and awkward because calibration tasks are intentionally building and revising a working calibration set. What matters more is that updates stay behind a narrow API and, ideally, produce auditable change records.

8. **Keep temporary child-task workspaces mutable.** The copy-on-execute behavior in `basetask.py` is a good pattern. It gives tasks a mutable sandbox while preserving a stable outer context boundary. This is one area where mutability is not just acceptable but useful.

9. **Treat `context.results` as history, not as general shared state.** Some direct inspection of prior results is unavoidable for reporting and debugging, but downstream processing should prefer explicitly published context state. Tightening that contract would reduce coupling to task order and make immutability boundaries clearer.

Overall, a fully immutable context would probably be the wrong target for the current pipeline because the framework is built around progressive state accumulation, result acceptance, and long-lived interactive sessions. A better target is **selective immutability**: immutable snapshots at task/process/reporting boundaries, frozen records for stable metadata, and tightly scoped mutation APIs for the genuinely stateful parts of the system.
