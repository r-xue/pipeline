# Pipeline Context Use Cases

## Overview

The pipeline context is the central state object used for a pipeline execution. It carries observation data, calibration state, imaging state, execution history and state, project metadata, and serves as the primary communication channel between pipeline stages.

This document catalogues the use cases of the current pipeline context as determined by examination of the codebase. The goal is to identify improvements to the current pipeline context design.

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

Each use case describes the required capabilities of the context system and the interactions through which those capabilities are exercised. They are written to be implementation-neutral — the goal is to capture what the context must do, not how the current pipeline implementation achieves it. Implementation notes are included under individual use cases where they provide important detail about the current pipeline realization.

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
| **Summary** | The context must populate (read) observation metadata (datasets, spectral windows, fields, antennas, scans, time ranges), make it queryable by all subsequent processing steps, and allow downstream tasks to access that metadata as processing progresses. When processing produces derived or transformed datasets (for example, calibrated, averaged, or subsetted outputs), the context should register those derived datasets and record lineage rather than mutating the original observation metadata. It must also be able to hold derived or cached metadata products created during import so later stages can reuse them efficiently. |
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
| **Summary** | When multiple MSes are registered in the context, downstream tasks must be able to look up and match metadata elements across them even when the MSes use different native numbering. The context must provide a unified identifier scheme (currently for spectral windows) that allows these elements to be referenced consistently across datasets, and must support data-type-aware lookup of MSes and their associated data columns.
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
| **Summary** | The context must store project-level metadata (proposal code, PI, telescope, desired sensitivities, etc.) and make it available to all components, e.g., for decision-making in heuristics and to label outputs in reports. |
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
| **Summary** | The context must allow calibration tasks to register and update solutions and to record both the coverage where a solution applies and the source(s) from which it was derived. Downstream tasks must be able to query for all calibrations applicable to a given data selection. The context must distinguish between calibrations pending application and those already applied. Registration must support registering multiple calibrations atomically as part of a single operation, and it must also support de-registration/removal of trial or reverted calibrations so alternative solutions can be tested or failed attempts rolled back. |
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
| **Summary** | The context must allow imaging state — target lists, imaging parameters, masks, thresholds, and sensitivity estimates — to be computed by one step and read or refined by later steps. Multiple steps may contribute to a progressively refined imaging configuration. |
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
| **Summary** | The context must maintain typed registries of produced image products with add/query semantics. Later tasks must be able to discover previously produced science, calibrator, RMS, and sub-product images through these registries. |
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
| **Summary** | The context must track which processing stage is currently executing and maintain a stable, ordered record of completed stages. Stage identity and ordering must remain coherent across session saves and resumes. |
| **Invariant** | The currently executing stage is identifiable and completed stages are recorded in stable order. |

**Implementation notes:**

- `context.stage`, `context.task_counter`, `context.subtask_counter` track progress

---

### UC-08 — Preserve Per-Stage Execution Record

| | |
|-------|---------|
| **Actor(s)** | Report generators, pipeline operators, workflow orchestration layer |
| **Summary** | The context must preserve a complete execution record for each completed stage, including timing, traceback information, outcomes, and the arguments used to invoke it. This record must support reporting, post-mortem diagnosis of failures, and resumption after interruption. |
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
| **Summary** | When a task produces outputs that change the processing state (e.g., new calibrations, updated flag summaries, image products, revised parameters), the context must provide a mechanism for those outputs to become available to subsequent processing steps before they execute. UC-04, UC-05, UC-06, and UC-16 are specific instances of this pattern. |
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
| **Summary** | Within a stage, the context must be usable as a temporary working space for child-task execution. Child tasks must be able to modify context state destructively while they run, including adding, removing, or replacing tentative calibration and processing state, without requiring explicit cleanup logic. Only outputs that are explicitly accepted into the enclosing task's context should survive stage execution. |
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
| **Summary** | The state stored by the context must remain consistent and usable regardless of which orchestration driver created or resumed it. It must be creatable and resumable from non-interactive and interactive drivers (ex: PPR command lists, XML procedures, interactive task calls), support driver-injected run metadata, and tolerate partial execution controls and breakpoint-driven stop/resume semantics. |
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
| **Summary** | The context must be able to serialize the complete processing state to disk (all observation data, calibration state, execution history, imaging state, project metadata, etc.) and later restore it so that processing can resume from the saved point, provided the data file state is consistent with the context snapshot. The serialization must preserve enough state to resume; backward compatibility across pipeline releases is not guaranteed. On restore, paths must be adaptable to a new filesystem environment. |
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
| **Summary** | When work is distributed across parallel workers, each worker needs access to the current processing state (observation metadata, calibration state). The context must provide a mechanism for workers to obtain a consistent snapshot of that state, and the snapshot mechanism must support efficient distribution to workers. |
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
| **Summary** | After parallel workers complete, the context must collect their individual results and incorporate them into the shared processing state. The aggregation must be safe (no conflicting concurrent writes) and complete before the next sequential step begins. |
| **Postcondition** | The processing state reflects the combined outcomes of all parallel workers. |

---

### UC-15 — Provide Read-Only State for Reporting

| | |
|-------|---------|
| **Actor(s)** | Report generators (weblog, quality reports, reproducibility scripts, pipeline statistics) |
| **Summary** | The context must provide read-only access to the observation metadata, project metadata, execution history (including per-stage domain-specific outputs such as flag summaries and plot references), QA outcomes, log references, and path information needed to generate reporting products such as weblogs, quality reports, reproducibility scripts, and pipeline statistics. |
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
| **Summary** | After each processing step completes, the context must make the relevant processing state available to QA handlers so they can evaluate the outcome against quality thresholds, which may depend on e.g. telescope, project parameters, or observation properties. The resulting quality scores must be recorded and remain retrievable for reporting. |
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
| **Summary** | The context must allow an operator to inspect the current processing state, for example: which datasets are registered, what calibrations exist, how many steps have completed, and what their outcomes were. On failure, a snapshot of the state must be available for post-mortem analysis. |
| **Invariant** | The current processing state is inspectable at any point during execution, and sufficient information is retained to diagnose failures after the fact. |

---

### UC-18 — Manage Telescope- and Array-Specific State

| | |
|-------|---------|
| **Actor(s)** | Telescope-specific tasks and heuristics, array-specific tasks and heuristics |
| **Summary** | The context must support conditional telescope-specific and array-specific extensions to the processing state. These extensions must be available to the tasks and heuristics that need them, including cases where one array mode within a telescope family has materially different state requirements from another. Generic pipeline components must not depend on or require knowledge of those telescope- or array-specific extensions. |
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
| **Summary** | The context must make the datasets, calibration state, image products, reports, scripts, path information, project identifiers, and any other information needed for export available through the processing state so export tasks can assemble them into a deliverable product package. |
| **Invariant** | The information needed to assemble a deliverable product package is accessible through the processing state. |

---

## 3. Missing Capabilities (GAPs) and Implications for Context Design

This section records capabilities the current pipeline context design cannot yet support. Not every item below is strictly a "context" feature, but each implies changes to context responsibilities, schema, or interfaces. A separate, more exhaustive gap analysis mapping these use cases to project requirements is recommended.

### GAP-01 — Asynchronous Execution of Independent Work

| | |
|-------|---------|
| **Actor(s)** | Workflow orchestration layer, parallel task scheduler |
| **Summary** | The context must support asynchronous execution at multiple granularities (stage-level and within-stage parallelism) while preventing inconsistent processing state. Tasks must be able to proceed independently without waiting for others to complete when task dependencies allow. This differs from the current parallel-worker pattern, which waits for all work to finish before proceeding. |
| **Invariant** | Independent tasks may run asynchronously but must not produce conflicting state. |
| **Postcondition** | Results from asynchronously executed tasks are fully and consistently incorporated into processing state before any dependent work begins. |
 

### GAP-02 — Distributed Execution Without a Shared Filesystem

| | |
|-------|---------|
| **Actor(s)** | Workflow orchestration layer, distributed workers |
| **Summary** | Execution must be possible across nodes that do not share a filesystem. Artifacts, datasets, and processing state must be addressable and accessible without relying on local POSIX paths. |
| **Postcondition** | Processing completes across distributed nodes with context-hosted references providing the necessary artifact access. |
 

### GAP-03 — Provenance and Reproducibility

| | |
|-------|---------|
| **Actor(s)** | Pipeline operator, auditor, reproducibility tooling |
| **Summary** | The context must record sufficient provenance – software versions, exact input identities/hashes, task parameters, per-stage state, hardware and execution-environment details (CPU architecture, node/cluster specification, kernel and MPI versions, workload-manager/scheduler configuration, and relevant scheduler limits) — to enable precise reproduction and audit of past runs. |
| **Postcondition** | Any past processing step can be reproduced or audited using the recorded provenance chain. |
 

### GAP-04 — Partial Re-Execution / Targeted Stage Re-Run

| | |
|-------|---------|
| **Actor(s)** | Pipeline operator, developer, workflow engine |
| **Summary** | The context must support selectively re-running one or more mid-pipeline stages with new parameters while preserving unaffected stages. Downstream stages that depend on changed outputs must be invalidated or recomputed. |
| **Postcondition** | Processing state reflects the re-run outcomes; affected downstream stages are invalidated or updated; unaffected stages remain intact. |
 

### GAP-05 — External System Integration (Archive, Scheduling, QA Dashboards)

| | |
|-------|---------|
| **Actor(s)** | QA dashboards, monitoring tools, archive ingest systems, schedulers |
| **Summary** | External systems need timely access to processing state (current stage, processing time, QA results, lifecycle events) without waiting for offline product files. The context should expose the necessary state via queryable interfaces or event feeds. |
| **Invariant** | External consumers can access the processing state they require while it remains current. |
| **Postcondition** | External systems can track processing progress and lifecycle transitions in near real time. |
 

### GAP-06 — Programming-Language / Client-Framework Access to Context

| | |
|-------|---------|
| **Actor(s)** | Non-Python clients (C++, Julia, JavaScript dashboards), external tools |
| **Summary** | Non-Python clients and external tools must be able to access context state and artifacts through a stable, language-neutral API. Mission-critical processing needs (metadata management, heuristics, transactional/production workflows) must be covered as a priority; auxiliary functionality such as QA, statistics, and dashboard rendering may be served by separate higher-level APIs. |
| **Postcondition** | Clients in any supported programming language can query context state and artifacts through a stable typed API without coupling themselves to the underlying storage representation. |
 

### GAP-07 — Streaming / Incremental Processing

| | |
|-------|---------|
| **Actor(s)** | Data ingest systems, workflow engine, incremental processing tasks |
| **Summary** | Support incremental dataset registration (adding new scans or execution blocks to a live session), incremental detection and processing of new data, and versioned results so re-runs produce new versions rather than overwriting. |
| **Postcondition** | New data may be incorporated into an active session and processed incrementally without restarting the pipeline from scratch. |
 

### GAP-08 — Heterogeneous dataset coordination and flexible matching semantics

| | |
|-------|---------|
| **Actor(s)** | Data import tasks, calibration tasks, imaging tasks, heuristics, pipeline operators |
| **Summary** | Calibration tasks, imaging tasks, and heuristics must be able to match and coordinate data across heterogeneous collections of MSes that may not share native SPW numbering, column layout, or other assumptions. Downstream tasks must be able to select the matching semantics appropriate to their use: calibration tasks require exact SPW matching; imaging tasks require partial/overlap matching (including by frequency or channel range) to combine related spectral windows. Matching must extend beyond SPWs to cover fields, sources, and data column layouts. Where automated matching is ambiguous or fails, heuristics or users must be able to supply explicit mapping rules or override the default matching behavior, with overrides recorded alongside their rationale.
| **Invariant** | SPW, field, source, and data-column identity are queryable across all registered datasets, regardless of whether those datasets share native numbering or column layout. |
| **Postconditions** | Calibration and imaging tasks can look up applicable SPWs, fields, sources, and data columns across an arbitrary collection of heterogeneous MSes using the appropriate matching semantics for their use, and any user or heuristic overrides are recorded alongside their rationale. |
 
| **Notes** | UC-02 covers the baseline cross-MS lookup capability currently supported by the context: a unified SPW identifier scheme with a single name-based matching strategy. GAP-08 extends this to multiple selectable matching semantics, additional metadata dimensions (fields, sources, column layouts), and user/heuristic override hooks — none of which are currently supported. |

---

## 4. Architectural Observations

### The context is a "big ball of state", by design

The current approach is extremely flexible for a long-running, stateful CASA session, but there is no explicit schema boundary between persisted state, ephemeral caches, runtime-only services, and large artifacts. Tasks can (and do) add new fields in an ad-hoc way over time.

UC-08 shows the same problem in behavioral form rather than structural form: once the contract for cross-stage state exchange stopped being enforced strictly, some code continued to use dedicated merged context state while other code began reaching into `context.results` directly. That drift increased coupling to task ordering and result-object internals, which is a useful concrete example of how context creep develops in practice.

### Persistence is pickle-based

Pickle works for short-lived resume/debug use cases, but it is fragile across version changes, risky as a long-term archive format, and not friendly to multi-writer or multi-process updates. The codebase mitigates size by proxying stage results to disk, but the context itself remains a potentially large and unstable object graph.

### Two orchestration planes converge on the same context

Task-driven (interactive CLI) and command-list-driven (PPR / XML procedures) execution both produce and consume the same context. They differ in how inputs are marshalled, how paths are selected, and how resume is initiated, but the persisted context is the same object.

---

## 5. Improvement Suggestions for the Current Context Design

These are phrased as requirements and design directions, not as a call to rewrite everything immediately.

### 1) Split "context data" from "context services"

Define a minimal, explicit **ContextData** model that is typed, schema-versioned, and serializable in a stable format (JSON/MsgPack/Arrow). Attach runtime-only services (CASA tool handles, caches, heuristics engines) around it rather than mixing them into the same object.

### 2) Introduce a ContextStore interface

Replace "pickle a Python object graph" with a storage abstraction (`get`, `put`, `list_runs`). Backends can start simple (SQLite) and grow (Postgres/object store) without changing task logic.

Put a middleware/facade layer above that store so language-specific APIs talk to a stable service contract rather than directly to storage layout or serialization details.

### 3) Make state transitions explicit (event-sourced or patch-based)

The existing event bus (`pipeline.infrastructure.eventbus`) could be elevated to record task lifecycle events and key state changes, yielding reproducibility, easier partial rebuilds, and better distributed orchestration.

Related contract choice: define a narrow published-state surface for cross-stage communication. Task results should remain execution records, while only declared state transitions or published outputs become part of the downstream contract.

### 4) Treat large artifacts as references, not context fields

Store large arrays/images/tables in an artifact store and carry only references in context data. This avoids "accidentally pickle a GiB array" and makes distribution/cloud execution more realistic.

### 5) Remove reliance on global interactive stacks for non-interactive execution

Make tasks accept an explicit context handle. Keep interactive convenience wrappers but do not make them the core contract.

### 6) Represent the execution plan as context data

Record the effective execution plan (linear or DAG) alongside run state to support provenance, partial execution, and targeted re-runs.

### 7) Adopt a versioned compatibility policy

Define whether operational contexts must be resumable within a supported release window (with schema versioning + migrations) versus best-effort for development contexts.

### 8) Enforce published outputs as the only cross-stage contract

Require each task to declare what shared state it publishes and what context capabilities it reads. Downstream tasks should be allowed to depend only on those declared published outputs, not on another task's raw `Results` object or on undeclared ad-hoc context attributes. In practice, this means separating execution history from shared processing state and validating reads/writes in development and CI so contract drift is caught early rather than becoming context creep.

### 9) Support asynchronous and incremental execution

Allow independent tasks or subtasks to execute asynchronously where data and dependency graphs permit. Provide safe commit/merge semantics, conflict detection, and partial-progress visibility so asynchronous workflows do not corrupt shared state.

### 10) Support distributed execution without a shared filesystem

Represent large artifacts and intermediate products as resolvable references and store them in remote/object stores. Ensure workers running on nodes without a shared POSIX filesystem can resolve, read, and write artifacts reliably.

### 11) Record provenance and execution-environment metadata

Capture software versions, input identities/hashes, task parameters, execution-environment metadata (node, kernel, MPI), and decision rationale so processing steps are reproducible and auditable.

### 12) Provide explicit partial re-execution / targeted re-run support

Support re-running selected stages with new parameters while identifying and invalidating affected downstream stages. Provide tooling to compute the dependency frontier and to apply deterministic invalidation and recomputation policies.

### 13) Expose state and events for external system integration

Offer queryable interfaces or event feeds so external systems (archives, schedulers, QA dashboards) can access current stage, progress, QA results, and lifecycle events without parsing offline product files.

### 14) Provide a language-neutral, client-friendly API surface

Complement `ContextStore` with a stable, language-neutral API (gRPC/REST/GraphQL) and clear type contracts so non-Python clients and dashboards can integrate with processing state and the artifact catalog.

### 15) Support streaming / incremental data ingestion and processing

Allow adding new scans or execution blocks to a live session and processing them incrementally with versioned outputs so re-runs produce new versions rather than silently overwriting prior outputs.

### 16) Support heterogeneous-dataset coordination and flexible matching semantics

Provide configurable matching semantics across MS collections (exact SPW, frequency overlap, column-layout-aware matching, field/source heuristics) and record any user or heuristic overrides alongside their rationale.

---

## 6. Context Contract Summary

The following capabilities appear to be **hard requirements** for any refined or improved context design, derived from current behavior and internal usage patterns:

**System-level requirements:**

- Run identity: `context_id`, recipe/procedure name, inputs, operator/mode
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
