# Heuristics Development and Integration in the Pipeline

The development and implemnetation process for heuristics can vary significantly, ranging from direct drop-ins to light adaptations, heavy refactoring, or even complete rewrites to become Pipeline native. 
While leveraging existing pipeline infrastructure is always encouraged, heuristics development has traditionally occurred within standalone CASA environments with varying maturity states. This approach often leads to longer adaptation periods, duplicated efforts, and communication gaps when handing over to developers

To streamline this process, we strongly encourage heuristics development to leverage Pipeline infrastructure early on:

* Unified Development Environment: During the heuristics development and adaptation period, heuristics contributors and Pipeline developers should use the same codebase (e.g., a temporary Pipeline repository branch), workspace (e.g., open-bitbucket.nrao), and CASA version.

* Collaborative Tools: Use draft pull requests and open Jira tickets for technical discussions and design decisions.

* Shared Documentation: Document the development process on a shared version-controlled platform (e.g., markdown/RST, Jupyter notebooks, inside the Pipeline repository, GoogleDocs, Overleaf) to ensure transparency and ease of access.


## Accessing Lightly-Adapted Prototype Modules in the Pipeline Codebase

For self-contained prototype modules with generic Python/CASA dependencies, you can easily access these modules and functions after drop-in. Follow the steps below to access them under the pipeline namespace:

1. Insert the pipeline branch local clone into the system path:

```console
sys.path.insert(0, os.path.abspath(os.path.expanduser('~/abc/pipeline-branch-local-clone')))
```

2. Access the prototype modules and functions in CASA:


```python
CASA <1>: from pipeline.extern.almarenorm import alma_renorm
CASA <2>: help(alma_renorm)
```

Help on function `alma_renorm` in module `pipeline.extern.almarenorm`

```python
alma_renorm(
    vis: str, spw: List[int], create_cal_table: bool, 
    threshold: Union[NoneType, float], excludechan: Dict, 
    atm_auto_exclude: bool, bwthreshspw: Dict, caltable: str
) -> tuple

Interface function for ALMA Pipeline: this runs the ALMA renormalization
heuristic for input vis, and returns statistics and metadata required by
the Pipeline renormalization task.

Args:
    vis: Path to the measurement set to renormalize.
    spw: Spectral Window IDs to process.
    create_cal_table: Save renorm corrections to a calibration table.
    threshold: The threshold above which the scaling must exceed for the
              renormalization correction to be applied. If set to None, then use
              a pre-defined threshold appropriate for the ALMA Band.
    excludechan: Dictionary with keys set to SpW IDs (as string), and
                 values set to channels to exclude for that SpW.
    correct_atm: Get the transmission profiles for the SPW bandpass (or phase) and 
                 target(s). The BP (or phase) and target(s) autocorr are compared 
                 in establishing the scaling in cases spectra where transmission 
                 is low.
```

Help on function `get_flagged_solns_per_spw` in module `pipeline.hif.heuristics.auto_selfcal.selfcal_helpers`

get_flagged_solns_per_spw(spwlist, gaintable)

```python
get_flagged_solns_per_spw(spwlist,
Calculate the number of flagged and unflagged solutions per spectral window (spw).

This function examines a gain table and calculates the number of flagged and unflagged 
solutions for each spectral window (spw) provided in the spwlist. It also calculates 
the fraction of flagged solutions.

Args:
    spwlist (list): List of spectral window IDs to examine.
    gaintable (str): Path to the gain table directory.

Returns:
    tuple: A tuple containing three elements:
        - nflags (list): Number of flagged solutions per spw.
        - nunflagged (list): Number of unflagged solutions per spw.
        - fracflagged (numpy.ndarray): Fraction of flagged solutions per spw.

Raises:
    FileNotFoundError: If the specified gain table directory does not exist.
```

During the translation/adaptation development period, both developers and prototype contributors can still access, refactor, and experiment from the same ticket codebase. This allows for co-development and discussion of technical matters using the Bitbucket draft PR interface.

## Checklist for Adaptation Process

* Compatibility of Dependency Libraries: Ensure compatibility with different versions of dependencies, such as Astropy and Numpy, considering potential API changes.
* Code Style: Adhere to the established code style guidelines to maintain consistency across the codebase.
* Unspecified Implicit Dependencies: Identify and document any implicit dependencies, for example, functions from `analysis_utils`.
* Avoid Duplications: Utilize existing helper functions within the Pipeline infrastructure to prevent redundant code.

## Uses of `pipeline.extern`

Rules of thumb for `pipeline.extern` Namespace:

* `pipeline.extern` should only host strictly dropped-in modules/classes with no dependencies on Pipeline modules, classes, or functions outside of that namespace.
* These dropped-in modules should be functional outside of the Pipeline codebase and are likely being used or considered as standalone tools in a CASA/Python environment

However, during the early stages of heuristics implementation (e.g., a ticket branch or a temporary testing branch), to assist heuristics contributors in navigating the Pipeline codebase, early adaptation of prototyping with dependencies on Pipeline native modules/classes is acceptable.