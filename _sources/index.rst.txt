Pipeline Documentation
======================

.. toctree::
   :maxdepth: 2
   :caption: Pipeline Tasks

   pipeline_tasks/pipeline_tasks.rst

.. autosummary::
   :toctree: _autosummary
   :caption: Pipeline Task (sphinx-autosummary)

   pipeline.h.cli
   pipeline.hif.cli
   pipeline.hifa.cli
   pipeline.hifv.cli
   pipeline.hsd.cli
   pipeline.hsdn.cli

   pipeline.domain
   pipeline.infrastructure.launcher

.. inheritance-diagram:: pipeline.h.tasks pipeline.hif.tasks pipeline.hifa.tasks

.. inheritance-diagram:: sphinx.ext.inheritance_diagram.InheritanceDiagram
   :parts: 1

.. autosummary::
   :toctree: _autosummary
   :caption: API Reference
   :template: custom-module-template.rst
   :recursive:
   

   pipeline.domain
   pipeline.infrastructure.launcher


.. toctree::
   :maxdepth: 2
   :caption: Pipeline Heuristics

   heuristics/field_parameter.md
   heuristics/FlaggingTasks.md


.. toctree::
   :maxdepth: 2
   :caption: Releases etc.

   releases
   modular
   dependencies

.. toctree::
   :maxdepth: 3
   :caption: Pipeline Basics

   basics

.. toctree::
   :maxdepth: 3
   :caption: Task Classes

   task_classes

.. toctree::
   :maxdepth: 3
   :caption: Developer Notes

   develdocmd/ways_to_run_the_pipeline.md
   develdocmd/comparing_pipeline_executions.md
   develdocmd/building_the_pipeline.md

   develdocmd/ALMA-Imaging-Workflow.md
   develdocmd/VLA-Imaging-Workflow.md
   develdocmd/VLASS-SE-CONT-Imaging-Workflow.md
   develdocmd/VLASS-SE-CUBE-Imaging-Workflow.md
   develdocmd/selfcal_workflow.md

   develdocmd/pipeline_tests.md
   develdocmd/DataType_Testing.md
   develdocmd/QA_scores.md
   develdocmd/DeveloperDocumentation.md

   develdocmd/python3_conversion_notes.md
   develdocmd/releases.md
   develdocmd/recipes.md




.. toctree::
   :maxdepth: 2
   :caption: Examples Notes (from Jupyter Notebooks)

   examples/test1.ipynb

.. toctree::
   :maxdepth: 2
   :caption: Example Notes (from .rst)
   
   examples/test2.rst   

Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
