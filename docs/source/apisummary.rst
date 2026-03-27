Pipeline Tasks
==============

.. autosummary::
   :toctree: _autosummary
   :caption: Pipeline Task CLIs
   :nosignatures:
   :template: custom-module-template.rst

   pipeline.h.cli
   pipeline.hif.cli
   pipeline.hifa.cli
   pipeline.hifv.cli
   pipeline.hsd.cli
   pipeline.hsdn.cli

Pipeline Modules
================

domain/infrastructure
---------------------

.. toctree::
   :maxdepth: 2

   automodapi_src/pipeline.domain
   automodapi_src/pipeline.infrastructure.launcher
   automodapi_src/pipeline.infrastructure.project
   automodapi_src/pipeline.infrastructure.callibrary
   automodapi_src/pipeline.infrastructure.imagelibrary
   automodapi_src/pipeline.infrastructure.vdp

``pipeline.h*.tasks``
---------------------

.. toctree::
   :maxdepth: 2

   automodapi_src/pipeline.h.tasks
   automodapi_src/pipeline.hif.tasks
   automodapi_src/pipeline.hifa.tasks
   automodapi_src/pipeline.hifv.tasks
   automodapi_src/pipeline.hsd.tasks
   automodapi_src/pipeline.hsdn.tasks

Inheritance Diagrams for Pipeline ``Task``/``Inputs``/``Results`` Classes
=============================================================

``ImportData`` task classes as an example
------------------------------------

|importdataclasses_diagram|

``Task`` Classes
--------------------

|taskclasses_diagram|

``Inputs`` Classes
----------------------

|inputsclasses_diagram|

``Results`` Classes
-----------------------

|resultsclasses_diagram|


Complete API Reference
======================

.. autosummary::
   :toctree: _autosummary
   :caption: Complete API Reference
   :template: custom-module-template.rst
   :recursive:

   pipeline.cli
   pipeline.config
   pipeline.environment
   pipeline.recipereducer
   pipeline.runpipeline
   pipeline.runvlapipeline
   pipeline.domain
   pipeline.h
   pipeline.hif
   pipeline.hifa
   pipeline.hifv
   pipeline.hsd
   pipeline.hsdn
   pipeline.infrastructure
   pipeline.qa
   pipeline.recipes