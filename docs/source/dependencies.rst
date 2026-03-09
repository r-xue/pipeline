Dependencies of ``Pipeline``
============================

.. _Pipeline: https://open-bitbucket.nrao.edu/projects/PIPE
.. _casaconfig: https://github.com/casangi/casaconfig
.. _pipeline-testdata: https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline-testdata/browse
.. _casatasks.wvrgcal: https://casadocs.readthedocs.io/en/stable/api/tt/casatasks.calibration.wvrgcal.html#casatasks.calibration.wvrgcal

Runtime
-------

CASA6-equivalent Python environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    `Pipeline`_ is a Python package that depends on CASA6 components and various
    general-purpose Python libraries. Like any Python package, it is only compatible
    with specific versions of Python and its third-party dependencies. In practice,
    each release is tested and validated solely against a monolithic CASA6 release
    tied to a particular ALMA/VLA cycle. That monolithic release is a self-contained
    Python environment bundling a portable interpreter and all required libraries.
    CASA6 itself has limited platform support; see the
    `CASA6 compatibility matrix <https://casadocs.readthedocs.io/en/stable/notebooks/introduction.html#Compatibility>`_
    for OS and library requirements.

To encourage the use and development of ``Pipeline`` in a standard Python
environment (e.g. conda), the essential components for a CASA6-equivalent
setup are listed below. All of these are included in the Pipeline-dedicated
CASA releases.

- python3
- ipython
- pip
- numpy
- scipy
- matplotlib
- casatools
- casatasks
- casaplotms
- casampi

.. note::

    ``casadata`` and ``almatasks`` are no longer required as of ``CASA6 >= 6.6.1``
    thanks to the introduction of `casaconfig`_ and the migration of the
    `casatasks.wvrgcal`_ task. ``casaconfig`` is implicitly required by
    ``casatasks``.

Other Pipeline dependencies
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following libraries are required by Pipeline but are **not** included in
the CASA6 monolithic distribution.

Python packages (see also ``requirements.txt`` in the source repository):

- cachetools
- mako
- pypubsub
- intervaltree
- logutils
- ps_mem
- astropy
- bdsf (VLASS source detection and mask creation)

Unix command-line tools:

- **ImageMagick**

  - ``convert``: generates thumbnails on Linux; macOS uses the built-in ``sips``
    instead. Also used by ``ImageMagickWriter`` from ``matplotlib.animation``
    in ``hsd/tasks/common/rasterutil.py`` (developer use only).
  - ``montage``: required by ``hifa_renorm``.

- **poppler-utils**

  - ``pdfunite``: required by ``hifa_renorm``.

- **Git**: used to obtain the Pipeline revision description string.
- **Xvfb / xvfb-run**: required by the ``casampi`` module for headless
  execution.
- **uncompress**: used by ``tec_maps.py`` in CASA for the VLA task
  ``hifv_priorcals``.


Development Tools
-----------------

Packaging
^^^^^^^^^

- ``wheel``: for installing older Python packages.
- ``csscompressor``: minifies CSS during installation.

Testing
^^^^^^^

- ``pytest``
- ``pytest-cov``

Code quality
^^^^^^^^^^^^

- ``pydocstyle``
- ``pycodestyle``
- ``memory_profiler``
- ``line_profiler``

Documentation
^^^^^^^^^^^^^

- ``sphinx``

Version control
^^^^^^^^^^^^^^^

- ``Git LFS``: for managing the `pipeline-testdata`_ repository.

