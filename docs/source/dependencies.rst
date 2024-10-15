Dependencies of ``Pipeline``
============================


Runtime
--------

.. _Pipeline: https://open-bitbucket.nrao.edu/projects/PIPE

Dependencies for building a functional CASA6 environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. note:: 

    `Pipeline`_ is a Python-base package with CASA6 components as dependencies, therefore, it only operates limited on the Python versions and the platform supported by specific CASA versions (see the `CASA6 compatibility matrix <https://casadocs.readthedocs.io/en/stable/notebooks/introduction.html#Compatibility>`_) and others dependency libraries specifically required by Pipeine  Please also check the OS/libraries compatibility and requirements for running CASA.


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
    
    `casadata` and `almatasks` are no longer required for ``CASA6>=6.6.1``. Already bundled inside the CASA+Pipeline release/prerelease tarball. `casaconfig` is implicitly required by `casatasks``

Additional Dependencies Required by Pipeline 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

 - Python (see ``requirements.txt``)

 - cachetools
 - mako
 - pypubsub
 - intervaltree
 - logutils
 - ps_mem
 - astropy
 - bdsf (for VLASS source detection & mask creation)

 - \*unix-cmd

 - ImageMagick
 - the "convert" cmd:
 - Linux only for generating thumbnails; macOS uses "sips" which comes with OS.
 - Also used by ImageMagickWriter from matplotlib.animation in hsd/tasks/common/rasterutil.py (only for developers)
 - the "montage" cmd: required from hifa_renorm
 - poppler-utils
 - the "pdfunite" cmd: required from hifa_renorm
 - Git: for obtaining the Pipeline revision description string.
 - Xvfb/xvfb-run: xvfb from OS is required for the casampi module, which is mandatory for running 
 - uncompress: used by tec_maps.py in CASA for VLA pipeline task hifv_priorcals


Optional Dependencies
---------------------

Testing
^^^^^^^

 - pytest
 - ...


Packaging
^^^^^^^^^

 - wheel: for installing older Python packages
 - csscompressor: minify CSS during installation

Development tools
^^^^^^^^^^^^^^^^^

 - pydocs

Documentation
^^^^^^^^^^^^^

 - sphinx


\*unix-cmd
^^^^^^^^^^

 - Git LFS: for managing the `pipeline-testdata`_ repo

.. _pipeline-testdata: https://open-bitbucket.nrao.edu/projects/PIPE/repos/pipeline-testdata/browse
