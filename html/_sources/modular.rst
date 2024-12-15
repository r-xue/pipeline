Run the Pipeline in a Conda environment
================================================

.. warning::

  running and developing the pipeline from a Conda environment is not officially supported or validated for observatory operation, and the information provided here is for demonstration purposes only.

Setup the environment -  step-by-step
-------------------------------------

- Install ``Miniforge``: We recommend the minimal `miniforge`_ installer, which only includes the `conda-forge`_ channel by default.
  
.. _miniforge: https://github.com/conda-forge/miniforge
.. _conda-forge: https://conda-forge.org
.. _CASA6: https://casadocs.readthedocs.io/en/stable/
.. _Pipeline: https://open-bitbucket.nrao.edu/projects/PIPE
.. _casashell: https://casadocs.readthedocs.io/en/stable/api/casashell.html
.. _casatasks: https://casadocs.readthedocs.io/en/stable/api/casatasks.html
.. _casatools: https://casadocs.readthedocs.io/en/stable/api/casatools.html

- Reproduce a Python environment with modular `CASA6`_ components and their dependency libraries and helper tools, e.g., `openmpi <https://www.open-mpi.org>`_.

  - Fetch source code:

    .. code-block:: bash

      git clone https://open-bitbucket.nrao.edu/scm/pipe/pipeline.git --branch PIPE-1669-run-dev-pipeline-with-modular-casa6
      cd pipeline
  
  - Recreate the Conda environment with all CASA6 components for `Pipeline`_ development:

    .. code-block:: bash

      conda env create --file=pipe1669.yaml

    or

    .. code-block:: bash

      conda create -n pipe1669
      conda env update --file=pipe1669.yaml

    This will set up a Conda environment named 'pipe1669'.  
    You might also update an existing environment:

    .. code-block:: bash

      conda update -n pipe1669 --all
      conda env update --file=pipe1669.yaml

    To clean up an existing environment and start fresh:

    .. code-block:: bash

      conda env remove -n pipe1669

    If you just want to remove one component:

    .. code-block:: bash

      conda remove -n pipe1669 mpi4py

  - Dev install `Pipeline`_:

    .. code-block:: bash

      conda activate pipe1669
      pip install -e .

    or to use an add-on library for development and experimental purposes, try:

    .. code-block:: bash

      pip install -e .[dev]

.. note::

  **``pipe1669.yaml`` vs. ``requirements.txt``**
  

  - The scope of ``pipe1669.yaml`` is to create a pseudo-monolithic CASA6-like Python environment.
  - The ``pyproject.toml`` + ``requirements.txt`` handle `Pipeline`_ installation within that environment and are designed to work for both monolithic and modular `CASA6`_ cases.

Run `Pipeline`_
------------

Typical use pattern (see more details below):

.. code-block:: bash

    conda activate pipe1669
    xvfb-run -a python run_pipeline.py

``run_pipeline.py`` is a demo script. An example context could be:

.. code-block:: python

    import pipeline.recipereducer, os
    pipeline.recipereducer.reduce(vis=['../rawdata/uid___A002_Xc46ab2_X15ae_repSPW_spw16_17_small.ms'],
                                  procedure='procedure_hifa_calimage.xml', loglevel='debug')

Serial session
^^^^^^^^^^^^^^

- A standard Python session:

  .. code-block:: bash

      PYTHONNOUSERSITE=1 OMP_NUM_THREADS=8 xvfb-run -a python ../scripts/run_pipeline.py

- Via `casashell`_:

  .. code-block:: bash

      PYTHONNOUSERSITE=1 OMP_NUM_THREADS=8 xvfb-run -a python -m casashell --nologger --log2term --agg -c ../scripts/run_pipeline.py

Parallel (``mpicasa``) session
^^^^^^^^^^^^^^^^^^^^^^^^^^

As reported in `CAS-14037 <https://open-jira.nrao.edu/browse/CAS-14037>`, to avoid circular imports during the `casampi` process initialization, you need to execute `casampi.private.start_mpi` before and outside the scope of `casatasks`_ or `casashell`_ (which implicitly import `casatasks`). As a workaround, include the following boilerplate command:

  .. code-block:: python

      try:
          import casampi.private.start_mpi  # assign the client and server roles
          import casatasks                  # ensure the time-based logfile name
      except (ImportError, RuntimeError) as error:
          pass

- A standard Python session:

  .. code-block:: bash

      PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 xvfb-run -a \
        mpirun -display-allocation -display-map -oversubscribe --mca btl_vader_single_copy_mechanism none -x OMP_NUM_THREADS -n 4 \
        python -c "import casampi.private.start_mpi; exec(open('../scripts/run_pipeline.py').read())"

  If you run a parallel CASA session without going through `casashell`_ (e.g., `mpirun -n 4 python run_script.py`), place the code snippet above at the beginning of your Python script before any `casatasks` import actions to avoid deadlocks.

- Via `casashell`_:

  .. code-block:: bash

      PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 xvfb-run -a \
        mpirun -display-allocation -display-map -oversubscribe --mca btl_vader_single_copy_mechanism none -x OMP_NUM_THREADS -n 4 \
        python -c "import casampi.private.start_mpi; import casashell.__main__" --nologger --log2term --agg -c ../scripts/run_pipeline.py

  If you run a parallel CASA session with `casashell`, you need to add the code snippet inside `~/.casa/config.py`. Failure to do so will result in a deadlock the first time `casatasks` is imported. Note that we use `python -c "import casampi.private.start_mpi; import casashell.__main__"` instead of `python -m casashell`_ so that `start_mpi` runs before `casatasks`_ is imported.

.. note::

  **Notes on macOS (including Apple Silicon)**

  - Running a `mpicasa` session on macOS

    A parallel Pipeline data processing session might hang on macOS at the compleation of the job due to lingering `casaplotms.app` sub-processes. 
    This behavior appears to be different from Linux, potentially caused by the fact that each ``casaplotms`` process spawned from a MPIserver proceess runs as a macOS "app".
    Although this doesn’t affect the data processing, to ensure a clean exit, one might need to use the following snippet at the end of your Python job script:

    .. code-block:: python

        def close_plotms_on_mpiservers():
            try:
                from casampi.MPIEnvironment import MPIEnvironment
                mpi_server_list = MPIEnvironment.mpi_server_rank_list()
                client.push_command_request('from casaplotms import plotmstool', block=True,
                                            target_server=mpi_server_list)
                rs_list = client.push_command_request('plotmstool.__proc!=None', block=True,
                                                    target_server=mpi_server_list)
                servers_with_active_plotms = [rs['server'] for rs in rs_list if rs['ret']]
                if servers_with_active_plotms:
                    print(f'servers with active plotms instances: {servers_with_active_plotms}')
                    client.push_command_request('plotmstool.__proc.kill()', block=True, target_server=servers_with_active_plotms)
            except:
                pass
        
        close_plotms_on_mpiservers()

    In addition, ``xvfb-run`` is unavailable on macOS, therefore, you are not able to use it for headless session. To complete a Pipeline processing session with casaplotms as nesscarity, one has to remotely login in GUI and casaplotms GUI will show up in the desktop GUI end and can't be forwarded.

    - With ``casashell``:

      .. code-block:: bash

          PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 \
            mpirun -display-allocation -display-map -oversubscribe --mca btl_vader_single_copy_mechanism none -x OMP_NUM_THREADS -x PYTHONNOUSERSITE -n 4 \
            python -c "import casampi.private.start_mpi; import casashell.__main__" --nologger --log2term --agg -c ../scripts/run_pltest.py

    - Without ``casashell``:

      .. code-block:: bash

          PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 \
            mpirun -display-allocation -display-map -oversubscribe --mca btl_vader_single_copy_mechanism none -x OMP_NUM_THREADS -x PYTHONNOUSERSITE -n 4 \
            python -c "import casampi.private.start_mpi; exec(open('../scripts/run_pltest.py').read())"

  - ``x86_64`` vs ``arm64``

    * `casatools`_ wheels started become available on both ``x86_64`` and ``arm64`` archiectures of macOS (see the `CASA6 compaibility matrix <https://casadocs.readthedocs.io/en/stable/notebooks/introduction.html#Compatibility>`_)

    * Because the `casatools`_ wheels only start being built from CASA ver>=6.6.4 with Py3.10 on the ARM64 (Apple Silicon) platform:

      * If your local conda install architecture is "arm64 (Apple Silicon)" rather than "x86_64", you have to manually edit "python=3.8"
      to "python=3.10" below for the Python version specification. You will also skip the installation of two optional dependencies: "almatasks" and "pybsdf"

      * If your local **Conda installation architecture** is "x86_64" (either an Intel Macs or Apple Silicon Macs with the Rosetta 2 emulation layer),
      you can keep using the "python=3.8" in the Conda environment for full compatibility and support of all Pipeline features. Note one might need to set
      an environment variable (SYSTEM_VERSION_COMPAT=0) for your Conda command because `Conda/Python3.8 <https://github.com/pypa/packaging/pull/319>`_ is too old for pip to recognize the macOS version labels. If you local Conda architetuere is "arm64" (native on Apple Silicon Macs), please choose the Python 3.10 setup (as specified in the latest `pipe1669.yaml``).
      The support of CPU architectures from `miniforge`_ can be found `here <https://github.com/conda-forge/miniforge?tab=readme-ov-file#miniforge3>`_


Useful shorthand
----------------

Useful aliases/shortcuts to emulate monolithic CASA executables:

.. code-block:: bash

    conda activate pipe1669

    export casa_omp_num_threads=4
    export casa_mpi_nproc=4
    export TMPDIR=/tmp    
    
    export casa6_opts_custom='--nologger --log2term --agg'
    export mpirun_custom='mpirun -display-allocation -display-map -oversubscribe --mca btl_vader_single_copy_mechanism none --mca btl ^openib -x OMP_NUM_THREADS -x PYTHONNOUSERSITE'
    export xvfb_run_auto='xvfb-run -a' # Debian, Ubuntu, RedHat8, etc.

    alias casa6='PYTHONNOUSERSITE=1 OMP_NUM_THREADS=${casa_omp_num_threads} python -m casashell'
    alias casa6mpi='PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 ${mpirun_custom} -n ${casa_mpi_nproc} python -c "import casampi.private.start_mpi; import casashell.__main__"'

    # For Linux only, not applicable on macOS
    alias casa6_xvfb='PYTHONNOUSERSITE=1 OMP_NUM_THREADS=${casa_omp_num_threads} ${xvfb_run_auto} python -m casashell'
    alias casa6mpi_xvfb='PYTHONNOUSERSITE=1 OMP_NUM_THREADS=1 ${xvfb_run_auto} ${mpirun_custom} -n ${casa_mpi_nproc} python -c "import casampi.private.start_mpi; import casashell.__main__"'

For executing a headless parallel Pipeline processing session on Linux, one could try:

.. code-block:: bash
  
    casa6mpi_xvfb ${casa6_opts_custom} -c ../scripts/run_pltest.py

If you prefer running on with a 4-core mpicasa session (1 client + 7 servers), one could do:

.. code-block:: bash
  
    casa_mpi_nproc=8 casa6mpi_xvfb ${casa6_opts_custom} -c ../scripts/run_pltest.py



