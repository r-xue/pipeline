"""Helper functions for using Dask for parallel processing in the pipeline.

This module provides functions to start and stop a Dask cluster,
submit tasks to the cluster, and retrieve results.  It also includes
functions to determine if the current process is a Dask worker and
whether Dask is ready for use.  The module leverages Dask's `delayed`
functionality and `Future` objects for parallel task execution and
result management.  It also handles merging of CASA command logs from
distributed workers.

The module uses a custom `FutureTask` class to encapsulate the submission
and retrieval of results from Dask futures.  The `future_exec` function
is used by Dask workers to execute the actual pipeline tasks.  The
`is_dask_ready` function checks if a Dask client is available and
`tier0futures` is enabled, indicating readiness for parallel processing
using Dask.
"""

from __future__ import annotations

import atexit
import datetime
import importlib.util
import os
import socket
from importlib.resources import files
from pprint import pformat
from typing import Dict, Optional, Tuple, Union

import casatasks

import pipeline.infrastructure.logging as logging
from pipeline.config import config
from pipeline.infrastructure.utils import get_obj_size, human_file_size

# Check if all required dask modules are available
dask_spec = importlib.util.find_spec('dask')
distributed_spec = importlib.util.find_spec('distributed')
dask_jobqueue_spec = importlib.util.find_spec('dask_jobqueue')
dask_available = all([dask_spec, distributed_spec, dask_jobqueue_spec])

is_mpi_session = False
is_mpi_worker = False
if importlib.util.find_spec('casampi'):
    from mpi4py import MPI
    from casampi.MPIEnvironment import MPIEnvironment

    is_mpi_session = MPIEnvironment.is_mpi_enabled
    is_mpi_worker = MPIEnvironment.is_mpi_enabled and not MPIEnvironment.is_mpi_client
    comm = MPI.COMM_WORLD
    mpi_rank = comm.Get_rank()
    rank = comm.Get_rank()
    size = comm.Get_size()

if dask_available and not is_mpi_worker:
    # We should avoid importing dask in MPI worker processes as some Python libraaries (e.g. signal)
    # are only allowed in main thread of the main interpreter.
    import dask
    from dask.distributed import Client, LocalCluster, WorkerPlugin
    from dask.distributed.worker import Worker
    from dask_jobqueue import HTCondorCluster, SLURMCluster
else:
    Client = None
    Worker = None
    LocalCluster = None
    SLURMCluster = None


daskclient: Optional[Client] = None
tier0futures: bool = False

LOG = logging.get_logger(__name__)

__all__ = [
    'is_worker',
    'daskclient',
    'tier0futures',
    'dask_available',
    'is_dask_ready',
]


def sanitize_env_for_children():
    # Remove the usual OpenMPI/PMIx env keys that confuse MPI_Init in child processes.
    # This is best done on rank 0 *only* and only when you know removing them won't break other MPI ranks.

    # mpi_prefixes=("OMPI_", "PMI_", "PMIX", "PRTE", "SLURM_", 'OPAL_')
    mpi_prefixes = ('PMIX', 'PRTE', 'OMPI', 'PMIX_SERVER', 'PRTE_')
    for prefix in mpi_prefixes:
        for key in list(os.environ):
            if key.startswith(prefix):
                os.environ.pop(key, None)


def is_worker() -> bool:
    """Determine if the current process is running as a Dask worker or MPI-Server

    This function checks if the current Python process is executing inside a Dask
    worker. It uses a reliable method that works even during the worker setup
    process, specifically by checking for active worker instances using weak
    references.

    See: https://stackoverflow.com/questions/78589634/what-is-the-cleanest-way-to-detect-whether-im-running-in-a-dask-worker

    Note that currently is it's running a mpicasa session, this will always return
    true before the hybride-mod (co-operating mpi cluster + dask cluster) is fully
    tested.

    Returns:
        bool: True if the process is a Dask worker, False otherwise.
    """
    is_worker = False
    if (dask_available and Worker._instances) or is_mpi_session:
        is_worker = True
    return is_worker


def is_daskclient_allowed():
    """Check if Dask client/initialization is allowed in the current process

    Returns:
        bool: True if a Dask client/cluster initialization is permitted, False otherwise.
    """
    # disallow starting dask client if running inside a MPI worker process
    # this can be identifeded by both RANK or casampi APIs.
    if is_mpi_worker or mpi_rank != 0:
        return False
    # disallow starting dask client if running inside a dask worker process,
    # identified by env variables.
    if os.getenv('DASK_WORKER_NAME') is not None or os.getenv('DASK_PARENT') is not None:
        return False
    # disallow starting dask client if there are existing dask worker instances,
    # identified by weak references.
    if dask_available and Worker._instances:
        return False
    if not dask_available:
        return False
    return True


class FutureTask:
    """Encapsulates the submission and retrieval of results from Dask futures.

    This class simplifies the interaction with Dask futures, providing a
    consistent interface for submitting tasks and retrieving results. It also
    handles merging of CASA command logs from distributed workers.
    """

    def __init__(self, executable):
        """Submits a task to the Dask cluster.

        Args:
            executable: The executable object to be run on a Dask worker.
        """
        LOG.debug(
            'submitting a FutureTask %s from the dask client: %s',
            executable,
            human_file_size(get_obj_size(executable)),
        )
        self.future = daskclient.submit(future_exec, executable)

    def get_result(self):
        """Retrieves the result from the Dask future and merges CASA logs.

        Returns:
            The result of the task execution.

        Raises:
            Exception: If an error occurs during task execution or log merging.
        """
        task_result, tier0_executable = self.future.result()
        LOG.debug(
            'Received the task execution result (%s) from a worker for executing %s; content:',
            human_file_size(get_obj_size(task_result)),
            tier0_executable,
        )
        LOG.debug(pformat(task_result))

        self._merge_casa_commands(tier0_executable.logs)

        return task_result

    def _merge_casa_commands(self, logs):
        """Merges CASA command logs from a worker into the client-side log."""
        LOG.debug('return request logs: %s', logs)

        response_logs = logs
        client_cmdfile = response_logs.get('casa_commands')
        tier0_cmdfile = response_logs.get('casa_commands_tier0')

        if all(isinstance(cmdfile, str) and os.path.exists(cmdfile) for cmdfile in [client_cmdfile, tier0_cmdfile]):
            LOG.info('Merge %s into %s', tier0_cmdfile, client_cmdfile)
            with open(client_cmdfile, 'a', encoding='utf-8') as client:
                with open(tier0_cmdfile, 'r', encoding='utf-8') as tier0:
                    client.write(tier0.read())
                os.remove(tier0_cmdfile)
        else:
            LOG.debug('Cannot find Tier0 casa_commands.log; no merge needed')


def future_exec(tier0_executable):
    """Execute a pipeline task on a Dask worker.

    This function is called by Dask workers to execute pipeline tasks. It
    retrieves the executable from the Tier0Executable object and executes it.
    The result and the Tier0Executable object are then returned.

    Args:
        tier0_executable: The Tier0Executable object containing the executable.

    Returns:
        A tuple containing the result of the task execution and the
        Tier0Executable object.
    """
    executable = tier0_executable.get_executable()

    ret = executable()
    LOG.debug(
        'Buffering the execution return (%s) of %s',
        human_file_size(get_obj_size(ret)),
        tier0_executable,
    )

    return ret, tier0_executable


def is_dask_ready():
    """Check if Dask is ready for parallel task execution.

    Returns:
        bool: True if a Dask client is available and tier0futures is enabled,
              False otherwise.
    """
    return bool(daskclient) and tier0futures


def session_startup(casa_config: Dict[str, Optional[str]], loglevel: Optional[str] = None) -> Tuple[str, str]:
    """Initializes a CASA session with custom configurations and log settings.

    This function updates the casaconfig attributes, sets the CASA log file, and adjusts
    the log filtering level for casalogsink.

    Args:
        casa_config: A dictionary containing CASA configuration attributes. Keys are
                     attribute names (e.g., 'logfile', 'nworkers'), and values are the
                     desired settings. Values can be None, in which case the attribute
                     is not modified.
        loglevel: Optional pipeline log level string:
                        critical, error, warning, attention, info, debug, todo, trace.
                  If provided, the CASA log filter level is adjusted accordingly. If None,
                  casa loglevel defaults to 'INFO1'.

    Returns:
        A tuple containing:
            - The path to the CASA log file (str).
            - The CASA log filter level (str).
    """
    from casaconfig import config

    print('setlogfile', casa_config)

    # Update casaconfig attributes
    for key, value in casa_config.items():
        if hasattr(config, key) and value is not None:
            setattr(config, key, value)
            if key == 'logfile':
                pass
                # casatasks.casalog.setlogfile(value)
    # Initialize casatasks and get log file

    casalogfile = casatasks.casalog.logfile()

    # Adjust log filtering level
    casaloglevel = 'INFO1'
    if loglevel is not None:
        # map pipeline loglevel to casa loglevel, e.g. PL-attention is eqauivelentt to INFO1
        casaloglevel = logging.CASALogHandler.get_casa_priority(logging.LOGGING_LEVELS[loglevel])

    casatasks.casalog.filter(casaloglevel)

    return casalogfile, casaloglevel


def start_daskcluster(
    dask_config: Optional[Dict[str, Union[str, int, bool]]] = None,
) -> Optional[Client]:
    """Start a Dask cluster based on configuration settings.

    This function initializes a Dask cluster, either locally or via SLURM,
    based on the configuration provided. It also sets up a Dask client,
    registers a cleanup function, and initializes worker processes.

    Args:
        dask_config: Optional dictionary containing Dask configuration settings.
                     If None, the default configuration from `config['pipeconfig']['dask']` is used.

    Returns:
        Client: A Dask client instance if the cluster is successfully started,
                None otherwise.
    """
    global daskclient, tier0futures

    def custom_worker_init(logfile):
        # os.environ['_CASA_LOGFILE'] = logfile
        print(f'Initialized worker with PID: {os.getpid()}, logfile: {logfile}')

    class CustomInitPlugin(WorkerPlugin):
        def setup(self, worker):
            logfile = casatasks.casalog.logfile()
            print(
                f'Initialized {worker.id} @ {worker.address} - {socket.gethostname()} - PID: {os.getpid()}\n    logfile: {logfile}'
            )

    if not dask_available:
        LOG.warning('dask[distributed] not installed; skipping...')
        return

    dask_config_session = config['pipeconfig']['dask']
    LOG.debug('dask config (session): \n %s', pformat(dask_config_session))
    dask.config.update_defaults(dask_config_session)
    if dask_config is not None:
        LOG.debug('dask config (user): \n %s', pformat(dask_config))
        dask.config.update_defaults(dask_config)

    # attached the exect client casaconfig issue which can be used to help initialze workers
    dask.config.update_defaults({'casaconfig': config['casaconfig']})

    # Optionally, print the config to see what is loaded
    # LOG.info(dask.config.config)

    # Retrieve settings from the config
    # scheduler_port = dask.config.get('distributed.scheduler.port', default=None)

    dashboard_address: Optional[str] = dask.config.get('dashboard_address', default=None)
    n_workers: Optional[int] = dask.config.get('n_workers', default=None)
    clustertype: Optional[str] = dask.config.get('clustertype', default=None)

    if is_daskclient_allowed():
        cluster: Union[LocalCluster, SLURMCluster, None] = None
        path_to_resources_pkg = str(files('pipeline'))
        preload_script = os.path.join(path_to_resources_pkg, 'cleanup_mpi_environment.py')

        if clustertype == 'local':
            sanitize_env_for_children()
            if n_workers is None or n_workers <= 0:
                n_workers = _default_n_workers()
            cluster = LocalCluster(
                n_workers=n_workers,
                dashboard_address=dashboard_address,
                processes=True,  # True to avoid GIL, but risk of worker process inherit the MPI environment, leading to MPI_Init failure:  Open MPI gets confused because those child processes are not properly registered as ranks
                threads_per_worker=1,
                # multiprocessing_context="spawn",
                scheduler_port=0,  # random free port to avoid conflicts
                # preload=[preload_script]
            )
        elif clustertype == 'slurm':
            if n_workers is None or n_workers <= 0:
                n_workers = _default_n_workers()
            cluster = SLURMCluster(
                n_workers=n_workers,
                name=dask.config.get('jobqueue.slurm.name', default=None),
                silence_logs='debug',
                # log_directory='dask-logs',
                scheduler_options={'dashboard_address': dashboard_address},
                worker_extra_args=['--preload', preload_script],
            )
            cluster.scale(n_workers)
            LOG.debug('dask SLURMCluster job script: \n %s', pformat(cluster.job_script()))
        elif clustertype == 'htcondor':
            sanitize_env_for_children()
            if n_workers is None or n_workers <= 0:
                n_workers = _default_n_workers()
            cluster = HTCondorCluster(
                n_workers=n_workers,
                name=dask.config.get('jobqueue.htcondor.name', default=None),
                scheduler_options={'dashboard_address': dashboard_address},
                worker_extra_args=['--preload', preload_script],
            )
            LOG.debug('dask HTCondorCluster job script: \n %s', pformat(cluster.job_script()))
        else:
            LOG.warning('dask cluster specification (%s) not valid, skipping!', clustertype)
            return None

        QUEUE_WAIT = 60

        daskclient = Client(cluster)

        # Wait for all workers to become available.
        # This is necessary on HTCondor/SLURM clusters where workers are spawned asynchronously.
        start_time = datetime.datetime.now()
        LOG.info(
            'starting %s workers at %s (waiting up to %d seconds)',
            n_workers,
            start_time.strftime('%Y-%m-%d %H:%M:%S'),
            QUEUE_WAIT,
        )

        daskclient.wait_for_workers(n_workers, timeout=QUEUE_WAIT)

        end_time = datetime.datetime.now()
        elapsed = (end_time - start_time).total_seconds()
        LOG.info(
            'Acquired %d workers at %s (waited %.2f seconds)',
            n_workers,
            end_time.strftime('%Y-%m-%d %H:%M:%S'),
            elapsed,
        )

        # prefer using plugin which automatically applied to new workers joined by scaling
        daskclient.register_plugin(CustomInitPlugin())

        # daskclient.run(session_startup, {})
        # casaconfig: Dict[str, Optional[str]] = {"logfile": casalogfile}
        # import casatasks
        # casalogfile: Optional[str] = None
        # casalogfile = casatasks.casalog.logfile()
        # daskclient.run(custom_worker_init, casalogfile)

        tier0futures = bool(dask.config.get('tier0futures', default=None))

        atexit.register(stop_daskcluster)

    else:
        if daskclient is not None:
            LOG.warning('dask cluster already started.')

    if daskclient:
        LOG.info('Cluster dashboard: %s', daskclient.dashboard_link)
        LOG.info('   client:  %s', daskclient)
        LOG.info('   cluster: %s', daskclient.cluster)

        def get_status(dask_worker: Worker) -> tuple[str, str]:
            return dask_worker.status, dask_worker.id

        status: Dict[str, tuple[str, str]] = daskclient.run(get_status)
        if status:
            LOG.info('worker status: \n %s', pformat(status))

    return daskclient


def exec_func(fn: callable, *args, include_client: bool = True, **kwargs) -> None:
    """Execute the same function on both client and MPI server processes.

    This function enables synchronized execution across MPI infrastructure. It's particularly useful
    for setup tasks that need consistent state across all processes, such as changing the working directory.

    Args:
        fn: The function to execute.
        *args: Positional arguments to pass to the function.
        include_client: If True, the function is also executed on the client process.
        **kwargs: Keyword arguments to pass to the function.
    """
    # Execute on client if requested

    global daskclient, tier0futures

    if include_client:
        fn(*args, **kwargs)

    # Execute on all server processes in a blocking operation
    if daskclient is not None:
        daskclient.run(fn, *args, **kwargs)


def stop_daskcluster() -> None:
    """Stop the Dask cluster and close the client.

    This function shuts down the Dask cluster and closes the associated client.
    It should be registered with `atexit` to ensure proper cleanup.
    """
    global daskclient

    if not dask_available:
        LOG.warning('DASK not installed; skipping')
        return

    if daskclient is not None:
        LOG.info('closing the dask cluster/client at %s', str(daskclient.dashboard_link))
        if daskclient.status == 'running':
            daskclient.close()
        else:
            LOG.warning('client already closed')
        if daskclient.cluster.status.value == 'running':
            daskclient.cluster.close()
        else:
            LOG.warning('cluster already closed')
        daskclient = None

    return


def _default_n_workers():
    """default n_worker heuristics."""

    from dask.system import CPU_COUNT as _num_core_cgroup

    # cape the fallback local/jobqueue worker number at 4
    _default_n_workers_local = min(_num_core_cgroup, 4)

    # discover the cores assigned to slurm/htcondor jobs
    _num_slurm_core = int(os.getenv('SLURM_NTASKS', 0))
    if _num_slurm_core > 0:
        _default_n_workers_local = _num_slurm_core
    _num_condor_core = int(os.getenv('PYTHON_CPU_COUNT', 0))
    if _num_condor_core > 0:
        _default_n_workers_local = _num_condor_core

    return _default_n_workers_local
