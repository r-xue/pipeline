import getpass
import os
import socket
import subprocess
import sys
import textwrap
import time

import casatasks
import casatools
import dask
import pipeline
import pipeline.infrastructure.executeppr as eppr
import pipeline.infrastructure.executeppr as vlaeppr
import pipeline.recipereducer

try:
    from dask.distributed import Client, LocalCluster #, SubprocessCluster
except ImportError:
    pass
from pipeline.infrastructure import daskhelpers
from pipeline import cli_args, session_config

# unbuffered stdout/stderr/stdin
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
sys.stdin = os.fdopen(sys.stdin.fileno(), 'r', buffering=1)


def session_startup(casa_config, loglevel=None):

    # update the casaconfig attributes before importing the casatasks module

    import casatasks
    from casaconfig import config
    for key, value in casa_config.items():
        if hasattr(config, key) and value is not None:
            # print(key,value)
            setattr(config, key, value)
            if key=='logfile':
                casatasks.casalog.setlogfile(value)

    # initialize casatasks with the custom configurations

    import casatasks
    casalogfile = casatasks.casalog.logfile()

    # adjust the log filtering level for casalogsink

    casaloglevel = 'INFO1'
    # import pipeline.infrastructure.logging as logging
    # if loglevel is not None:
    #     casaloglevel = logging.CASALogHandler.get_casa_priority(logging.LOGGING_LEVELS[loglevel])
    casatasks.casalog.filter(casaloglevel)

    return casalogfile, casaloglevel


def create_slurm_job(session_profile, filename='session.job'):
    """generate a Slurm/sbatch-compatible shell script with all flags for the session.

    reference: https://slurm.schedmd.com/sbatch.html

    note:   use bash (instead of sh) to get some control operator working.
            /bin/sh is soft linked to /bin/bash cvpost/nmpost, so it doesn't make a difference.
            /bin/sh is soft linked to /bin/dash on Ubuntu
    """

    # directive_name, directive_comment, directive_alias_in_profile, directive_default
    # directive_default:
    #   ''      : no value attached, just a flag
    #   None    : skip
    directives = [('no-requeue', 'Do not requeue jobs after a node failure', 'none-requeue', ''),
                  ('export', 'Export all environment variables to job', 'export', 'ALL'),
                  ('job-name', 'Job name', 'job-name', None),
                  ('nodelist', 'Node list', 'nodelist', None),
                  ('chdir', 'Working directory', 'chdir', './'),
                  ('output', "the batch script's standard output", 'output', 'slurm-%j.out'),
                  ('error', "the batch script's standard error", 'error', 'slurm-%j.out'),
                  ('partition', 'Request a specific partition', 'partition', 'batch'),
                  ('mail-type', 'Send email on begin, end, and fail of job',
                   'mail-type', 'BEGIN,END,FAIL'),
                  ('nodes', 'Request exactly 1 node', 'nodes', None),
                  ('ntasks', 'Request n_lprocs cores on the same node.', 'ntasks', None),
                  ('mail-user', 'Default is submitter', 'mail-user', None),
                  ('time', 'request days/hrs', 'time', None),
                  ('cpus-per-task', 'Advise the Slurm controller that ensuing job steps will require ncpus number of processors per task', 'cpus-per-task', None),
                  ('mem-per-cpu', 'Minimum memory required per usable allocated CPU',
                   'mem-per-cpu', None),  # 2G
                  ('mem-bind', 'Bind tasks to memory', 'mem-bind', None),
                  ('mem', 'Amount of memory needed by the whole job. 0 is all mem on the node', 'mem', None)]  # 16G

    # jobrequest_content = ['#!/bin/bash -e', '']
    jobrequest_content = ['#!/bin/bash', '']

    used_keys = []
    for directive in directives:
        key, comment, key_in_profile, value = directive
        #  The --mem, --mem-per-cpu and --mem-per-gpu options are mutually exclusive.
        if key == 'mem' and 'mem-per-cpu' in used_keys:
            continue
        specs = session_profile.get(key_in_profile, None)
        if specs is not None:
            value = specs
        if value is not None:
            if value == '':
                line = f'#SBATCH --{key} # {comment}'
            else:
                line = f'#SBATCH --{key}={value} # {comment}'
            jobrequest_content.append(line)
            used_keys.append(key)
    jobrequest_content.append('')
    for cmd in session_profile['cmds']:
        jobrequest_content.append(cmd)

    jobrequest_content = '\n'.join(jobrequest_content)

    with open(filename, "w+") as f:
        f.writelines(textwrap.dedent(jobrequest_content))

    return


def main():
    """Run a Pipeline data processing session from the console."""

    # from pprint import pprint
    # pprint(session_config)

    # optionally switch on xvfb
    # if session_config['pipeconfig'].get('xvfb', False):
    #    from pyvirtualdisplay import Display
    #    disp = Display(visible=0, size=(2048, 2048))
    #    disp.start()

    # session intialization
    loglevel = session_config['pipeconfig']['loglevel']
    session_config['casaconfig'] = session_config.get('casaconfig', {})
    # casalogfile, _ = session_startup(session_config['casaconfig'], loglevel=loglevel)

    logger = pipeline.infrastructure.get_logger('main')

    username = getpass.getuser()
    hostname = socket.gethostname()
    casatools_version = casatools.version_string()
    logger.info('-'*80)
    logger.info('hostname:           {}'.format(hostname))
    logger.info('user:               {}'.format(username))
    logger.info('casatools_versuon:  {}'.format(casatools_version))
    logger.info('-'*80)
    logger.info('session logfile %s', casatasks.casalog.logfile())

    env = os.environ.copy()

    slurm_config = session_config.get('slurm', {'cmds': []})

    # mpirun + cli-opts
    mpirun_opt = []
    if session_config.get('casampi', None) is not None:
        casampi_config = session_config['casampi']
        mpirun_opt = ['mpirun', '-display-allocation', '-display-map', '--mca btl_vader_single_copy_mechanism none --mca btl ^openib']
        mpirun_opt.append('-x OMP_NUM_THREADS -x PYTHONNOUSERSITE')
        if casampi_config['oversubscribe']:
            mpirun_opt.append('-oversubscribe')
        if casampi_config['n']:
            mpirun_opt.append('-n {}'.format(casampi_config['n']))

    # xvfb-run + cli-opts
    xvfb_run_opt = []

    # the CLI wrapper appending; may be unnesscary
    #   for dask-based x11 applications: pyvirtualdisplay will do the work.
    #   for mpicasa sessions: casampi takes care of x11 for mpiservers;
    #       pyvirtualdisplay takes care of the client process.
    #
    if session_config['pipeconfig'].get('xvfb', False):
        xvfb_run_opt.append('xvfb-run -a')

    # conda + cli-opts
    conda_opt = []
    conda_activate = []
    if session_config['pipeconfig']['conda_env']:
        conda_opt.append('conda run -n {} --live-stream'.format(session_config['pipeconfig']['conda_env']))
        # conda_activate.append('conda activate {}'.format(session_config['pipeconfig']['conda_env']))

    # any env variables
    env_vars = []
    if session_config.get('casampi', None) is not None:
        casampi_omp_num_threads = session_config['casampi'].get('omp_num_threads', 1)
        env_vars.append('OMP_NUM_THREADS={}'.format(casampi_omp_num_threads))
    else:
        omp_num_threads = session_config['pipeconfig'].get('omp_num_threads', 4)
        env_vars.append('OMP_NUM_THREADS={}'.format(omp_num_threads))

    FLUX_SERVICE_URL = session_config['pipeconfig'].get('FLUX_SERVICE_URL', 'https://almascience.org/sc/flux')
    FLUX_SERVICE_URL_BACKUP = session_config['pipeconfig'].get('FLUX_SERVICE_URL_BACKUP', 'https://asa.alma.cl/sc/flux')
    env_vars.append('FLUX_SERVICE_URL={}'.format(FLUX_SERVICE_URL))
    env_vars.append('FLUX_SERVICE_URL_BACKUP={}'.format(FLUX_SERVICE_URL_BACKUP))

    # pipeline cli interface
    #   * need to start mpiclient/server initialization outside of this module to avoid curcular import for a casampi session.
    #   * this is functionally eqauivelent to `>python -m pipeline` or `>pairs`
    #   * try `>python -m pipeline --help`
    pipe_opt = ['python -c "import casampi.private.start_mpi; from pipeline.__main__ import main; main()"']

    # print('-->', sys.orig_argv)
    # print('-->', sys.argv)

    pipe_opt.extend(sys.argv[1:])
    pipe_opt.append('--local')

    # assemble the final pipe-run call

    cmd = env_vars
    cmd.extend(conda_opt)
    cmd.extend(xvfb_run_opt)
    cmd.extend(mpirun_opt)
    cmd.extend(pipe_opt)
    cmd = ' '.join(cmd)

    # setup the working dir

    chdir = './'
    if session_config['pipeconfig'].get('chdir', None) is not None:
        chdir = session_config['pipeconfig'].get('chdir', None)

    chdir = os.path.abspath(os.path.expanduser(chdir))
    # create the working directory from the parent process
    logger.info(f'create/check-in: {chdir}')
    os.makedirs(chdir, exist_ok=True)

    # future proofing in the console process
    chdir_update = ['mkdir -p {} && cd {} || exit -1'.format(chdir, chdir)]

    slurm_config['cmds'] = conda_activate+slurm_config['cmds']
    slurm_config['cmds'] = chdir_update+slurm_config['cmds']
    slurm_config['cmds'].append(cmd)
    slurm_config['chdir'] = chdir

    if not cli_args.local:

        job_filename = os.path.join(chdir, 'session.job')
        create_slurm_job(slurm_config, filename=job_filename)

        if 'slurm' in session_config and 'SLURM_JOB_ID' not in env:
            jobrequest_cmd = f"sbatch {job_filename}"
            logger.info(f'Execute the session:  {jobrequest_cmd}')
            if not cli_args.dry_run:
                cp = subprocess.run(jobrequest_cmd, shell=True,
                                    check=False, env=env, capture_output=True)
                logger.info(cp.stdout.decode().strip())
                time.sleep(1.0)
                cp = subprocess.run(
                    f'squeue --format="%7i %13P %9u %7T %11M %11l %5D %2C %2c/%7m %16R %50j %50Z" -u {username}',
                    shell=True, check=True,
                    capture_output=True)
                logger.info(cp.stdout.decode().strip())
        else:
            jobrequest_cmd = f'bash -i {job_filename}'
            logger.info(f'Execute the session:  {jobrequest_cmd}')
            if not cli_args.dry_run:
                cp = subprocess.run(jobrequest_cmd, shell=True,
                                    check=False, env=env, capture_output=False)

        return

    # start a Dask cluster for dask-assisted tier0 parallelization specified by a configuration file
    if __name__ in ['pipeline.__main__', '__main__'] and cli_args.session is not None and session_config.get('dask', None):
        # Load custom configuration file
        dask_config = session_config['dask']
        dask.config.update_defaults(dask_config)

        # Optionally, print the config to see what is loaded
        logger.info(dask.config.config)

        # Retrieve settings from the config
        n_workers = dask.config.get('distributed.worker.n_workers', default=None)
        threads_per_worker = dask.config.get('distributed.worker.nthreads', default=None)
        scheduler_port = dask.config.get('distributed.scheduler.port', default=None)
        dashboard_port = dask.config.get('distributed.scheduler.dashboard.port', default=None)

        from distributed import Nanny
        # Set up the cluster based on the config
        cluster = LocalCluster(
            n_workers=n_workers,
            worker_class=Nanny,
            processes=True,  # explicitly True to avoid GIL
            threads_per_worker=threads_per_worker,
            scheduler_port=0, # scheduler_port,
            # dashboard_address=f":{dashboard_port}" if dashboard_port else None,  # Set the dashboard port
            dashboard_address=f":0" if dashboard_port else None,  # Set the dashboard port
        )

        # from dask_jobqueue import SLURMCluster
        # cluster = SLURMCluster(
        #     n_workers=2,
        #     queue='queue',
        #     cores=2,
        #     memory='123GB',interface='eno1')

        # cluster=SubprocessCluster(n_workers=4, threads_per_worker=1)
        # print(cluster)

        # Connect the client to the cluster
        daskclient = Client(cluster)
        logger.info("Cluster dashboard: %s", daskclient.dashboard_link)

        # sideload the daskclient to the new `daskhelpers` module
        daskhelpers.daskclient = daskclient
        logger.info('%s', daskclient)
        print(dask_config.get('tier0futures', None))
        daskhelpers.tier0futures = bool(dask_config.get('tier0futures', None))

        # Properly initailize the worker process state
        casalogfile = casatasks.casalog.logfile()
        session_config['casaconfig']['logfile'] = casalogfile
        logger.info('run: %s', session_config['casaconfig'])
        daskclient.run(session_startup, session_config['casaconfig'], loglevel)
        daskclient.run(os.getpid)
        def get_status(dask_worker):
            return dask_worker.status, dask_worker.id
        status=daskclient.run(get_status)
        from pprint import pformat
        logger.info('worker status: %s', pformat(status))

    if cli_args.dry_run:
        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return

    if cli_args.session is not None:

        pipeconfig = session_config['pipeconfig']
        # logger.info('pipeconfig cli_args: %s', pipeconfig)

        if cli_args.vlappr is not None:
            pipeconfig['vlappr'] = cli_args.vlappr
        if cli_args.ppr is not None:
            pipeconfig['ppr'] = cli_args.ppr

        if pipeconfig['procedure'] is not None:
            pipeline.recipereducer.reduce(
                procedure=pipeconfig['procedure'],
                vis=pipeconfig['vis'],
                loglevel=pipeconfig.get('loglevel', None))

        if pipeconfig['vlappr'] is not None:
            vlaeppr.executeppr(pipeconfig['vlappr'], importonly=False)

        if pipeconfig['ppr'] is not None:
            eppr.executeppr(pipeconfig['ppr'], importonly=False)

        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return


if __name__ == "__main__":
    main()
