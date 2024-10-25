import argparse
import getpass
import os
import socket
import subprocess
import sys
import textwrap
import time

import yaml

# unbuffered stdout/stderr/stdin
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
sys.stdin = os.fdopen(sys.stdin.fileno(), 'r', buffering=1)


def session_startup(casa_config, loglevel=None):

    # update the casaconfig attributes before importing the casatasks module

    from casaconfig import config
    for key, value in casa_config.items():
        if hasattr(config, key) and value is not None:
            setattr(config, key, value)

    # initialize casatasks with the custom configurations

    import casatasks
    casalogfile = casatasks.casalog.logfile()

    # adjust the log filtering level for casalogsink
    import pipeline.infrastructure.logging as logging
    casaloglevel = 'INFO1'
    if loglevel is not None:
        casaloglevel = logging.CASALogHandler.get_casa_priority(logging.LOGGING_LEVELS[loglevel])
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
                  ('job-name', 'Job name', 'name', None),
                  ('nodelist', 'Node list', 'nodelist', None),
                  ('chdir', 'Working directory', 'path', './'),
                  ('output', "the batch script's standard output", 'output', None),
                  ('error', "the batch script's standard error", 'error', None),
                  ('partition', 'Request a specific partition', 'partition', 'batch'),
                  ('mail-type', 'Send email on begin, end, and fail of job',
                   'mail-type', 'BEGIN,END,FAIL'),
                  ('nodes', 'Request exactly 1 node', 'nodes', None),
                  ('ntasks', 'Request n_lprocs cores on the same node.', 'ntasks', None),
                  ('mail-user', 'Default is submitter', 'email', None),
                  ('time', 'request days/hrs', 'time', None),
                  ('cpus-per-task', 'Advise the Slurm controller that ensuing job steps will require ncpus number of processors per task', 'cpus-per-task', None),
                  ('mem-per-cpu', 'Minimum memory required per usable allocated CPU',
                   'mem_per_cpu', None),  # 2G
                  ('mem-bind', 'Bind tasks to memory', 'mem_bind', None),
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


def cli_interface():
    """Run a Pipeline data processing session from the console."""

    description = r"""
 The Pipeline CLI interface ("python -m pipeline" or "paris") creates customized Pipeline workflow sessions from different user/developer interfaces:
            * a Pipeline data processing session (pps) using recipereducer or ppr
            * or, a Pipeline testing session using recipereducer
 """

    parser = argparse.ArgumentParser(prog='python -m pipeline | paris', description=textwrap.dedent(description),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--vlappr', type=str, dest='vlappr', help='execute a VLA ppr')
    parser.add_argument('--ppr', type=str, dest='ppr', help='execute a ALMA ppr')
    parser.add_argument('--session-config', type=str, dest='session', help='run a custom pipeline data processing session using .yaml')

    parser.add_argument(
        '--dask-config', type=str, dest='dask_config',
        help='start a Dask cluster for dask-assisted tier0 parallelization specified by a configuration file')
    parser.add_argument('--logfile',
                        type=str, dest='logfile',
                        help="redirect stdout/stderr/casalog to the same log file")
    parser.add_argument('--dryrun',
                        dest="dry_run", action="store_true",
                        help="run a dry-run test; not really trigger the requested workflow")
    parser.add_argument(
        '--local', dest="local", action="store_true",
        help="execute a session/workflow from the initial parent process rather than in a subprocess or as a slurm job")
    args = parser.parse_args()

    session_config = {}
    if args.session is not None:
        with open(args.session) as f:
            session_config = yaml.safe_load(f)

    # from pprint import pprint
    # pprint(session_config)

    # optionally switch on xvfb
    if session_config['pipeconfig']['xvfb']:
        from pyvirtualdisplay import Display
        disp = Display(visible=0, size=(2048, 2048))
        disp.start()

    # session intialization
    loglevel = session_config['pipeconfig']['loglevel']
    casalogfile, _ = session_startup(session_config['casaconfig'], loglevel=loglevel)

    import casatasks
    import casatools
    import dask
    from dask.distributed import Client, LocalCluster

    import pipeline
    import pipeline.infrastructure.executeppr as eppr
    import pipeline.infrastructure.executeppr as vlaeppr
    import pipeline.recipereducer
    from pipeline.infrastructure import daskhelpers

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
    if session_config['pipeconfig']['xvfb']:
        xvfb_run_opt.append('xvfb-run -a')

    # conda + cli-opts
    conda_opt = []
    conda_activate = []
    if session_config['pipeconfig']['conda_env']:
        conda_opt.append('conda run -n {} --live-stream'.format(session_config['pipeconfig']['conda_env']))
        conda_activate.append('conda activate {}'.format(session_config['pipeconfig']['conda_env']))

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
    pipe_opt = ['python -c "import casampi.private.start_mpi; from pipeline.__main__ import cli_interface; cli_interface()"']

    # print('-->', sys.orig_argv)
    # print('-->', sys.argv)

    pipe_opt.extend(sys.argv[1:])
    pipe_opt.append('--local')

    # assemble the final cmd call

    cmd = env_vars
    # cmd.extend(conda_opt)
    cmd.extend(xvfb_run_opt)
    cmd.extend(mpirun_opt)
    cmd.extend(pipe_opt)
    cmd = ' '.join(cmd)

    slurm_config['cmds'] = conda_activate+slurm_config['cmds']
    slurm_config['cmds'].append(cmd)

    if not args.local:

        job_filename = 'session.job'
        create_slurm_job(slurm_config, filename=job_filename)
        # import pprint
        # pprint.pprint(slurm_config)

        if 'slurm' in session_config and 'SLURM_JOB_ID' not in env:
            jobrequest_cmd = f"sbatch {job_filename}"
            logger.info(f'Execute the session:  {jobrequest_cmd}\n')
            if not args.dry_run:
                cp = subprocess.run(jobrequest_cmd, shell=True,
                                    check=False, env=env, capture_output=True)
                logger.info(cp.stdout.decode())
                time.sleep(1.0)
                cp = subprocess.run(
                    f'squeue --format="%7i %13P %9u %7T %11M %11l %5D %2C %2c/%7m %16R %50j %50Z" -u {username}',
                    shell=True, check=True,
                    capture_output=True)
                logger.info(cp.stdout.decode())
        else:
            jobrequest_cmd = f'bash -i {job_filename}'
            logger.info(f'Execute the session:  {jobrequest_cmd}\n')
            if not args.dry_run:
                cp = subprocess.run(jobrequest_cmd, shell=True,
                                    check=False, env=env, capture_output=False)

        return

    # start the dask cluster if requested

    if __name__ == "__main__" and args.session is not None:

        # Load custom configuration file
        dask.config.update_defaults(session_config['dask'])

        # Optionally, print the config to see what is loaded
        logger.debug(dask.config.config)

        # Retrieve settings from the config
        n_workers = dask.config.get('distributed.worker.n_workers')
        threads_per_worker = dask.config.get('distributed.worker.nthreads')
        scheduler_port = dask.config.get('distributed.scheduler.port')
        dashboard_port = dask.config.get('distributed.scheduler.dashboard.port')

        # Set up the cluster based on the config
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            scheduler_port=scheduler_port,
            dashboard_address=f":{dashboard_port}"  # Set the dashboard port
        )

        # Connect the client to the cluster
        daskclient = Client(cluster)

        logger.info("Cluster dashboard: %s", daskclient.dashboard_link)
        daskclient = Client(cluster)

        # sideload the daskclient to the new `daskhelpers` module
        daskhelpers.daskclient = daskclient
        logger.info('%s', daskclient)
        session_config['casaconfig']['logfile'] = casalogfile
        daskclient.run(session_startup, session_config['casaconfig'], loglevel)

    if args.dry_run:
        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return

    if args.session is not None:

        pipeconfig = session_config['pipeconfig']
        logger.info('pipeconfig args: %s', pipeconfig)

        if args.vlappr is not None:
            pipeconfig['vlappr'] = args.vlappr
        if args.ppr is not None:
            pipeconfig['ppr'] = args.ppr

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

    cli_interface()
