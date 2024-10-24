import argparse
import getpass
import os
import socket
import sys
import textwrap

# unbuffered stdout/stderr/stdin
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)
sys.stdin = os.fdopen(sys.stdin.fileno(), 'r', buffering=1)


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
    parser.add_argument('--reduce', type=str, dest='reduce', help='run the recipereducer over a dataset')

    parser.add_argument(
        '--dask-config', type=str, dest='dask_config',
        help='start a Dask cluster for dask-assisted tier0 parallelization specified by a configuration file')
    parser.add_argument('--dryrun',
                        dest="dry_run", action="store_true",
                        help="run a dry-run test; not really trigger the requested workflow")
    parser.add_argument('--logfile',
                        type=str, dest='logfile',
                        help="redirect stdout/stderr/casalog to the same log file")
    args = parser.parse_args()

    from pyvirtualdisplay import Display
    disp = Display(visible=0, size=(2048, 2048))
    disp.start()
    import yaml
    if args.reduce is not None:
        with open(args.reduce) as f:
            reduce_config = yaml.safe_load(f)
            loglevel = reduce_config.get('loglevel', None)

    session_config(logfile=args.logfile, loglevel=loglevel)

    import casatasks
    import casatools
    import dask
    import yaml
    from dask.distributed import Client, LocalCluster

    import pipeline
    import pipeline.infrastructure.executeppr as eppr
    import pipeline.infrastructure.executeppr as vlaeppr
    import pipeline.infrastructure.logging as logging
    import pipeline.recipereducer
    from pipeline.infrastructure import daskhelpers

    logger = pipeline.infrastructure.get_logger('main')

    # pcasa_logger()
    username = getpass.getuser()
    hostname = socket.gethostname()
    casatools_version = casatools.version_string()
    logger.info('-'*80)
    logger.info('hostname:           {}'.format(hostname))
    logger.info('user:               {}'.format(username))
    logger.info('casatools_versuon:  {}'.format(casatools_version))
    logger.info('-'*80)

    logger.info('session logfile %s', casatasks.casalog.logfile())

    if __name__ == "__main__":

        if args.dask_config is not None:

            # Load custom configuration file

            with open(args.dask_config) as f:
                defaults = yaml.safe_load(f)
            dask.config.update_defaults(defaults)
            # dask.config.set(scheduler="synchronous")

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
            daskclient.run(session_config, casatasks.casalog.logfile(), loglevel)

    if args.ppr is not None:
        logger.info('executeppr: %s', args.ppr)
        if not args.dry_run:
            eppr.executeppr(args.ppr, importonly=False)
        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return

    if args.vlappr is not None:
        logger.info('executeppr: %s', args.vlappr)
        if not args.dry_run:
            vlaeppr.executeppr(args.vlappr, importonly=False)
        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return

    if args.reduce is not None:
        with open(args.reduce) as f:
            reduce_config = yaml.safe_load(f)
        logger.info('recipereducer args: %s', reduce_config)
        if not args.dry_run:
            if reduce_config.get('loglevel', None) is not None:
                loglevel = reduce_config.get('loglevel')
                casaloglevel = logging.CASALogHandler.get_casa_priority(logging.LOGGING_LEVELS[loglevel])
                casatasks.casalog.filter(casaloglevel)
            pipeline.recipereducer.reduce(
                vis=reduce_config['vis'],
                procedure=reduce_config['procedure'],
                loglevel=reduce_config.get('loglevel', None))

        if daskhelpers.daskclient is not None:
            daskhelpers.daskclient.close()
        return


def session_config(logfile=None, loglevel=None):

    # need to update the casaconfig attribute before importing the casatasks module
    from casaconfig import config
    if logfile is not None:
 config.logfile = logfile

    import casatasks

    if logfile is not None:
        casatasks.casalog.setlogfile(logfile)

    import pipeline.infrastructure.logging as logging
    if loglevel is not None:
        casaloglevel = logging.CASALogHandler.get_casa_priority(logging.LOGGING_LEVELS[loglevel])
        casatasks.casalog.filter(casaloglevel)


if __name__ == "__main__":

    cli_interface()
