import argparse
import textwrap
import yaml


def cli_interface():
    """Run a Pipeline data processing session from the console."""

    description = r"""
 The Pipeline CLI interface ("python -m pipeline" or "paris") creates customized Pipeline workflow sessions form a configuration YAML file.
 """

    parser = argparse.ArgumentParser(prog='python -m pipeline | paris', description=textwrap.dedent(description),
                                     formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--vlappr', type=str, dest='vlappr', help='execute a VLA ppr')
    parser.add_argument('--ppr', type=str, dest='ppr', help='execute a ALMA ppr')
    parser.add_argument('--config', type=str, dest='session', help='run a custom pipeline data processing session using .yaml')

    parser.add_argument('--logfile',
                        type=str, dest='logfile',
                        help="redirect stdout/stderr/casalog to the same log file")
    parser.add_argument('--dryrun',
                        dest="dry_run", action="store_true",
                        help="run a dry-run test; not actually trigger the requested workflow")
    parser.add_argument(
        '--local', dest="local", action="store_true",
        help="execute a session/workflow from the main process rather than in a subprocess or as a slurm job")
    args = parser.parse_args()

    session_config = {}
    if args.session is not None:
        with open(args.session) as f:
            session_config = yaml.safe_load(f)

    return args, session_config
