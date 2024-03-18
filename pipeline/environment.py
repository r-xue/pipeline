"""
environment.py defines functions and variables related to the execution environment.
"""
import multiprocessing
import os
import platform
import re
import resource
import subprocess
import sys

from importlib.metadata import version, PackageNotFoundError
from importlib.util import find_spec

import casatasks

from .infrastructure import mpihelpers
from .infrastructure.mpihelpers import MPIEnvironment
from .infrastructure import utils
from .infrastructure import casa_tools
from .infrastructure.version import get_version_string_from_git
from importlib.metadata import version, PackageNotFoundError

__all__ = ['casa_version', 'casa_version_string', 'compare_casa_version', 'cpu_type', 'hostname', 'host_distribution',
           'logical_cpu_cores', 'memory_size', 'pipeline_revision', 'role', 'cluster_details', 'dependency_details']
from .infrastructure import logging
LOG = logging.get_logger(__name__)

def _cpu_type():
    """
    Get a user-friendly string description of the host CPU.

    :return: CPU description
    """
    system = platform.system()
    if system == 'Linux':
        all_info = subprocess.check_output('cat /proc/cpuinfo', shell=True).strip().decode(sys.stdout.encoding)
        model_names = {line for line in all_info.split('\n') if line.startswith('model name')}
        if len(model_names) != 1:
            return 'N/A'
        # get the text after the colon
        token = ''.join(model_names.pop().split(':')[1:])
        # replace any multispaces with one space
        return re.sub(r'\s+', ' ', token.strip())
    elif system == 'Darwin':
        return subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string']).strip().decode(sys.stdout.encoding)
    else:
        raise NotImplemented('Could not get CPU type for system {!s}'.format(system))


def _logical_cpu_cores():
    """
    Get the number of logical (not physical) CPU cores in this machine.

    :return: number of cores
    """
    return multiprocessing.cpu_count()


def _host_distribution():
    """
    Get a description of the host operating system.

    :return: host OS description
    """
    system = platform.system()
    if system == 'Linux':
        return _linux_os_release()
    elif system == 'Darwin':
        return 'macOS {!s}'.format(platform.mac_ver()[0])
    else:
        raise NotImplemented('Could not get host OS for system {!s}'.format(system))


def _linux_os_release():
    """Get the Linux distribution name.

    Note: verified on CentOS/RHEL/Ubuntu/Fedora
    """
    try:
        os_release = {}
        with open('/etc/os-release') as f:
            for line in f:
                line_split = line.split('=')
                if len(line_split) == 2:
                    os_release[line_split[0].upper()] = line_split[1].strip().strip('"')
        linux_dist = '{NAME} {VERSION}'.format(**os_release)
    except Exception as e:
        linux_dist = 'Linux (unknown distribution)'

    return linux_dist


def _hostname():
    """
    Get the FQDN for this machine.

    :return: FQDN of this machine
    """
    return platform.node()


def _memory_size():
    """
    Get the amount of memory on this machine.

    :return: memory size, in bytes
    :rtype: int
    """
    try:
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    except ValueError:
        # SC_PHYS_PAGES doesn't always exist on OS X
        system = platform.system()
        if system == 'Darwin':
            return int(subprocess.check_output(['sysctl', '-n', 'hw.memsize']).strip())
        else:
            raise NotImplemented('Could not determine memory size for system {!s}'.format(system))


# Determine pipeline version from Git or package.
def _pipeline_revision() -> str:
    """
    Get a string describing the pipeline revision and branch of the executing
    pipeline distribution if executing from a Git repo; as a fall-back,
    attempt to get version from built package.
    :return: string describing pipeline version.
    """

    version_str = 'unknown'
    # check the version of installed package matching 'Pipeline'
    try:
        # PIPE-1669: try to retrieve version from PEP566 metadata:
        # https://peps.python.org/pep-0566/
        # However, be aware that the metadata version string might be out-of-date
        # from the package being imported depending on your workflow
        # https://packaging.python.org/en/latest/guides/single-sourcing-package-version/#single-sourcing-the-version
        # In addition, the version string could be converted form compliance reasons:
        # https://peps.python.org/pep-0440/#local-version-segments
        # '1.0.0-dev1+PIPE-1243' -> 1.0.0-dev1+PIPE.1243'.
        version_str = version('pipeline')
        LOG.debug('Pipeline version found from importlib.metadata: %s', version_str)
    except PackageNotFoundError:
        # likely the package is not installed
        LOG.debug('Pipeline version is not found from importlib.metadata; '
                 'the package is likely not pip-installed but added at runtime.')

    # more reliable method with the string most correctly preserved.
    try:
        from ._version import version as version_str
        LOG.debug('Pipeline version found from pipeline._version: %s', version_str)
    except ModuleNotFoundError:
        pass

    # We try to check version via the Git history again; this monkey patch is to deal with
    # the situation that the PEP566 metadata might be out of date, e.g. developers
    # are using a Python interpreter with older Pipeline versions installed but add
    # the latest Git repo to sys.path for testing at runtime. The '.git' pre-check is intended
    # to avoid the unnecessary cost of subprocess call cost. In addition, it avoids pulling
    # the Git-based version string if the imported Python code is coming from a scratch directory
    # inside the repo, etc. build/lib/pipeline
    git_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.git'))
    src_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    if os.path.exists(git_path):
        LOG.debug('A Git repo is found at: %s', git_path)
        LOG.debug('Checking the Git history from %s', src_path)
        try:
            # Silently test if this is a Git repo; if not, a CalledProcessError Exception
            # will be triggered due to a non-zero subprocess exit status.
            subprocess.check_output(['git', 'rev-parse'], cwd=src_path, stderr=subprocess.DEVNULL)
            # If it's a Git repo, continue with fetching the desired Git-derived package version string.
            version_str = get_version_string_from_git(src_path)
            LOG.debug('Pipeline version derived from Git at runtime: %s', version_str)
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass

    return version_str


def _ulimit():
    """
    Get the ulimit setting.

    The 'too many open files' error during imaging is often due to an
    incorrect ulimit. The ulimit value is recorded per host to assist in
    diagnosing these errors.

    See: PIPE-350; PIPE-338

    :return: ulimit as string
    """
    # get soft limit on number of open files
    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)

    return '{}'.format(soft_limit)


def _role():
    if not MPIEnvironment.is_mpi_enabled:
        return 'Non-MPI Host'

    if MPIEnvironment.is_mpi_client:
        return 'MPI Client'
    else:
        return 'MPI Server'


def _cluster_details():
    env_details = [node_details]
    if mpihelpers.is_mpi_ready():
        mpi_results = mpihelpers.mpiclient.push_command_request('pipeline.environment.node_details', block=True,
                                                                target_server=mpihelpers.mpi_server_list)
        for r in mpi_results:
            env_details.append(r['ret'])

    return env_details


casa_version = casatasks.version()
casa_version_string = casatasks.version_string()
compare_casa_version = casa_tools.utils.compare_version


def _get_dependency_details(package_list=None):
    """Get dependency package version/path.

    See https://docs.python.org/3.8/library/importlib.metadata.html#metadata
    """
    if package_list is None:
        package_list = ['numpy', 'scipy', 'matplotlib', 'astropy', 'bdsf',
                        'casatools', 'casatasks', 'almatasks', 'casadata',
                        'casampi', 'casaplotms']

    package_details = dict.fromkeys(package_list)
    for package in package_list:
        try:
            package_version = version(package)
            module_spec = find_spec(package)
            if module_spec is not None:
                package_details[package] = {'version': package_version, 'path': os.path.dirname(module_spec.origin)}
        except PackageNotFoundError:
            pass
    return package_details


cpu_type = _cpu_type()
hostname = _hostname()
host_distribution = _host_distribution()
iers_info = utils.IERSInfo()
logical_cpu_cores = _logical_cpu_cores()
memory_size = _memory_size()
role = _role()
pipeline_revision = _pipeline_revision()
ulimit = _ulimit()
dependency_details = _get_dependency_details()

node_details = {
    'cpu': cpu_type,
    'hostname': hostname,
    'os': host_distribution,
    'num_cores': logical_cpu_cores,
    'ram': memory_size,
    'role': role,
    'ulimit': ulimit
}
cluster_details = _cluster_details()
