"""
This module contains a wrapper function for every CASA task. The signature of
each methods exactly matches that of the CASA task it mirrors. However, rather
than executing the task directly when these methods are called,
CASATaskJobGenerator returns a JobRequest for every invocation; these jobs
then be examined and executed at a later date.

The CASA task implementations are located at run-time and proxies for each
task attached to this class at runtime. The name and signature of each
method will match those of the tasks in the CASA environment when this
module was imported.
"""
import functools
import shutil
import subprocess
import sys
from inspect import signature

import casaplotms
import casatasks

from . import logging
from .jobrequest import JobRequest

LOG = logging.get_logger(__name__)

__all__ = []


def register_task(func):
    """Register a JobRequest generator function."""
    __all__.append(func.__name__)
    return func


@register_task
def applycal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.applycal, *v, **k)


@register_task
def bandpass(*v, **k) -> JobRequest:
    return JobRequest(casatasks.bandpass, *v, **k)


@register_task
def calstat(*v, **k) -> JobRequest:
    return JobRequest(casatasks.calstat, *v, **k)


@register_task
def clearcal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.clearcal, *v, **k)


@register_task
def concat(*v, **k) -> JobRequest:
    return JobRequest(casatasks.concat, *v, **k)


@register_task
def delmod(*v, **k) -> JobRequest:
    return JobRequest(casatasks.delmod, *v, **k)


@register_task
def exportfits(*v, **k) -> JobRequest:
    return JobRequest(casatasks.exportfits, *v, **k)


@register_task
def gaincal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.gaincal, *v, **k)


@register_task
def flagcmd(*v, **k) -> JobRequest:
    return JobRequest(casatasks.flagcmd, *v, **k)


@register_task
def flagdata(*v, **k) -> JobRequest:
    return JobRequest(casatasks.flagdata, *v, **k)


@register_task
def flagmanager(*v, **k) -> JobRequest:
    return JobRequest(casatasks.flagmanager, *v, **k)


@register_task
def fluxscale(*v, **k) -> JobRequest:
    return JobRequest(casatasks.fluxscale, *v, **k)


@register_task
def gencal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.gencal, *v, **k)


@register_task
def hanningsmooth(*v, **k) -> JobRequest:
    return JobRequest(casatasks.hanningsmooth, *v, **k)


@register_task
def imdev(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imdev, *v, **k)


@register_task
def imval(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imval, *v, **k)


@register_task
def imfit(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imfit, *v, **k)


@register_task
def imhead(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imhead, *v, **k)


@register_task
def immath(*v, **k) -> JobRequest:
    return JobRequest(casatasks.immath, *v, **k)


@register_task
def immoments(*v, **k) -> JobRequest:
    return JobRequest(casatasks.immoments, *v, **k)


@register_task
def imregrid(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imregrid, *v, **k)


@register_task
def imsmooth(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imsmooth, *v, **k)


@register_task
def impbcor(*v, **k) -> JobRequest:
    return JobRequest(casatasks.impbcor, *v, **k)


@register_task
def importasdm(*v, **k) -> JobRequest:
    return JobRequest(casatasks.importasdm, *v, **k)


@register_task
def imstat(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imstat, *v, **k)


@register_task
def imsubimage(*v, **k) -> JobRequest:
    return JobRequest(casatasks.imsubimage, *v, **k)


@register_task
def initweights(*v, **k) -> JobRequest:
    return JobRequest(casatasks.initweights, *v, **k)


@register_task
def listobs(*v, **k) -> JobRequest:
    return JobRequest(casatasks.listobs, *v, **k)


@register_task
def mstransform(*v, **k) -> JobRequest:
    return JobRequest(casatasks.mstransform, *v, **k)


@register_task
def partition(*v, **k) -> JobRequest:
    return JobRequest(casatasks.partition, *v, **k)


@register_task
def plotants(*v, **k) -> JobRequest:
    return JobRequest(casatasks.plotants, *v, **k)


@register_task
def plotbandpass(*v, **k) -> JobRequest:
    return JobRequest(casatasks.plotbandpass, *v, **k)


def xvfb_plotms(self, *args, **kwargs):
    cmd = []
    xvfb_run = shutil.which('xvfb-run')
    python = sys.executable
    cmd = [xvfb_run, '-a', python, '-c']
    all_args = [f"{value!r}" for value in args] + [f"{name}={value!r}" for name, value in kwargs.items()]
    py_cmds = ['from casaplotms import plotms', 'plotms({})'.format(', '.join(all_args))]
    py_cmd = '; '.join(py_cmds)
    cmd.append(f'"{py_cmd}"')
    cmd = ' '.join(cmd)
    try:
        LOG.debug(f'subprocess-cmd: {cmd}')
        ret = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode().strip()
        LOG.debug(f'subprocess-ret: {ret}')
    except (FileNotFoundError, subprocess.CalledProcessError) as ex:
        ret = "error"
    return ret

# copy the casa/plotms callable attribute to the wrapper function.
xvfb_plotms.__name__ = casaplotms.plotms.__call__.__name__
xvfb_plotms.__doc__ = casaplotms.plotms.__call__.__doc__
xvfb_plotms.__wrapped__ = casaplotms.plotms.__call__
xvfb_plotms.__signature__ = signature(casaplotms.plotms.__call__)
xvfb_plotms.__defaults__ = casaplotms.plotms.__call__.__defaults__
xvfb_plotms.__kwdefaults__ = casaplotms.plotms.__call__.__kwdefaults__


@register_task
def plotms(*v, **k) -> JobRequest:
    xvfb_subprocess = True
    if xvfb_subprocess:
        return JobRequest(xvfb_plotms, *v, **k)
    else:
        return JobRequest(casaplotms.plotms, *v, **k)


@register_task
def plotweather(*v, **k) -> JobRequest:
    return JobRequest(casatasks.plotweather, *v, **k)


@register_task
def polcal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.polcal, *v, **k)


@register_task
def polfromgain(*v, **k) -> JobRequest:
    return JobRequest(casatasks.polfromgain, *v, **k)


@register_task
def setjy(*v, **k) -> JobRequest:
    return JobRequest(casatasks.setjy, *v, **k)


@register_task
def split(*v, **k) -> JobRequest:
    return JobRequest(casatasks.split, *v, **k)


@register_task
def statwt(*v, **k) -> JobRequest:
    return JobRequest(casatasks.statwt, *v, **k)


@register_task
def tclean(*v, **k) -> JobRequest:
    return JobRequest(casatasks.tclean, *v, **k)


@register_task
def uvcontsub(*v, **k) -> JobRequest:
    return JobRequest(casatasks.uvcontsub, *v, **k)


@register_task
def wvrgcal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.wvrgcal, *v, **k)


@register_task
def visstat(*v, **k) -> JobRequest:
    return JobRequest(casatasks.visstat, *v, **k)


@register_task
def sdatmcor(*v, **k) -> JobRequest:
    """Wrap casatasks.sdatmcor

    Returns:
        JobRequest instance
    """
    return JobRequest(casatasks.sdatmcor, *v, **k)


@register_task
def sdbaseline(*v, **k) -> JobRequest:
    return JobRequest(casatasks.sdbaseline, *v, **k)


@register_task
def sdcal(*v, **k) -> JobRequest:
    return JobRequest(casatasks.sdcal, *v, **k)


@register_task
def sdimaging(*v, **k) -> JobRequest:
    return JobRequest(casatasks.sdimaging, *v, **k)


@register_task
def tsdimaging(*v, **k) -> JobRequest:
    return JobRequest(casatasks.tsdimaging, *v, **k)


@register_task
def copyfile(*v, **k) -> JobRequest:
    return JobRequest(shutil.copyfile, *v, **k)


@register_task
def copytree(*v, **k) -> JobRequest:
    return JobRequest(shutil.copytree, *v, **k)


@register_task
def rmtree(*v, **k) -> JobRequest:
    return JobRequest(shutil.rmtree, *v, **k)


@register_task
def move(*v, **k) -> JobRequest:
    return JobRequest(shutil.move, *v, **k)


class CasaTasks:
    """A class to represent a collection of JobRequest-wrapped callables from CASA or Python modules.

    CasaTasks wraps frequently-used CASA tasks and Python functions into individual class methods that
    can create and execute JobRequests on-the-fly. Then their calls will be properly
    logged and recorded by the Pipeline logging framework:
        * casacalls-*.txt records all JobRequest executions.
        * casa_commands.log records JobRequest executed by the PipelineTask Executor instances.

    The "immediate-execution" interface offered by this class could help adapt extern Python
    scripts/module into Pipeline with minimal changes.

    Example 1):

        from casatasks import tclean

            can be replaced by:

        from pipeline.infrastructure.casa_tasks import casa_tasks
        tclean=casa_tasks.tclean

    Example 2):

        from pipeline.infrastructure.casa_tasks import CasaTasks
        ct = CasaTasks()
        ct.listobs(vis='my.ms')
        help(ct.listobs) # this behaves like casatasks.listobs
    """

    def __init__(self, executor=None):
        self._tasklist = __all__
        self._executor = executor
        for _fn in self._tasklist:
            setattr(self, _fn, self._logged_fn(getattr(sys.modules[__name__], _fn)))

    def _logged_fn(self, fn):
        """Get the wrapper function that can immediately create and execute JobRequest of a callable."""
        if self._executor is None:
            # Executions will be logged in casacalls-.txt
            @functools.wraps(fn)
            def func(*args, **kwargs):
                return fn(*args, **kwargs).execute()
            return func
        else:
            # Executions will be logged in both casacalls-.txt and casa-command.txt
            @functools.wraps(fn)
            def func(*args, **kwargs):
                return self._executor.execute(fn(*args, **kwargs))
            return func


# add an instance of executor-free CasaTasks under the module namespace
casa_tasks = CasaTasks()
