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
import sys

import casatasks
import casaplotms

# PIPE-2099: add the compatibility with the 'wvrgcal' task change from CAS-14218
if hasattr(casatasks, 'wvrgcal'):
    # wvrgcal was migrated into the casatasks package via CAS-14218
    almatasks = casatasks
else:
    # before CAS-14218, the task wvrgcal was under the almatasks package
    import almatasks

from pipeline import infrastructure
from pipeline.infrastructure import jobrequest

LOG = infrastructure.logging.get_logger(__name__)

__all__ = []


def register_task(func):
    """Register a JobRequest generator function."""
    __all__.append(func.__name__)
    return func


@register_task
def applycal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.applycal, *v, **k)


@register_task
def bandpass(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.bandpass, *v, **k)


@register_task
def calstat(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.calstat, *v, **k)


@register_task
def clearcal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.clearcal, *v, **k)


@register_task
def concat(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.concat, *v, **k)


@register_task
def delmod(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.delmod, *v, **k)


@register_task
def exportfits(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.exportfits, *v, **k)


@register_task
def gaincal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.gaincal, *v, **k)


@register_task
def getantposalma(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.getantposalma, *v, **k)


@register_task
def flagcmd(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.flagcmd, *v, **k)


@register_task
def flagdata(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.flagdata, *v, **k)


@register_task
def flagmanager(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.flagmanager, *v, **k)


@register_task
def fluxscale(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.fluxscale, *v, **k)


@register_task
def gencal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.gencal, *v, **k)


@register_task
def hanningsmooth(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.hanningsmooth, *v, **k)


@register_task
def imdev(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imdev, *v, **k)


@register_task
def imval(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imval, *v, **k)


@register_task
def imfit(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imfit, *v, **k)


@register_task
def imhead(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imhead, *v, **k)


@register_task
def immath(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.immath, *v, **k)


@register_task
def immoments(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.immoments, *v, **k)


@register_task
def imregrid(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imregrid, *v, **k)


@register_task
def imsmooth(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imsmooth, *v, **k)


@register_task
def impbcor(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.impbcor, *v, **k)


@register_task
def importasdm(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.importasdm, *v, **k)


@register_task
def imstat(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imstat, *v, **k)


@register_task
def imsubimage(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.imsubimage, *v, **k)


@register_task
def initweights(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.initweights, *v, **k)


@register_task
def listobs(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.listobs, *v, **k)


@register_task
def mstransform(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.mstransform, *v, **k)


@register_task
def partition(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.partition, *v, **k)


@register_task
def plotants(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.plotants, *v, **k)


@register_task
def plotbandpass(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.plotbandpass, *v, **k)


@register_task
def plotms(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casaplotms.plotms, *v, **k)


@register_task
def plotweather(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.plotweather, *v, **k)


@register_task
def polcal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.polcal, *v, **k)


@register_task
def polfromgain(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.polfromgain, *v, **k)


@register_task
def setjy(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.setjy, *v, **k)


@register_task
def split(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.split, *v, **k)


@register_task
def statwt(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.statwt, *v, **k)


@register_task
def tclean(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.tclean, *v, **k)


@register_task
def uvcontsub(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.uvcontsub, *v, **k)


@register_task
def wvrgcal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(almatasks.wvrgcal, *v, **k)


@register_task
def visstat(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.visstat, *v, **k)


@register_task
def rerefant(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.rerefant, *v, **k)


@register_task
def sdatmcor(*v, **k) -> jobrequest.JobRequest:
    """Wrap casatasks.sdatmcor

    Returns:
        JobRequest instance
    """
    return jobrequest.JobRequest(casatasks.sdatmcor, *v, **k)


@register_task
def sdbaseline(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.sdbaseline, *v, **k)


@register_task
def sdcal(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.sdcal, *v, **k)


@register_task
def tsdimaging(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(casatasks.tsdimaging, *v, **k)


@register_task
def copyfile(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(shutil.copyfile, *v, **k)


@register_task
def copytree(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(shutil.copytree, *v, **k)


@register_task
def rmtree(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(shutil.rmtree, *v, **k)


@register_task
def move(*v, **k) -> jobrequest.JobRequest:
    return jobrequest.JobRequest(shutil.move, *v, **k)


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
