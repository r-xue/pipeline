import copy
import inspect
import logging
import os

import almatasks
import casaplotms
import casatasks
import casatools
import casaviewer
from pipeline.infrastructure import casa_tasks, casa_tools

from .casa_tools import create_logging_class
from .jobrequest import JobRequest
from .logging import get_logger


def logging_casatasktool(obj, jobrequest_executor=None):
    """Get the equivalents of casatasks/casatools and Python logger in the Pipeline logging framework.

    For casatools classes/instances, the function will return their equivalents base on logging 
    classes (see infrastructure.casa_tools).
    For casatasks instances, a callable wrapper function around the JobRequest will be returned, with
    the Pipeline logging capability.
    For a Python logger instance, the function will create a python logger under the same name via infrastreucture.get_logger()
    """

    for package in (almatasks, casatasks, casaplotms, casaviewer):
        for name in package.__all__:
            if name.startswith('version'):
                # this is a workaround for CAS-13929
                continue
            if obj == getattr(package, name):
                if jobrequest_executor is None:
                    def executable(*args, **kwargs):
                        job_request = JobRequest(obj, *args, **kwargs)
                        return job_request.execute(dry_run=False)
                    return executable
                else:
                    def executable(*args, **kwargs):
                        job_request = JobRequest(obj, *args, **kwargs)
                        return jobrequest_executor.execute(job_request)
                    return executable

    for name in casatools.__all__:
        if name.startswith('version'):
            continue
        attr = getattr(casatools, name)
        if inspect.isclass(attr):
            if obj == attr:
                return create_logging_class(attr)
            if isinstance(obj, attr):
                return create_logging_class(attr)()

    if isinstance(obj, logging.Logger):
        if not hasattr(obj, 'attention'):
            obj.handlers.clear()
            obj = get_logger(obj.name)

    return obj


def logging_extern(module, jobrequest_executor=None):
    ret_module = copy.deepcopy(module)

    for k, v in vars(module).items():
        y = logging_casatasktool(v, jobrequest_executor=jobrequest_executor)
        if y != v:
            setattr(module, k, y)
        if inspect.ismodule(v):
            for k1, v1 in vars(v).items():
                y1 = logging_casatasktool(v1, jobrequest_executor=jobrequest_executor)
                if y1 != v1:
                    setattr(v, k1, y1)

    return ret_module


def execute_jobrequest(jobrequest, executor=None):
    if executor is None:
        ret = jobrequest.execute(dry_run=False)
    else:
        ret = executor.execute(jobrequest)
    return ret


def ms_transform(*args, link_pointing_table=True, executor=None, **kwargs):
    """A wrapper function of casatasks.mstransform.

    Compared with the standard mstransform(), this function provides several advantages:

        1. Exclude the pointing table when performing mstransform() and minimize unnecessary I/O operations.
        2. Optionally the input MS pointing table can be *hard*linked into the output MS, therefore,
            * The pointing table can still be used when calling tclean(usepointing=True) on the output MS
            * The table content doesn't take additional storage space and the pointing table is not copied during the call duration.

    To-Do:
        * parallelize multiple mstransform() calls on the same MS using the tier-0 parallelization.
            the parallelization should be wrapped in the pointingtable backup/restore actions.

    test examples:

    !rm -rf test.ms
    !cp -rf eb1_targets.ms test.ms
    !rm -rf test_small.ms
    from pipeline.hif.heuristics.selfcal_utils import ms_transform
    ms_transform(vis='test.ms',outputvis='test_small.ms',scan='16',datacolumn='data',hardlink_pointing_table=True)
    !ls -li test.ms/POINTING # veritfy inode
    !ls -li test_small.ms/POINTING   # veritfy inode

    !rm -rf test.ms
    !cp -rf eb1_targets.ms test.ms
    !rm -rf test_small.ms
    from pipeline.hif.heuristics.selfcal_utils import ms_transform
    ms_transform(vis='test.ms',outputvis='test_small.ms',scan='16',datacolumn='data',hardlink_pointing_table=False)
    !ls -lrtd test.ms/POINTING/*
    !ls -lrtd test_small.ms/POINTING/*



    """

    if 'vis' not in kwargs:
        vis = args[0]
    else:
        vis = kwargs['vis']
    outputvis = kwargs['outputvis']

    if not os.path.isdir(vis+'/POINTING_ORIGIN'):
        job = casa_tasks.move(vis+'/POINTING', vis+'/POINTING_ORIGIN')
        execute_jobrequest(job, executor=executor)
    with casa_tools.TableReader(vis+'/POINTING_ORIGIN', nomodify=True) as table:
        tabdesc = table.getdesc()
        dminfo = table.getdminfo()
    if os.path.isdir(vis+'/POINTING'):
        job = casa_tasks.rmtree(vis+'/POINTING')
        execute_jobrequest(job, executor=executor)
    tb = casa_tools._logging_table_cls()
    tb.create(vis+'/POINTING', tabdesc, dminfo=dminfo)
    tb.close

    ret = None

    try:
        job = casa_tasks.mstransform(*args, **kwargs)
        ret = execute_jobrequest(job, executor=executor)
        if link_pointing_table:
            job = casa_tasks.rmtree(outputvis+'/POINTING')
            execute_jobrequest(job, executor=executor)
            job = casa_tasks.copytree(vis+'/POINTING_ORIGIN', outputvis+'/POINTING', copy_function=os.link)
            execute_jobrequest(job, executor=executor)
    finally:
        # ensure the input vis integrity if expection is raised in mstransform call.
        if os.path.isdir(vis+'/POINTING'):
            job = casa_tasks.rmtree(vis+'/POINTING')
            execute_jobrequest(job, executor=executor)
        if os.path.isdir(vis+'/POINTING_ORIGIN'):
            job = casa_tasks.move(vis+'/POINTING_ORIGIN', vis+'/POINTING')
            execute_jobrequest(job, executor=executor)

    return ret
