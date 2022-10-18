import inspect
import logging

import almatasks
import casaplotms
import casatasks
import casatools
import casaviewer
import copy

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
    ret_module=copy.deepcopy(module)

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
