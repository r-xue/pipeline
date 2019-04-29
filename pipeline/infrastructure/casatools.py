from __future__ import absolute_import

import contextlib
import copy_reg
import datetime
import inspect
import operator
import os
import platform
import sys

from casac import casac
from taskinit import casalog

from . import logging

# standard logger that emits messages to stdout and CASA logger
LOG = logging.get_logger(__name__)

# logger for keeping a trace of CASA task and CASA tool calls.
# The filename incorporates the hostname to keep MPI client files distinct
CASACALLS_LOG = logging.get_logger('CASACALLS', stream=None, format='%(message)s', addToCasaLog=False,
                                   filename='casacalls-{!s}.txt'.format(platform.node().split('.')[0]))


def log_call(fn, level):
    """
    Decorate a function or method so that all invocations of that function or
    method are logged.

    :param fn: function to decorate
    :param level: log level (e.g., logging.INFO, logging.WARNING, etc.)
    :return: decorated function
    """
    def f(*args, **kwargs):
        # remove any keyword arguments that have a value of None or an empty
        # string, letting CASA use the default value for that argument
        kwargs = {k: v for k, v in kwargs.iteritems() if v not in (None, '')}

        # get the argument names and default argument values for the given
        # function
        code = fn.func_code
        argcount = code.co_argcount
        argnames = code.co_varnames[:argcount]

        positional = {k: v for k, v in zip(argnames, args)}

        def format_arg_value(arg_val):
            arg, val = arg_val
            return '%s=%r' % (arg, val)

        nameless = list(map(repr, args[argcount:]))
        positional = list(map(format_arg_value, positional.iteritems()))
        keyword = list(map(format_arg_value, kwargs.iteritems()))

        # don't want self in message as it is an object memory reference
        msg_args = [v for v in positional + nameless + keyword if not v.startswith('self=')]

        tool_call = '{!s}.{!s}({!s})'.format(fn.im_class.__name__, fn.__name__, ', '.join(msg_args))
        CASACALLS_LOG.log(level, tool_call)

        start_time = datetime.datetime.utcnow()
        try:
            return fn(*args, **kwargs)
        finally:
            end_time = datetime.datetime.utcnow()
            elapsed = end_time - start_time
            LOG.log(level, '{} CASA tool call took {}s'.format(tool_call, elapsed.total_seconds()))

    return f


def create_logging_class(cls, level=logging.TRACE, methods=None):
    """
    Return a class with all methods decorated to log method calls.

    :param cls: class to wrap
    :param level: log level for emitted messages
    :param methods: methods to log calls for, or None to log all methods
    :return: the decorated class
    """
    bound_methods = {name: method for (name, method) in inspect.getmembers(cls, inspect.ismethod)
                     if not name.startswith('__') and not name.endswith('__')}

    if methods:
        bound_methods = {name: method for name, method in bound_methods.iteritems() if name in methods}

    logging_override_methods = {name: log_call(method, level)
                                for name, method in bound_methods.iteritems()}

    cls_name = 'Logging{!s}'.format(cls.__name__.capitalize())
    new_cls = type(cls_name, (cls,), logging_override_methods)

    return new_cls


# wrappers around CASA tool classes that create CASA tools where method calls
# are logged with log level Y.
# The assigned log level for tools should be DEBUG or lower, otherwise the log
# file is created and written to even on non-debug pipeline runs, where
# loglevel=INFO. The default log level is TRACE.
#
# Example:
# _logging_imager_cls = create_logging_class(casac.imager, logging.DEBUG)
_logging_imager_cls = create_logging_class(casac.imager,
                                           level=logging.INFO, methods=('selectvis', 'apparentsens', 'advise'))
_logging_measures_cls = create_logging_class(casac.measures)
_logging_quanta_cls = create_logging_class(casac.quanta)
_logging_table_cls = create_logging_class(casac.table)
_logging_ms_cls = create_logging_class(casac.ms)
_logging_tableplot_cls = create_logging_class(casac.table)
_logging_calibrater_cls = create_logging_class(casac.calibrater)
_logging_calanalysis_cls = create_logging_class(casac.calanalysis)
_logging_msplot_cls = create_logging_class(casac.msplot)
_logging_calplot_cls = create_logging_class(casac.calplot)
_logging_agentflagger_cls = create_logging_class(casac.agentflagger)
_logging_image_cls = create_logging_class(casac.image)
_logging_imagepol_cls = create_logging_class(casac.imagepol)
_logging_simulator_cls = create_logging_class(casac.simulator)
_logging_componentlist_cls = create_logging_class(casac.componentlist)
_logging_coordsys_cls = create_logging_class(casac.coordsys)
_logging_regionmanager_cls = create_logging_class(casac.regionmanager)
_logging_spectralline_cls = create_logging_class(casac.spectralline)
_logging_utils_cls = create_logging_class(casac.utils)
_logging_deconvolver_cls = create_logging_class(casac.deconvolver)
_logging_vpmanager_cls = create_logging_class(casac.vpmanager)
_logging_vlafillertask_cls = create_logging_class(casac.vlafillertask)
_logging_atmosphere_cls = create_logging_class(casac.atmosphere)
_logging_msmd_cls = create_logging_class(casac.msmetadata)

imager = _logging_imager_cls()
measures = _logging_measures_cls()
quanta = _logging_quanta_cls()
table = _logging_table_cls()
ms = _logging_ms_cls()
tableplot = _logging_table_cls()
calibrater = _logging_calibrater_cls()
calanalysis = _logging_calanalysis_cls()
msplot = _logging_msplot_cls()
calplot = _logging_calplot_cls()
agentflagger = _logging_agentflagger_cls()
image = _logging_image_cls()
imagepol = _logging_imagepol_cls()
simulator = _logging_simulator_cls()
componentlist = _logging_componentlist_cls()
coordsys = _logging_coordsys_cls()
regionmanager = _logging_regionmanager_cls()
spectralline = _logging_spectralline_cls()
utils = _logging_utils_cls()
deconvolver = _logging_deconvolver_cls()
vpmanager = _logging_vpmanager_cls()
vlafillertask = _logging_vlafillertask_cls()
atmosphere = _logging_atmosphere_cls()
msmd = _logging_msmd_cls()

log = casalog


def post_to_log(comment='', echo_to_screen=True):
    log.post(comment)
    if echo_to_screen:
        sys.stdout.write('{0}\n'.format(comment))


def set_log_origin(fromwhere=''):
    log.origin(fromwhere)


def context_manager_factory(tool_cls, finalisers=None):
    """
    Create a context manager function that wraps the given CASA tool.

    The returned context manager function takes one argument: a filename. The
    function opens the file using the CASA tool, returning the tool so that it
    may be used for queries or other operations pertaining to the tool. The
    tool is closed once it falls out of scope or an exception is raised.
    """
    tool_name = tool_cls.__name__

    if finalisers is None:
        finalisers = ('close', 'done')

    @contextlib.contextmanager
    def f(filename, **kwargs):
        if not os.path.exists(filename):
            raise IOError('No such file or directory: {!r}'.format(filename))
        LOG.trace('%s tool: opening %r', tool_name, filename)
        tool_instance = tool_cls()
        tool_instance.open(filename, **kwargs)
        try:
            yield tool_instance
        finally:
            LOG.trace('{!s} tool: closing {!r}'.format(tool_name, filename))
            for method_name in finalisers:
                if hasattr(tool_instance, method_name):
                    m = operator.methodcaller(method_name)
                    m(tool_instance)
    return f


def selectvis_context_manager(tool_cls):
    """
    Create an imager tool context manager function that opens the MS using
    im.selectvis in read-only mode.

    The returned context manager function takes one argument: a filename. The
    function opens the file using the CASA imager tool, returning the tool so that it
    may be used for queries or other operations pertaining to the tool. The
    tool is closed once it falls out of scope or an exception is raised.
    """
    tool_name = tool_cls.__name__

    @contextlib.contextmanager
    def f(filename, **kwargs):
        if not os.path.exists(filename):
            raise IOError('No such file or directory: {!r}'.format(filename))
        LOG.trace('{!s} tool: opening {!r} using .selectvis(writeaccess=False)'.format(tool_name, filename))
        tool_instance = tool_cls()
        rtn = tool_instance.selectvis(filename, writeaccess=False, **kwargs)
        if rtn is False:
            raise Exception('selectvis did not return any data')
        try:
            yield tool_instance
        finally:
            LOG.trace('{!s} tool: closing {!r}'.format(tool_name, filename))
            if hasattr(tool_instance, 'close'):
                tool_instance.close()
            if hasattr(tool_instance, 'done'):
                tool_instance.done()

    return f


# context managers for frequently used CASA tools
CalAnalysis = context_manager_factory(_logging_calanalysis_cls)
ImageReader = context_manager_factory(_logging_image_cls)
ImagerReader = context_manager_factory(_logging_imager_cls)
MSReader = context_manager_factory(_logging_ms_cls, finalisers=['done'])
TableReader = context_manager_factory(_logging_table_cls, finalisers=['done'])
MSMDReader = context_manager_factory(_logging_msmd_cls)
SelectvisReader = selectvis_context_manager(_logging_imager_cls)
AgentFlagger = context_manager_factory(_logging_agentflagger_cls)

# C extensions cannot be pickled, so ignore the CASA logger on pickle and
# replace with it with the current CASA logger on unpickle
__tools = ['imager', 'measures', 'quanta', 'table', 'ms', 'tableplot', 
           'calibrater', 'msplot', 'calplot', 'agentflagger',
           'image', 'imagepol', 'simulator', 'componentlist', 'coordsys',
           'regionmanager', 'spectralline', 'utils', 'deconvolver',
           'vpmanager', 'vlafillertask', 'atmosphere', 'log', 'utils']

for tool in __tools:
    tool_type = type(globals()[tool])
    unpickler = lambda data: globals()[tool]
    pickler = lambda _: (unpickler, (tool, ))
    copy_reg.pickle(tool_type, pickler, unpickler)
