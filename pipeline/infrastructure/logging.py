import copy
import logging
import sys
import time
import types
import functools
from contextlib import contextmanager
from typing import Optional, Union, List

from casatasks import casalog

import logutils
import logutils.colorize as colorize

from logging import CRITICAL, WARNING, ERROR, INFO, DEBUG

# Register three new logging levels with the standard logger
TRACE = 5
logging.addLevelName(TRACE, 'TRACE')
colorize.ColorizingStreamHandler.level_map[TRACE] = (None, 'blue', False)

TODO = 13
logging.addLevelName(TODO, 'TODO')
colorize.ColorizingStreamHandler.level_map[TODO] = ('black', 'yellow', True)

ATTENTION = 25
logging.addLevelName(ATTENTION, 'ATTENTION')
colorize.ColorizingStreamHandler.level_map[ATTENTION] = ('white', 'blue', False)

# PIPE-1699: this is to replicate the modification from d86115b to the logutils 
# source code originally saved in pipeline/extern/logutils.
colorize.ColorizingStreamHandler.level_map[INFO] = (None, None, False)

LOGGING_LEVELS = {'critical'  : CRITICAL,
                  'error'     : ERROR,
                  'warning'   : WARNING,
                  'attention' : ATTENTION,
                  'info'      : INFO,
                  'debug'     : DEBUG,
                  'todo'      : TODO,
                  'trace'     : TRACE}

# Begin with a default log level of NOTSET. All loggers created at module level
# import time will use this logging level.
logging_level = logging.NOTSET

# initialise the root logger so that module-level messages can be logged.
# Change this to see lower level messages from module-level statements at import
# time
logging.getLogger().setLevel(INFO)
_loggers = []


def pipeline_origin(method):
    """Use 'pipeline' as the CASAlog Origin by default."""
    @functools.wraps(method)
    def pipeline_as_origin(self, *args, **kwargs):
        if casalog.getOrigin() != 'pipeline':
            casalog.origin('pipeline')
        retval = method(self, *args, **kwargs)
        return retval
    return pipeline_as_origin


class CASALogHandler(logging.Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    to the CASA log.
    """

    def __init__(self, log=None):
        """
        Initialize the handler.

        If log is not specified, the current CASA global logger is used.
        """
        logging.Handler.__init__(self)
        if log is None:
            log = casalog
        self._log = log
        self.setFormatter(logging.Formatter('%(message)s', ''))

    def flush(self):
        """
        Flushes the stream.
        """
        pass

    @pipeline_origin
    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record. The
        record is then written to the stream with a trailing newline. If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.
        """
        try:
            msg = self.format(record)
            priority = CASALogHandler.get_casa_priority(record.levelno)
            origin = record.name
            log = self._log
            log.post(msg, priority, origin)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    @staticmethod
    def get_casa_priority(lvl):
        if lvl >= ERROR:
            return 'ERROR'
        if lvl >= WARNING:
            return 'WARN'
        if lvl >= ATTENTION:
            return 'INFO1'
        if lvl >= INFO:
            return 'INFO'
        if lvl >= DEBUG:
            return 'DEBUG1'
        if lvl >= TRACE:
            return 'DEBUG2'
        return 'INFO'


# This format is more useful when debugging as it gives more details on who
# emitted the message. Try it!
#               format='%(asctime)s %(name)s %(funcName)s\n%(levelname)s: %(message)s',

def get_logger(name,
               format='%(asctime)s %(levelname)s: %(message)s',
               datefmt='%Y-%m-%d %H:%M:%S',
               stream=sys.stdout, level=None,
               filename=None, filemode='w', filelevel=None,
               propagate=False,
               addToCasaLog=True):
    """Do basic configuration for the logging system. Similar to
    logging.basicConfig but the logger ``name`` is configurable and both a
    file output and a stream output can be created. Returns a logger object.

    The default behaviour is to create a StreamHandler which writes to
    sys.stdout, set a formatter using the "%(message)s" format string, and add
    the handler to the ``name`` logger.

    A number of optional keyword arguments may be specified, which can alter
    the default behaviour.

    :param name: Logger name
    :param format: handler format string
    :param datefmt: handler date/time format specifier
    :param stream: initialize the StreamHandler using ``stream``
        (None disables the stream, default=sys.stdout)
    :param level: logger level (default=current pipeline log level).
    :param filename: create FileHandler using ``filename`` (default=None)
    :param filemode: open ``filename`` with specified filemode ('w' or 'a')
    :param filelevel: logger level for file logger (default=``level``)
    :param propagate: propagate message to parent (default=False)
    :param addToCasaLog: emit log message to CASA logs too (default=True)

    :returns: logging.Logger object
    """
    if level is None:
        level = logging_level

    # Get a logger for the specified name
    logger = logging.getLogger(name)

    def trace(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'TRACE'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

    def todo(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'TODO'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        if self.isEnabledFor(TODO):
            self._log(TODO, msg, args, **kwargs)

    def attention(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'ATTENTION'.

        To pass exception information, use the keyword argument exc_info with
        a true value, e.g.

        logger.info("Houston, we have a %s", "interesting problem", exc_info=1)
        """
        if self.isEnabledFor(ATTENTION):
            self._log(ATTENTION, msg, args, **kwargs)

    logger.trace = types.MethodType(trace, logger)
    logger.todo = types.MethodType(todo, logger)
    logger.attention = types.MethodType(attention, logger)

    logger.setLevel(logging_level)
    fmt = UTCFormatter(format, datefmt)
    logger.propagate = propagate

    # Remove existing handlers, otherwise multiple handlers can accrue
    for hdlr in logger.handlers[:]:
        logger.removeHandler(hdlr)

    # Add handlers. Add NullHandler if no file or stream output so that
    # modules don't emit a warning about no handler.
    if not (filename or stream):
        logger.addHandler(logutils.NullHandler())

    if filename:
        # delay = 1 so that file is not opened until used
        hdlr = logging.FileHandler(filename, filemode, delay=1)
        if filelevel is None:
            filelevel = level
        hdlr.setLevel(filelevel)
        hdlr.setFormatter(fmt)
        logger.addHandler(hdlr)

    if stream:
        hdlr = colorize.ColorizingStreamHandler(stream)
        hdlr.setLevel(level)
        hdlr.setFormatter(fmt)
        logger.addHandler(hdlr)

    hdlr = CASALogHandler()
#     hdlr.setLevel(level)
    if addToCasaLog:
        logger.addHandler(hdlr)

    _loggers.append(logger)

    return logger


def set_logging_level(logger=None, level='info'):
    level_no = LOGGING_LEVELS.get(level, logging.NOTSET)

    if logger is not None:
        logger.setLevel(level_no)

    else:
        module = sys.modules[__name__]
        setattr(module, 'logging_level', level_no)

        for logger in _loggers:
            logger.setLevel(level_no)

    import pipeline.infrastructure.mpihelpers as mpihelpers
    if mpihelpers.is_mpi_ready():
        cmd = 'pipeline.infrastructure.logging.set_logging_level(level=%r)' % level
        mpihelpers.mpiclient.push_command_request(cmd,
                                                  block=True,
                                                  target_server=mpihelpers.mpi_server_list)

    #     casa_level = CASALogHandler.get_casa_priority(level_no)
    #     casatools.log.filter(casa_level)


def add_handler(handler):
    """
    Add given handler to all registered loggers.
    """
    for l in _loggers:
        l.addHandler(handler)


def remove_handler(handler):
    """
    Remove specified handler from all registered loggers.
    """
    for l in _loggers:
        l.removeHandler(handler)


def suspend_handler(handler_class):
    """
    Remove and return any logger of the given class from the list of active
    loggers.

    :param handler_class: the class to remove
    :return: set of handler classes removed by this call
    """
    to_remove = {h for l in _loggers for h in l.handlers if isinstance(h, handler_class)}
    for h in to_remove:
        remove_handler(h)
    return to_remove


class SuspendCapturingLogger(object):
    def __enter__(self):
        self.__removed = suspend_handler(CapturingHandler)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__removed:
            for l in self.__removed:
                add_handler(l)

        # don't suppress any raised exception
        return False


class CapturingHandler(logging.Handler):
    """
    A handler class which buffers logging records above a certain threshold
    in memory.
    """

    def __init__(self, level=WARNING):
        """
        Initialize the handler.
        """
        super(CapturingHandler, self).__init__(level)
        self.buffer = []

    def emit(self, record):
        """
        Emit a record.

        Append the record to the buffer.
        """
        # The Traceback object attached to the LogRecord exception tuple is
        # not serializable. Replace it with a substitute wrapper object that
        # is.
        if record.exc_info is not None:
            exc_type, exc_value, tb = record.exc_info
            record.exc_info = (exc_type, exc_value, Traceback(tb))

        # Copy record and call format method to avoid multiple formatting
        # which causes an error. This is necessary because CapturingHandler.emit
        # could be called against the same instance multiple times
        # (e.g., when task calls are nested by subtask calls).
        rec_copy = copy.deepcopy(record)
        rec_copy.msg = self.format(rec_copy)

        self.buffer.append(rec_copy)

    def flush(self):
        """
        Override to implement custom flushing behaviour.

        This version just zaps the buffer to empty.
        """
        self.buffer = []

    def close(self):
        """
        Close the handler.

        This version just flushes and chains to the parent class' close().
        """
        self.flush()
        logging.Handler.close(self)


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


# Code, Frame and Traceback are serializable substitutes for the Traceback
# logged with exceptions
class Code(object):
    def __init__(self, code):
        self.co_filename = code.co_filename
        self.co_name = code.co_name


class Frame(object):
    def __init__(self, frame):
        self.f_globals = {"__file__": frame.f_globals["__file__"]}
        self.f_code = Code(frame.f_code)


class Traceback(object):
    def __init__(self, tb):
        self.tb_frame = Frame(tb.tb_frame)
        self.tb_lineno = tb.tb_lineno
        if tb.tb_next is None:
            self.tb_next = None
        else:
            self.tb_next = Traceback(tb.tb_next)


@contextmanager
def log_level(name, level=logging.WARNING, filter=None):
    """Context manager to temporarily adjust the logging level of a logger.
    
    This context manager allows you to set a temporary logging level and 
    optionally apply a logging filter to a logger. Once the context is 
    exited, the logger's original logging level and filters are restored.

    Parameters:
    name (str): The name of the logger to adjust.
    level (int, optional): The logging level to set for the logger. 
                           Defaults to logging.WARNING (30).
    filter (logging.Filter, optional): A logging filter to add to the logger.
                                       Defaults to None.
    
    Usage example:
    with log_level('my_logger', logging.DEBUG):
        # The logger 'my_logger' will use DEBUG level within this block
        pass

    The default level is WARNING, which means that all messages with 
    level <= 30 are filtered.

    """
    logger = logging.getLogger(name)
    current_level = logger.getEffectiveLevel()
    if level is not None:
        logger.setLevel(level)
    if filter is not None:
        logger.addFilter(filter)
    try:
        yield
    finally:
        if level is not None:
            logger.setLevel(current_level)
        if filter is not None:
            logger.removeFilter(filter)


@contextmanager
def log_filtermsg(msglist: Union[str, List[str]]):
    """Context manager to temporarily filter out specific messages from the CASA global logger.
    
    Note (as of CASA ver6.6.1):
    * The use of this function will clear the list of message filters rather than reset back to 
      the initial state due to the limitation of casalog API.
    * casalog.filterMsg(None) will not alter/clear the internal filter list.
    * Each casalog.filterMsg(msg) call will add additional element(s) in the filter.
    """
    casalog.filterMsg(msglist)
    try:
        yield
    finally:
        casalog.clearFilterMsgList()


LOG = get_logger(__name__)
