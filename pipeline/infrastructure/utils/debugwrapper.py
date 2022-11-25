"""Debug wrapper decorator class."""
import functools
import inspect
import logging
import os

import pipeline.infrastructure as infrastructure


def debugwrapper(msg: str=''):
    """Decorate methods to debug

    Args:
        msg (str, optional): debug message. Defaults to ''.
    """
    def wrapper(func):
        logger = infrastructure.get_logger(func.__name__)

        @functools.wraps(func)
        def f(*args, **kwargs):
            retval = func(*args, **kwargs)
            if logger.level <= logging.DEBUG:
                value = kwargs['value'] if 'value' in kwargs else args[-1]
                __debug(logger, args[0], value, msg)
            return retval
        return f
    return wrapper


def __debug(logger: logging.Logger, cls: object, obj: object, msg: str):
    """Output debug strings.

    Args:
        cls : caller class object
        obj : object to output
        msg : action message
    """
    outerframes = inspect.getouterframes(inspect.currentframe())
    for i, frame in enumerate(inspect.getouterframes(inspect.currentframe())):
        if not frame.filename.endswith(__file__):
            break
    source = os.path.basename(outerframes[i].filename)
    clsname = cls.__class__.__name__
    logger.debug(f'{source}[{outerframes[i].lineno}] {msg}: {clsname}.{outerframes[i-1].function}: {type(obj)} {obj} '
                 f'at {outerframes[i].function}')
