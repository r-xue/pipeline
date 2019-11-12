import datetime

from casatasks import casalog

from .. import utils


def h_save(filename=None):
    context = utils.get_context()    
    context.save(filename)
