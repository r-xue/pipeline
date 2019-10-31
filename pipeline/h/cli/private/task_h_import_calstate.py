from casatasks import casalog

from .. import utils


def h_import_calstate(filename):
    context = utils.get_context()  
    context.callibrary.import_state(filename)
