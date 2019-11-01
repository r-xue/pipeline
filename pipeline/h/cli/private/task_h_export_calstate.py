from casatasks import casalog

from .. import utils


def h_export_calstate(filename=None, state=None):
    context = utils.get_context()
    if state == 'applied':
        context.callibrary.export_applied(filename)
    else:
        context.callibrary.export(filename)
