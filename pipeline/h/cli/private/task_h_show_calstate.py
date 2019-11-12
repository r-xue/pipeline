import sys

from casatasks import casalog

from .. import utils


def h_show_calstate():
    context = utils.get_context()  
    sys.stdout.write('Current on-the-fly calibration state:\n\n')
    sys.stdout.write(str(context.callibrary.active))
    sys.stdout.write('\n')
