from casatasks import casalog

import pipeline
from .. import utils


def h_weblog():
    context = utils.get_context()    
    pipeline.show_weblog(context)
