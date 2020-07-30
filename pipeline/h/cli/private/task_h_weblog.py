from casatasks import casalog

import pipeline


def h_weblog(pipelinemode=None, relpath=None):
    pipeline.show_weblog(index_path=relpath)
