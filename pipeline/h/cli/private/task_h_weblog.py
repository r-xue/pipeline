from casatasks import casalog

from pipeline import show_weblog


def h_weblog(pipelinemode=None, relpath=None):
    show_weblog(index_path=relpath)
