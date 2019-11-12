from casatasks import casalog

from .. import cli
import pipeline.infrastructure.launcher as launcher


def h_resume(filename=None):
    pipeline = launcher.Pipeline(context=filename)

    cli.stack[cli.PIPELINE_NAME] = pipeline

    return pipeline.context
