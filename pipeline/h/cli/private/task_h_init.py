from casatasks import casalog

from .. import cli
import pipeline.infrastructure.launcher as launcher
import pipeline.infrastructure.basetask as basetask


def h_init(pipelinemode=None, loglevel=None, plotlevel=None, weblog=None, overwrite=None, dryrun=None,
           acceptresults=None):

    # TBD: DECIDE WHETHER DRY RUN REALLY MAKES SENSE FOR THIS TASK AND IF
    # SO HOW TO IMPLEMENT IT.

    # TBD: CASA PARAMETER CHECKS BEFORE CREATING A CONTEXT ?

    # Create the pipeline and store the Pipeline object in the stack
    pipeline = launcher.Pipeline(loglevel=loglevel, plotlevel=plotlevel)
    cli.stack[cli.PIPELINE_NAME] = pipeline

    basetask.DISABLE_WEBLOG = not weblog

    return pipeline.context
