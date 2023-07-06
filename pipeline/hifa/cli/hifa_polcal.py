import sys

import pipeline.h.cli.utils as utils


def hifa_polcal(vis=None, solint_chavg=None, vs_stats=None, vs_thresh=None, pipelinemode=None, dryrun=None,
                acceptresults=None):
    """
    hifa_polcal ---- Derive instrumental polarisation calibration for ALMA.


    Derive the instrumental polarisation calibrations for ALMA using the
    polarisation calibrators.

    --------- parameter descriptions ---------------------------------------------

    vis             The list of input MeasurementSets. Defaults to the list of MeasurementSets
                    specified in the pipeline context

                    example: ['M32A.ms', 'M32B.ms']
    solint_chavg    Channel averaging to include in solint for gaincal steps
                    producing X-Y delay, X-Y phase, and leakage solutions.
                    Default: '5MHz'.
    vs_stats        List of visstat statistics to use for diagnostic comparison
                    between session MS and individual MSes in that session.
                    Default: ['min','max','mean']
    vs_thresh       Threshold to use in diagnostic comparison of visstat
                    statistics; relative differences larger than this threshold
                    are reported in the CASA log.
                    Default:
    pipelinemode    The pipeline operating mode. In 'automatic' mode the pipeline
                    determines the values of all context defined pipeline inputs automatically.
                    In interactive mode the user can set the pipeline context defined
                    parameters manually. In 'getinputs' mode the users can check the settings
                    of all pipeline parameters without running the task.
    dryrun          Run the commands (True) or generate the commands to be run but do not
                    execute (False).
    acceptresults   Add the results of the task to the pipeline context (True) or
                    reject them (False).

    --------- examples -----------------------------------------------------------


    1. Compute the polarisation calibrations:

    hifa_polcal()

    """

    ##########################################################################
    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #
    ##########################################################################

    # create a dictionary containing all the arguments given in the
    # constructor
    all_inputs = vars()

    # get the name of this function for the weblog, eg. 'hif_flagdata'
    task_name = sys._getframe().f_code.co_name

    # get the context on which this task operates
    context = utils.get_context()

    # execute the task
    results = utils.execute_task(context, task_name, all_inputs)

    return results
