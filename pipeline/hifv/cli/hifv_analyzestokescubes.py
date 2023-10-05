import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_analyzestokescubes(vis=None, acceptresults=None):

    """
    hifv_analyzestokescubes ---- Characterize stokes IQUV flux densities as a function of frequency for VLASS coarse cube images

    Characterize stokes IQUV flux densities as a function of frequency for VLASS Coarse Cube (CC) images

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of MeasurementSets
                  specified in the h_init or hifv_importdata task.

    acceptresults Add the results of the task to the pipeline context (True) or
                  reject them (False).

                  default: True

    --------- examples -----------------------------------------------------------


    1. Basic analyzestokescubes task

    >>> hifv_analyzestokescubes()


    """


    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #

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
