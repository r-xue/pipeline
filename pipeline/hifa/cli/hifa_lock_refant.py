import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_lock_refant(vis=None, acceptresults=None):
    """
    hifa_lock_refant ---- Lock reference antenna list

    hifa_lock_refant marks the reference antenna list as "locked" for specified
    measurement sets, preventing modification of the refant list by subsequent
    tasks.

    After executing hifa_lock_refant, all subsequent gaincal calls will by
    default be executed with refantmode='strict'.

    The refant list can be unlocked with the hifa_unlock_refant task.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                  List of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the pipeline context.

                  Example: vis=['ngc5921.ms']
    acceptresults
                  Add the results into the pipeline context.

    --------- examples -----------------------------------------------------------

    1. Lock the refant list for all MSes in pipeline context:

    >>> hifa_lock_refant()

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
