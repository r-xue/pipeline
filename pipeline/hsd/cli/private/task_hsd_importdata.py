import sys

import pipeline.h.cli.utils as utils


def hsd_importdata(vis=None, session=None, pipelinemode=None, asis=None, process_caldevice=None, overwrite=None,
                   nocopy=None, bdfflags=None, lazy=None, with_pointing_correction=None, createmms=None, dryrun=None,
                   acceptresults=None):

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