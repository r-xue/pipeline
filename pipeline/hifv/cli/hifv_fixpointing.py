import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hifv_fixpointing(vis=None, dryrun=None, acceptresults=None):

    """
    hifv_fixpointing ---- Base fixpointing task

    --------- parameter descriptions ---------------------------------------------

    vis           List of input visibility data
    dryrun        Run the task (False) or display task command (True)
    acceptresults Add the results into the pipeline context
    [1;42mRETURNS[1;m          void

    --------- examples -----------------------------------------------------------

    
    The hifv_fixpointing task
    
    Keyword arguments:
    
    vis -- List of visisbility  data files. These may be ASDMs, tar files of ASDMs,
    MSs, or tar files of MSs, If ASDM files are specified, they will be
    converted  to MS format.
    default: []
    example: vis=['X227.ms', 'asdms.tar.gz']

    dryrun -- Run the commands (True) or generate the commands to be run but
    do not execute (False).
    default: True
    
    acceptresults -- Add the results of the task to the pipeline context (True) or
    reject them (False).
    default: True
    
    Output:
    
    results -- The results object for the pipeline task is returned.
    
    
    Examples
    
    1. Basic fixpointing task
    
    hifv_fixpointing()


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