import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_solint(vis=None, dryrun=None, acceptresults=None, limit_short_solint=None,
                refantignore=None):

    """
    hifv_solint ---- Determines different solution intervals

    
    The hifv_solint task determines different solution intervals. Note that the short solint value is switched to 'int' when
    the minimum solution interval corresponds to one integration.

    
    Output:
    
    results -- The results object for the pipeline task is returned.
    
    
    --------- parameter descriptions ---------------------------------------------

    vis                List of visibility data files. These may be ASDMs, tar files of ASDMs,
                       MSes, or tar files of MSes, If ASDM files are specified, they will be
                       converted  to MS format.
                       example: vis=['X227.ms', 'asdms.tar.gz']
    dryrun             Run the commands (True) or generate the commands to be run but
                       do not execute (False).  This is a pipeline task execution mode.
    acceptresults      Add the results of the task to the pipeline context (True) or
                       reject them (False).  This is a pipeline task execution mode.
    limit_short_solint Keyword argument in units of seconds to limit the short solution interval.
                       Can be a string or float numerical value in units of seconds of '0.45' or 0.45.
                       Can be set to a string value of 'int'.
    refantignore       String list of antennas to ignore
                       Example:  refantignore='ea02,ea03'

    --------- examples -----------------------------------------------------------
    
    
    1. Determines different solution intervals:
    
    >>> hifv_solint()


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
