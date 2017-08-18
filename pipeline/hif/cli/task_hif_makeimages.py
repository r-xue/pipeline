import sys

from taskinit import casalog

import pipeline.h.cli.utils as utils

def hif_makeimages(vis=None, target_list=None, weighting=None,
                   robust=None, noise=None, npixels=None, hm_masking=None,
                   hm_sidelobethreshold=None, hm_noisethreshold=None,
                   hm_lownoisethreshold=None, hm_negativethreshold=None,
                   hm_minbeamfrac=None, hm_growiterations=None,
                   hm_cleaning=None, tlimit=None, masklimit=None,
                   maxncleans=None, cleancontranges=None, subcontms=None, parallel=None,
                   pipelinemode=None, dryrun=None, acceptresults=None):

    # create a dictionary containing all the arguments given in the
    # constructor
    all_inputs = vars()

    task_name = 'MakeImages'

    ##########################################################################
    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #
    ##########################################################################
    
    # get the name of this function for the weblog, eg. 'hif_flagdata'
    fn_name = sys._getframe().f_code.co_name

    # get the context on which this task operates
    context = utils.get_context()
    
    # execute the task    
    results = utils.execute_task(context, task_name, all_inputs, fn_name)

    return results
