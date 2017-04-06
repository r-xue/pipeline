import sys

from taskinit import casalog

import pipeline.h.cli.utils as utils


def hif_tclean(vis=None, imagename=None, intent=None, field=None, spw=None,
               spwsel_lsrk=None, spwsel_topo=None,
               uvrange=None, specmode=None, gridder=None, deconvolver=None,
               nterms=None, outframe=None, imsize=None, cell=None,
               phasecenter=None, stokes=None, nchan=None, start=None,
	       width=None, nbin=None, weighting=None,
               robust=None, noise=None, npixels=None,
	       restoringbeam=None, hm_masking=None,
               hm_autotest=None, hm_cleaning=None, mask=None,
	       niter=None, threshold=None, tlimit=None, masklimit=None,
	       maxncleans=None, cleancontranges=None, subcontms=None,
               parallel=None,
	       pipelinemode=None, dryrun=None, acceptresults=None):

    # create a dictionary containing all the arguments given in the
    # constructor
    all_inputs = vars()

    task_name = 'Tclean'

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
