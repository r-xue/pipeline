import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_lowgainflag(vis=None, intent=None, spw=None, refant=None, flag_nmedian=None, fnm_lo_limit=None,
                    fnm_hi_limit=None, tmef1_limit=None, dryrun=None, acceptresults=None):

    """
    hif_lowgainflag ---- Flag antennas with low or high gain

    
    Flag antennas with unusually low or high gain.
    
    Deviant antennas are detected by outlier analysis of a view showing their
    amplitude gains, pre-applying a temporary bandpass and phase solution.
    This view is a list of 2D images with axes 'Scan' and 'Antenna'; there
    is one image for each spectral window and intent. A flagcmd to flag all data
    for an antenna will be generated by any gain that is outside the range
    [fnm_lo_limit * median, fnm_hi_limit * median].
    
    Output
    
    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  '': use all MeasurementSets in the context
                  
                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    intent        A string containing the list of intents to be checked for
                  antennas with deviant gains. The default is blank, which
                  causes the task to select the 'BANDPASS' intent.
    spw           The list of spectral windows and channels to which the
                  calibration will be applied. Defaults to all science windows
                  in the pipeline context.
                  
                  Examples: spw='17', spw='11, 15'
    refant        A string containing a prioritized list of reference antenna
                  name(s) to be used to produce the gain table. Defaults to the
                  value(s) stored in the pipeline context. If undefined in the
                  pipeline context defaults to the CASA reference antenna naming
                  scheme.
                  
                  Examples: refant='DV01', refant='DV06,DV07'
    flag_nmedian  Whether to flag figures of merit greater than
                  fnm_hi_limit * median or lower than fnm_lo_limit * median.
                  (default: True)
    fnm_lo_limit  Flag values lower than fnm_lo_limit * median (default: 0.5)
    fnm_hi_limit  Flag values higher than fnm_hi_limit * median (default: 1.5)
    niter         The maximum number of iterations to run of the sequence:
                  solve for amplitude gains, assess statistics, flag spw/antenna
                  combinations that are outliers (default: 2)
    tmef1_limit   Threshold for "too many entirely flagged" -
                  the critical fraction of antennas whose solutions are entirely
                  flagged in the flagging view of an spw for this stage:
                  if the fraction is equal or greater than this value, then flag
                  the visibility data from all antennas in this spw
                  (default: 0.666)
    dryrun        Run the task (False) or just display the command (True)
    acceptresults Add the results of the task to the pipeline context (True) or
                  reject them (False).

    --------- examples -----------------------------------------------------------

    


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
