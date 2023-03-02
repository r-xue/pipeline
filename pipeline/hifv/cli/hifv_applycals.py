import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hifv_applycals(vis=None, field=None, intent=None, spw=None, antenna=None, applymode=None, flagbackup=None,
                   flagsum=None, flagdetailedsum=None, gainmap=None, pipelinemode=None, dryrun=None,
                   acceptresults=None):

    """
    hifv_applycals ---- Apply calibration tables to measurement set

    hifv_applycals applies the precomputed calibration tables stored in the pipeline
    context to the set of visibility files using predetermined field and
    spectral window maps and default values for the interpolation schemes.
    
    Users can interact with the pipeline calibration state using the tasks
    h_export_calstate and h_import_calstate.

    --------- parameter descriptions ---------------------------------------------

    vis             List of visibility data files. These may be ASDMs, tar files of ASDMs,
                    MSes, or tar files of MSes, If ASDM files are specified, they will be
                    converted  to MS format.
                    example: vis=['X227.ms', 'asdms.tar.gz']
    field           A string containing the list of field names or field ids to which
                    the calibration will be applied. Defaults to all fields in the pipeline
                    context.  Only can be set in pipelinemode='interactive'.
                    example: '3C279', '3C279, M82'
    intent          A string containing the list of intents against which the
                    selected fields will be matched. Defaults to all supported intents
                    in the pipeline context.  Only can be set in pipelinemode='interactive'.
                    example: '*TARGET*'
    spw             The list of spectral windows and channels to which the calibration
                    will be applied. Defaults to all science windows in the pipeline.
                    Only can be set in pipelinemode='interactive'.
                    example: '17', '11, 15'
    antenna         The list of antennas to which the calibration will be applied.
                    Defaults to all antennas. Not currently supported.
                    Only can be set in pipelinemode='interactive'.
    applymode       Calibration apply mode
                    ''='calflagstrict': calibrate data and apply flags from solutions using
                        the strict flagging convention
                    'trial': report on flags from solutions, dataset entirely unchanged
                    'flagonly': apply flags from solutions only, data not calibrated
                    'calonly': calibrate data only, flags from solutions NOT applied
                    'calflagstrict':
                    'flagonlystrict': same as above except flag spws for which calibration is
                        unavailable in one or more tables (instead of allowing them to pass
                        uncalibrated and unflagged)
    flagbackup      Backup the flags before the apply.  Only can be set in pipelinemode='interactive'.
    flagsum         Compute before and after flagging summary statistics
    flagdetailedsum Compute detailed flagging statistics
    gainmap         Mode to map gainfields to scans.
    pipelinemode    The pipeline operating mode. In 'automatic' mode the pipeline
                    determines the values of all context defined pipeline inputs
                    automatically.  In 'interactive' mode the user can set the pipeline
                    context defined parameters manually.  In 'getinputs' mode the user
                    can check the settings of all pipeline parameters without running
                    the task.
    dryrun          Run the commands (True) or generate the commands to be run but
                    do not execute (False).  This is a pipeline task execution mode.
    acceptresults   Add the results of the task to the pipeline context (True) or
                    reject them (False).  This is a pipeline task execution mode.

    --------- examples -----------------------------------------------------------

    
    
    
    Output:
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned
    
    Issues
    
    There is some discussion about the appropriate values of calwt. Given
    properly scaled data, the correct value should be the CASA default of True.
    However at the current time ALMA is suggesting that calwt be set to True for
    applying observatory calibrations, e.g. antenna positions, WVR, and system
    temperature corrections, and to False for applying instrument calibrations,
    e.g. bandpass, gain, and flux.
    
    
    Examples
    
    1. Run the final applycals stage of the VLA CASA pipeline.
    
    hifv_applycals()


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