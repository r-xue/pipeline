import sys

import pipeline.h.cli.utils as utils


def hsd_applycal(vis=None, field=None, intent=None, spw=None, antenna=None,
    applymode=None, calwt=None, flagbackup=None, pipelinemode=None, dryrun=None,
    acceptresults=None):

    """
    hsd_applycal ---- Apply the calibration(s) to the data

    
    Apply the calibration to the data.
    
    hif_applycal applies the precomputed calibration tables stored in the pipeline
    context to the set of visibility files using predetermined field and
    spectral window maps and default values for the interpolation schemes.
    
    Users can interact with the pipeline calibration state using the tasks
    h_export_calstate and h_import_calstate.
    
    Output:
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of MeasurementSets
                  in the pipeline context. Parameter not available in pipelinemode='automatic'.
                  example: ['X227.ms']
    field         A string containing the list of field names or field ids to which
                  the calibration will be applied. Defaults to all fields in the pipeline context.
                  Parameter not available in pipelinemode='automatic'.
                  example: '3C279', '3C279, M82'
    intent        A string containing the list of intents against which the 
                  selected fields will be matched. Defaults to all supported intents
                  in the pipeline context. Parameter not available in pipelinemode='automatic'.
                  example: '*TARGET*'
    spw           The list of spectral windows and channels to which the calibration
                  will be applied. Defaults to all science windows in the pipeline context.
                  Parameter not available in pipelinemode='automatic'.
                  example: '17', '11, 15'
    antenna       The list of antennas to which the calibration will be applied.
                  Defaults to all antennas. Not currently supported.
    applymode       Calibration apply mode
                    'calflag': calibrate data and apply flags from solutions
                    ''='calflagstrict': same as above except flag spws for which calibration is
                        unavailable in one or more tables (instead of allowing them to pass
                        uncalibrated and unflagged)
                    'trial': report on flags from solutions, dataset entirely unchanged
                    'flagonly': apply flags from solutions only, data not calibrated
                    'flagonlystrict': same as above except flag spws for which calibration is
                        unavailable in one or more tables
                    'calonly': calibrate data only, flags from solutions NOT applied
    calwt         Calibrate the weights as well as the data.
                  Parameter not available in pipelinemode='automatic'.
    flagbackup    Backup the flags before the applyBackup the flags before the apply.
                  Parameter not available in pipelinemode='automatic'.
    pipelinemode  The pipeline operating mode. In 'automatic' mode the pipeline
                  determines the values of all context defined pipeline inputs automatically.
                  In interactive mode the user can set the pipeline context defined parameters
                  manually.  In 'getinputs' mode the user can check the settings of all
                  pipeline parameters without running the task.
    dryrun        Run task (False) or display the command(True).
                  Available only when pipelinemode='interactive'.
    acceptresults Add the results of the task to the pipeline context (True) or
                  reject them (False). Available only when pipelinemode='interactive'.

    --------- examples -----------------------------------------------------------

    
    
    1. Apply the calibration to the target data
    
    hsd_applycal (intent='TARGET')
    
    
    Issues
    
    There is some discussion about the appropriate values of calwt. Given
    properly scaled data, the correct value should be the CASA default of True.
    However at the current time ALMA is suggesting that calwt be set to True for
    applying observatory calibrations, e.g. antenna positions, WVR, and system
    temperature corrections, and to False for applying instrument calibrations,
    e.g. bandpass, gain, and flux.


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
