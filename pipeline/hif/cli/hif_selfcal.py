import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hif_selfcal(vis=None, field=None, spw=None, contflie=None,
                apply=None, parallel=None, recal=None,
                amplitude_selfcal=None, gaincal_minsnr=None,
                minsnr_to_proceed=None, delta_beam_thres=None,
                apply_cal_mode_default=None, rel_thresh_scaling=None,
                dividing_factor=None, check_all_spws=None,
                pipelinemode=None, dryrun=None, acceptresults=None):
    """hif_selfcal ----- Determine and apply self-calibration with the science target data
    

    --------- parameter descriptions ---------------------------------------------

    vis                     The list of input MeasurementSets. Defaults to the list of
                            MeasurementSets specified in the h_init or hif_importdata task.
                            default = "": use all MeasurementSets in the context
    field                   Select fields to image. Use field name(s) NOT id(s). Mosaics
                            are assumed to have common source / field names.  If intent is
                            specified only fields with data matching the intent will be
                            selected. The fields will be selected from MeasurementSets in
                            "vis".
                            default= "" Fields matching intent, one image per target source.  
    spw                     Select spectral windows to image.
                            "": Images will be computed for all science spectral windows.
    apply                   Apply final selfcal solutions back to the input MeasurementSets.
                            default = True     
    amplitude_selfcal       Attempt amplitude self-calibration following phase-only self-calibration; 
                            if median time between scans of a given target is < 150s, 
                            solution intervals of 300s and inf will be attempted, otherwise just 
                            inf will be attempted.
                            default = False
    gaincal_minsnr          Minimum S/N for a solution to not be flagged by gaincal.
                            default = 2.0
    minsnr_to_proceed       Minimum estimated S/N on a per antenna basis to attempt self-calibration 
                            of a source.
                            default = 3.0
    delta_beam_thresh       Allowed fractional change in beam size for selfcalibration to accept 
                            results of a solution interval.
                            default = 0.05
    apply_cal_mode_default  Apply mode to use for applycal task 
                            during self-calibration; same options as applycal.
                            default = 'calflag'
    rel_thresh_scaling      Scaling type to determine how clean thresholds 
                            per solution interval should be determined going from the starting 
                            clean threshold to 3.0 * RMS for the final solution interval.
                            default='log10', options: 'linear', 'log10', or 'loge' (natural log)
    dividing_factor         Scaling factor 
                            to determine clean threshold for first self-calibration solution interval.
                            Equivalent to (Peak S/N / dividing_factor) *RMS = First clean threshold;
                            however, if (Peak S/N / dividing_factor) *RMS is < 5.0; a value of 5.0 
                            is used for the first clean threshold.
                            default = 40 for < 8 GHz; 15 for > 8 GHz
    check_all_spws          If True, the S/N of mfs images created on a per-spectral-window basis will 
                            be compared at the initial stages final self-calibration.
                            default=False                   
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
