import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hifa_antpos(vis=None, caltable=None, hm_antpos=None, antenna=None, offsets=None, antposfile=None, pipelinemode=None,
                dryrun=None, acceptresults=None, threshold=None):

    """
    hifa_antpos ---- Derive an antenna position calibration table

    
    The hifa_antpos task corrects the antenna positions recorded in the ASDMs using
    updated antenna position calibration information determined after the
    observation was taken.
    
    Corrections can be input by hand, read from a file on disk, or in the future
    by querying an ALMA database service.
    
    The antenna positions file is in 'csv' format containing 6 comma-delimited
    columns as shown below. This file should not include blank lines, including
    after the end of the last entry. The default name of this file is 'antennapos.csv'.
    
    Contents of example 'antennapos.csv' file:
    
    ms,antenna,xoffset,yoffset,zoffset,comment
    uid___A002_X30a93d_X43e.ms,DV11,0.000,0.010,0.000,"No comment"
    uid___A002_X30a93d_X43e.dup.ms,DV11,0.000,-0.010,0.000,"No comment"
    
    The offset values in this file are in meters.
    
    The corrections are used to generate a calibration table which is recorded
    in the pipeline context and applied to the raw visibility data, on the fly to
    generate other calibration tables, or permanently to generate calibrated
    visibilities for imaging.
    
    If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.
    
    Note: the hm_antpos 'online' option will be implemented when the observing system
    provides an antenna position determination service.

    --------- parameter descriptions ---------------------------------------------

    vis           List of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the pipeline context.
                  
                  Example: vis=['ngc5921.ms']
    caltable      List of names for the output calibration tables. Defaults
                  to the standard pipeline naming convention.
                  
                  Example: caltable=['ngc5921.gcal']
    hm_antpos     Heuristics method for retrieving the antenna position
                  corrections. The options are 'online' (not yet implemented), 'manual',
                  and 'file'.
                  
                  Example: hm_antpos='manual'
    antenna       The list of antennas for which the positions are to be corrected
                  if hm_antpos is 'manual'.
                  
                  Example: antenna='DV05,DV07'
    offsets       The list of antenna offsets for each antenna in 'antennas'. Each
                  offset is a set of 3 floating point numbers separated by commas, specified
                  in the ITRF frame.
                  
                  Example: offsets=[0.01, 0.02, 0.03, 0.03, 0.02, 0.01]
    antposfile    The file(s) containing the antenna offsets. Used if hm_antpos
                  is 'file'.
    pipelinemode  The pipeline operating mode. In 'automatic' mode the pipeline
                  determines the values of all context defined pipeline inputs automatically.
                  In interactive mode the user can set the pipeline context defined parameters
                  manually. In 'getinputs' mode the user can check the settings of
                  all pipeline parameters without running the task.
    dryrun        Run the commands (True) or generate the commands to be run but
                  do not execute (False).
    acceptresults Automatically accept the results of the task into the pipeline context (True)
                  or reject them (False).
    threshold     Highlight antenna position offsets greater than this value in the weblog. Units are wavelengths and the default is 1.0.
                  
                  Example: threshold=1.0

    --------- examples -----------------------------------------------------------

    
    1. Correct the position of antenna 5 for all the visibility files in a single
    pipeline run:
    
    hifa_antpos(antenna='DV05', offsets=[0.01, 0.02, 0.03])
    
    2. Correct the position of antennas for all the visibility files in a single
    pipeline run using antenna positions files on disk. These files are assumed
    to conform to a default naming scheme if 'antposfile' is unspecified by the
    user:
    
    hifa_antpos(hm_antpos='file', antposfile='myantposfile.csv')


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