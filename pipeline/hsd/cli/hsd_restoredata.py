import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hsd_restoredata(vis=None, session=None, products_dir=None, copytoraw=None, rawdata_dir=None, lazy=None,
                    bdfflags=None, ocorr_mode=None, asis=None, hm_rasterscan=None, pipelinemode=None, dryrun=None, acceptresults=None):

    """
    hsd_restoredata ---- Restore flagged and calibration single dish data from a pipeline run

    The hsd_restoredata task restores flagged and calibrated MeasurementSets
    from archived ASDMs and pipeline flagging and calibration date products.
    
    The hsd_restoredata task restores flagged and calibrated data from archived
    ASDMs and pipeline flagging and calibration data products. Pending archive
    retrieval support hsd_restoredata assumes that the required products
    are available in the rawdata_dir in the format produced by the
    hifa_exportdata task.
    
    hsd_restoredata assumes that the following entities are available in the raw
    data directory:
    
    o the ASDMs to be restored
    o for each ASDM in the input list:
    o a compressed tar file of the final flagversions file, e.g.
    uid___A002_X30a93d_X43e.ms.flagversions.tar.gz
    o a text file containing the applycal instructions, e.g.
    uid___A002_X30a93d_X43e.ms.calapply.txt
    o a compressed tar file containing the caltables for the parent session,
    e.g. uid___A001_X74_X29.session_3.caltables.tar.gz
    
    hsd_restoredata performs the following operations:
    
    o imports the ASDM(s)
    o removes the default MS.flagversions directory created by the filler
    o restores the final MS.flagversions directory stored by the pipeline
    o restores the final set of pipeline flags to the MS
    o restores the final calibration state of the MS
    o restores the final calibration tables for each MS
    o applies the calibration tables to each MS
    
    When importing the ASDM and converting it to a Measurement Set (MS), if the
    output MS already exists in the output directory, then the importasdm
    conversion step is skipped, and the existing MS will be imported instead.
    
    Output:
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of raw visibility data files to be restored. Assumed to be
                  in the directory specified by rawdata_dir.
                  
                  example: vis=['uid___A002_X30a93d_X43e']
    session       List of sessions one per visibility file.
                  
                  example: session=['session_3']
    products_dir  Name of the data products directory to copy calibration
                  products from. The parameter is effective only when copytoraw = True.
                  When copytoraw = False, calibration products in rawdata_dir will be used.
                  
                  example: products_dir='myproductspath'
    copytoraw     Copy calibration and flagging tables from products_dir to
                  rawdata_dir directory.
                  
                  example: copytoraw=False
    rawdata_dir   Name of the raw data directory.
                  
                  example: rawdata_dir='myrawdatapath'
    lazy          Use the lazy filler option
                  
                  example: lazy=True
    bdfflags      Set the BDF flags
                  
                  example: bdfflags=False
    ocorr_mode    Set ocorr_mode
                  
                  example: ocorr_mode='ca'
    asis          Set list of tables to import asis.
                  
                  example: asis='Source Receiver'
    hm_rasterscan Heuristics method for raster scan analysis. Two analysis modes,
                  time-domain analysis ('time') and direction analysis ('direction'), are available.
                  Default is 'time'.
    pipelinemode  The pipeline operating mode. In 'automatic' mode the pipeline
                  determines the values of all context defined pipeline inputs automatically.
                  In 'interactive' mode the user can set the pipeline context defined
                  parameters manually.  In 'getinputs' mode the user can check the settings
                  of all pipeline parameters without running the task.
    dryrun        Run the commands (True) or generate the commands to be run but
                  do not execute (False).
    acceptresults Add the results of the task to the pipeline context (True) or
                  reject them (False).

    --------- examples -----------------------------------------------------------

    
    1. Restore the pipeline results for a single ASDM in a single session
    
    hsd_restoredata (vis=['uid___A002_X30a93d_X43e'], session=['session_1'], ocorr_mode='ao')


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