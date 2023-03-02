import sys

from casatasks import casalog

import pipeline.h.cli.utils as utils


def hifa_restoredata(vis=None, session=None, products_dir=None, copytoraw=None, rawdata_dir=None, lazy=None,
                     bdfflags=None, ocorr_mode=None, asis=None, pipelinemode=None, dryrun=None, acceptresults=None):

    """
    hifa_restoredata ---- Restore flagged and calibration interferometry data from a pipeline run

    
    The hifa_restoredata task restores flagged and calibrated MeasurementSets
    from archived ASDMs and pipeline flagging and calibration date products.
    
    The hifa_restoredata restores flagged and calibrated data from archived
    ASDMs and pipeline flagging and calibration data products. Pending archive
    retrieval support hifa_restoredata assumes that the required products
    are available in the rawdata_dir in the format produced by the
    hifa_exportdata task.
    
    hifa_restoredata assumes that the following entities are available in the raw
    data directory:
    
    o the ASDMs to be restored
    o for each ASDM in the input list:
    o a compressed tar file of the final flagversions file, e.g.
    uid___A002_X30a93d_X43e.ms.flagversions.tar.gz
    o a text file containing the applycal instructions, e.g.
    uid___A002_X30a93d_X43e.ms.calapply.txt
    o a compressed tar file containing the caltables for the parent session,
    e.g. uid___A001_X74_X29.session_3.caltables.tar.gz
    
    hifa_restoredata performs the following operations:
    
    o imports the ASDM(s)
    o removes the default MS.flagversions directory created by the filler
    o restores the final MS.flagversions directory stored by the pipeline
    o restores the final set of pipeline flags to the MS
    o restores the final calibration state of the MS
    o restores the final calibration tables for each MS
    o applies the calibration tables to each MS
    
    When importing the ASDM and converting it to a Measurement Set (MS), if the
    output MS already exists in the output directory, then the importasdm
    conversion step is skipped, and instead the existing MS will be imported.
    
    Output:
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of raw visibility data files to be restored. 
                  Assumed to be in the directory specified by rawdata_dir.
                  (can be set only in 'interactive mode')
                  example: vis=['uid___A002_X30a93d_X43e']
    session       List of sessions one per visibility file. 
                  (can be set only in 'interactive mode')
                  
                  example: session=['session_3']
    products_dir  Name of the data products directory to copy calibration
                  products from. The parameter is effective only when 
                  copytoraw = True.  When copytoraw = False, calibration
                  products in rawdata_dir will be used.
                  (can be set only in 'interactive mode')
                  
                  example: products_dir='myproductspath'
    copytoraw     Copy calibration and flagging tables from products_dir to
                  rawdata_dir directory.
                  (can be set only in 'interactive mode')
                  
                  example: copytoraw=False
    rawdata_dir   Name of the rawdata subdirectory. 
                  (can be set only in 'interactive mode')
                  
                  example: rawdata_dir='myrawdatapath'
    lazy          Use the lazy filler option.
                  (can be set only in 'interactive mode')
                  
                  example: lazy=True
    bdfflags      Set the BDF flags.
                  (can be set only in 'interactive mode')
                  
                  example: bdfflags=False
    ocorr_mode    Set ocorr_mode.
                  (can be set only in 'interactive mode')
                  
                  example: ocorr_mode='ca'
    asis          Set list of tables to import as is.
                  (can be set only in 'interactive mode')
                  
                  example: asis='Source Receiver'
    pipelinemode  The pipeline operating mode. In 'automatic' mode 
                  the pipeline determines the values of all context 
                  defined pipeline inputs automatically.
                  (can be set in any pipeline mode)
    dryrun        Run the commands (False) or generate the commands to be
                  run but do not execute (True).
    acceptresults Add the results of the task to the pipeline context (True)
                  or reject them (False).

    --------- examples -----------------------------------------------------------

    
    1. Restore the pipeline results for a single ASDM in a single session:
    
    hifa_restoredata(vis=['uid___A002_X30a93d_X43e'], session=['session_1'],
    ocorr_mode='ca')


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