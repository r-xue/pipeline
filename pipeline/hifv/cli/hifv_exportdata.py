import sys

import pipeline.h.cli.utils as utils


def hifv_exportdata(vis=None, session=None, imaging_products_only=None, exportmses=None, tarms=None, exportcalprods=None,
    pprfile=None, calintents=None, calimages=None, targetimages=None, products_dir=None,
    gainmap=None, pipelinemode=None, dryrun=None, acceptresults=None):

    """
    hifv_exportdata ---- Prepare and export interferometry and imaging data

    
    The hifv_exportdata task for the VLA CASA pipeline exports the data defined
    in the pipeline context and exports it to the data products directory,
    converting and or packing it as necessary.
    
    The current version of the task exports the following products
    
    o an XML file containing the pipeline processing request
    o a tar file per ASDM / MS containing the final flags version OR the MS if tarms is False
    o a text file per ASDM / MS containing the final calibration apply list
    o a FITS image for each selected calibrator source image
    o a FITS image for each selected science target source image
    o a tar file per session containing the caltables for that session
    o a tar file containing the file web log
    o a text file containing the final list of CASA commands
    
    
    Issues
    
    Support for merging the calibration state information into the pipeline
    context / results structure and retrieving it still needs to be added.
    
    Support for merging the clean results into the pipeline context / results
    structure and retrieving it still needs to be added.
    
    Support for creating the final pipeline results entity still needs to
    be added.
    
    Session information is not currently handled by the pipeline context.
    By default all ASDMs are combined into one session.
    
    Output:
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                   List of visibility data files for which flagging and calibration
                          information will be exported. Defaults to the list maintained in the
                          pipeline context. Not available in pipelinemode='automatic'.
                          example: vis=['X227.ms', 'X228.ms']
    session               List of sessions one per visibility file. Currently defaults
                          to a single virtual session containing all the visibility files in vis.
                          In the future, this will default to the set of observing sessions defined
                          in the context. Not available in pipelinemode='automatic'.
                          example: session=['session1', 'session2']
    imaging_products_only Export science target imaging products only
    exportmses            Export the final MeasurementSets instead of the final flags,
                          calibration tables, and calibration instructions.
    tarms                 Tar final MeasurementSets
    exportcalprods        Export flags and caltables in addition to MeasurementSets.
                          this parameter is only valid when exportmses = True.
    pprfile               Name of the pipeline processing request to be exported. Defaults
                          to a file matching the template 'PPR_*.xml'. Not available in pipelinemode='automatic'.
                          example: pprfile=['PPR_GRB021004.xml']
    calintents            List of calibrator image types to be exported. Defaults to
                          all standard calibrator intents, 'BANDPASS', 'PHASE', 'FLUX'.
                          Not available in pipelinemode='automatic'.
                          example: 'PHASE'
    calimages             List of calibrator images to be exported. Defaults to all
                          calibrator images recorded in the pipeline context.
                          Not available in pipelinemode='automatic'.
                          example: calimages=['3C454.3.bandpass', '3C279.phase']
    targetimages          List of science target images to be exported. Defaults to all
                          science target images recorded in the pipeline context.
                          Not available in pipelinemode='automatic'.
                          example: targetimages=['NGC3256.band3', 'NGC3256.band6']
    products_dir          Name of the data products subdirectory. Defaults to './'
                          Not available in pipelinemode='automatic'.
                          example: '../products'
    gainmap               The value of gainmap parameter in hifv_restoredata task put in
                          casa_piperestorescript.py
    pipelinemode          The pipeline operating mode. In 'automatic' mode the pipeline
                          determines the values of all context defined pipeline inputs automatically.
                          In 'interactive' mode the user can set the pipeline context defined
                          parameters manually.  In 'getinputs' mode the user can check the settings
                          of all pipeline parameters without running the task.
    dryrun                Run the task (False) or display task command (True).
                          Only available in pipelinemode='interactive'.
    acceptresults         Add the results of the task to the pipeline context (True) or
                          reject them (False). Only available in pipelinemode='interactive'.

    --------- examples -----------------------------------------------------------

    
    
    1. Export the pipeline results for a single session to the data products
    directory
    
    !mkdir ../products
    hifv_exportdata (products_dir='../products')
    
    2. Export the pipeline results to the data products directory specify that
    only the gain calibrator images be saved.
    
    !mkdir ../products
    hifv_exportdata (products_dir='../products', calintents='*PHASE*')


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