import sys

from casatasks import casalog

from . import utils


def h_exportdata(vis=None, session=None, imaging_products_only=None, exportmses=None, pprfile=None, calintents=None,
                 calimages=None, targetimages=None, products_dir=None, pipelinemode=None, dryrun=None,
                 acceptresults=None):

    """
    h_exportdata ---- Prepare interferometry data for export

    
    The hif_exportdata task exports the data defined in the pipeline context
    and exports it to the data products directory, converting and or
    packing it as necessary.
    
    The current version of the task exports the following products
    
    o an XML file containing the pipeline processing request
    o a tar file per ASDM / MS containing the final flags version
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
    
    Returns
    
    If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                   List of visibility data files for which flagging and calibration
                          information will be exported. Defaults to the list maintained in the
                          pipeline context.
                          Can only be set in pipelinemode='interactive'
                          example: vis=['X227.ms', 'X228.ms']
    session               session -- List of sessions one per visibility file. Defaults
                          to a single virtual session containing all the visibility files in vis. 
                          Can only be set in pipelinemode='interactive'
                          example: session=['session1', 'session2']
    imaging_products_only Export the science target image products only
    exportmses            Export MeasurementSets defined in vis instead of flags,
                          caltables, and calibration instructions.
                          Can only be set in pipelinemode='interactive'
                          example: exportmses = True
    pprfile               Name of the pipeline processing request to be exported. Defaults
                          to a file matching the template 'PPR_*.xml'.
                          Can only be set in pipelinemode='interactive'
                          example: pprfile=['PPR_GRB021004.xml']
    calintents            calintents -- List of calibrator image types to be exported. Defaults to
                          all standard calibrator intents 'BANDPASS', 'PHASE', 'FLUX'
                          Can only be set in pipelinemode='interactive'
                          example: calintents='PHASE'
    calimages             List of calibrator images to be exported. Defaults to all
                          calibrator images recorded in the pipeline context.
                          Can only be set in pipelinemode='interactive'
                          example: calimages=['3C454.3.bandpass', '3C279.phase']
    targetimages          List of science target images to be exported.
                          Science target images recorded in the pipeline context.
                          Can only be set in pipelinemode='interactive'
                          example: targetimages=['NGC3256.band3', 'NGC3256.band6']
    products_dir          Name of the data products subdirectory.
                          Can only be set in pipelinemode='interactive'
                          example: products_dir='../products'
    pipelinemode          The pipeline operating mode. In 'automatic' mode the pipeline
                          determines the values of all context defined pipeline inputs automatically.
                          In 'interactive' mode the user can set the pipeline context defined
                          parameters manually.  In 'getinputs' mode the user can check the settings
                          of all pipeline parameters without running the task.
    dryrun                Run the task (False) or display task command (True)
    acceptresults         Add the results into the pipeline context

    --------- examples -----------------------------------------------------------

    
    Examples
    
    1. Export the pipeline results for a single session to the data products
    directory
    
    !mkdir ../products
    hif_exportdata (products_dir='../products')
    
    2. Export the pipeline results to the data products directory specify that
    only the gain calibrator images be saved.
    
    !mkdir ../products
    hif_exportdata (products_dir='../products', calintents='*PHASE*')


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
