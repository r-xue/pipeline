import sys

from casatasks import casalog

from . import utils


def h_importdata(vis=None, session=None, pipelinemode=None, asis=None, process_caldevice=None, overwrite=None,
                 nocopy=None, bdfflags=None, lazy=None, ocorr_mode=None, createmms=None, dryrun=None,
                 acceptresults=None):

    """
    h_importdata ---- Imports data into the interferometry pipeline

    
    The h_importdata task loads the specified visibility data into the pipeline
    context unpacking and / or converting it as necessary.
    
    If the 'overwrite' input parameter is set to False and the task is
    asked to convert an input ASDM input to an MS, then when the output
    MS already exists in the output directory, the importasdm
    conversion step is skipped, and the existing MS will be imported
    instead.
    
    results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.
    

    --------- parameter descriptions ---------------------------------------------

    vis               List of visibility data files. These may be ASDMs, tar files of ASDMs,
                      MSs, or tar files of MSs, If ASDM files are specified, they will be
                      converted to MS format.
                      example: vis=['X227.ms', 'asdms.tar.gz']
    session           List of sessions to which the visibility files belong. Defaults to a 
                      single session containing all the visibility files, otherwise
                      a session must be assigned to each vis file.
                      example: session=['session_1', 'session_2']
    pipelinemode      The pipeline operating mode. In 'automatic' mode the pipeline
                      determines the values of all context defined pipeline inputs
                      automatically. In 'interactive' mode the user can set the pipeline
                      context defined parameters manually. In 'getinputs' mode the user
                      can check the settings of all pipeline parameters without running
                      the task.
    asis              ASDM tables to convert as is
                      default: 'Antenna Station Receiver CalAtmosphere'
                      Can only be set in pipelinemode='interactive'
                      example: 'Receiver', ''
    process_caldevice Ingest the ASDM caldevice table.
                      Can only be set in pipelinemode='interactive'
    overwrite         Overwrite existing files on import.
                      Can only be set in pipelinemode='interactive'.
                      When converting ASDM to MS, if overwrite=False and the MS
                      already exists in output directory, then this existing MS
                      dataset will be used instead.
    nocopy            When importing an MS, disable copying of the MS to the working
                      directory.  Can only be set in pipelinemode='interactive'.
    bdfflags          Apply BDF flags on import. Can only be set in 
                      pipelinemode='interactive'.
    lazy              Use the lazy import option. Can only be set in
                      pipelinemode='interactive'.
    ocorr_mode        Read in cross- and auto-correlation data(ca), cross-
                      correlation data only (co), or autocorrelation data only (ao).
    createmms         Create a multi-MeasurementSet ('true') ready for parallel
                      processing, or a standard MeasurementSet ('false'). The default setting
                      ('automatic') creates an MMS if running in a cluster environment.
                      Can only be set in pipelinemode='interactive'
    dryrun            Run the task (False) or display task command (True)
    acceptresults     Add the results into the pipeline context

    --------- examples -----------------------------------------------------------

    
    Examples
    
    1. Load an ASDM list in the ../rawdata subdirectory into the context"
    
    h_importdata(vis=['../rawdata/uid___A002_X30a93d_X43e',
    '../rawdata/uid_A002_x30a93d_X44e'])
    
    2. Load an MS in the current directory into the context:
    
    h_importdata(vis=[uid___A002_X30a93d_X43e.ms])
    
    3. Load a tarred ASDM in ../rawdata into the context:
    
    h_importdata(vis=['../rawdata/uid___A002_X30a93d_X43e.tar.gz'])
    
    4. Check the h_importdata inputs, then import the data:
    
    myvislist = ['uid___A002_X30a93d_X43e.ms', 'uid_A002_x30a93d_X44e.ms']
    h_importdata(vis=myvislist, pipelinemode='getinputs')
    h_importdata(vis=myvislist)
    
    5. Load an ASDM but check the results before accepting them into the context.
    results = h_importdata(vis=['uid___A002_X30a93d_X43e.ms'],
    acceptresults=False)
    results.accept()
    
    6. Run in dryrun mode before running for real
    results = h_importdata(vis=['uid___A002_X30a93d_X43e.ms'], dryrun=True)
    results = h_importdata(vis=['uid___A002_X30a93d_X43e.ms'])

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