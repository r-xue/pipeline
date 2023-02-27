import sys

import pipeline.h.cli.utils as utils


def hifa_bandpass(vis=None, caltable=None, field=None, intent=None, spw=None, antenna=None, hm_phaseup=None,
                  phaseupsolint=None, phaseupbw=None, phaseupsnr=None, phaseupnsols=None, hm_bandpass=None, solint=None,
                  maxchannels=None, evenbpints=None, bpsnr=None, minbpsnr=None, bpnsols=None, combine=None, refant=None,
                  solnorm=None, minblperant=None, minsnr=None, unregister_existing=None, pipelinemode=None, dryrun=None, 
                  acceptresults=None):

    """
    hifa_bandpass ---- Compute bandpass calibration solutions

    
    The hifa_bandpass task computes a bandpass solution for every specified science
    spectral window. By default a 'phaseup' pre-calibration is performed
    and applied on the fly to the data, before the bandpass is computed.
    
    The hif_refant task may be used to pre-compute a prioritized list of
    reference antennas.
    
    If pipeline mode is 'getinputs' then None is returned. Otherwise,
    the results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                 List of input MeasurementSets. Defaults to the list of
                        MeasurementSets specified in the pipeline context.
                        
                        Example: vis=['ngc5921.ms']
    caltable            List of names for the output calibration tables. Defaults
                        to the standard pipeline naming convention.
                        
                        Example: caltable=['ngc5921.gcal']
    field               The list of field names or field ids for which bandpasses are
                        computed. Set to field='' by default, which means the task will select all
                        fields.
                        
                        Example: field='3C279', field='3C279,M82'
    intent              A string containing a comma delimited list of intents against
                        which the selected fields are matched. Set to intent='' by default, which
                        means the task will select all data with the BANDPASS intent.
                        
                        Example: intent='*PHASE*'
    spw                 The list of spectral windows and channels for which bandpasses are
                        computed. Set to spw='' by default, which means the task will select all
                        science spectral windows.
                        
                        Example: spw='11,13,15,17'
    antenna             Set of data selection antenna IDs
    hm_phaseup          The pre-bandpass solution phaseup gain heuristics. The options are:
                        'snr': compute solution required to achieve the specified SNR
                        'manual': use manual solution parameters
                        '': skip phaseup
                        
                        Example: hm_phaseup='manual'
    phaseupsolint       The phase correction solution interval in CASA syntax.
                        Used when hm_phaseup='manual' or as a default if the hm_phaseup='snr'
                        heuristic computation fails.
                        
                        Example: phaseupsolint='300s'
    phaseupbw           Bandwidth to be used for phaseup. Used when hm_phaseup='manual'.
                        
                        Example: phaseupbw='' to use entire bandpass
                                 phaseupbw='500MHz' to use central 500MHz
    phaseupsnr          The required SNR for the phaseup solution. Used to calculate
                        the phaseup time solint, and only if hm_phaseup='snr'.
                        
                        Example: phaseupsnr=10.0
    phaseupnsols        The minimum number of phaseup gain solutions. Used only if
                        hm_phaseup='snr'.
                        
                        Example: phaseupnsols=4
    hm_bandpass         The bandpass solution heuristics. The options are:
                        'snr': compute the solution required to achieve the specified SNR
                        'smoothed': simple smoothing heuristics
                        'fixed': use the user defined parameters for all spws
    solint              Time and channel solution intervals in CASA syntax.
                        
                        Default is solint='inf', which is used when hm_bandpass='fixed'.
                        If hm_bandpass is set to 'snr', then the task will attempt to compute and use
                        an optimal SNR-based solint (and warn if this solint is not good enough).
                        If hm_bandpass is set to 'smoothed', the task will use a smoothed solint.
    maxchannels         The bandpass solution smoothing factor in channels. The
                        solution interval is bandwidth / 240. Set to 0 for no smoothing.
                        Used if hm_bandpass='smoothed'.
                        
                        Example: maxchannels=0
    evenbpints          Force the per spw frequency solint to be evenly divisible
                        into the spw bandpass if hm_bandpass='snr'.
                        
                        Example: evenbpints=False
    bpsnr               The required SNR for the bandpass solution. Used only if
                        hm_bandpass='snr'.
                        
                        Example: bpsnr=30.0
    minbpsnr            The minimum required SNR for the bandpass solution
                        when strong atmospheric lines exist in Tsys spectra.
                        Used only if hm_bandpass='snr'.
                        
                        Example: minbpsnr=10.0
    bpnsols             The minimum number of bandpass solutions. Used only if
                        hm_bandpass='snr'.
    combine             Data axes to combine for solving. Axes are '', 'scan', 'spw',
                        'field' or any comma-separated combination.
                        
                        Example: combine='scan,field'
    refant              List of reference antenna names. Defaults to the value(s) stored in the
                        pipeline context. If undefined in the pipeline context defaults to
                        the CASA reference antenna naming scheme.
                        
                        Example: refant='DV06,DV07'
    solnorm             Normalise the bandpass solution
    minblperant         Minimum number of baselines required per antenna for each solve.
                        Antennas with fewer baselines are excluded from solutions.
    minsnr              Solutions below this SNR are rejected in the phaseup and bandpass solves
    unregister_existing Unregister all bandpass calibrations from the pipeline context
                        before registering the new bandpass calibrations from this task.
    pipelinemode        The pipeline operating mode. In 'automatic' mode the pipeline
                        determines the values of all context defined pipeline inputs automatically.
                        In interactive mode the user can set the pipeline context defined parameters
                        manually. In 'getinputs' mode the user can check the settings of
                        all pipeline parameters without running the task.
    dryrun              Run the commands (True) or generate the commands to be run but
                        do not execute (False).
    acceptresults       Automatically accept the results of the task into the pipeline context (True)
                        or reject them (False).

    --------- examples -----------------------------------------------------------

    
    1. Compute a channel bandpass for all visibility files in the pipeline
    context using the CASA reference antenna determination scheme:
    
    hifa_bandpass()
    
    2. Same as the above but precompute a prioritized reference antenna list:
    
    hif_refant()
    hifa_bandpass()


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
