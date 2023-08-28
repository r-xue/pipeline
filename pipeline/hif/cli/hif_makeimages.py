import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_makeimages(vis=None, target_list=None, hm_masking=None,
                   hm_sidelobethreshold=None, hm_noisethreshold=None, hm_lownoisethreshold=None,
                   hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None,
                   hm_dogrowprune=None, hm_minpercentchange=None, hm_fastnoise=None, hm_nsigma=None,
                   hm_perchanweightdensity=None, hm_npixels=None, hm_cyclefactor=None, hm_minpsffraction=None,
                   hm_maxpsffraction=None, hm_weighting=None, hm_cleaning=None, tlimit=None, drcorrect=None, masklimit=None,
                   cleancontranges=None, calcsb=None, hm_mosweight=None, overwrite_on_export=None, parallel=None,
                   dryrun=None, acceptresults=None):

    """
    hif_makeimages ---- Compute clean map


    Compute clean results from a list of specified targets.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                     The list of input MeasurementSets. Defaults to the list of
                            MeasurementSets specified in the h_init or hif_importdata task.
                            '': use all MeasurementSets in the context

                            Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    target_list             Dictionary specifying targets to be imaged; blank will read list from context
    hm_masking              Clean masking mode. Options are 'centralregion', 'auto',
                            'manual' and 'none'
    hm_sidelobethreshold    sidelobethreshold * the max sidelobe level
    hm_noisethreshold       noisethreshold * rms in residual image
    hm_lownoisethreshold    lownoisethreshold * rms in residual image
    hm_negativethreshold    negativethreshold * rms in residual image
    hm_minbeamfrac          Minimum beam fraction for pruning
    hm_growiterations       Number of binary dilation iterations for growing the mask
    hm_dogrowprune          Do pruning on the grow mask

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    hm_minpercentchange     Mask size change threshold
    hm_fastnoise            Faster noise calculation for automask or nsigma stopping

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    hm_nsigma               Multiplicative factor for rms-based threshold stopping
    hm_perchanweightdensity Calculate the weight density for each channel independently

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    hm_npixels              Number of pixels to determine uv-cell size for super-uniform weighting
    hm_cyclefactor          Scaling on PSF sidelobe level to compute the minor-cycle stopping threshold
    hm_minpsffraction       PSF fraction that marks the max depth of cleaning in the minor cycle
    hm_maxpsffraction       PSF fraction that marks the minimum depth of cleaning in the minor cycle
    hm_weighting            Weighting scheme (natural,uniform,briggs,briggsabs[experimental],briggsbwtaper[experimental])
    hm_cleaning             Pipeline cleaning mode
    tlimit                  Times the sensitivity limit for cleaning
    drcorretion             Override the default heuristics-based DR correction (for ALMA data only)
    masklimit               Times good mask pixels for cleaning
    cleancontranges         Clean continuum frequency ranges in cubes
    calcsb                  Force (re-)calculation of sensitivities and beams
    hm_mosweight            Mosaic weighting

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    overwrite_on_export     Replace existing image products when h/hifa/hifv_exportdata is
                            called.
                            If False, images that would have the same FITS name on export,
                            are amended to include a version number.  For example, if
                            oussid.J1248-4559_ph.spw21.mfs.I.pbcor.fits would already be
                            exported by a previous call to hif_makeimags, then
                            'oussid.J1248-4559_ph.spw21.mfs.I.pbcor.v2.fits' would also be
                            exported to the products/ directory. The first exported
                            product retains the same name.  Additional products start
                            counting with 'v2', 'v3', etc.
    parallel                Clean images using MPI cluster
    dryrun                  Run the task (False) or just display the command (True)
    acceptresults           Add the results to the pipeline context

    --------- examples -----------------------------------------------------------

    Compute clean results for all imaging targets defined in a previous hif_makeimlist
    or hif_editimlist call:

    hif_makeimages()

    Compute clean results overriding automatic masking choice:

    hif_makeimages(hm_masking='centralregion')


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
