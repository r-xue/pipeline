# General imports

import os
import sys
import traceback
import inspect

# Make sure CASA exceptions are rethrown
try:
    if not  __rethrow_casa_exceptions:
        def_rethrow = False
    else:
        def_rethrow = __rethrow_casa_exceptions
except:
    def_rethrow = False

__rethrow_casa_exceptions = True

# Pipeline imports
import pipeline.infrastructure.casatools as casatools

IMPORT_ONLY = 'Import only'

# Run the procedure
def hifatargets (vislist, importonly=False, pipelinemode='automatic', interactive=True):

    echo_to_screen = interactive
    casatools.post_to_log ("Beginning pipeline target imaging run ...")

    try:
        # Initialize the pipeline
        h_init()

        # Load the data
        hifa_importdata (vis=vislist, dbservice=False, pipelinemode=pipelinemode)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Split out the target data
        hif_mstransform (pipelinemode=pipelinemode)

        # Flag the target data
        hifa_flagtargets (pipelinemode=pipelinemode)

        # Check imaging parameters against PI specified values
        hifa_imageprecheck(pipelinemode=pipelinemode)

        # Check product size limits and mitigate imaging parameters
        hif_checkproductsize(maxcubesize=40.0, maxcubelimit=60.0, maxproductsize=350.0)

        # Make a list of expected targets to be cleaned in mfs mode (used for continuum subtraction)
        hif_makeimlist (specmode='mfs', pipelinemode=pipelinemode)

        # Find continuum frequency ranges
        hif_findcont(pipelinemode=pipelinemode)

        # Fit the continuum using frequency ranges from hif_findcont
        hif_uvcontfit(pipelinemode=pipelinemode)

        # Subtract the continuum fit
        hif_uvcontsub(pipelinemode=pipelinemode)

        # Make clean mfs images for the selected targets
        hif_makeimages (pipelinemode=pipelinemode)

        # Make a list of expected targets to be cleaned in cont (aggregate over all spws) mode
        hif_makeimlist (specmode='cont', pipelinemode=pipelinemode)

        # Make clean cont images for the selected targets
        hif_makeimages (pipelinemode=pipelinemode)

        # Make a list of expected targets to be cleaned in continuum subtracted cube mode
        hif_makeimlist (specmode='cube', pipelinemode=pipelinemode)

        # Make clean continuum subtracted cube images for the selected targets
        hif_makeimages (pipelinemode=pipelinemode)

        # Make a list of expected targets to be cleaned in continuum subtracted PI cube mode
        hif_makeimlist (specmode='repBW', pipelinemode=pipelinemode)

        # Make clean continuum subtracted PI cube
        hif_makeimages (pipelinemode=pipelinemode)

        # Export the data
        hifa_exportdata(pipelinemode=pipelinemode)

    except Exception as e:
        if str(e) == IMPORT_ONLY:
            casatools.post_to_log ("Exiting after import step ...",
                echo_to_screen=echo_to_screen)
        else:
            casatools.post_to_log ("Error in procedure execution ...",
                echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casatools.post_to_log (errstr, echo_to_screen=echo_to_screen)

    finally:

        # Save the results to the context
        h_save()

        casatools.post_to_log ("Terminating procedure execution ...",
            echo_to_screen=echo_to_screen)

        # Restore previous state
        __rethrow_casa_exceptions = def_rethrow
