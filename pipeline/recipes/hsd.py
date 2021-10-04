# General imports

import os
import sys
import traceback
import inspect

# Pipeline imports
from pipeline.infrastructure import casa_tools

IMPORT_ONLY = 'Import only'

ITERATION = 2


# Run the procedure
def hsdms(vislist, importonly=False, pipelinemode='automatic',
          interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning pipeline run ...")

    try:
        # Initialize the pipeline
        h_init()

        # Load the data
        hsd_importdata(vis=vislist, pipelinemode=pipelinemode)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Deterministic flagging
        hsd_flagdata(pipelinemode=pipelinemode)

        # Tsys calibration
        h_tsyscal(pipelinemode=pipelinemode)

        # Flag system temperature calibration
        hsd_tsysflag(pipelinemode=pipelinemode)

        # Compute the sky calibration
        hsd_skycal(pipelinemode=pipelinemode)

        # Compute the Kelvin to Jansky calibration
        hsd_k2jycal(pipelinemode=pipelinemode)

        # Apply the calibrations
        hsd_applycal(pipelinemode=pipelinemode)

        # Calibration of residual atmospheric transmission
        hsd_atmcor(pipelinemode=pipelinemode)

        # # Improve line mask for baseline subtraction by executing 
        # # hsd_baseline and hsd_blflag iteratively
        for i in range(ITERATION):

            # Baseline subtraction with automatic line detection
            hsd_baseline(pipelinemode=pipelinemode)

            # Flag data based on baseline quality
            hsd_blflag(pipelinemode=pipelinemode)

        # Imaging
        hsd_imaging(pipelinemode=pipelinemode)

        # Export the data
        hsd_exportdata(pipelinemode=pipelinemode)

    except Exception as e:
        if str(e) == IMPORT_ONLY:
            casa_tools.post_to_log("Exiting after import step ...", echo_to_screen=echo_to_screen)
        else:
            casa_tools.post_to_log("Error in procedure execution ...", echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)

    finally:

        # Save the results to the context
        h_save()

        casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
