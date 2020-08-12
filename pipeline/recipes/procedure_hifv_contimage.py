import traceback

IMPORT_ONLY = 'Import only'

# Run the procedure
def hifv (vislist, importonly=False, pipelinemode='automatic', interactive=True):
    import pipeline

    import pipeline.infrastructure.casatools as casatools
    pipeline.initcli()

    echo_to_screen = interactive
    casatools.post_to_log ("Beginning VLA pipeline continuum imaging run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')

        # Load the data
        hifv_importdata(vis=vislist, pipelinemode=pipelinemode)
        if importonly:
            raise Exception(IMPORT_ONLY)

        hif_mstransform(pipelinemode=pipelinemode)

        hif_checkproductsize(maximsize=16384, pipelinemode=pipelinemode)

        hif_makeimlist(specmode='cont', pipelinemode=pipelinemode)

        hif_makeimages(hm_masking='none', hm_cyclefactor=3.0)

        # Export the data
        hifv_exportdata(imaging_products_only=True, pipelinemode=pipelinemode)

    except Exception as e:
        if str(e) == IMPORT_ONLY:
            casatools.post_to_log("Exiting after import step ...", echo_to_screen=echo_to_screen)
        else:
            casatools.post_to_log("Error in procedure execution ...", echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casatools.post_to_log(errstr, echo_to_screen=echo_to_screen)

    finally:

        # Save the results to the context
        h_save()

        casatools.post_to_log("VLA CASA Pipeline finished.  Terminating procedure execution ...",
                              echo_to_screen=echo_to_screen)
