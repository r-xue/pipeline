# General imports

import traceback

# sys.path.insert (0, os.path.expandvars("$SCIPIPE_HEURISTICS"))

# Make sure CASA exceptions are rethrown
try:
    if not  __rethrow_casa_exceptions:
        def_rethrow = False
    else:
        def_rethrow = __rethrow_casa_exceptions
except:
    def_rethrow = False

__rethrow_casa_exceptions = False


# IMPORT_ONLY = 'Import only'
IMPORT_ONLY = ''


# Run the procedure
def hifv (vislist, importonly=False, pipelinemode='automatic', interactive=True):
    import pipeline

    # Pipeline imports
    import pipeline.infrastructure.casatools as casatools
    pipeline.initcli()

    echo_to_screen = interactive
    casatools.post_to_log ("Beginning VLA pipeline calibration run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')
        # h_init(loglevel='trace', plotlevel='summary')

        # Load the data
        hifv_importdata (vis=vislist, pipelinemode=pipelinemode)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Hanning smooth the data
        hifv_hanning(pipelinemode=pipelinemode)

        # Flag known bad data
        hifv_flagdata(pipelinemode=pipelinemode, scan=True, hm_tbuff='1.5int',
                       intents='*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*, *UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*')

        # Fill model columns for primary calibrators
        hifv_vlasetjy(pipelinemode=pipelinemode)

        # Gain curves, opacities, antenna position corrections, 
        # requantizer gains (NB: requires CASA 4.1!)
        hifv_priorcals(pipelinemode=pipelinemode)

        # Initial test calibrations using bandpass and delay calibrators
        hifv_testBPdcals(pipelinemode=pipelinemode)

        # Identify and flag basebands with bad deformatters or rfi based on
        # bp table amps and phases
        hifv_flagbaddef(pipelinemode=pipelinemode)

        # Flag spws that have no calibration at this point
        # hifv_uncalspw(pipelinemode=pipelinemode, delaycaltable='testdelay.k', bpcaltable='testBPcal.b')

        # Flag possible RFI on BP calibrator using rflag
        hifv_checkflag(pipelinemode=pipelinemode)

        # DO SEMI-FINAL DELAY AND BANDPASS CALIBRATIONS
        # (semi-final because we have not yet determined the spectral index of the bandpass calibrator)
        hifv_semiFinalBPdcals(pipelinemode=pipelinemode)

        # Use flagdata rflag mode again on calibrators
        hifv_checkflag(pipelinemode=pipelinemode, checkflagmode='semi')

        # Re-run semi-final delay and bandpass calibrations
        hifv_semiFinalBPdcals(pipelinemode=pipelinemode)

        # Flag spws that have no calibration at this point
        # hifv_uncalspw(pipelinemode=pipelinemode, delaycaltable='delay.k', bpcaltable='BPcal.b')

        # Determine solint for scan-average equivalent
        hifv_solint(pipelinemode=pipelinemode)

        # Do the flux density boostrapping -- fits spectral index of
        # calibrators with a heuristics determined fit order
        hifv_fluxboot2(pipelinemode=pipelinemode)

        # Make the final calibration tables
        hifv_finalcals(pipelinemode=pipelinemode)

        # Polarization calibration
        # hifv_circfeedpolcal(pipelinemode=pipelinemode)

        # Apply all the calibrations and check the calibrated data
        hifv_applycals(pipelinemode=pipelinemode)

        # Flag spws that have no calibration at this point
        # hifv_uncalspw(pipelinemode=pipelinemode, delaycaltable='finaldelay.k', bpcaltable='finalBPcal.b')

        # Now run all calibrated data, including the target, through rflag
        hifv_targetflag(pipelinemode=pipelinemode, intents='*CALIBRATE*,*TARGET*')

        # Calculate data weights based on standard deviation within each spw
        hifv_statwt(pipelinemode=pipelinemode)

        # Plotting Summary
        hifv_plotsummary(pipelinemode=pipelinemode)

        # Make a list of expected point source calibrators to be cleaned
        hif_makeimlist(intent='PHASE,BANDPASS', specmode='cont', pipelinemode=pipelinemode)

        # Make clean images for the selected calibrators
        hif_makeimages(hm_masking='none')

        # Export the data
        # hifv_exportdata(pipelinemode=pipelinemode)

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

        # Restore previous state
        __rethrow_casa_exceptions = def_rethrow
