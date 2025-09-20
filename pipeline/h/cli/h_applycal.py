from . import utils


# docstring and type hints: inherits from h.tasks.applycal.applycal.ApplycalInputs.__init__
@utils.cli_wrapper
def h_applycal(vis=None, field=None, intent=None, spw=None, antenna=None, parang=None, applymode=None, flagbackup=None,
               flagsum=None, flagdetailedsum=None, parallel=None):
    """Apply precomputed calibrations to the data.

    h_applycal applies the precomputed calibration tables stored in the pipeline
    context to the set of visibility files using predetermined field and
    spectral window maps and default values for the interpolation schemes.

    Users can interact with the pipeline calibration state using the tasks
    h_export_calstate and h_import_calstate.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Apply the calibration to the target data

        >>> hif_applycal (intent='TARGET')

    """
