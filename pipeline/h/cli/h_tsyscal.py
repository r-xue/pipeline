from . import utils


# docstring and type hints: inherits from h.tasks.tsyscal.tsyscal.TsyscalInputs.__init__
@utils.cli_wrapper
def h_tsyscal(vis=None, caltable=None, chantol=None, parallel=None):
    """Derive a Tsys calibration table.

    Derive the Tsys calibration for list of ALMA MeasurementSets.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Standard call

        >>> h_tsyscal()
    """
