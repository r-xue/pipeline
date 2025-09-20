import pipeline.h.cli.utils as utils


# docstring and type hints: inherits from hif.tasks.analyzealpha.analyzealpha.AnalyzealphaInputs.__init__
@utils.cli_wrapper
def hif_analyzealpha(vis=None, image=None, alphafile=None, alphaerrorfile=None):
    """Extract spectral index from intensity peak in VLA/VLASS images.

    The results object for the pipeline task is returned.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Basic analyzealpha task

        >>> hif_analyzealpha()
    """
