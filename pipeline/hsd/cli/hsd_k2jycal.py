import pipeline.h.cli.utils as utils


# docstring and type hints: inherits from hsd.tasks.k2jycal.k2jycal.SDK2JyCalInputs.__init__
@utils.cli_wrapper
def hsd_k2jycal(dbservice=None, endpoint=None, reffile=None,
                infiles=None, caltable=None):
    """Obtain and apply the Kelvin-to-Jansky conversion factors.

    Reads Kelvin-to-Jansky (Jy/K) conversion factors from a ``jyperk_query.csv`` file (when
    ``dbservice=True``, the default, these are queried from the online database) or from a
    manually provided ``jyperk.csv`` file. Factors are stored per MS, per spw, per antenna,
    and per polarization.

    The WebLog lists the applied Jy/K factors and displays plots of them:

    - For MSs with fewer than 5 EBs: a scatter plot of factors.
    - For MOUSs with 5 or more EBs: a box plot. EBs identified as outliers (following the
      matplotlib boxplot definition) are plotted as individual points with their EB name labeled.

    .. figure:: /figures/jy2k.png
       :scale: 60%
       :alt: Jy/K conversion factor plots

       Plots of Jy/K conversion factors. (a) Fewer than 5 EBs: scatter plot.
       (b) 5 or more EBs without outliers: box plot. (c) 5 or more EBs with
       outliers: box plot with points and EB names indicated.

    Notes:
        QA scoring:

        - QA = 1.0 if Jy/K conversion factors are available for all data.
        - QA = 0.0 if Jy/K conversion factors are missing for any data.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Derive and apply Jy/K calibration using the online database:

        >>> hsd_k2jycal()

    """
