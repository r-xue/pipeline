import sys

import pipeline.h.cli.utils as utils


# docstring and type hints: inherits from hifv.tasks.hanning.hanning.HanningInputs.__init__
@utils.cli_wrapper
def hifv_hanning(vis=None, maser_detection=None, spws_to_smooth=None):

    """Hanning smoothing on a dataset.

    The hifv_hanning task will hanning smooth a VLA dataset.

    Returns:
        The results object for the pipeline task is returned.

    Examples:
        1. Run the task to execute hanning smoothing on a VLA CASA pipeline loaded MeasurementSet:

        >>> hifv_hanning()

        2. Run the task with maser detection off and to only smooth spws 2 through 5.

        >>> hifv_hanning(maser_detection=False, spws_to_smooth='2~5')

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
