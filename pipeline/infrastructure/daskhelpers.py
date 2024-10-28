
from pipeline.domain.unitformat import file_size
from pipeline.infrastructure import logging
from pipeline.infrastructure.utils import get_obj_size

daskclient = None
tier0futures = True

LOG = logging.get_logger(__name__)


class FutureTask(object):

    def __init__(self, task, executor=None) -> None:

        LOG.debug('submitting a FutureTask to the dask client: {}'.format(
            task, file_size.format(get_obj_size(task))))

        self.__task = task
        self.__executor = executor

        if self.__executor:
            self.future = daskclient.submit(self.__executor.execute, self.__task)
        else:
            if not callable(self.__task):
                # for JobRequest or PipelineTask
                self.future = daskclient.submit(task.execute)
            else:
                # for FunctionCall
                self.future = daskclient.submit(self.__task)

    def get_result(self):

        task_result = self.future.result()
        return task_result

def is_dask_ready():
    """Return the availability of dask-base tier0 queue"""
    return bool(daskclient) and tier0futures