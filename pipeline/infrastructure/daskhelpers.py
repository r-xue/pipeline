
import pprint
import os

from pipeline.domain.unitformat import file_size
from pipeline.infrastructure import logging
from pipeline.infrastructure.utils import get_obj_size

daskclient = None
tier0futures = True

LOG = logging.get_logger(__name__)


class FutureTask(object):

    def __init__(self, executable):

        LOG.debug('submitting a FutureTask %s from the dask client: %s', executable, file_size.format(get_obj_size(executable)))
        self.future = daskclient.submit(future_exec, executable)

    def get_result(self):

        task_result, tier0_executable = self.future.result()
        LOG.debug('Received the task executation result (%s) from a worker for executing %s; content:',
                  file_size.format(get_obj_size(task_result)), tier0_executable)
        LOG.debug(pprint.pformat(task_result))

        self._merge_casa_commands(tier0_executable.logs)

        return task_result

    def _merge_casa_commands(self, logs):

        LOG.debug('return request logs: {}'.format(logs))

        response_logs = logs
        client_cmdfile = response_logs.get('casa_commands')
        tier0_cmdfile = response_logs.get('casa_commands_tier0')

        if all(isinstance(cmdfile, str) and os.path.exists(cmdfile) for cmdfile in [client_cmdfile, tier0_cmdfile]):
            LOG.info(f'Merge {tier0_cmdfile} into {client_cmdfile}')
            with open(client_cmdfile, 'a') as client:
                with open(tier0_cmdfile, 'r') as tier0:
                    client.write(tier0.read())
                os.remove(tier0_cmdfile)
        else:
            LOG.debug('Cannot find Tier0 casa_commands.log; no merge needed')


def future_exec(tier0_executable):
    """
    Execute a pipeline task.

    This function is used to recreate and execute tasks/jobrequests on cluster nodes.

    :param tier0_executable: the Tier0Executable task to execute
    :return: the Result returned by executing the task
    """
    executable = tier0_executable.get_executable()

    ret = executable()
    LOG.debug('Buffering the execution return (%s) of %s', file_size.format(get_obj_size(ret)), tier0_executable)

    return ret, tier0_executable


def is_dask_ready():
    """Return the availability of dask-base tier0 queue"""
    return bool(daskclient) and tier0futures
