import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class HanningInputs(vdp.StandardInputs):
    """Inputs class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

    """
    def __init__(self, context, vis=None):
        """
        Args:
            context (:obj:): Pipeline context
            vis(str, optional): String name of the measurement set

        """
        super(HanningInputs, self).__init__()
        self.context = context
        self.vis = vis


class HanningResults(basetask.Results):
    """Results class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

    The class inherits from basetask.Results

    """
    def __init__(self, final=None, pool=None, preceding=None):
        """
        Args:
            final(list): final list of tables (not used in this task)
            pool(list): pool list (not used in this task)
            preceding(list): preceding list (not used in this task)

        """

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(HanningResults, self).__init__()

        self.vis = None
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()

    def merge_with_context(self, context):
        """
        Args:
            context(:obj:): Pipeline context object
        """
        m = context.observing_run.measurement_sets[0]


@task_registry.set_equivalent_casa_task('hifv_hanning')
class Hanning(basetask.StandardTaskTemplate):
    """Class for the hifv_hanning pipeline smoothing task.  Used on VLA measurement sets.

        The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = HanningInputs

    def prepare(self):
        """Method where the hanning smoothing operation is executed.

        The MS SPECTRAL_WINDOW table is examined to see if the SDM_NUM_BIN value is greater than 1.
        If the value is great than 1, then hanning smoothing does not proceed.

        The CASA task hanningsmooth() is executed on the data, creating a temporary measurement set (MS).
        The original MS is removed from disk, and the temporary MS is renamed to the original MS.
        An exception in thrown if an error occurs.

        Return:
            HanningResults() type object
        """

        if self._checkpreaveraged():
            if not self._executor._dry_run:
                try:
                    self._do_hanningsmooth()
                    LOG.info("Removing original VIS " + self.inputs.vis)
                    shutil.rmtree(self.inputs.vis)
                    LOG.info("Renaming temphanning.ms to " + self.inputs.vis)
                    os.rename('temphanning.ms', self.inputs.vis)
                except Exception as ex:
                    LOG.warn('Problem encountered with hanning smoothing. ' + str(ex))
        else:
            LOG.warn("Data in this MS are pre-averaged.  CASA task hanningsmooth() was not executed.")

        return HanningResults()

    def analyse(self, results):
        """Determine the best parameters by analysing the given jobs before returning any final jobs to execute.

        Override method of basetask.StandardTaskTemplate.analyze()

        Args:
            jobs (list of class: `~pipeline.infrastructure.jobrequest.JobRequest`):
                the job requests generated by :func:`~SimpleTask.prepare`

        Returns:
            class:`~pipeline.api.Result`
        """
        return results

    def _do_hanningsmooth(self):
        """Execute the CASA task hanningsmooth

        Return:
            Executor class
        """

        task = casa_tasks.hanningsmooth(vis=self.inputs.vis,
                                        datacolumn='data',
                                        outputvis='temphanning.ms')

        return self._executor.execute(task)

    def _checkpreaveraged(self):
        """Examine to see if the SDM_NUM_BIN value from the SPECTRAL_WINDOW table is greater than 1.

        Return: Boolean
            False if sdm_num_bin > 1; True otherwise
        """

        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            # effective_bw = table.getvarcol('EFFECTIVE_BW')
            # resolution = table.getvarcol('RESOLUTION')
            try:
                sdm_num_bin = table.getvarcol('SDM_NUM_BIN')
                max_sdm_num_bin = max([sdm_num_bin[key][0] for key in sdm_num_bin])
            except Exception as e:
                max_sdm_num_bin = 1
                LOG.debug('Column SDM_NUM_BIN was not found in the SDM.  Proceeding with hanning smoothing.')

        # return not(resolution['r1'][0][0] < effective_bw['r1'][0][0])

        if max_sdm_num_bin > 1:
            return False
        else:
            return True
