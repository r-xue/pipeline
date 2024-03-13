import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.domain.measures import FrequencyUnits

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

        def create_message(smoothing_windows):
            message_lists = [[smoothing_windows[0]]]
            for x in smoothing_windows[1:]:
                if int(x) != int(message_lists[-1][-1]) + 1:
                    message_lists.append([])
                message_lists[-1].append(x)
            message = ""
            for message_list in message_lists:
                if len(message_list) > 1:
                    message += "{}~{}, ".format(message_list[0], message_list[-1])
                else:
                    message += message_list[0] + ", "
            if message[-2:] == ", ":
                message = message[:-2]
            message = message.replace("~", "-")
            message = ", and".join(message.rsplit(",", 1))
            return message

        spw_preaverage = self._getpreaveraged()
        if not spw_preaverage:
            LOG.info("All spectral windows were selected for hanning smoothing")
            try:
                self._do_hanningsmooth()
                LOG.info("Removing original VIS " + self.inputs.vis)
                shutil.rmtree(self.inputs.vis)
                LOG.info("Renaming temphanning.ms to " + self.inputs.vis)
                os.rename('temphanning.ms', self.inputs.vis)
            except Exception as ex:
                LOG.warning('Problem encountered with hanning smoothing. ' + str(ex))
            finally:
                smoothing_windows = self.inputs.vis.spectral_windows
        else:
            smoothing_windows = self._getsmoothingwindows(spw_preaverage)
            if smoothing_windows:
                message = create_message(smoothing_windows)
                LOG.info("Smoothing spectral window(s) {}.".format(message))
                with casa_tools.MSReader(self.inputs.vis, nomodify=False) as ms:
                    staql = {'spw': ",".join(smoothing_windows)}
                    ms.msselect(staql)
                    ms.hanningsmooth('data')
            else:
                LOG.info("None of the SPWs were selected for smoothing.")
        if smoothing_windows:
            self._track_hsmooth(smoothing_windows)

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
    
    def _getsmoothingwindows(self, spw_preaverage):
        """Retrieve a list of windows that are not pre-averaged and should be hanning-smoothed

        Args:
            spw_preaverage(dict): SDM_NUM_BIN column from SPECTRAL_WINDOW table of vis

        Return: List
            Spectral window IDs that need to be hanning smoothed; an empty list will mean no windows should be smoothed
        """

        smooth_windows = list()
        for key, value in spw_preaverage.items():
            spw = str(int(key.split("r")[1]) - 1)
            self._checkmaserline(spw)
            if value > 1:
                # smooth preaveraged windows if maser line possible in window to avoid Gibbs ringing
                if self._checkmaserline(spw):
                    smooth_windows.append(spw)
            else:
                smooth_windows.append(spw)

        return sorted(smooth_windows, key=int)

    def _getpreaveraged(self):
        """Return SDM_NUM_BIN table row if it exists. Empty dict signifies to smooth all windows.

        Return: Dict
        """

        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            try:
                sdm_num_bin = table.getvarcol('SDM_NUM_BIN')
            except Exception as e:
                sdm_num_bin = dict()
                LOG.debug('Column SDM_NUM_BIN was not found in the SDM.')

        return sdm_num_bin

    def _checkmaserline(self, spw: str):
        """Confirm if known maser line(s) appear in frequency range of spectral window

        Args:
            spw(dict): spectral window number

        Return: Boolean
            True if maser line may exist in window; False otherwise
        """

        def freq_to_vel(rest_freq, obs_freq):
            c_kms = 2.99792458e5
            return ((rest_freq - obs_freq) / rest_freq) * c_kms

        maser_dict = {
            'OH (1)': 1612231000,
            'OH (2)': 1665401800,
            'OH (3)': 1667359000,
            'OH (4)': 1720530000,
            'H2O': 22235080000,
            'CH3OH (1)': 6668519200,
            'CH3OH (2)': 1217859700,
            'SiOv0': 43423858000,
            'SiOv1': 43122079000,
            'SiOv2': 42820582000,
            'SiOv3': 42519373000,
            '29SiOv0': 42879916000,
            '30SiOv0': 42373359000,
            'SiS': 18154880000,
        }

        qaTool = casa_tools.quanta
        suTool = casa_tools.synthesisutils

        for ms in self.inputs.context.observing_run.measurement_sets:
            if ms.name == self.inputs.vis:
                ms_info = ms
        
        spw_info = ms_info.get_spectral_window(spw)
        freq_low = spw_info._min_frequency.convert_to(newUnits=FrequencyUnits.HERTZ).value
        freq_high = spw_info._max_frequency.convert_to(newUnits=FrequencyUnits.HERTZ).value
        if spw_info._ref_frequency_frame != 'TOPO':
            LOG.info("Spectral window reference frame not TOPO. Skipping maser detection.")
            return False

        to_lsrk = suTool.advisechansel(msname=ms_info.name, spwselection=spw, getfreqrange=True, freqframe='LSRK')
        freq_low = float(qaTool.getvalue(qaTool.convert(to_lsrk['freqstart'], 'Hz')))
        freq_high = float(qaTool.getvalue(qaTool.convert(to_lsrk['freqend'], 'Hz')))

        for value in maser_dict.values():
            vel_low = freq_to_vel(value, freq_low)
            vel_high = freq_to_vel(value, freq_high)
            if abs(vel_low) <= 200 or abs(vel_high) <= 200:
                LOG.info("Maser line possible in spw {}. Hanning smoothing will be applied.".format(spw))
                return True
        return False

    def _track_hsmooth(self, spws: list):
        LOG.info("Writing Hanning smoothing information to SPECTRAL_WINDOW table of MS.")
        desc = {'OFFLINE_HANNING_SMOOTH': {'comment': 'Offline Hanning Smooth Flag',
                                   'dataManagerGroup': 'StandardStMan',
                                   'dataManagerType': 'StandardStMan',
                                   'keywords': {"SPW_IDs": spws},
                                   'maxlen': 0,
                                   'option': 0,
                                   'valueType': 'int'}}
        with casa_tools.TableReader(self.inputs.vis + "/SPECTRAL_WINDOW", nomodify=False) as tb:
            tb.addcols(desc)
