import os
import string

from casatasks.private import flaghelper

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.infrastructure.filenamer import sanitize_for_ms

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'Flagtargetsdata',
    'FlagtargetsdataInputs',
    'FlagtargetsdataResults'
]


class FlagtargetsdataInputs(vdp.StandardInputs):
    """Inputs class for the hifv_flagtargetsdata pipeline task.  Used on VLA measurement sets.

    The class inherits from vdp.StandardInputs.

        .. py:attribute:: context

        the (:class:`~pipeline.infrastructure.launcher.Context`) holding all
        pipeline state

    .. py:attribute:: vis

        a string or list of strings containing the MS name(s) on which to
        operate

    .. py:attribute:: output_dir

        the directory to which pipeline data should be sent

    .. py:attribute:: flagbackup

        a boolean indicating whether whether existing flags should be backed
        up before new flagging begins.

    .. py:attribute:: template

        A boolean indicating whether flagging templates are to be applied.

    .. py:attribute:: filetemplate

        The filename of the ASCII file that contains the flagging template
    """
    # Search order of input vis
    # PIPE-2313: removed continuum and line datatypes to be processed later
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    flagbackup = vdp.VisDependentProperty(default=True)
    template = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def filetemplate(self):
        vis_root = sanitize_for_ms(self.vis)
        return vis_root + '.flagtargetstemplate.txt'

    @filetemplate.convert
    def filetemplate(self, value):
        if isinstance(value, str):
            return list(value.replace('[', '').replace(']', '').replace("'", "").split(','))
        else:
            return value

    @vdp.VisDependentProperty
    def inpfile(self):
        vis_root = sanitize_for_ms(self.vis)
        return os.path.join(self.output_dir, vis_root + '.flagtargetscmds.txt')

    def __init__(
            self,
            context,
            vis=None,
            output_dir=None,
            flagbackup=None,
            template=None,
            filetemplate=None
            ):
        """
        Args:
            vis(str): String name of the pre-split measurement set
            output_dir(str):  Output directory
            flagbackup(bool):  Back up flags or not
            template(bool):  Used template or not
            filetemplate(str):  String filename of the flagging template to use; flags from
              template will be applied to all relevant MSes
        """

        super().__init__()

        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.flagbackup = flagbackup
        self.template = template
        self.filetemplate = filetemplate

    def to_casa_args(self, vis):
        """
        Translate the input parameters of this class to task parameters
        required by the CASA task flagdata. The returned object is a
        dictionary of flagdata arguments as keyword/value pairs.

        Args:
            vis(str): String name of the measurement set to be flagged

        Return:
            Dict: dictionary of CASA task inputs
        """
        return {'vis': vis,
                'mode': 'list',
                'action': 'apply',
                'inpfile': self.inpfile,
                'flagbackup': self.flagbackup}


class FlagtargetsdataResults(basetask.Results):
    """Results class for the hifv_flagtargetsdata pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.Results.

    """
    def __init__(self, summaries, flagcmds, mses=None):
        """
        Args:
            summaries(List):  Flagging summaries
            flagcmds(List):  List of string flagging commands
            mses(List): List of measurement sets that were processed
        """
        if mses is None:
            mses = []
        super().__init__()
        self.pipeline_casa_task = 'Flagtargetsdata'
        self.summaries = summaries
        self._flagcmds = flagcmds
        self.mses = mses

    def flagcmds(self):
        return self._flagcmds

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        pass

    def __repr__(self):
        # Step through the summary list and print a few things.
        # SUBTRACT flag counts from previous agents, because the counts are
        # cumulative.
        s = 'Flagtargetsdata flagging results:\n'

        # iterate through list of summaries
        for vis_idx, summary in enumerate(self.summaries):
            s += f'\t{self.mses[vis_idx].name}\n'
            for idx in range(0, len(summary)):
                flagcount = int(summary[idx]['flagged'])
                totalcount = int(summary[idx]['total'])

                # From the second summary onwards, subtract counts from the previous
                # one
                if idx > 0:
                    flagcount = flagcount - int(summary[idx-1]['flagged'])

                countper = 100.0*flagcount/totalcount
                s += (f"\t\tSummary {idx} ({summary[idx]['name']}) :  "
                      f"Flagged : {flagcount} out of {totalcount} ({countper:.2f}%)\n")

        return s


@task_registry.set_equivalent_casa_task('hifv_flagtargetsdata')
@task_registry.set_casa_commands_comment('Flagtargetsdata')
class Flagtargetsdata(basetask.StandardTaskTemplate):
    """Class for the hifv_flagtargetsdata pipeline task.  Used on VLA measurement sets.

    The class inherits from basetask.StandardTaskTemplate

    """
    Inputs = FlagtargetsdataInputs

    def prepare(self):

        inputs = self.inputs

        mses = self._create_mses(inputs)

        summaries = []
        flag_cmds_list = []
        for ms in mses:
            flag_cmds = self._get_flag_commands()
            flag_str = '\n'.join(flag_cmds)

            with open(inputs.inpfile, 'w') as stream:
                stream.writelines(flag_str)

            LOG.debug('Flag commands for %s:\n%s', ms.name, flag_str)

            # Map the pipeline inputs to a dictionary of CASA task arguments
            task_args = inputs.to_casa_args(ms.name)

            # create and execute a flagdata job using these task arguments
            job = casa_tasks.flagdata(**task_args)
            summary_dict = self._executor.execute(job)

            agent_summaries = dict((v['name'], v) for v in summary_dict.values())

            ordered_agents = ['before', 'template']

            summary_reps = [agent_summaries[agent]
                            for agent in ordered_agents
                            if agent in agent_summaries]
            summaries.append(summary_reps)
            flag_cmds_list.append(flag_cmds)

        return FlagtargetsdataResults(summaries=summaries, flagcmds=flag_cmds_list, mses=mses)

    def analyse(self, results):
        """
        Analyse the results of the flagging operation.

        This method does not perform any analysis, so the results object is
        returned exactly as-is, with no data massaging or results items
        added. If additional statistics needed to be calculated based on the
        post-flagging state, this would be a good place to do it.
        """
        return results

    def _get_flag_commands(self):
        """
        Get the flagging commands as a string suitable for flagdata.
        """
        # create a local variable for the inputs associated with this instance
        inputs = self.inputs

        # create list which will hold the flagging commands
        flag_cmds = ["mode='summary' name='before'"]

        # flag template?
        if inputs.template:
            if not os.path.exists(inputs.filetemplate):
                LOG.warning('Template flag file \'%s\' for \'%s\' not found.'
                            % (inputs.filetemplate, inputs.ms.basename))
            else:
                flag_cmds.extend(self._read_flagfile(inputs.filetemplate))
            flag_cmds.append("mode='summary' name='template'")

        return flag_cmds

    @staticmethod
    def _read_flagfile(filename):
        if not os.path.exists(filename):
            LOG.warning('%s does not exist' % filename)
            return []

        # strip out comments and empty lines to leave the real commands.
        # This is so we can compare the number of valid commands to the number
        # of commands specified in the file and complain if they differ
        return [cmd for cmd in flaghelper.readFile(filename)
                if not cmd.strip().startswith('#')
                and not all(c in string.whitespace for c in cmd)]

    def _create_mses(self, inputs):
        """
        Create the MS list for multi-ms processing; checks for various datatypes
        """

        mses = []
        for dtype in [DataType.REGCAL_CONT_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE]:
            ms = inputs.context.observing_run.get_measurement_sets_of_type([dtype])
            if ms:
                mses.append(ms[0])

        return mses
