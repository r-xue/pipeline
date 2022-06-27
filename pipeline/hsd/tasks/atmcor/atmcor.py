"""Offline ATM correction stage."""
import os
from typing import List, Optional, Union

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.h.heuristics import fieldnames
from pipeline.hsd.tasks.common.inspection_util import generate_ms, inspect_reduction_group, merge_reduction_group
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.utils import relative_path
from .. import common

LOG = logging.get_logger(__name__)


class SDATMCorrectionInputs(vdp.StandardInputs):
    """Inputs class for SDATMCorrection task."""
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    atmtype = vdp.VisDependentProperty(default=1)
    dtem_dh = vdp.VisDependentProperty(default=-5.6)
    h0 = vdp.VisDependentProperty(default=2.0)
    intent = vdp.VisDependentProperty(default='TARGET')

    @atmtype.convert
    def atmtype(self, value: Union[int, str]) -> int:
        """Convert atmtype into int.

        Args:
            value: atmtype value

        Returns:
            atmtype in integer
        """
        if isinstance(value, str):
            value = int(value)
        return value

    @vdp.VisDependentProperty
    def infiles(self) -> str:
        """Return infiles.

        infiles is an alias of vis

        Returns:
            infiles string
        """
        return self.vis

    @infiles.convert
    def infiles(self, value: str) -> str:
        """Update infiles and vis consistently.

        Args:
            value: new infiles value

        Returns:
            input value
        """
        self.vis = value
        return value

    @vdp.VisDependentProperty
    def antenna(self) -> str:
        """Return antenna selection.

        By default, empty string (all antennas) is returned.

        Returns:
            antenna selection string
        """
        return ''

    @antenna.convert
    def antenna(self, value: str) -> str:
        """Convert antenna selection string.

        Args:
            value: input antenna selection

        Returns:
            converted antenna selection
        """
        antennas = self.ms.get_antenna(value)
        # if all antennas are selected, return ''
        if len(antennas) == len(self.ms.antennas):
            return ''
        return utils.find_ranges([a.id for a in antennas])

    @vdp.VisDependentProperty
    def field(self) -> str:
        """Return field selection string.

        By default, only fields that matches given intent are returned.

        Returns:
            field selection string
        """
        # this will give something like '0542+3243,0343+242'
        field_finder = fieldnames.IntentFieldnames()
        intent_fields = field_finder.calculate(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    @vdp.VisDependentProperty
    def spw(self) -> str:
        """Return spw selection string.

        By default, channelized spws are returned.

        Returns:
            spw selection string
        """
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @vdp.VisDependentProperty
    def pol(self) -> str:
        """Return pol selection string.

        By default, polarizatons corresponding to selected spws are selected.

        Returns:
            pol selecton string
        """
        # filters polarization by self.spw
        selected_spwids = [int(spwobj.id) for spwobj in self.ms.get_spectral_windows(self.spw, with_channels=True)]
        pols = set()
        for idx in selected_spwids:
            pols.update(self.ms.get_data_description(spw=idx).corr_axis)

        return ','.join(pols)

    def __init__(self,
                 context: Context,
                 atmtype: Optional[Union[int, str, List[int], List[str]]] =None,
                 dtem_dh: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] =None,
                 h0: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] =None,
                 infiles: Optional[Union[str, List[str]]] =None,
                 antenna: Optional[Union[str, List[str]]] =None,
                 field: Optional[Union[str, List[str]]] =None,
                 spw: Optional[Union[str, List[str]]] =None,
                 pol: Optional[Union[str, List[str]]] =None):
        """Initialize Inputs instance for hsd_atmcor.

        Args:
            context: pipeline context
            atmtype: enumeration for atmospheric transmission model
            dtem_dh: temperature gradient [K/km]
            h0: scale height for water [km]. Defaults to None.
            infiles: MS selection. Defaults to None.
            antenna: antenna selection. Defaults to None.
            field: field selection. Defaults to None.
            spw: spw selection. Defaults to None.
            pol: polarization selection. Defaults to None.
        """
        super().__init__()

        self.context = context
        self.atmtype = atmtype
        self.dtem_dh = dtem_dh
        self.h0 = h0
        self.infiles = infiles
        self.antenna = antenna
        self.field = field
        self.spw = spw
        self.pol = pol

    def _identify_datacolumn(self, vis: str) -> str:
        """Identify data column.

        Args:
            vis: MS name

        Raises:
            Exception: no datacolumn exists

        Returns:
            datacolumn parameter
        """
        datacolumn = ''
        with casa_tools.TableReader(vis) as tb:
            colnames = tb.colnames()

        names = (('CORRECTED_DATA', 'corrected'),
                 ('FLOAT_DATA', 'float_data'),
                 ('DATA', 'data'))
        for name, value in names:
            if name in colnames:
                datacolumn = value
                break

        if len(datacolumn) == 0:
            raise Exception('No datacolumn is found.')

        return datacolumn

    def get_caltable_from_callibrary(self) -> str:
        """Retrieve k2jycal caltable name from callibrary.

        Returns:
            Name of the caltable.
            Return empty string if k2jycal caltable is not applied.
        """
        applied_state = self.context.callibrary.applied
        calto = callibrary.CalTo(vis=self.vis)
        state_for_vis = applied_state.trimmed(self.context, calto)
        caltables = state_for_vis.get_caltable(caltypes=('amp', 'gaincal'))

        k2jycal_caltable = ''
        if len(caltables) > 0:
            k2jycal_caltable = caltables.pop()

        return k2jycal_caltable

    def get_gainfactor(self) -> Union[float, str]:
        """Retrieve k2jycal table from callibrary.

        Returns:
            name of the k2jycal table or 1.0
        """
        k2jycal_caltable = self.get_caltable_from_callibrary()
        gainfactor = 1.0
        if k2jycal_caltable:
            gainfactor = k2jycal_caltable
        return gainfactor

    def to_casa_args(self) -> dict:
        """Return task arguments for sdatmcor.

        Returns:
            task arguments for sdatmcor
        """
        args = super().to_casa_args()

        # infile
        args.pop('infiles', None)
        infile = args.pop('vis')
        args['infile'] = infile

        # datacolumn
        args['datacolumn'] = self._identify_datacolumn(infile)

        # outfile
        if 'outfile' not in args:
            basename = os.path.basename(infile.rstrip('/'))
            suffix = '.atmcor.atmtype{}'.format(args['atmtype'])
            outfile = basename + suffix
            args['outfile'] = relative_path(os.path.join(self.output_dir, outfile))

        # ganfactor
        args['gainfactor'] = self.get_gainfactor()

        # overwrite is always True
        args['overwrite'] = True

        # spw -> outputspw
        args['outputspw'] = args.pop('spw', '')

        # pol -> correlation
        args['correlation'] = args.pop('pol', '')

        # correlation selection should be empty
        # to avoid strange error in VI/VB2 framework
        args['correlation'] = ''

        # intent should include OFF_SOURCE data (for validation purpose)
        # TODO: remove it after PIPE-1062 is implemented
        args['intent'] = 'OBSERVE_TARGET#ON_SOURCE'

        return args


class SDATMCorrectionResults(common.SingleDishResults):
    """Results instance for hsd_atmcor."""

    def __init__(self,
                 task: Optional[basetask.StandardTaskTemplate] =None,
                 success: Optional[bool] =None,
                 outcome: Optional[str] =None):
        """Initialize results instance for hsd_atmcor.

        Args:
            task: task class. Defaults to None.
            success: task execution was successful or not. Defaults to None.
            outcome: outcome of the task execution. name of the output MS. Defaults to None.
        """
        super().__init__(task, success, outcome)
        # outcome is the name of output file from sdatmcor
        self.atmcor_ms_name = outcome
        self.out_mses = []

    def merge_with_context(self, context: Context):
        """Merge execution result of atmcor stage into pipeline context.

        Args:
            pipeline context
        """
        super().merge_with_context(context)

        # register output MS domain object and reduction_group to context
        target = context.observing_run
        for ms in self.out_mses:
            # remove existing MS in context if the same MS is already in list.
            oldms_index = None
            for index, oldms in enumerate(target.get_measurement_sets()):
                if ms.name == oldms.name:
                    oldms_index = index
                    break
            if oldms_index is not None:
                LOG.info('Replace {} in context'.format(ms.name))
                del target.measurement_sets[oldms_index]

            # Adding mses to context
            LOG.info('Adding {} to context'.format(ms.name))
            target.add_measurement_set(ms)
            # Initialize callibrary
            calto = callibrary.CalTo(vis=ms.name)
            LOG.info('Registering {} with callibrary'.format(ms.name))
            context.callibrary.add(calto, [])
            # register output MS to processing group
            reduction_group = inspect_reduction_group(ms)
            merge_reduction_group(target, reduction_group)

    def _outcome_name(self) -> str:
        """Return representative string for the outcome.

        Any string that represents outcome is returned.
        In case of hsd_atmcor, output MS name is returned.

        Returns:
            output MS name for sdatmcor
        """
        return os.path.basename(self.atmcor_ms_name)


@task_registry.set_equivalent_casa_task('hsd_atmcor')
@task_registry.set_casa_commands_comment(
    'Apply offline correction of atmospheric transmission model.'
)
class SerialSDATMCorrection(basetask.StandardTaskTemplate):
    """Offline ATM correction task."""

    Inputs = SDATMCorrectionInputs

    def prepare(self) -> SDATMCorrectionResults:
        """Execute task and produce results instance.

        Raises:
            Exception: execution of sdatmcor was failed

        Returns:
            results instance for hsd_atmcor stage
        """
        args = self.inputs.to_casa_args()
        LOG.info('Processing parameter for sdatmcor: %s', args)
        job = casa_tasks.sdatmcor(**args)
        task_exec_status = self._executor.execute(job)
        LOG.info('atmcor: task_exec_status = %s', task_exec_status)

        if not os.path.exists(args['outfile']):
            raise Exception('Output MS does not exist. It seems sdatmcor failed.')

        if task_exec_status is None:
            # no news is good news, this is a sign of success
            is_successful = True
        elif task_exec_status is False:
            # it indicates any problem
            is_successful = False
        else:
            # unexpected, mark as failed
            is_successful = False

        results = SDATMCorrectionResults(
            task=self.__class__,
            success=is_successful,
            outcome=args['outfile']
        )

        return results

    def analyse(self, result: SDATMCorrectionResults) -> SDATMCorrectionResults:
        """Analyse results produced by prepare method.

        Do nothing at this moment.

        Args:
            result: results instance

        Returns:
            input results instance
        """
        # Generate domain object of baselined MS
        in_ms = self.inputs.ms
        new_ms = generate_ms(result.atmcor_ms_name, in_ms)
        new_ms.set_data_column(DataType.ATMCORR, 'DATA')
        result.out_mses.append(new_ms)
        return result


### Tier-0 parallelization
class HpcSDATMCorrectionInputs(SDATMCorrectionInputs):
    """Inputs for parallel implementation of offline ATM correction."""

    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self,
                 context: Context,
                 atmtype: Optional[Union[int, str, List[int], List[str]]] =None,
                 dtem_dh: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] =None,
                 h0: Optional[Union[float, str, dict, List[float], List[str], List[dict]]] =None,
                 infiles: Optional[Union[str, List[str]]] =None,
                 antenna: Optional[Union[str, List[str]]] =None,
                 field: Optional[Union[str, List[str]]] =None,
                 spw: Optional[Union[str, List[str]]] =None,
                 pol: Optional[Union[str, List[str]]] =None,
                 parallel: Optional[bool] =None):
        """Initialize Inputs instance for hsd_atmcor.

        Args:
            context: pipeline context
            atmtype: enumeration for atmospheric transmission model
            dtem_dh: temperature gradient [K/km]
            h0: scale height for water [km]. Defaults to None.
            infiles: MS selection. Defaults to None.
            antenna: antenna selection. Defaults to None.
            field: field selection. Defaults to None.
            spw: spw selection. Defaults to None.
            pol: polarization selection. Defaults to None.
            parallel: enable Tier-0 parallelization or not. Defaults to None.
        """
        super().__init__(context, atmtype, infiles, antenna, field, spw, atmtype)
        self.parallel = parallel


# @task_registry.set_equivalent_casa_task('hsd_atmcor')
# @task_registry.set_casa_commands_comment(
#     'Apply offline correction of atmospheric transmission model.'
# )
class HpcSDATMCorrection(sessionutils.ParallelTemplate):
    """Parallel implementation of offline ATM correction task."""

    Inputs = HpcSDATMCorrectionInputs
    Task = SerialSDATMCorrection

    def __init__(self, inputs: HpcSDATMCorrectionInputs):
        """Initialize parallel ATM correction task.

        Args:
            inputs instance for parallel ATM correction task
        """
        super().__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis: str, exception: Exception) -> basetask.FailedTaskResults:
        """Produce failed task results.

        Args:
            vis: name of the MS
            exception: original exception

        Returns:
            FailedTaskResults instance
        """
        LOG.error('Error operating target flag for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)
