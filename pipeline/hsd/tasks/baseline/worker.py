"""Worker task for baseline subtraction."""
import abc
import os

from typing import TYPE_CHECKING, Any, List, Optional, Type, Union

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
from pipeline.infrastructure.utils import relative_path
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataTable, DataType
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.hsd.heuristics import CubicSplineFitParamConfig
from pipeline.hsd.tasks.common import utils as sdutils
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from . import plotter
from .. import common

if TYPE_CHECKING:
    import numpy as np

    from pipeline.infrastructure.api import Heuristic
    from pipeline.infrastructure.launcher import Context
    from pipeline.hsd.tasks.common.utils import RGAccumulator

LOG = infrastructure.get_logger(__name__)


class BaselineSubtractionInputsBase(vdp.StandardInputs):
    """Base class of inputs for baseline subtraction task."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    DATACOLUMN = {'CORRECTED_DATA': 'corrected',
                  'DATA': 'data',
                  'FLOAT_DATA': 'float_data'}

    @vdp.VisDependentProperty
    def colname(self) -> str:
        """Return name of existing data column in MS.

        The following column names are examined in order, and return
        the name if the column exists in MS. If multiple columns exist,
        the following list shows the priority.

            - CORRECTED_DATA
            - DATA
            - FLOAT_DATA

        For example, if MS has CORRECTED_DATA and DATA columns,
        CORRECTED_DATA will be returned.

        Returns:
            Data column name
        """
        colname = ''
        if isinstance(self.vis, str):
            with casa_tools.TableReader(self.vis) as tb:
                candidate_names = ['CORRECTED_DATA',
                                   'DATA',
                                   'FLOAT_DATA']
                for name in candidate_names:
                    if name in tb.colnames():
                        colname = name
                        break
        return colname

    def to_casa_args(self) -> dict:
        """Convert Inputs instance to the list of keyword arguments for sdbaseline.

        Note that core parameters such as blfunc will be set dynamically through
        the heuristics or inside task.

        Returns:
            Keyword arguments for sdbaseline
        """
        args = super(BaselineSubtractionInputsBase, self).to_casa_args()  # {'vis': self.vis}
        prefix = os.path.basename(self.vis.rstrip('/'))

        # blparam
        if self.blparam is None or len(self.blparam) == 0:
            args['blparam'] = relative_path(os.path.join(self.output_dir, prefix + '_blparam.txt'))
        else:
            args['blparam'] = self.blparam

        # baseline caltable filename
        if self.bloutput is None or len(self.bloutput) == 0:
            namer = caltable_heuristic.SDBaselinetable()
            bloutput = relative_path(namer.calculate(output_dir=self.output_dir,
                                                            stage=self.context.stage,
                                                            **args))
            args['bloutput'] = bloutput
        else:
            args['bloutput'] = self.bloutput

        # outfile
        if ('outfile' not in args or
                args['outfile'] is None or
                len(args['outfile']) == 0):
            args['outfile'] = relative_path(os.path.join(self.output_dir, prefix + '_bl'))

        args['datacolumn'] = self.DATACOLUMN[self.colname]

        return args


class BaselineSubtractionResults(common.SingleDishResults):
    """Results class to hold the result of baseline subtraction."""

    def __init__(self,
                 task: Optional[Type[basetask.StandardTaskTemplate]] = None,
                 success: Optional[bool] = None,
                 outcome: Any = None) -> None:
        """Construct BaselineSubtractionResults instance.

        Args:
            task: Task class that produced the result.
            success: Whether task execution is successful or not.
            outcome: Outcome of the task execution.
        """
        super(BaselineSubtractionResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context: 'Context') -> None:
        """Merge result instance into context.

        No specific merge operation is done.

        Args:
            context: Pipeline context.
        """
        super(BaselineSubtractionResults, self).merge_with_context(context)

    def _outcome_name(self) -> str:
        """Return string representation of outcome.

        Returns:
            Name of the blparam file with its format
        """
        # outcome should be a name of blparam text file
        return 'blparam: "%s" bloutput: "%s"' % (self.outcome['blparam'], self.outcome['bloutput'])


class BaselineSubtractionWorkerInputs(BaselineSubtractionInputsBase):
    """Inputs class for baseline subtraction tasks."""

    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    vis = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    plan = vdp.VisDependentProperty(default=None)
    fit_order = vdp.VisDependentProperty(default='automatic')
    switchpoly = vdp.VisDependentProperty(default=True)
    edge = vdp.VisDependentProperty(default=(0, 0))
    deviationmask = vdp.VisDependentProperty(default={})
    bloutput = vdp.VisDependentProperty(default=None)
    org_directions_dict = vdp.VisDependentProperty(default=None)

    @vdp.VisDependentProperty
    def prefix(self) -> str:
        """Return the prefix for several output files of sdbaseline.

        Prefix is the basename of the MS.

        Returns:
            Prefix string
        """
        return os.path.basename(self.vis.rstrip('/'))

    @vdp.VisDependentProperty
    def blparam(self) -> str:
        """Return blparam file name.

        Name is constructed from input MS name.

        Returns:
            The blparam file name
        """
        return self.prefix + '_blparam.txt'

    @vdp.VisDependentProperty(readonly=True)
    def field(self) -> List[int]:
        """Return list of field ids to process.

        Returned list should conform with the list of MS and
        each field id is translated into the one for corresponding
        MS in the list.

        Returns:
            List of field ids to process
        """
        return self.plan.get_field_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def antenna(self) -> List[int]:
        """Return list of antenna ids to process.

        Returned list should conform with the list of MS and
        each antenna id is translated into the one for corresponding
        MS in the list.

        Returns:
            List of antenna ids to process
        """
        return self.plan.get_antenna_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def spw(self) -> List[int]:
        """Return list of spectral window (spw) ids to process.

        Returned list should conform with the list of MS and
        each spw id is translated into the one for corresponding
        MS in the list.

        Returns:
            List of spw ids to process
        """
        return self.plan.get_spw_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def grid_table(self) -> List[Union[int, float, 'np.ndarray']]:
        """Return list of grid tables to process.

        Returned list should conform with the list of MS and
        each grid table is supposed to be processed together
        with corresponding MS in the list.

        Returns:
            List of grid tables to process
        """
        return self.plan.get_grid_table_list()

    @vdp.VisDependentProperty(readonly=True)
    def channelmap_range(self) -> List[List[List[Union[int, bool]]]]:
        """Return list of line ranges to process.

        Returned list should conform with the list of MS and
        each channelmap range is supposed to be processed together
        with corresponding MS in the list.

        Returns:
            List of channelmap ranges to process
        """
        return self.plan.get_channelmap_range_list()

    def __init__(
        self,
        context: 'Context',
        vis: Optional[Union[str, List[str]]] = None,
        plan: Optional[Union['RGAccumulator', List['RGAccumulator']]] = None,
        fit_order: Optional[int] = None,
        switchpoly: Optional[bool] = None,
        edge: Optional[List[int]] = None,
        deviationmask: Optional[Union[dict, List[dict]]] = None,
        blparam: Optional[Union[str, List[str]]] = None,
        bloutput: Optional[Union[str, List[str]]] = None,
        org_directions_dict: Optional[dict] = None
    ) -> None:
        """Construct BaselineSubtractionWorkerInputs instance.

        Args:
            context: Pipeline context
            vis: Name of the MS or list of MSs. Defaults to None,
                 which is to process all MSs registered to the context.
            plan: Set of metadata for baseline subtraction, or List of
                  the them. Defaults to None. The task may fail if None
                  is given.
            fit_order: Baseline fitting order. Defaults to None, which
                       is to perform heuristics for fitting order.
            switchpoly: Whther or not fall back to low order polynomial
                        fit when large mask exist at the edge of spw.
                        Defaults to True if None is given.
            edge: Edge channels to exclude. Defaults to None, which means
                  that all channels are processed.
            deviationmask: List of deviation masks. Defaults to empty list
                           if None is given.
            blparam: Name of the blparam file name. Defaults to
                     '{name_of_ms}_blparam.txt' if None is given.
            bloutput: Name of the bloutput name. Defaults to the name
                      following pipeline product naming convention
                      (see to_casa_args method) if None is given.
            org_directions_dict: Original source direction for ephemeris
                                 correction. Defaults to None. This is
                                 required only when target source is
                                 ephemeris object.
        """
        super(BaselineSubtractionWorkerInputs, self).__init__()

        self.context = context
        self.vis = vis
        self.plan = plan
        self.fit_order = fit_order
        self.switchpoly = switchpoly
        self.edge = edge
        self.deviationmask = deviationmask
        self.blparam = blparam
        self.bloutput = bloutput
        self.org_directions_dict = org_directions_dict

# Base class for workers
class BaselineSubtractionWorker(basetask.StandardTaskTemplate):
    """Abstract worker class for baseline subtraction."""

    Inputs = BaselineSubtractionWorkerInputs

    @abc.abstractproperty
    def Heuristics(self) -> Type['Heuristic']:
        """Return heuristics class.

        This abstract property must be implemented in each subclass.

        Returns:
            A reference to the :class:`Heuristic` class.
        """
        raise NotImplementedError

    is_multi_vis_task = False

    def __init__(self, inputs: BaselineSubtractionWorkerInputs):
        """Construct BaselineSubtractionWorker instance.

        Args:
            inputs: BaselineSubtractionWorkerInputs instance
        """
        super(BaselineSubtractionWorker, self).__init__(inputs)

        # initialize plotter
        self.datatable = DataTable(sdutils.get_data_table_path(self.inputs.context,
                                                               self.inputs.ms))

    def prepare(self) -> BaselineSubtractionResults:
        """Perform baseline subtraction.

        Call sdbaseline task with optimized parameters. Parameter values such
        as function type, fitting order, etc. are optimized by the heuristics
        class defined in Heuristics attribute.

        Returns:
            BaselineSubtractionResults instance
        """
        vis = self.inputs.vis
        ms = self.inputs.ms
        origin_ms = self.inputs.context.observing_run.get_ms(ms.origin_ms)
        rowmap = sdutils.make_row_map_between_ms(origin_ms, vis)
        fit_order = self.inputs.fit_order
        edge = self.inputs.edge
        args = self.inputs.to_casa_args()
        blparam = args['blparam']
        bloutput = args['bloutput']
        outfile = args['outfile']
        datacolumn = args['datacolumn']

        process_list = self.inputs.plan
        deviationmask_list = self.inputs.deviationmask
        LOG.info('deviationmask_list={}'.format(deviationmask_list))

        field_id_list = self.inputs.field
        antenna_id_list = self.inputs.antenna
        spw_id_list = self.inputs.spw
        LOG.debug('subgroup member for %s:\n\tfield: %s\n\tantenna: %s\n\tspw: %s',
                  ms.basename,
                  field_id_list,
                  antenna_id_list,
                  spw_id_list)

        # initialization of blparam file
        # blparam file needs to be removed before starting iteration through
        # reduction group
        if os.path.exists(blparam):
            LOG.debug('Cleaning up blparam file for %s', vis)
            os.remove(blparam)

        #datatable = DataTable(context.observing_run.ms_datatable_name)

        for (field_id, antenna_id, spw_id) in process_list.iterate_id():
            if (field_id, antenna_id, spw_id) in deviationmask_list:
                deviationmask = deviationmask_list[(field_id, antenna_id, spw_id)]
            else:
                deviationmask = None
            blparam_heuristic = self.Heuristics(switchpoly=self.inputs.switchpoly)
            formatted_edge = list(common.parseEdge(edge))
            out_blparam = blparam_heuristic(self.datatable, ms, rowmap,
                                            antenna_id, field_id, spw_id,
                                            fit_order, formatted_edge,
                                            deviationmask, blparam)
            assert out_blparam == blparam

        # execute sdbaseline
        job = casa_tasks.sdbaseline(infile=vis, datacolumn=datacolumn, blmode='fit', dosubtract=True,
                                    blformat='table', bloutput=bloutput,
                                    blfunc='variable', blparam=blparam,
                                    outfile=outfile, overwrite=True)
        self._executor.execute(job)

        outcome = {'infile': vis,
                   'blparam': blparam,
                   'bloutput': bloutput,
                   'outfile': outfile}
        results = BaselineSubtractionResults(success=True, outcome=outcome)
        return results

    def analyse(self, results: BaselineSubtractionResults) -> BaselineSubtractionResults:
        """Generate plots from baseline subtraction results.

        Args:
            results: BaselineSubtractionResults instance

        Raises:
            RuntimeError: Source name is invalid or not found in the domain object

        Returns:
            BaselineSubtractionResults instance
        """
        # plot
        # initialize plot manager
        ms = self.inputs.ms
        outfile = results.outcome['outfile']
        plot_manager_base = plotter.BaselineSubtractionPlotManagerBase(ms, outfile, self.inputs.context, self.datatable)
        plot_manager = plotter.BaselineSubtractionPlotManager(ms, outfile, self.inputs.context, self.datatable)
        quality_manager = plotter.BaselineSubtractionQualityManager(ms, outfile, self.inputs.context, self.datatable)

        org_directions_dict = self.inputs.org_directions_dict
        accum = self.inputs.plan
        deviationmask_list = self.inputs.deviationmask
        LOG.info('deviationmask_list={}'.format(deviationmask_list))

        formatted_edge = list(common.parseEdge(self.inputs.edge))
        status = plot_manager_base.initialize()

        plot_list = []
        stats = []
        for (field_id, antenna_id, spw_id, grid_table, channelmap_range) in accum.iterate_all():

            LOG.info('field %s antenna %s spw %s', field_id, antenna_id, spw_id)
            if (field_id, antenna_id, spw_id) in deviationmask_list:
                deviationmask = deviationmask_list[(field_id, antenna_id, spw_id)]
            else:
                deviationmask = None

            if status:
                fields = ms.get_fields(field_id=field_id)
                source_name = fields[0].source.name
                if source_name not in org_directions_dict:
                    raise RuntimeError("source_name {} not found in org_directions_dict (sources found are {})"
                                       "".format(source_name, list(org_directions_dict.keys())))
                org_direction = org_directions_dict[source_name]
                plot_list.extend(plot_manager.plot_spectra_with_fit(field_id, antenna_id, spw_id,
                                                                    org_direction,
                                                                    grid_table,
                                                                    deviationmask, channelmap_range, formatted_edge))
                stats.extend(quality_manager.calculate_baseline_quality_stat(field_id, antenna_id, spw_id, 
                                                                          org_direction,
                                                                          grid_table, 
                                                                          deviationmask, channelmap_range, formatted_edge)) ###
        plot_manager_base.finalize()

        results.outcome['plot_list'] = plot_list
        results.outcome['baseline_quality_stat'] = stats
        return results


# Worker class for cubic spline fit
class CubicSplineBaselineSubtractionWorker(BaselineSubtractionWorker):
    """Worker class of cspline baseline subtraction.

    This is an implementation of BaselineSubtractionWorker class based on
    segmented cubic spline fitting. Heuristics property returns
    CubicSplineFitParamConfig heuristics class.
    """

    Inputs = BaselineSubtractionWorkerInputs
    Heuristics = CubicSplineFitParamConfig


# Tier-0 Parallelization
class HpcBaselineSubtractionWorkerInputs(BaselineSubtractionWorkerInputs):
    """Variant of BaselineSubtractionWorkerInputs for parallel processing."""

    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(
        self,
        context: 'Context',
        vis: Optional[Union[str, List[str]]] = None,
        plan: Optional[Union['RGAccumulator', List['RGAccumulator']]] = None,
        fit_order: Optional[int] = None,
        switchpoly: Optional[bool] = None,
        edge: Optional[List[int]] = None,
        deviationmask: Optional[Union[dict, List[dict]]] = None,
        blparam: Optional[Union[str, List[str]]] = None,
        bloutput: Optional[Union[str, List[str]]] = None,
        org_directions_dict: Optional[dict] = None,
        parallel: Optional[bool] = None
    ) -> None:
        """Construct HPCBaselineSubtractionWorkerInputs instance.

        Args:
            context: Pipeline context
            vis: Name of the MS or list of MSs. Defaults to None,
                 which is to process all MSs registered to the context.
            plan: Set of metadata for baseline subtraction, or List of
                  the set. Defaults to None. The task may fail if None
                  is given.
            fit_order: Baseline fitting order. Defaults to None, which
                       is to perform heuristics for fitting order.
            switchpoly: Whther or not fall back to low order polynomial
                        fit when large mask exist at the edge of spw.
                        Defaults to True if None is given.
            edge: Edge channels to exclude. Defaults to None, which means
                  that all channels are processed.
            deviationmask: List of deviation masks. Defaults to empty list
                           if None is given.
            blparam: Name of the blparam file name. Defaults to
                     '{name_of_ms}_blparam.txt' if None is given.
            bloutput: Name of the bloutput name. Defaults to the name
                      following pipeline product naming convention
                      (see to_casa_args method) if None is given.
            org_directions_dict: Original source direction for ephemeris
                                 correction. Defaults to None. This is
                                 required only when target source is
                                 ephemeris object.
            parallel: Turn on/off parallal processing. Defaults to None.
                      If None is given, do parallel processing only when
                      pipeline runs on mpicasa environment.
        """
        super(HpcBaselineSubtractionWorkerInputs, self).__init__(context, vis=vis, plan=plan,
                                                                 fit_order=fit_order, switchpoly=switchpoly,
                                                                 edge=edge, deviationmask=deviationmask,
                                                                 blparam=blparam, bloutput=bloutput,
                                                                 org_directions_dict=org_directions_dict)
        self.parallel = parallel


# This is abstract class since Task is not specified yet
class HpcBaselineSubtractionWorker(sessionutils.ParallelTemplate):
    """Template class for parallel baseline subtraction task.

    This class is a template for parallel processing that executes
    the task specified by Task property. Parallel processing is
    enabled when parallel attribute of Inputs instance is True and
    pipeline runs on mpicasa environment.

    Note that this is abstract class. Task property must be implemented
    in each subclass.
    """

    Inputs = HpcBaselineSubtractionWorkerInputs

    def __init__(self, inputs: HpcBaselineSubtractionWorkerInputs) -> None:
        """Construct HpcBaselineSubtractionWorker instance.

        Args:
            inputs: HpcBaselineSubtractionWorkerInputs instance
        """
        super(HpcBaselineSubtractionWorker, self).__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis: str, exception: Type[Exception]) -> basetask.FailedTaskResults:
        """Return FailedTaskResults instance on failure.

        Args:
            vis: Name of the MS
            exception: Exception type

        Returns:
            FailedTaskResults instance
        """
        LOG.error('Error operating baseline subtraction for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)


class HpcCubicSplineBaselineSubtractionWorker(HpcBaselineSubtractionWorker):
    """Parallel processing task for cspline baseline subtraction.

    This executes CubicSplineBaselineSubtractionWorker in parallel.
    """

    Task = CubicSplineBaselineSubtractionWorker

    def __init__(self, inputs: HpcBaselineSubtractionWorkerInputs) -> None:
        """Construct HpcBaselineSubtractionWorkerInputs instance.

        Args:
            inputs: HpcBaselineSubtractionWorkerInputs instance
        """
        super(HpcCubicSplineBaselineSubtractionWorker, self).__init__(inputs)
