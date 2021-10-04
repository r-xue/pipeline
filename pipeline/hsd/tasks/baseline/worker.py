import abc
import os

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

LOG = infrastructure.get_logger(__name__)


class BaselineSubtractionInputsBase(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    DATACOLUMN = {'CORRECTED_DATA': 'corrected',
                  'DATA': 'data',
                  'FLOAT_DATA': 'float_data'}

    @vdp.VisDependentProperty
    def colname(self):
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

    def to_casa_args(self):
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
    def __init__(self, task=None, success=None, outcome=None):
        super(BaselineSubtractionResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(BaselineSubtractionResults, self).merge_with_context(context)

    def _outcome_name(self):
        # outcome should be a name of blparam text file
        return 'blparam: "%s" bloutput: "%s"' % (self.outcome['blparam'], self.outcome['bloutput'])


class BaselineSubtractionWorkerInputs(BaselineSubtractionInputsBase):
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
    def prefix(self):
        return os.path.basename(self.vis.rstrip('/'))

    @vdp.VisDependentProperty
    def blparam(self):
        return self.prefix + '_blparam.txt'

    @vdp.VisDependentProperty(readonly=True)
    def field(self):
        return self.plan.get_field_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def antenna(self):
        return self.plan.get_antenna_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def spw(self):
        return self.plan.get_spw_id_list()

    @vdp.VisDependentProperty(readonly=True)
    def grid_table(self):
        return self.plan.get_grid_table_list()

    @vdp.VisDependentProperty(readonly=True)
    def channelmap_range(self):
        return self.plan.get_channelmap_range_list()

    def __init__(self, context, vis=None, plan=None,
                 fit_order=None, switchpoly=None,
                 edge=None, deviationmask=None, blparam=None, bloutput=None, org_directions_dict=None):
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
    Inputs = BaselineSubtractionWorkerInputs

    @abc.abstractproperty
    def Heuristics(self):
        """
        A reference to the :class:`Heuristics` class.
        """
        raise NotImplementedError

    is_multi_vis_task = False

    def __init__(self, inputs):
        super(BaselineSubtractionWorker, self).__init__(inputs)

        # initialize plotter
        self.datatable = DataTable(sdutils.get_data_table_path(self.inputs.context,
                                                               self.inputs.ms))

    def prepare(self):
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

    def analyse(self, results):
        # plot
        # initialize plot manager
        plot_manager = plotter.BaselineSubtractionPlotManager(self.inputs.context, self.datatable)
        outfile = results.outcome['outfile']
        ms = self.inputs.ms
        org_directions_dict = self.inputs.org_directions_dict
        accum = self.inputs.plan
        deviationmask_list = self.inputs.deviationmask
        LOG.info('deviationmask_list={}'.format(deviationmask_list))
        formatted_edge = list(common.parseEdge(self.inputs.edge))
        status = plot_manager.initialize(ms, outfile)
        plot_list = []
        stats = {}
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
                stats.update(plot_manager.baseline_quality_stat)
        plot_manager.finalize()

        results.outcome['plot_list'] = plot_list
        results.outcome['baseline_quality_stat'] = stats
        return results


# Worker class for cubic spline fit
class CubicSplineBaselineSubtractionWorker(BaselineSubtractionWorker):
    Inputs = BaselineSubtractionWorkerInputs
    Heuristics = CubicSplineFitParamConfig


# Tier-0 Parallelization
class HpcBaselineSubtractionWorkerInputs(BaselineSubtractionWorkerInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, vis=None, plan=None,
                 fit_order=None, switchpoly=None,
                 edge=None, deviationmask=None, blparam=None, bloutput=None,
                 parallel=None, org_directions_dict=None):
        super(HpcBaselineSubtractionWorkerInputs, self).__init__(context, vis=vis, plan=plan,
                                                                 fit_order=fit_order, switchpoly=switchpoly,
                                                                 edge=edge, deviationmask=deviationmask,
                                                                 blparam=blparam, bloutput=bloutput,
                                                                 org_directions_dict=org_directions_dict)
        self.parallel = parallel


# This is abstract class since Task is not specified yet
class HpcBaselineSubtractionWorker(sessionutils.ParallelTemplate):
    Inputs = HpcBaselineSubtractionWorkerInputs

    def __init__(self, inputs):
        super(HpcBaselineSubtractionWorker, self).__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis, exception):
        LOG.error('Error operating baseline subtraction for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)


class HpcCubicSplineBaselineSubtractionWorker(HpcBaselineSubtractionWorker):
    Task = CubicSplineBaselineSubtractionWorker

    def __init__(self, inputs):
        super(HpcCubicSplineBaselineSubtractionWorker, self).__init__(inputs)
