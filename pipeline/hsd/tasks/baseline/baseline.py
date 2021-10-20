"""Spectral baseline subtraction stage."""
import collections
import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.sessionutils as sessionutils
from pipeline.domain import DataType
from pipeline.hsd.heuristics import MaskDeviationHeuristic
from pipeline.infrastructure import task_registry
from . import maskline
from . import worker
from .. import common
from ..common import compress
from pipeline.hsd.tasks.common.inspection_util import generate_ms, inspect_reduction_group, merge_reduction_group
from ..common import utils

# import memory_profiler
LOG = infrastructure.get_logger(__name__)


class SDBaselineInputs(vdp.StandardInputs):
    """
    Inputs for baseline subtraction
    """
    # Search order of input vis
    processing_data_type = [DataType.ATMCORR, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    infiles = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    spw = vdp.VisDependentProperty(default='')
    pol = vdp.VisDependentProperty(default='')
    field = vdp.VisDependentProperty(default='')
    linewindow = vdp.VisDependentProperty(default=[])
    linewindowmode = vdp.VisDependentProperty(default='replace')
    edge = vdp.VisDependentProperty(default=(0, 0))
    broadline = vdp.VisDependentProperty(default=True)
    fitorder = vdp.VisDependentProperty(default=-1)
    fitfunc = vdp.VisDependentProperty(default='cspline')
    switchpoly = vdp.VisDependentProperty(default=True)
    clusteringalgorithm = vdp.VisDependentProperty(default='hierarchy')
    deviationmask = vdp.VisDependentProperty(default=True)

    # Synchronization between infiles and vis is still necessary
    @vdp.VisDependentProperty
    def vis(self):
        return self.infiles

    # handle conversion from string to bool
    @switchpoly.convert
    def switchpoly(self, value):
        converted = None
        if isinstance(value, bool):
            converted = value
        elif isinstance(value, str):
            for b in (True, False):
                s = str(b)
                if value in (s.lower(), s.upper(), s.capitalize()):
                    converted = b
                    break
        assert converted is not None
        return converted

    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, infiles=None, antenna=None, spw=None, pol=None, field=None,
                 linewindow=None, linewindowmode=None, edge=None, broadline=None, fitorder=None,
                 fitfunc=None, switchpoly=None, clusteringalgorithm=None, deviationmask=None, parallel=None):
        super(SDBaselineInputs, self).__init__()

        self.context = context
        self.infiles = infiles
        self.antenna = antenna
        self.spw = spw
        self.pol = pol
        self.field = field
        self.linewindow = linewindow
        self.linewindowmode = linewindowmode
        self.edge = edge
        self.broadline = broadline
        self.fitorder = fitorder
        self.fitfunc = fitfunc
        self.switchpoly = switchpoly
        self.clusteringalgorithm = clusteringalgorithm
        self.deviationmask = deviationmask
        self.parallel = parallel

    def to_casa_args(self):
        infiles = self.infiles
        if isinstance(self.infiles, list):
            self.infiles = infiles[0]
        args = super(SDBaselineInputs, self).to_casa_args()
        self.infiles = infiles

        if 'antenna' not in args:
            args['antenna'] = ''
        return args


class SDBaselineResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(SDBaselineResults, self).__init__(task, success, outcome)
        self.out_mses = []

    #@utils.profiler
    def merge_with_context(self, context):
        super(SDBaselineResults, self).merge_with_context(context)

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

        # increment iteration counter (only to in MS for now)
        # register detected lines to reduction group member (to both in and out MS)
        reduction_group = target.ms_reduction_group
        for b in self.outcome['baselined']:
            group_id = b['group_id']
            member_list = b['members']
            lines = b['lines']
            channelmap_range = b['channelmap_range']
            group_desc = reduction_group[group_id]
            for (ms, field, ant, spw) in utils.iterate_group_member(reduction_group[group_id], member_list):
                group_desc.iter_countup(ms, ant, spw, field)
                out_ms = target.get_ms(name=self.outcome['vis_map'][ms.name])
                for m in [ms, out_ms]:
                    group_desc.add_linelist(lines, m, ant, spw, field,
                                            channelmap_range=channelmap_range)

        # merge deviation_mask with context
        for ms in target.measurement_sets:
            if not hasattr(ms, 'deviation_mask'): ms.deviation_mask = None
        if 'deviation_mask' in self.outcome:
            for (basename, masks) in self.outcome['deviation_mask'].items():
                ms = target.get_ms(basename)
                ms.deviation_mask = {}
                for field in ms.get_fields(intent='TARGET'):
                    for antenna in ms.antennas:
                        for spw in ms.get_spectral_windows(science_windows_only=True):
                            key = (field.id, antenna.id, spw.id)
                            if key in masks:
                                ms.deviation_mask[key] = masks[key]
                out_ms = target.get_ms(name=self.outcome['vis_map'][ms.name])
                out_ms.deviation_mask = ms.deviation_mask

    def _outcome_name(self):
        return '\n'.join(['Reduction Group {0}: member {1}'.format(b['group_id'], b['members'])
                          for b in self.outcome['baselined']])


@task_registry.set_equivalent_casa_task('hsd_baseline')
@task_registry.set_casa_commands_comment(
    'Subtracts spectral baseline by least-square fitting with N-sigma clipping. Spectral lines are automatically '
    'detected and examined to determine the region that is masked to protect these features from the fit.\n'
    'This stage performs a pipeline calculation without running any CASA commands to be put in this file.'
)
class SDBaseline(basetask.StandardTaskTemplate):
    Inputs = SDBaselineInputs
    is_multi_vis_task = True

#    @memory_profiler.profile
    def prepare(self):
        LOG.debug('Starting SDMDBaseline.prepare')
        inputs = self.inputs
        context = inputs.context
        reduction_group = context.observing_run.ms_reduction_group
        vis_list = inputs.vis
        ms_list = inputs.ms
        args = inputs.to_casa_args()

        window = inputs.linewindow
        windowmode = inputs.linewindowmode
        LOG.info('{}: window={}, windowmode={}'.format(self.__class__.__name__, window, windowmode))
        edge = inputs.edge
        broadline = inputs.broadline
        fitorder = 'automatic' if inputs.fitorder is None or inputs.fitorder < 0 else inputs.fitorder
        fitfunc = inputs.fitfunc
        switchpoly = inputs.switchpoly
        clusteringalgorithm = inputs.clusteringalgorithm
        deviationmask = inputs.deviationmask

        # mkdir stage_dir if it doesn't exist
        stage_number = context.task_counter
        stage_dir = os.path.join(context.report_dir, "stage%d" % stage_number)
        if not os.path.exists(stage_dir):
            os.makedirs(stage_dir)

        # loop over reduction group

        # This is a dictionary for deviation mask that will be merged with top-level context
        deviation_mask = collections.defaultdict(dict)

        # collection of field, antenna, and spw ids in reduction group per MS
        registry = collections.defaultdict(utils.RGAccumulator)

        # outcome for baseline subtraction
        baselined = []

        # dictionay of org_direction
        org_directions_dict = {}

        LOG.debug('Starting per reduction group processing: number of groups is %d', len(reduction_group))
        for (group_id, group_desc) in reduction_group.items():
            LOG.info('Processing Reduction Group %s', group_id)
            LOG.info('Group Summary:')
            for m in group_desc:
                LOG.info('\tMS "{ms}" Antenna "{antenna}" (ID {antenna_id}) Spw {spw} Field "{field}" (ID {field_id})'.format(
                         ms=m.ms.basename,
                         antenna=m.antenna_name,
                         antenna_id=m.antenna_id, spw=m.spw_id,
                         field=m.field_name,
                         field_id=m.field_id))

                # scan for org_direction and find the first one in group
                msobj = context.observing_run.get_ms(m.ms.basename)
                field_id = m.field_id
                fields = msobj.get_fields( field_id = field_id )
                source_name = fields[0].source.name
                if source_name not in org_directions_dict:
                    if fields[0].source.is_eph_obj or fields[0].source.is_known_eph_obj:
                        org_direction = fields[0].source.org_direction
                    else:
                        org_direction = None
                    org_directions_dict.update( {source_name:org_direction} )
                    LOG.info( "registered org_direction[{}]={}".format( source_name, org_direction ) )

            # skip channel averaged spw
            nchan = group_desc.nchan
            LOG.debug('nchan for group %s is %s', group_id, nchan)
            if nchan == 1:
                first_member = group_desc[0]
                vspw = context.observing_run.real2virtual_spw_id(first_member.spw.id, first_member.ms)
                LOG.info('Skip channel averaged spw %s.', vspw)
                continue

            LOG.debug('spw=\'%s\'', args['spw'])
            LOG.debug('vis_list=%s', vis_list)
            member_list = numpy.fromiter(
                utils.get_valid_ms_members2(group_desc, ms_list, args['antenna'], args['field'], args['spw']),
                dtype=numpy.int32)
            # skip this group if valid member list is empty
            LOG.debug('member_list=%s', member_list)
            if len(member_list) == 0:
                LOG.info('Skip reduction group %s', group_id)
                continue

            member_list.sort()

            # assume all members have same iteration
            first_member = group_desc[member_list[0]]
            iteration = first_member.iteration
            LOG.debug('iteration for group %s is %s', group_id, iteration)

            LOG.info('Members to be processed:')
            for (gms, gfield, gant, gspw) in utils.iterate_group_member(group_desc, member_list):
                LOG.info('\tMS "%s" Field ID %s Antenna ID %s Spw ID %s',
                          gms.basename, gfield, gant, gspw)

            # Deviation Mask
            # NOTE: deviation mask is evaluated per ms per field per spw
            if deviationmask:
                LOG.info('Apply deviation mask to baseline fitting')
                dvtasks = []
                dvparams = collections.defaultdict(lambda: ([], [], []))
                for (ms, fieldid, antennaid, spwid) in utils.iterate_group_member(group_desc, member_list):
                    if (not hasattr(ms, 'deviation_mask')) or ms.deviation_mask is None:
                        ms.deviation_mask = {}
                    if (fieldid, antennaid, spwid) not in ms.deviation_mask:
                        LOG.debug('Evaluating deviation mask for %s field %s antenna %s spw %s',
                                  ms.basename, fieldid, antennaid, spwid)
                        #mask_list = self.evaluate_deviation_mask(ms.name, fieldid, antennaid, spwid,
                        #                                         consider_flag=True)
                        dvparams[ms.name][0].append(fieldid)
                        dvparams[ms.name][1].append(antennaid)
                        dvparams[ms.name][2].append(spwid)
                    else:
                        deviation_mask[ms.basename][(fieldid, antennaid, spwid)] = ms.deviation_mask[(fieldid, antennaid, spwid)]
                for (vis, params) in dvparams.items():
                    field_list, antenna_list, spw_list = params
                    dvtasks.append(deviation_mask_heuristic(vis=vis, field_list=field_list, antenna_list=antenna_list,
                                                            spw_list=spw_list, consider_flag=True, parallel=self.inputs.parallel))
                for vis, dvtask in dvtasks:
                    dvmasks = dvtask.get_result()
                    field_list, antenna_list, spw_list = dvparams[vis]
                    ms = context.observing_run.get_ms(vis)
                    for field_id, antenna_id, spw_id, mask_list in zip(field_list, antenna_list, spw_list, dvmasks):
                        # key: (fieldid, antennaid, spwid)
                        key = (field_id, antenna_id, spw_id)
                        LOG.debug('deviation mask: key %s %s %s mask %s', field_id, antenna_id, spw_id, mask_list)
                        ms.deviation_mask[key] = mask_list
                        deviation_mask[ms.basename][key] = ms.deviation_mask[key]
                        LOG.debug('evaluated deviation mask is %s', ms.deviation_mask[key])
            else:
                LOG.info('Deviation mask is disabled by the user')
            LOG.debug('deviation_mask=%s', deviation_mask)

            # Spectral Line Detection and Validation
            # MaskLine will update DataTable.MASKLIST column
            maskline_inputs = maskline.MaskLine.Inputs(context, iteration, group_id, member_list,
                                                       window, windowmode, edge, broadline, clusteringalgorithm)
            maskline_task = maskline.MaskLine(maskline_inputs)
            maskline_result = self._executor.execute(maskline_task, merge=False)
            grid_table = maskline_result.outcome['grid_table']
            if grid_table is None:
                LOG.info('Skip reduction group %s', group_id)
                continue
            compressed_table = compress.CompressedObj(grid_table)
            del grid_table

            detected_lines = maskline_result.outcome['detected_lines']
            channelmap_range = maskline_result.outcome['channelmap_range']
            cluster_info = maskline_result.outcome['cluster_info']
            flag_digits  = maskline_result.outcome['flag_digits']

            # register ids to per MS id collection
            for i in member_list:
                member = group_desc[i]
                registry[member.ms].append(member.field_id, member.antenna_id, member.spw_id,
                                           grid_table=compressed_table, channelmap_range=channelmap_range)

            # add entry to outcome
            baselined.append({'group_id': group_id, 'iteration': iteration,
                              'members': member_list,
                              'lines': detected_lines,
                              'channelmap_range': channelmap_range,
                              'clusters': cluster_info,
                              'flag_digits': flag_digits,
                              'org_direction': org_direction })

        # - end of the loop over reduction group

        blparam_file = lambda ms: ms.basename.rstrip('/') \
            + '_blparam_stage{stage}.txt'.format(stage=stage_number)
        vis_map = {} # key and value are input and output vis name, respectively
        plot_list = []
        baseline_quality_stat = {}
#         plot_manager = plotter.BaselineSubtractionPlotManager(self.inputs.context, datatable)

        # Generate and apply baseline fitting solutions
        vislist = [ms.name for ms in registry]
        plan = [registry[ms] for ms in registry]
        blparam = [blparam_file(ms) for ms in registry]
        deviationmask_list = [deviation_mask[ms.basename] for ms in registry]
        # 21/05/2018 TN temporal workaround
        # I don't know how to use vdp.ModeInputs so directly specify worker task class here
        worker_cls = worker.HpcCubicSplineBaselineSubtractionWorker
        fitter_inputs = vdp.InputsContainer(worker_cls, context,
                                            vis=vislist, plan=plan,
                                            fit_order=fitorder, switchpoly=switchpoly,
                                            edge=edge, blparam=blparam,
                                            deviationmask=deviationmask_list,
                                            parallel=self.inputs.parallel,
                                            org_directions_dict=org_directions_dict)
        fitter_task = worker_cls(fitter_inputs)
        fitter_results = self._executor.execute(fitter_task, merge=False)

        # Check if fitting was successful
        fitting_failed = False
        if isinstance(fitter_results, basetask.FailedTaskResults):
            fitting_failed = True
            failed_results = basetask.ResultsList([fitter_results])
        elif isinstance(fitter_results, basetask.ResultsList) and numpy.any([isinstance(r, basetask.FailedTaskResults) for r in fitter_results]):
            fitting_failed = True
            failed_results = basetask.ResultsList([r for r in fitter_results
                                                   if isinstance(r, basetask.FailedTaskResults)])
        if fitting_failed:
            for r in failed_results:
                r.origtask_cls = self.__class__
            return failed_results

        results_dict = dict((os.path.basename(r.outcome['infile']), r) for r in fitter_results)
        for ms in context.observing_run.measurement_sets:
            if ms.basename not in results_dict:
                continue

            result = results_dict[ms.basename]
            vis = result.outcome['infile']

            outfile = result.outcome['outfile']
            LOG.debug('infile: {0}, outfile: {1}'.format(os.path.basename(vis), os.path.basename(outfile)))
            vis_map[ms.name] = outfile

            if 'plot_list' in result.outcome:
                plot_list.extend(result.outcome['plot_list'])
            if 'baseline_quality_stat' in result.outcome:
                baseline_quality_stat.update(result.outcome['baseline_quality_stat'])


        outcome = {'baselined': baselined,
                   'vis_map': vis_map,
                   'edge': edge,
                   'deviation_mask': deviation_mask,
                   'plots': plot_list,
                   'baseline_quality_stat': baseline_quality_stat}
        results = SDBaselineResults(task=self.__class__,
                                    success=True,
                                    outcome=outcome)

        return results

    def analyse(self, result):
        # Generate domain object of baselined MS
        for infile, outfile in result.outcome['vis_map'].items():
            in_ms = self.inputs.context.observing_run.get_ms(infile)
            new_ms = generate_ms(outfile, in_ms)
            new_ms.set_data_column(DataType.BASELINED, 'DATA')
            result.out_mses.append(new_ms)

        return result

    @staticmethod
    def evaluate_deviation_mask(vis, field_id, antenna_id, spw_id, consider_flag=False):
        """
        Create deviation mask using MaskDeviation heuristic
        """
        h = MaskDeviationHeuristic()
        mask_list = h.calculate(vis=vis, field_id=field_id, antenna_id=antenna_id, spw_id=spw_id,
                                consider_flag=consider_flag)
        return mask_list


class HeuristicsTask(object):
    def __init__(self, heuristics_cls, *args, **kwargs):
        self.heuristics = heuristics_cls()
        #print(args, kwargs)
        self.args = args
        self.kwargs = kwargs
        #print(self.args, self.kwargs)

    def execute(self, dry_run=False):
        if dry_run:
            return []

        return self.heuristics.calculate(*self.args, **self.kwargs)

    def get_executable(self):
        return lambda: self.execute(dry_run=False)


class DeviationMaskHeuristicsTask(HeuristicsTask):
    def __init__(self, heuristics_cls, vis, field_list, antenna_list, spw_list, consider_flag=False):
        super(DeviationMaskHeuristicsTask, self).__init__(heuristics_cls, vis=vis, consider_flag=consider_flag)
        self.vis = vis
        self.field_list = field_list
        self.antenna_list = antenna_list
        self.spw_list = spw_list

    def execute(self, dry_run=False):
        if dry_run:
            return {}

        result = []
        for field_id, antenna_id, spw_id in zip(self.field_list, self.antenna_list, self.spw_list):
            self.kwargs.update({'field_id': field_id, 'antenna_id': antenna_id, 'spw_id': spw_id})
            mask_list = super(DeviationMaskHeuristicsTask, self).execute(dry_run)
            result.append(mask_list)
        return result


def deviation_mask_heuristic(vis, field_list, antenna_list, spw_list, consider_flag=False, parallel=None):
    #parallel_wanted = mpihelpers.parse_mpi_input_parameter(parallel)
    mytask = DeviationMaskHeuristicsTask(MaskDeviationHeuristic, vis=vis, field_list=field_list,
                            antenna_list=antenna_list, spw_list=spw_list, consider_flag=consider_flag)
    #if parallel_wanted:
    if False:
        task = mpihelpers.AsyncTask(mytask)
    else:
        LOG.trace('Deviation Mask Heuristic always runs in serial mode.')
        task = mpihelpers.SyncTask(mytask)
    return vis, task


def test_deviation_mask_heuristic(spw=None):
    import glob
    vislist = glob.glob('uid___A002_X*.ms')
    print('vislist={0}'.format(vislist))
    field_list = [1, 1, 1]
    antenna_list = [0, 1, 2]
    spw_list = [17, 17, 17] if spw is None else [spw, spw, spw]
    consider_flag = True
    import time
    start_time = time.time()
    serial_tasks = [deviation_mask_heuristic(vis=vis, field_list=field_list, antenna_list=antenna_list, spw_list=spw_list, consider_flag=consider_flag, parallel=False) for vis in vislist]
    serial_results = [(v, t.get_result()) for v, t in serial_tasks]
    end_time = time.time()
    print('serial execution duration {0}sec'.format(end_time-start_time))
    start_time = time.time()
    parallel_tasks = [deviation_mask_heuristic(vis=vis, field_list=field_list, antenna_list=antenna_list, spw_list=spw_list, consider_flag=consider_flag, parallel=True) for vis in vislist]
    parallel_results = [(v, t.get_result()) for v, t in parallel_tasks]
    end_time = time.time()
    print('parallel execution duration {0}sec'.format(end_time-start_time))

    for vis, smask in serial_results:
        for _vis, pmask in parallel_results:
            if vis == _vis:
                for field_id in field_list:
                    for antenna_id in antenna_list:
                        for spw_id in spw_list:
                            print('vis "{0}", field {1} antenna {2} spw {3}:'.format(vis, field_id, antenna_id, spw_id))
                            print('     serial mask: {0}'.format(smask))
                            print('   parallel mask: {0}'.format(pmask))
                            print('   {0}'.format(smask == pmask))
