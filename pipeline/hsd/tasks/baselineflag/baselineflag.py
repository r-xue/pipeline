import os
import collections

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.utils import absolute_path
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import fieldnames
from pipeline.h.tasks.applycal.applycal import reshape_flagdata_summary
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from . import worker
from .flagsummary import SDBLFlagSummary
from .. import common
from ..common import utils as sdutils

LOG = infrastructure.get_logger(__name__)


class SDBLFlagInputs(vdp.StandardInputs):
    """
    Inputs for single dish flagging
    """
    def __to_numeric(self, val):
        return sdutils.to_numeric(val)

    def __to_bool(self, val):
        return sdutils.to_bool(val)

    def __to_int(self, val):
        return int(val)

    intent = vdp.VisDependentProperty(default='TARGET')
    iteration = vdp.VisDependentProperty(default=5, fconvert=__to_int)
    flag_tsys = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    tsys_thresh = vdp.VisDependentProperty(default=3.0, fconvert=__to_numeric)
    flag_prfre = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    prfre_thresh = vdp.VisDependentProperty(default=3.0, fconvert=__to_numeric)
    flag_pofre = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    pofre_thresh = vdp.VisDependentProperty(default=1.3333, fconvert=__to_numeric)
    flag_prfr = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    prfr_thresh = vdp.VisDependentProperty(default=4.5, fconvert=__to_numeric)
    flag_pofr = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    pofr_thresh = vdp.VisDependentProperty(default=4.0, fconvert=__to_numeric)
    flag_prfrm = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    prfrm_thresh = vdp.VisDependentProperty(default=5.5, fconvert=__to_numeric)
    prfrm_nmean = vdp.VisDependentProperty(default=5, fconvert=__to_int)
    flag_pofrm = vdp.VisDependentProperty(default=True, fconvert=__to_bool)
    pofrm_thresh = vdp.VisDependentProperty(default=5.0, fconvert=__to_numeric)
    pofrm_nmean = vdp.VisDependentProperty(default=5, fconvert=__to_int)
    plotflag = vdp.VisDependentProperty(default=True, fconvert=__to_bool)

    @vdp.VisDependentProperty
    def infiles(self):
        return self.vis

    @infiles.convert
    def infiles(self, value):
        self.vis = value
        return value

    @iteration.convert
    def iteration(self, value):
        return int(value)

    edge = vdp.VisDependentProperty(default=[0, 0])

    @edge.convert
    def edge(self, value):
        return sdutils.to_list(value)

    @vdp.VisDependentProperty
    def antenna(self):
        return ''

    @antenna.convert
    def antenna(self, value):
        antennas = self.ms.get_antenna(value)
        # if all antennas are selected, return ''
        if len(antennas) == len(self.ms.antennas):
            return ''
        return utils.find_ranges([a.id for a in antennas])
#         return ','.join([str(a.id) for a in antennas])

    @vdp.VisDependentProperty
    def field(self):
        # this will give something like '0542+3243,0343+242'
        field_finder = fieldnames.IntentFieldnames()
        intent_fields = field_finder.calculate(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @vdp.VisDependentProperty
    def pol(self):
        # filters polarization by self.spw
        selected_spwids = [int(spwobj.id) for spwobj in self.ms.get_spectral_windows(self.spw, with_channels=True)]
        pols = set()
        for idx in selected_spwids:
            pols.update(self.ms.get_data_description(spw=idx).corr_axis)

        return ','.join(pols)

    def __init__(self, context, output_dir=None,
                 iteration=None, edge=None, flag_tsys=None, tsys_thresh=None,
                 flag_prfre=None, prfre_thresh=None,
                 flag_pofre=None, pofre_thresh=None,
                 flag_prfr=None, prfr_thresh=None,
                 flag_pofr=None, pofr_thresh=None,
                 flag_prfrm=None, prfrm_thresh=None, prfrm_nmean=None,
                 flag_pofrm=None, pofrm_thresh=None, pofrm_nmean=None,
                 plotflag=None,
                 infiles=None, antenna=None, field=None,
                 spw=None, pol=None):
        super(SDBLFlagInputs, self).__init__()

        # context and vis/infiles must be set first so that properties that require
        # domain objects can be function
        self.context = context
        self.infiles = infiles
        self.output_dir = output_dir
        # task specific parameters
        self.iteration = iteration
        self.edge = edge
        self.flag_tsys = flag_tsys
        self.tsys_thresh = tsys_thresh
        self.flag_prfre = flag_prfre
        self.prfre_thresh = prfre_thresh
        self.flag_pofre = flag_pofre
        self.pofre_thresh = pofre_thresh
        self.flag_prfr = flag_prfr
        self.prfr_thresh = prfr_thresh
        self.flag_pofr = flag_pofr
        self.pofr_thresh = pofr_thresh
        self.flag_prfrm = flag_prfrm
        self.prfrm_thresh = prfrm_thresh
        self.prfrm_nmean = prfrm_nmean
        self.flag_pofrm = flag_pofrm
        self.pofrm_thresh = pofrm_thresh
        self.pofrm_nmean = pofrm_nmean
        self.plotflag = plotflag
        self.antenna = antenna
        self.field = field
        self.spw = spw
        self.pol = pol

        ### Default Flag rule
        from . import SDFlagRule
        self.FlagRuleDictionary = SDFlagRule.SDFlagRule
        # MUST NOT configure FlagRuleDictionary here.

    def _configureFlagRule(self):
        """A private method to convert input parameters to FlagRuleDictionary"""
        d = {'TsysFlag': (self.flag_tsys, [self.tsys_thresh]),
             'RmsPreFitFlag': (self.flag_prfr, [self.prfr_thresh]),
             'RmsPostFitFlag': (self.flag_pofr, [self.pofr_thresh]),
             'RmsExpectedPreFitFlag': (self.flag_prfre, [self.prfre_thresh]),
             'RmsExpectedPostFitFlag': (self.flag_pofre, [self.pofre_thresh]),
             'RunMeanPreFitFlag': (self.flag_prfrm, [self.prfrm_thresh, self.prfrm_nmean]),
             'RunMeanPostFitFlag': (self.flag_pofrm, [self.pofrm_thresh, self.pofrm_nmean])}
        keys = ['Threshold', 'Nmean']
        for k, v in d.items():
            (b, p) = v
            if b == True:
                self.activateFlagRule(k)
                for i in range(len(p)):
                    self.FlagRuleDictionary[k][keys[i]] = p[i]
            elif b == False:
                self.deactivateFlagRule(k)
            else:
                raise RuntimeError("Invalid flag operation definition for %s" % k)

    def activateFlagRule(self, key):
        """Activates a flag type specified by the input parameter in FlagRuleDictionary"""
        if key in self.FlagRuleDictionary:
            self.FlagRuleDictionary[key]['isActive'] = True
        else:
            raise RuntimeError('Error: %s not in predefined Flagging Rules' % key)

    def deactivateFlagRule(self, key):
        """Deactivates a flag type specified by the input parameter in FlagRuleDictionary"""
        if key in self.FlagRuleDictionary:
            self.FlagRuleDictionary[key]['isActive'] = False
        else:
            raise RuntimeError('Error: %s not in predefined Flagging Rules' % key)


class SDBLFlagResults(common.SingleDishResults):
    """
    The results of SDFalgData
    """
    def __init__(self, task=None, success=None, outcome=None):
        super(SDBLFlagResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(SDBLFlagResults, self).merge_with_context(context)

    def _outcome_name(self):
        return 'none'


# @task_registry.set_equivalent_casa_task('hsd_blflag')
# @task_registry.set_casa_commands_comment(
#     'Perform row-based flagging based on noise level and quality of spectral baseline subtraction.\n'
#     'This stage performs a pipeline calculation without running any CASA commands to be put in this file.'
# )
class SerialSDBLFlag(basetask.StandardTaskTemplate):
    """
    Single dish flagging class.
    """
    ##################################################
    # Note
    # The class uses _handle_multiple_vis framework.
    # Method, prepare() is called per MS. Inputs.ms
    # holds "an" MS instance to be processed.
    ##################################################    
    Inputs = SDBLFlagInputs

    def prepare(self):
        """
        Iterates over reduction group and invoke flagdata worker function in each clip_niteration.
        """
        inputs = self.inputs
        context = inputs.context
        # name of MS to process
        cal_name = inputs.ms.name
        bl_name = inputs.ms.work_data
        in_ant = inputs.antenna
        in_spw = inputs.spw
        in_field = inputs.field
        in_pol = '' if inputs.pol in ['', '*'] else inputs.pol.split(',')
        clip_niteration = inputs.iteration
        reduction_group = context.observing_run.ms_reduction_group
        # configure FlagRuleDictionary
        # this has to be done in runtime rather than in Inputs.__init__
        # to accommodate later overwrite of parameters.
        inputs._configureFlagRule()
        flag_rule = inputs.FlagRuleDictionary

        LOG.debug("Flag Rule for %s: %s" % (cal_name, flag_rule))

        # sumarize flag before execution
        full_intent = utils.to_CASA_intent(inputs.ms, inputs.intent)
        flagdata_summary_job = casa_tasks.flagdata(vis=bl_name, mode='summary',
                                                   antenna=in_ant, field=in_field,
                                                   spw=in_spw, intent=full_intent,
                                                   spwcorr=True, fieldcnt=True,
                                                   name='before')
        stats_before = self._executor.execute(flagdata_summary_job)

        # collection of field, antenna, and spw ids in reduction group per MS
        registry = collections.defaultdict(sdutils.RGAccumulator)

        # loop over reduction group (spw and source combination)
        flagResult = []
        for group_id, group_desc in reduction_group.items():
            LOG.debug('Processing Reduction Group %s' % group_id)
            LOG.debug('Group Summary:')
            for m in group_desc:
                LOG.debug('\t%s: Antenna %d (%s) Spw %d Field %d (%s)' %
                          (os.path.basename(m.ms.name), m.antenna_id,
                           m.antenna_name, m.spw_id, m.field_id, m.field_name))

            nchan = group_desc.nchan
            if nchan == 1:
                LOG.info('Skipping a group of channel averaged spw')
                continue

            field_sel = ''
            if len(in_field) == 0:
                # fine, just go ahead
                field_sel = in_field
            elif group_desc.field_name in [x.strip('"') for x in in_field.split(',')]:
                # pre-selection of the field name
                field_sel = group_desc.field_name
            else:
                # no field name is included in in_field, skip
                LOG.info('Skip reduction group {:d}'.format(group_id))
                continue

            # Which group in group_desc list should be processed
            member_list = list(common.get_valid_ms_members(group_desc, [cal_name], in_ant, field_sel, in_spw))
            LOG.trace('group %s: member_list=%s' % (group_id, member_list))

            # skip this group if valid member list is empty
            if len(member_list) == 0:
                LOG.info('Skip reduction group %d' % group_id)
                continue

            member_list.sort()  # list of group_desc IDs to flag
            antenna_list = [group_desc[i].antenna_id for i in member_list]
            spwid_list = [group_desc[i].spw_id for i in member_list]
            ms_list = [group_desc[i].ms for i in member_list]
            fieldid_list = [group_desc[i].field_id for i in member_list]
            temp_dd_list = [ms_list[i].get_data_description(spw=spwid_list[i])
                            for i in range(len(member_list))]
            pols_list = [[corr for corr in ddobj.corr_axis if (in_pol == '' or corr in in_pol)]
                         for ddobj in temp_dd_list]
            del temp_dd_list

            for i in range(len(member_list)):
                member = group_desc[member_list[i]]
                registry[member.ms].append(field_id=member.field_id,
                                           antenna_id=member.antenna_id,
                                           spw_id=member.spw_id,
                                           pol_ids=pols_list[i])

        # per-MS loop
        for msobj, accumulator in registry.items():
            rowmap = None
            if absolute_path(cal_name) == absolute_path(bl_name):
                LOG.warn("%s is not yet baselined. Skipping flag by post-fit statistics for the data."
                         " MASKLIST will also be cleared up. You may go on flagging but the statistics"
                         " will contain line emission." % inputs.ms.basename)
            else:
                # row map generation is very expensive. Do as few time as possible
                _ms = context.observing_run.get_ms(msobj.name)
                rowmap = sdutils.make_row_map_for_baselined_ms(_ms)

            antenna_list = accumulator.get_antenna_id_list()
            fieldid_list = accumulator.get_field_id_list()
            spwid_list = accumulator.get_spw_id_list()
            pols_list = accumulator.get_pol_ids_list()

            LOG.info("*"*60)
            LOG.info('Members to be processed:')
            for antenna_id, field_id, spw_id, pol_ids in zip(antenna_list, fieldid_list, spwid_list, pols_list):
                LOG.info("\t{}:: Antenna {} ({}) Spw {} Field {} ({}) Pol '{}'".format(
                    msobj.basename,
                    antenna_id,
                    msobj.antennas[antenna_id].name,
                    spw_id,
                    field_id,
                    msobj.fields[field_id].name,
                    ','.join(pol_ids)))

            LOG.info("*"*60)

            nchan = 0
            # Calculate flag and update DataTable
            flagging_inputs = worker.SDBLFlagWorkerInputs(
                context, clip_niteration,
                msobj.name, antenna_list, fieldid_list,
                spwid_list, pols_list, nchan, flag_rule,
                rowmap=rowmap)
            flagging_task = worker.SDBLFlagWorker(flagging_inputs)

            flagging_results = self._executor.execute(flagging_task, merge=False)
            thresholds = flagging_results.outcome
            # Summary
            if not basetask.DISABLE_WEBLOG:
                renderer = SDBLFlagSummary(context, msobj,
                                           antenna_list, fieldid_list, spwid_list,
                                           pols_list, thresholds, flag_rule)
                result = self._executor.execute(renderer, merge=False)
                flagResult += result

        # Calculate flag fraction after operation.
        # flag summary for By Topic Page
        flagkwargs = ["spw='{!s}' intent='{}' fieldcnt=True mode='summary' name='AntSpw{:0>3}'".format(spw.id, full_intent, spw.id)
                              for spw in self.inputs.ms.get_spectral_windows()]
        detailed_flag_job = casa_tasks.flagdata(vis=bl_name, mode='list', inpfile=flagkwargs, flagbackup=False)
        detailed_flag_result = self._executor.execute(detailed_flag_job)
        # Statistics for task weblog
        stats_after = self.__reorganize_flag_stat(detailed_flag_result)
        stats_after['name'] = 'after'

        outcome = {'flagdata_summary': [stats_before, stats_after],
                   'summary': flagResult}
        results = SDBLFlagResults(task=self.__class__,
                                  success=True,
                                  outcome=outcome)
        results.flagsummary = reshape_flagdata_summary(detailed_flag_result)

        return results

    def analyse(self, result):
        return result

    def __reorganize_flag_stat(self, in_stat: dict) -> dict:
        """
        Reorganize flag statistics dictionary.

        This method reorganizes flag statistics generated by list mode of
        flagdata task with fieldcnt=True and constructs accumulated flag
        statistics per source.

        Args:
            in_stats: flag statistic dictionary by list mode in the form,
                {'report0': {'sourcename': {'flagged': 0, 'total': 222222,
                                            'spw': {'9': {'flagged': 0, 'total': 111111},
                                                            '11': ....},
                                            'antenna': {....},
                                                    .... },
                                .... },
                'report1': ....}

        Returns:
            Accumulated per source flag statistics dictionary in the form,
                 {'sourcename': {'flagged': 222, 'total': 777777,
                                 'spw': {'9': {'flagged': 111, 'total': 444444},
                                              '11': ....},
                                'antenna': {....},
                                     .... },
                    .... }
        """
        out_stat = {'type': 'summary'}
        ignore_keys = ['name', 'type']
        sum_keys = ['flagged', 'total']
        
        for rep_summary in in_stat.values(): # per report loop
            for source, source_summary in rep_summary.items(): # per source loop
                if source in ignore_keys: continue
                if not source in out_stat: # source names
                    out_stat[source] = dict((k, 0) for k in sum_keys)
                for gtype, summary in source_summary.items(): # group type loop (e.g., spw)
                    if gtype in sum_keys: # flagged and total
                        out_stat[source][gtype] += summary
                        continue
                    elif gtype not in out_stat[source]:
                        out_stat[source][gtype] = dict()
                    for idx, id_val in summary.items(): # per (e.g., spw) ID loop
                        if idx not in out_stat[source][gtype]:
                            out_stat[source][gtype][idx] = dict((k, 0) for k in sum_keys)
                        for k in sum_keys:
                            out_stat[source][gtype][idx][k] += id_val[k]
        return out_stat


### Tier-0 parallelization
class HpcSDBLFlagInputs(SDBLFlagInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, output_dir=None,
                 iteration=None, edge=None,
                 flag_tsys=None, tsys_thresh=None,
                 flag_prfre=None, prfre_thresh=None,
                 flag_pofre=None, pofre_thresh=None,
                 flag_prfr=None, prfr_thresh=None,
                 flag_pofr=None, pofr_thresh=None,
                 flag_prfrm=None, prfrm_thresh=None, prfrm_nmean=None,
                 flag_pofrm=None, pofrm_thresh=None, pofrm_nmean=None,
                 plotflag=None,
                 infiles=None, antenna=None, field=None,
                 spw=None, pol=None, parallel=None):
        super(HpcSDBLFlagInputs, self).__init__(
            context, output_dir=output_dir,
            iteration=iteration, edge=edge,
            flag_tsys=flag_tsys, tsys_thresh=tsys_thresh,
            flag_prfre=flag_prfre, prfre_thresh=prfre_thresh,
            flag_pofre=flag_pofre, pofre_thresh=pofre_thresh,
            flag_prfr=flag_prfr, prfr_thresh=prfr_thresh,
            flag_pofr=flag_pofr, pofr_thresh=pofr_thresh,
            flag_prfrm=flag_prfrm, prfrm_thresh=prfrm_thresh, prfrm_nmean=prfrm_nmean,
            flag_pofrm=flag_pofrm, pofrm_thresh=pofrm_thresh, pofrm_nmean=pofrm_nmean,
            plotflag=plotflag,
            infiles=infiles, antenna=antenna, field=field, spw=spw, pol=pol)
        self.parallel = parallel


@task_registry.set_equivalent_casa_task('hsd_blflag')
@task_registry.set_casa_commands_comment(
    'Perform row-based flagging based on noise level and quality of spectral baseline subtraction.\n'
    'This stage performs a pipeline calculation without running any CASA commands to be put in this file.'
)
class HpcSDBLFlag(sessionutils.ParallelTemplate):
    Inputs = HpcSDBLFlagInputs
    Task = SerialSDBLFlag

    def __init__(self, inputs):
        super(HpcSDBLFlag, self).__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis, exception):
        LOG.error('Error operating target flag for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)
