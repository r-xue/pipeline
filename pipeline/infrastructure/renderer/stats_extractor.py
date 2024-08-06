import abc
import copy
import collections
from collections import OrderedDict
from typing import List, Union, Dict

from pipeline.h.tasks.applycal.applycal import ApplycalResults
from pipeline.hif.tasks.applycal.ifapplycal import IFApplycal
from pipeline.hifa.tasks.gfluxscaleflag.resultobjects import GfluxscaleflagResults
from pipeline.hifa.tasks.flagging.flagdeteralma import FlagDeterALMAResults
from pipeline.infrastructure.basetask import Results, ResultsList
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure import logging
from pipeline.infrastructure.renderer import regression
from pipeline.h.tasks.common import flagging_renderer_utils as flagutils
from pipeline.infrastructure import pipeline_statistics as pstats
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class StatsExtractor(object, metaclass=abc.ABCMeta):
    """Adapted from the RegressisonExtractor,
    this class is the base class for a pipeline statistics extractor
    which uses a result to extract statistics information.
    """
    # the Results class this handler is expected to handle
    result_cls = None
    # if result_cls is a list, the type of classes it is expected to contain
    child_cls = None
    # the task class that generated the results, or None if it should handle
    # all results of this type regardless of which task generated it
    generating_task = None

    def is_handler_for(self, result:Union[Results, ResultsList]) -> bool:
        """
        Return True if this StatsExtractor can process the Result.

        :param result: the task Result to inspect
        :return: True if the Result can be processed
        """
        # if the result is not a list or the expected results class,
        # return False
        if not isinstance(result, self.result_cls):
            return False

        # this is the expected class and we weren't expecting any
        # children, so we should be able to handle the result
        if self.child_cls is None and (self.generating_task is None
                                       or result.task is self.generating_task
                                       or ( hasattr(self.generating_task, 'Task') and result.task is self.generating_task.Task) ):
            return True

        try:
            if all([isinstance(r, self.child_cls) and
                    (self.generating_task is None or r.task is self.generating_task)
                    for r in result]):
                return True
            return False
        except:
            # catch case when result does not have a task attribute
            return False

    @abc.abstractmethod
    def handle(self, result: Results, context=None) -> pstats.PipelineStatistics:
        """
        [Abstract] Extract pipeline statistics values

        This method should return a PipelineStatistics object

        :param result:
        :return:
        """
        raise NotImplemented


class StatsExtractorRegistry(object):
    """
    The registry and manager of the stats result extractor framework.

    The responsibility of the StatsResultRegistry is to pass Results to
    Extractors that can handle them.
    """
    def __init__(self):
        """Constractor of this class."""
        self.__plugins_loaded = False
        self.__handlers = []

    def add_handler(self, handler: StatsExtractor) -> None:
        """
        Push StatsExtractor into handlers list __handler.

        Args:
            handler: StatsExtractor
        """
        task = handler.generating_task.__name__ if handler.generating_task else 'all'
        child_name = ''
        if hasattr(handler.child_cls, '__name__'):
            child_name = handler.child_cls.__name__
        elif isinstance(handler.child_cls, collections.abc.Iterable):
            child_name = str([x.__name__ for x in handler.child_cls])
        container = 's of %s' % child_name
        s = '{}{} results generated by {} tasks'.format(handler.result_cls.__name__, container, task)
        LOG.debug('Registering {} as new pipeline stats handler for {}'.format(handler.__class__.__name__, s))
        self.__handlers.append(handler)

    def handle(self, result: Union[Results, ResultsList], context=None):
        """
        Extract values from corresponding StatsExtractor object of Result object.
        """
        if not self.__plugins_loaded:
            for plugin_class in regression.get_all_subclasses(StatsExtractor):
                self.add_handler(plugin_class())
            self.__plugins_loaded = True

        # this is the list which will contain extracted values
        extracted = []

        # Process leaf results first
        if isinstance(result, collections.abc.Iterable):
            for r in result:
                d = self.handle(r, context)
                union(extracted, d)

        # process the group-level results.
        for handler in self.__handlers:
            if handler.is_handler_for(result):
                LOG.debug('{} extracting stats results for {}'.format(handler.__class__.__name__,
                                                                           result.__class__.__name__))
                d = handler.handle(result, context)
                union(extracted, d)

        return extracted


# default StatsExtractorRegistry initialization
registry = StatsExtractorRegistry()


class FlagDeterALMAResultsExtractor(StatsExtractor):
    result_cls = FlagDeterALMAResults
    child_cls = None

    def handle(self, result: FlagDeterALMAResults, context) -> OrderedDict:

        intents_to_summarise = flagutils.intents_to_summarise(context)
        flag_table_intents = ['TOTAL', 'SCIENCE SPWS']
        flag_table_intents.extend(intents_to_summarise)

        flag_totals = {}
        flag_totals = utils.dict_merge(flag_totals,
            flagutils.flags_for_result(result, context, intents_to_summarise=intents_to_summarise))

        reasons_to_export = ['online', 'shadow', 'qa0', 'qa2', 'before', 'template']

        output_dict = {}
        for ms in flag_totals:
            output_dict[ms] = {}
            for reason in flag_totals[ms]:
                for intent in flag_totals[ms][reason]:
                    if reason in reasons_to_export:
                        if "TOTAL" in intent:
                            new = float(flag_totals[ms][reason][intent][0])
                            total = float(flag_totals[ms][reason][intent][1])
                            percentage = new/total * 100
                            output_dict[ms][reason] = percentage
        mous = context.get_oussid()
        ps = pstats.PipelineStatistics(name="flagdata_percentage",
                                       value=output_dict,
                                       longdesc="temporory value for testing",
                                       eb=ms,
                                       mous=mous,
                                       level=pstats.PipelineStatisticsLevel.EB)
        return ps


class FluxcalflagStatsExtractor(StatsExtractor):
    result_cls = GfluxscaleflagResults
    child_cls = None

    def handle(self, result:GfluxscaleflagResults, context):
        """
        Args:
            result: GfluxscaleflagResults object
        """
        summaries_by_name = {s['name']: s for s in result.cafresult.summaries}

        num_flags_before = summaries_by_name['before']['flagged']

        if 'after' in summaries_by_name:
            num_flags_after = summaries_by_name['after']['flagged']
        else:
            num_flags_after = num_flags_before

        ps = pstats.PipelineStatistics(name="fluxscaleflags",
                                                    value=int(num_flags_after),
                                                    longdesc="rows after",
                                                    mous=context.get_oussid(),
                                                    level=pstats.PipelineStatisticsLevel.MOUS)
        return ps


class ApplycalRegressionExtractor(StatsExtractor):
    """
    Stats test result extractor for applycal tasks.
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = IFApplycal

    def handle(self, result: ApplycalResults, context):
        """
        Args:
            result: ApplycalResults object

        Returns:
            OrderedDict[str, float]
        """
        summaries_by_name = {s['name']: s for s in result.summaries}
        num_flags_after = summaries_by_name['applycal']['flagged']
        ps = pstats.PipelineStatistics(name="applycal_flags",
                                                    value=int(num_flags_after),
                                                    longdesc="rows after",
                                                    mous=context.get_oussid(),
                                                    level=pstats.PipelineStatisticsLevel.MOUS)
        return ps


def get_stats_from_results(context: Context) -> List[pstats.PipelineStatistics]:
    """
    Gathers all possible pipeline statistics from results.
    """
    unified = []
    for results_proxy in context.results:
        results = results_proxy.read()
        union(unified, registry.handle(results, context))

    return unified


def union(lst: List, new: Union[pstats.PipelineStatistics, List[pstats.PipelineStatistics]]) -> List[pstats.PipelineStatistics]:
    """
    Combines lst which is always a list, with new,
    which could be a list of PipelineStatistics objects
    or an individual PipelineStatistics object.
    """
    union = copy.deepcopy(lst)

    if isinstance(new, list):
        for elt in new:
            union.append(elt)
    else:
        union.append(new)

    return union


def generate_stats(context) -> Dict:
    """
    Gathers statistics from the context and results and returns a representation
    of them as a dict.
    """
    stats_collection = []

    # First, gather statistics about the project and pipeline run info
    # directly from the context
    product_run_info = pstats.generate_product_pl_run_info(context)
    stats_collection.extend(product_run_info)

    # Next, gather statistics from the results objects
    stats_from_results = get_stats_from_results(context)
    stats_collection.extend(stats_from_results)

    # Construct dictionary representation of all pipeline stats
    final_dict = pstats.to_nested_dict(stats_collection)

    return final_dict
