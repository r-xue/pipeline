"""
Adaped from regression extractor classes
"""
import abc
import collections
import re
from collections import OrderedDict
from typing import List, Union
from pipeline.infrastructure.renderer import regression
from pipeline.infrastructure import pipeline_statistics

from pipeline.h.tasks.applycal.applycal import ApplycalResults
from pipeline.hif.tasks.applycal.ifapplycal import IFApplycal
from pipeline.hifa.tasks.fluxscale.gcorfluxscale import GcorFluxscaleResults
from pipeline.hifa.tasks.gfluxscaleflag.resultobjects import GfluxscaleflagResults
from pipeline.infrastructure.basetask import Results, ResultsList
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure import logging


LOG = logging.get_logger(__name__)


class StatsExtractor(regression.RegressionExtractor):
    """The mandatory base class

    # the Results class this handler is expected to handle
    result_cls = None
    # if result_cls is a list, the type of classes it is expected to contain
    child_cls = None
    # the task class that generated the results, or None if it should handle
    # all results of this type regardless of which task generated it
    generating_task = None"""

    @abc.abstractmethod
    def handle(self, result:Results):
        """
        [Abstract] Extract values for testing.

        This method should return a dict of

        {'applycal.new_flags.science': 0.34,
         'applycal.new_flags.bandpass': 0.53}

        :param result:
        :return:
        """
        raise NotImplemented


class StatsExtractorRegistry(object):
    """
    The registry and manager of the regression result extractor framework.

    The responsibility of the RegressionResultRegistry is to pass Results to
    RegressionExtractors that can handle them.
    """

    def __init__(self):
        """Constractor of this class."""
        self.__plugins_loaded = False
        self.__handlers = []

    def add_handler(self, handler: StatsExtractor) -> None:
        """
        Push RegressionExtractor into handlers list __handler.

        Args:
            handler: RegressionExtractor
        """
        task = handler.generating_task.__name__ if handler.generating_task else 'all'
        child_name = ''
        if hasattr(handler.child_cls, '__name__'):
            child_name = handler.child_cls.__name__
        elif isinstance(handler.child_cls, collections.abc.Iterable):
            child_name = str([x.__name__ for x in handler.child_cls])
        container = 's of %s' % child_name
        s = '{}{} results generated by {} tasks'.format(handler.result_cls.__name__, container, task)
        LOG.debug('Registering {} as new regression result handler for {}'.format(handler.__class__.__name__, s))
        self.__handlers.append(handler)

    def handle(self, result: Union[Results, ResultsList]):
        """
        Extract values from corresponding Extractor object of Result object.

        Args:
            result: Results or ResultsList[Results]
        Return:
            Dict of extracted values
        """
        if not self.__plugins_loaded:
            for plugin_class in get_all_subclasses(StatsExtractor):
                self.add_handler(plugin_class())
            self.__plugins_loaded = True

        # this is the list which will contain extracted values tuples
        extracted = []

        # Process leaf results first
        if isinstance(result, collections.abc.Iterable):
            for r in result:
                d = self.handle(r)
                extracted.append(d)

        # process the group-level results.
        for handler in self.__handlers:
            if handler.is_handler_for(result):
                LOG.debug('{} extracting stats results for {}'.format(handler.__class__.__name__,
                                                                           result.__class__.__name__))
                d = handler.handle(result)
                extracted.append(d)

        LOG.info(extracted)
        return extracted


# default StatsExtractorRegistry initialization
registry = StatsExtractorRegistry()


class FluxcalflagStatsExtractor(StatsExtractor):
    result_cls = GfluxscaleflagResults
    child_cls = None

    def handle(self, result:GfluxscaleflagResults) -> OrderedDict:
        """
        Args:
            result: GfluxscaleflagResults object

        Returns:
            OrderedDict[str, float]
        """
        summaries_by_name = {s['name']: s for s in result.cafresult.summaries}

        num_flags_before = summaries_by_name['before']['flagged']

        if 'after' in summaries_by_name:
            num_flags_after = summaries_by_name['after']['flagged']
        else:
            num_flags_after = num_flags_before

        ps = pipeline_statistics.PipelineStatistics(name="fluxscaleflags",
                                                    value=int(num_flags_after),
                                                    longdesc="rows after",
                                                    level="MOUS")

        # TODO: populate this value
        calibrated_flux = {}

        # ps = pipeline_statistics.PipelineStatistics(name="gfluxscale_calibrated_flux",
        #                                             value=calibrated_flux,
        #                                             longdesc="calibrated flux of the calibrator per intent per spw",
        #                                             level="EB/INTENT/SPW",
        #                                             units="Jy")
        return ps


# class GcorFluxscaleStatsExtractor(StatsExtractor):
#     result_cls = GcorFluxscaleResults
#     child_cls = None

#     def handle(self, result:GcorFluxscaleResults) -> OrderedDict:
#         """
#         Args:
#             result: GfluxscaleflagResults object

#         Returns:
#             OrderedDict[str, float]
#         """
#         # _, fluxes = fluxscale_renderer.make_flux_table(context, result)

#         ps = pipeline_statistics.PipelineStatistics(name="gfluxscale_calibrated_flux",
#                                                     value=calibrated_flux,
#                                                     longdesc="calibrated flux of the calibrator per intent per spw",
#                                                     level="EB/INTENT/SPW",
#                                                     units="Jy")
#         return ps


class ApplycalRegressionExtractor(StatsExtractor):
    """
    Regression test result extractor for applycal tasks.

    The extracted values are:
       - the number of flagged and scanned rows before this task
       - the number of flagged and scanned rows after this task
       - QA score
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = IFApplycal

    def handle(self, result: ApplycalResults) -> OrderedDict:
        """
        Args:
            result: ApplycalResults object

        Returns:
            OrderedDict[str, float]
        """
        summaries_by_name = {s['name']: s for s in result.summaries}
        num_flags_after = summaries_by_name['applycal']['flagged']
        ps = pipeline_statistics.PipelineStatistics(name="applycal_flags",
                                                    value=int(num_flags_after),
                                                    longdesc="rows after",
                                                    level="MOUS")
        return ps


def extract_qa_score_regression(prefix:str, result:Results) -> OrderedDict:
    """
    Create QA strings are properties of result, and insert them to OrderedDict.

    Args:
        prefix: Prefix string
        result: Result object

    Returns:
        OrderedDict
    """
    d = OrderedDict()
    for qa_score in result.qa.pool:
        metric_name = qa_score.origin.metric_name
        # Remove all non-word characters (everything except numbers and letters)
        metric_name = re.sub(r"[^\w\s]", '', metric_name)
        # Replace all runs of whitespace with a single dash
        metric_name = re.sub(r"\s+", '-', metric_name)

        metric_score = qa_score.origin.metric_score
        score_value = qa_score.score

        d['{}.qa.metric.{}'.format(prefix, metric_name)] = metric_score
        d['{}.qa.score.{}'.format(prefix, metric_name)] = score_value
    return d


def get_stats_from_results(context: Context) -> List[str]:
    """
    get stats required to be fetched from the results.
    """
    unified = []
    for results_proxy in context.results:
        results = results_proxy.read()
        unified.append(registry.handle(results))

    return unified


def get_all_subclasses(cls: StatsExtractor) -> List[StatsExtractor]:
    """
    Get all subclasses from RegressionExtractor classes tree recursively.

    Args:
        cls: root class of subclasses

    Returns:
        list of RegressionExtractor
    """
    subclasses = cls.__subclasses__()
    for subclass in subclasses:
        subclasses += get_all_subclasses(subclass)
    return subclasses
