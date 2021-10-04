"""
Classes of Regression Framework.

The regression module contains base classes and plugin registry for the
pipeline's regression test value extractor framework.

This module contains two classes:

    * RegressionExtractor: the base class for extractor plug-ins
    * RegressionExtractorRegistry: the registry and manager for plug-ins

Tasks provide and register their own extractors that each extends
RegressionExtractor. These extractor plug-ins analyse the results of a task,
extracting pertinent values and writing them to a dict. The keys of the
output dict identify the value; the values of the dict are the extracted
values themselves.

The pipeline QA framework is activated whenever a Results instance is accepted
into the pipeline context. The pipeline QA framework operates by calling
is_handler_for(result) on each registered QAPlugin, passing it the the accepted
Results instance for inspection. QAPlugins that claim to handle the Result are
given the Result for processing. In this step, the QA framework calls
QAPlugin.handle(context, result), the method overridden by the task-specific
QAPlugin.
"""
import abc
import collections
import os.path
import re
from collections import OrderedDict
from typing import List, Union

from pipeline.domain.measures import FluxDensityUnits
from pipeline.h.tasks.applycal.applycal import ApplycalResults
from pipeline.h.tasks.common.commonfluxresults import FluxCalibrationResults
from pipeline.hif.tasks.applycal.ifapplycal import IFApplycal
from pipeline.hifa.tasks.fluxscale.gcorfluxscale import GcorFluxscale
from pipeline.hifa.tasks.gfluxscaleflag.resultobjects import GfluxscaleflagResults
from pipeline.hsd.tasks.applycal.applycal import HpcSDApplycal
from pipeline.hsd.tasks.baselineflag.baselineflag import SDBLFlagResults
from pipeline.hsd.tasks.baselineflag.baselineflag import HpcSDBLFlag
from pipeline.hsd.tasks.imaging.imaging import SDImaging
from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResults
from pipeline.infrastructure.basetask import Results, ResultsList, StandardTaskTemplate
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.taskregistry import task_registry
from pipeline.infrastructure import logging
from pipeline.hsd.tasks.restoredata.restoredata import SDRestoreDataResults, SDRestoreData
from pipeline.hsdn.tasks.restoredata.restoredata import NRORestoreDataResults, NRORestoreData
from pipeline.domain.measurementset import MeasurementSet

LOG = logging.get_logger(__name__)


class RegressionExtractor(object, metaclass=abc.ABCMeta):
    """The mandatory base class for all regression test result extractors."""

    # the Results class this handler is expected to handle
    result_cls = None
    # if result_cls is a list, the type of classes it is expected to contain
    child_cls = None
    # the task class that generated the results, or None if it should handle
    # all results of this type regardless of which task generated it
    generating_task = None

    def is_handler_for(self, result:Union[Results, ResultsList]) -> bool:
        """
        Return True if this RegressionExtractor can process the Result.

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
    def handle(self, result:Results) -> OrderedDict:
        """
        [Abstract] Extract values for testing.

        This method should return a dict of

        {'applycal.new_flags.science': 0.34,
         'applycal.new_flags.bandpass': 0.53}

        :param result:
        :return:
        """
        raise NotImplemented


class RegressionExtractorRegistry(object):
    """
    The registry and manager of the regression result extractor framework.

    The responsibility of the RegressionResultRegistry is to pass Results to
    RegressionExtractors that can handle them.
    """

    def __init__(self):
        """Constractor of this class."""
        self.__plugins_loaded = False
        self.__handlers = []

    def add_handler(self, handler: RegressionExtractor) -> None:
        """
        Push RegressionExtractor into handlers list __handler.

        Args:
            handler: RegressionExtractor
        """
        task = handler.generating_task.__name__ if handler.generating_task else 'all'
        child_name = ''
        if hasattr(handler.child_cls, '__name__'):
            child_name = handler.child_cls.__name__
        elif isinstance(handler.child_cls, collections.Iterable):
            child_name = str([x.__name__ for x in handler.child_cls])
        container = 's of %s' % child_name
        s = '{}{} results generated by {} tasks'.format(handler.result_cls.__name__, container, task)
        LOG.debug('Registering {} as new regression result handler for {}'.format(handler.__class__.__name__, s))
        self.__handlers.append(handler)


    def handle(self, result:Union[Results, ResultsList]) -> OrderedDict:
        """
        Extract values from corresponding Extractor object of Result object.

        Args:
            result: Results or ResultsList[Results]
        Return:
            Dict of extracted values
        """
        if not self.__plugins_loaded:
            for plugin_class in get_all_subclasses( RegressionExtractor ):
                self.add_handler(plugin_class())
            self.__plugins_loaded = True

        # this is the list which will contain extracted values tuples
        extracted = {}

        # Process leaf results first
        if isinstance(result, collections.Iterable):
            for r in result:
                d = self.handle(r)
                extracted = union(extracted, d)

        # process the group-level results.
        for handler in self.__handlers:
            if handler.is_handler_for(result):
                LOG.debug('{} extracting regression results for {}'.format(handler.__class__.__name__,
                                                                           result.__class__.__name__))
                d = handler.handle(result)
                extracted = union(extracted, d)

        return extracted


def union(d1: dict, d2: dict) -> OrderedDict:
    """
    Return the union of two dicts.
    
    It raises an exception if duplicate keys are detected in the input dicts.

    Args:
        d1, d2: dict for unioning
    Returns:
        OrderedDict unioned two dict
    """
    intersection = key_intersection(d1, d2)
    if intersection:
        raise ValueError('Regression keys are duplicated: {}'.format(intersection))
    # dict keys and values should be strings, so ok to shallow copy
    # OrderedDict is used to store results in processing order.
    u = OrderedDict(d1)
    u.update(d2)
    return u


def key_intersection(d1: dict, d2: dict) -> set:
    """
    Compare keys of two dicts, returning duplicate keys.

    Args:
        d1, d2: dict for comparison
    Returns:
        duplicated keys dict
    """
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    return d1_keys.intersection(d2_keys)


# default RegressionExtractorRegistry initialization
registry = RegressionExtractorRegistry()


class FluxcalflagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifa_gfluxscaleflag.

    The extracted values are:
       - the number of flagged rows before this task
       - the number of flagged rows after this task
       - QA score
    """

    result_cls = GfluxscaleflagResults
    child_cls = None

    def handle(self, result:GfluxscaleflagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: GfluxscaleflagResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, result.task)

        summaries_by_name = {s['name']: s for s in result.cafresult.summaries}

        num_flags_before = summaries_by_name['before']['flagged']

        if 'after' in summaries_by_name:
            num_flags_after = summaries_by_name['after']['flagged']
        else:
            num_flags_after = num_flags_before

        d = OrderedDict()
        d['{}.num_rows_flagged.before'.format(prefix)] = int(num_flags_before)
        d['{}.num_rows_flagged.after'.format(prefix)] = int(num_flags_after)

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class GcorFluxscaleRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hifa_gcorfluxscale.

    The extracted values are:
       - Stokes I for each field and spw, in Jy
    """

    result_cls = FluxCalibrationResults
    child_cls = None
    generating_task = GcorFluxscale

    def handle(self, result:FluxCalibrationResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: FluxCalibrationResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for field_id, measurements in result.measurements.items():
            for m in measurements:
                key = '{}.field_{}.spw_{}.I'.format(prefix, field_id, m.spw_id)
                d[key] = str(m.I.to_units(FluxDensityUnits.JANSKY))

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class ApplycalRegressionExtractor(RegressionExtractor):
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

    def handle(self, result:ApplycalResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: ApplycalResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)
        
        summaries_by_name = {s['name']: s for s in result.summaries}

        num_flags_before = summaries_by_name['before']['flagged']
        num_flags_after = summaries_by_name['applycal']['flagged']

        d = OrderedDict()
        d['{}.num_rows_flagged.before'.format(prefix)] = int(num_flags_before)
        d['{}.num_rows_flagged.after'.format(prefix)] = int(num_flags_after)

        flag_summary_before = summaries_by_name['before']
        for scan_id, v in flag_summary_before['scan'].items():
            d['{}.scan_{}.num_rows_flagged.before'.format(prefix, scan_id)] = int(v['flagged'])

        flag_summary_after = summaries_by_name['applycal']
        for scan_id, v in flag_summary_after['scan'].items():
            d['{}.scan_{}.num_rows_flagged.after'.format(prefix, scan_id)] = int(v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class SDApplycalRegressionExtractor(ApplycalRegressionExtractor):
    """
    Regression test result extractor for sd_applycal.

    It extends ApplycalRegressionExtractor in order to use the same extraction logic.
    """

    result_cls = ApplycalResults
    child_cls = None
    generating_task = HpcSDApplycal


class SDBLFlagRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_blfrag.

    The extracted values are:
       - the number of flagged and scanned rows before this task
       - the number of flagged and scanned rows after this task
    """

    result_cls = SDBLFlagResults
    child_cls = None
    generating_task = HpcSDBLFlag

    def handle(self, result:SDBLFlagResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDBLFlagResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()

        for summary in result.outcome['flagdata_summary']:
            name = prop = None
            for k, v in summary.items():
                if 'name' == k:
                    name = v
                elif isinstance(v, dict):
                    prop = v
            if name is not None and prop is not None:
                d['{}.num_rows_flagged.{}'.format(prefix, name)] = int(prop['flagged'])
                for scan_id, v in prop['scan'].items():
                    d['{}.scan_{}.num_rows_flagged.{}'.format(prefix, scan_id, name)] = int(v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class SDImagingRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_imaging.

    The extracted values are:
        - QA score
    """

    result_cls = SDImagingResults
    child_cls = None
    generating_task = SDImaging

    def handle(self, result:SDImagingResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDImagingResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class SDRestoredataRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for sd_restoredata.

    The extracted values are:
        - the number of flagged and scanned rows after this task
    """

    result_cls = SDRestoreDataResults
    child_cls = None
    generating_task = SDRestoreData

    def handle(self, result:SDRestoreDataResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: SDRestoreDataResults object
        
        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for session, mses in result.flagging_summaries.items():
            for ms_name, ms in mses.items():
                for target_name, target in ms.items():
                    if isinstance(target, dict):
                        d['{}.target_{}.num_rows_flagged'.format(prefix, target_name)] = int(target['flagged'])
                        for scan_id, v in target['scan'].items():
                            d['{}.target_{}.scan_{}.num_rows_flagged'.format(prefix, target_name, scan_id)] = int(v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


class NRORestoredataRegressionExtractor(RegressionExtractor):
    """
    Regression test result extractor for hsdn_restoredata.

    The extracted values are:
        - the number of flagged and scanned rows after this task
    """

    result_cls = NRORestoreDataResults
    child_cls = None
    generating_task = NRORestoreData

    def handle(self, result: NRORestoreDataResults) -> OrderedDict:
        """
        Extract values for testing.

        Args:
            result: NRORestoreDataInputs object

        Returns:
            OrderedDict[str, float]
        """
        prefix = get_prefix(result, self.generating_task)

        d = OrderedDict()
        for session, mses in result.flagging_summaries.items():
            for ms_name, ms in mses.items():
                for target_name, target in ms.items():
                    if isinstance(target, dict):
                        d['{}.target_{}.num_rows_flagged'.format(prefix, target_name)] = int(target['flagged'])
                        for scan_id, v in target['scan'].items():
                            d['{}.target_{}.scan_{}.num_rows_flagged'.format(prefix, target_name, scan_id)] = int(
                                v['flagged'])

        qa_entries = extract_qa_score_regression(prefix, result)
        d.update(qa_entries)

        return d


def get_prefix(result:Results, task:StandardTaskTemplate) -> str:
    """
    Return a string used to prefix string of rows of result text.

    Args:
        result: Result object
        task: Task object
    
    Returns:
        prefix string
    """
    # A value of result.inputs['vis'] of some classes (ex: SDImagingResults) is a list object
    res_vis = result.inputs['vis'][0] if isinstance(result.inputs['vis'], list) else result.inputs['vis']
    vis, _ = os.path.splitext(os.path.basename(res_vis))
    casa_task = task_registry.get_casa_task(task)
    prefix = 's{}.{}.{}'.format(result.stage_number, casa_task, vis)
    return prefix


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


def extract_regression_results(context: Context) -> List[str]:
    """
    Extract regression result and return logs.

    Args:
        context: Context object
    
    Returns:
        String list for logging
    """
    unified = OrderedDict()
    for results_proxy in context.results:
        results = results_proxy.read()
        unified = union(unified, registry.handle(results))

    # return unified
    return ['{}={}'.format(k, v) for k, v in unified.items()]


def get_all_subclasses(cls: RegressionExtractor) -> List[RegressionExtractor]:
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


# TODO enable runtime comparisons?
