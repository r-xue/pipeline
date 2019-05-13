'''
The qaadapter module holds classes that regroup the results list, ordered by
stage number, into a structure ordered by task type. This regrouping is used
by the QA sections of the weblog.
'''
import collections
import json
import os
import weakref

import pipeline.infrastructure as infrastructure

LOG = infrastructure.get_logger(__name__)


class Topic(object):    
    """
    Topic is the base class for a Topic web log section. It should not be
    instantiated directly.    
    """
    def __init__(self, url, description):
        """
        Construct a new Topic.
        """
        # filename pointing to the more detailed T2-3-X sections for this Topic
        # grouping
        self.url = url

        # text shown in weblog describing this Topic summary section 
        self.description = description

        # set holding which Results classes have been registered in this topic
        self._registered_classes = set()

        # Within each Topic, results are gathered together by type,
        # eg. all bandpass tasks under a 'Bandpass' heading. This is
        # accomplished with the results_by_type dictionary, keyed by results
        # class and with the matching results objects as values, eg.
        # results_by_type[BandpassResults] = [bpresult1, bpresult2]
        self.results_by_type = {}

    def register_result_class(self, result_class):
        self._registered_classes.add(result_class)

    def assign_to_topics(self, results):
        """
        Allocating Results to the appropriate task type based on the Results
        classes registered to this Topic.

        :param results: the results
        :type results: a list of :class:`~pipeline.infrastructure.api.Result`
        """
        # collect each results node matching a recognised class for this
        # topic, adding it to the list held as the dictionary value
        in_section = filter(self.handles_result, results)
        d = collections.defaultdict(list)
        for result in in_section:
            ref = weakref.proxy(result)
            d[result.__class__].append(ref)

        self.results_by_type.clear()
        self.results_by_type.update(d)

    def handles_result(self, result):
        """
        Return True if the given Result is part of this topic.

        :param result: the task result
        :type result: :class:`~pipeline.infrastructure.api.Result`
        """
        if result.__class__ in self._registered_classes:
            return True

        # list containing handled results (not necessarily of the same class)
        if isinstance(result, collections.Iterable) \
                and all([o.__class__ in self._registered_classes for o in result]):
            return True

        return False

class DataSetTopic(Topic):
    """
    DataSetTopic collects together those results generated by data set
    handling tasks.
    """    
    def __init__(self):
        super(DataSetTopic, self).__init__('t2-3-1m.html', 'Data Sets')


class CalibrationTopic(Topic):
    """
    CalibrationTopic collects together those results generated by
    calibration tasks.
    """    
    def __init__(self):
        super(CalibrationTopic, self).__init__('t2-3-2m.html', 'Calibration')


class FlaggingTopic(Topic):
    """
    FlaggingTopic collects together those results generated by flagging
    tasks.
    """
    def __init__(self):
        super(FlaggingTopic, self).__init__('t2-3-3m.html', 'Flagging')


class LineFindingTopic(Topic):
    """
    LineFindingTopic collects together those results generated by
    line-finding tasks.
    """
    def __init__(self):
        super(LineFindingTopic, self).__init__('t2-3-4m.html', 'Line Finding')


class ImagingTopic(Topic):
    """
    ImagingTopic collects together those results generated by imaging
    tasks.
    """
    def __init__(self):
        super(ImagingTopic, self).__init__('t2-3-5m.html', 'Imaging')


class MiscellaneousTopic(Topic):
    """
    MiscellaneousTopic collects together those results generated by
    miscellaneous tasks that don't belong in any other category.
    """
    def __init__(self):
        super(MiscellaneousTopic, self).__init__('t2-3-6m.html', 'Miscellaneous')


class TopicRegistry(object):
    # holds the Topics into which we should push the results 
    topic_classes = [DataSetTopic,
                     CalibrationTopic,
                     FlaggingTopic,
                     LineFindingTopic,
                     ImagingTopic,
                     MiscellaneousTopic]

    def __init__(self):
        """
        Construct a new ResultsToTopicAdapter, allocating Results to the
        appropriate Topic sections.  

        :param results: the results
        """
        self._topics = collections.OrderedDict()
        for cls in self.topic_classes:
            self._topics[cls] = cls()

    def _register_to_topic(self, topic_class, result_class):
        LOG.debug('Registering %s to %s topic', 
                  result_class.__name__, topic_class.__name__)
        self._topics[topic_class].register_result_class(result_class)

    def assign_to_topics(self, results):
        for topic in self._topics.itervalues():
            topic.assign_to_topics(results)

    def register_to_dataset_topic(self, result_class):
        self._register_to_topic(DataSetTopic, result_class)

    def register_to_calibration_topic(self, result_class):
        self._register_to_topic(CalibrationTopic, result_class)

    def register_to_flagging_topic(self, result_class):
        self._register_to_topic(FlaggingTopic, result_class)

    def register_to_imaging_topic(self, result_class):
        self._register_to_topic(ImagingTopic, result_class)

    def register_to_linefinding_topic(self, result_class):
        self._register_to_topic(LineFindingTopic, result_class)

    def register_to_miscellaneous_topic(self, result_class):
        self._register_to_topic(MiscellaneousTopic, result_class)

    def get_dataset_topic(self):
        return self._topics[DataSetTopic]

    def get_calibration_topic(self):
        return self._topics[CalibrationTopic]

    def get_flagging_topic(self):
        return self._topics[FlaggingTopic]

    def get_imaging_topic(self):
        return self._topics[ImagingTopic]

    def get_linefinding_topic(self):
        return self._topics[LineFindingTopic]

    def get_miscellaneous_topic(self):
        return self._topics[MiscellaneousTopic]

    def get_topics(self):
        return self._topics

    def get_url(self, result):
        """
        Get the URL of the QA section appropriate to the given result.

        :param result: the task result
        :type result: :class:`~pipeline.infrastructure.api.Result`
        :rtype: the filename of the appropriate QA section
        """
        return [topic.url for topic in self._topics 
                if topic.handles_result(result)]


class QABaseAdapter(object):
    def __init__(self, context, result):
        if not getattr(result, 'qa', None):
            return

        self._context = context
        self._result = result
        self._qa = result.qa

        vis = self._result.inputs['vis']
        self._ms = self._context.observing_run.get_ms(vis)
        self._write_json(context, result)

    def _write_json(self, context, result):
        json_file = os.path.join(context.report_dir, 
                                 'stage%s' % result.stage_number, 
                                 'qa.json')

        LOG.trace('Writing QA data to %s' % json_file)
        with open(json_file, 'w') as fp:
            json.dump(self.scores, fp)


registry = TopicRegistry()

