import collections
import itertools
import operator
import os
from typing import List, Tuple, Union

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from . import DataType
from . import MeasurementSet

LOG = infrastructure.get_logger(__name__)


class ObservingRun(object):
    def __init__(self):        
        self.measurement_sets = []
        self.virtual_science_spw_ids = {}
        self.virtual_science_spw_names = {}
        self.virtual_science_spw_shortnames = {}

    def add_measurement_set(self, ms):
        if ms.basename in [m.basename for m in self.measurement_sets]:
            msg = '{0} is already in the pipeline context'.format(ms.name)
            LOG.error(msg)
            raise Exception(msg)

        # Initialise virtual science spw IDs from first MS 
        if self.measurement_sets == []:
            self.virtual_science_spw_ids = \
                dict((int(s.id), s.name) for s in ms.get_spectral_windows(science_windows_only=True))
            self.virtual_science_spw_names = \
                dict((s.name, int(s.id)) for s in ms.get_spectral_windows(science_windows_only=True))
            self.virtual_science_spw_shortnames = {}
            for name in self.virtual_science_spw_names:
                if 'ALMA' in name:
                    i = name.rfind('#')
                    if i != -1:
                        self.virtual_science_spw_shortnames[name] = name[:i]
                    else:
                        self.virtual_science_spw_shortnames[name] = name
                else:
                    self.virtual_science_spw_shortnames[name] = name
        else:
            for s in ms.get_spectral_windows(science_windows_only=True):
                if s.name not in self.virtual_science_spw_names:
                    msg = 'Science spw name {0} (ID {1}) of EB {2} does not match spw names of first EB. Virtual spw' \
                          ' ID mapping will not work.'.format(s.name, s.id,
                                                              os.path.basename(ms.name).replace('.ms', ''))
                    LOG.error(msg)

        self.measurement_sets.append(ms)

    def get_ms(self, name=None, intent=None):
        """Returns the first measurement set matching the given identifier. 
        Identifier precedence is name then intent.
        """
        if name:
            for ms in self.measurement_sets:
                if name in (ms.name, ms.basename):
                    return ms

            for ms in self.measurement_sets:
                # single dish data are registered without the MS suffix
                with_suffix = '%s.ms' % name
                if with_suffix in (ms.name, ms.basename):
                    return ms

            raise KeyError('No measurement set found with name {0}'.format(name))

        if intent:
            # Remove any extraneous characters, as intent could be specified
            # as *BANDPASS* for example
            intent = intent.replace('*', '')
            for ms in self.measurement_sets:
                for field in ms.fields:
                    if intent in field.intents:
                        return ms
            raise KeyError('No measurement set found with intent {0}'.format(intent))

    def get_measurement_sets(self, names=None, intents=None, fields=None):
        """
        Returns measurement sets matching the given arguments.
        """
        candidates = self.measurement_sets

        # filter out MeasurementSets with no vis hits
        if names is not None:
            candidates = [ms for ms in candidates
                          if ms.name in names]

        # filter out MeasurementSets with no intent hits
        if intents is not None:
            if isinstance(intents, str):
                intents = utils.safe_split(intents)
            intents = set(intents)

            candidates = [ms for ms in candidates
                          if intents.issubset(ms.intents)]

        # filter out MeasurementSets with no field name hits
        if fields is not None:
            if isinstance(fields, str):
                fields = utils.safe_split(fields)
            fields_to_match = set(fields)

            candidates = [ms for ms in candidates
                          if fields_to_match.isdisjoint({field.name for field in ms.fields})]

        return candidates

    def get_measurement_sets_of_type(self, dtypes: List[DataType],
                                     msonly: bool=True) -> Union[List[MeasurementSet],
                                                                 Tuple[collections.OrderedDict, DataType]]:
        """
        Return a list of MeasurementSet domain object with matching DataType.

        The method returns a list of MSes of the first matching DataType in the
        list of dtypes.

        Args:
            dtypes: Search order of DataType. The search starts with the
                first DataType in the list and fallbacks to another DataType
                in the list only if no MS is found with the searched DataType.
                The search order of DataType is in the order of elements in
                list. Search stops at the first DataType with which at least
                one MS is found.
            msonly: If True, return a list of MS domain object only.

        Returns:
            When msonly is True, a list of MeasurementSet domain objects of
            a matching DataType is returned.
            Otherwise, a tuple of an ordered dictionary and a matched DataType
            is returned. The ordered dictionary stores matching MS domain
            objects as keys and matching data column names as values. The order
            of elements is that appears in measurement_sets list attribute.
        """
        found = []
        column = []
        for dtype in dtypes:
            for ms in self.measurement_sets:
                dcol = ms.get_data_column(dtype)
                if dcol is not None:
                    found.append(ms)
                    column.append(dcol)
            if len(found) > 0:
                break
        if len(found) > 0:
            LOG.debug('Found {} MSes with type {}'.format(len(found), dtype))
        else:
            LOG.debug('No MSes are found with types {}'.format(dtypes))
        if msonly:
            return found
        else:
            ms_dict = collections.OrderedDict( (m, c) for m, c in zip(found, column))
            return ms_dict, dtype

    def get_fields(self, names=None):
        """
        Returns fields matching the given arguments from all measurement sets.
        """        
        match = [ms.fields for ms in self.measurement_sets]
        # flatten the fields lists to one sequence
        match = itertools.chain(*match)

        if names is not None:
            if isinstance(names, str):
                names = utils.safe_split(names)
            names = set(names)
            match = [f for f in match if f.name in names] 

        return match

    @staticmethod
    def get_real_spw_id_by_name(spw_name, target_ms):
        """
        :param spw_name: the spw name to convert
        :type spw_name: string
        :param target_ms: the MS to map spw_name to
        :type target_ms: domain.MeasurementSet
        """
        spw_id = None
        for spw in target_ms.get_spectral_windows(science_windows_only=True):
            if spw.name == spw_name:
                spw_id = spw.id
        return spw_id

    def get_virtual_spw_id_by_name(self, spw_name):
        """
        :param spw_name: the spw name to convert
        :type spw_name: string
        """
        return self.virtual_science_spw_names.get(spw_name, None)

    def virtual2real_spw_id(self, spw_id, target_ms):
        """
        :param spw_id: the spw id to convert
        :type spw_id: integer
        :param target_ms: the MS to map spw_id to
        :type target_ms: domain.MeasurementSet
        """
        return self.get_real_spw_id_by_name(self.virtual_science_spw_ids.get(int(spw_id), None), target_ms)

    def real2virtual_spw_id(self, spw_id, target_ms):
        """
        :param spw_id: the spw id to convert
        :type spw_id: integer
        :param target_ms: the MS to map spw_id to
        :type target_ms: domain.MeasurementSet
        """
        return self.get_virtual_spw_id_by_name(target_ms.get_spectral_window(int(spw_id)).name)

    def get_real_spwsel(self, spwsel, vis):
        """
        :param spwsel: the list of spw selections to convert
        :type spwsel: list of strings
        :param vis: the list of MS names to map spwsel to
        :type vis: list of MS names
        """
        real_spwsel = []
        for spwsel_item, ms_name in zip(spwsel, vis):
            real_spwsel_items = []
            for spw_item in spwsel_item.split(','):
                if spw_item.find(':') == -1:
                    real_spw_id = self.virtual2real_spw_id(int(spw_item), self.get_ms(ms_name))
                    real_spwsel_items.append(str(real_spw_id))
                else:
                    virtual_spw_id, selection = spw_item.split(':')
                    real_spw_id = self.virtual2real_spw_id(int(virtual_spw_id), self.get_ms(ms_name))
                    real_spwsel_items.append('%s:%s' % (str(real_spw_id), selection))
            real_spwsel.append(','.join(real_spwsel_items))
        return real_spwsel

    @property
    def start_time(self):
        if not self.measurement_sets:
            return None
        earliest, _ = min([(ms, utils.get_epoch_as_datetime(ms.start_time)) for ms in self.measurement_sets],
                          key=operator.itemgetter(1))
        return earliest.start_time

    @property
    def start_datetime(self):
        if not self.start_time:
            return None
        return utils.get_epoch_as_datetime(self.start_time)

    @property
    def end_time(self):
        if not self.measurement_sets:
            return None
        latest, _ = max([(ms, utils.get_epoch_as_datetime(ms.end_time)) for ms in self.measurement_sets],
                        key=operator.itemgetter(1))
        return latest.end_time

    @property
    def end_datetime(self):
        if not self.end_time:
            return None
        return utils.get_epoch_as_datetime(self.end_time)

    @property
    def project_ids(self):
        return {ms.project_id for ms in self.measurement_sets}

    @property
    def schedblock_ids(self):
        return {ms.schedblock_id for ms in self.measurement_sets}

    @property
    def execblock_ids(self):
        return {ms.execblock_id for ms in self.measurement_sets}

    @property
    def observers(self):
        return {ms.observer for ms in self.measurement_sets}
