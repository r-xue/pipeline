# Do not evaluate type annotations at definition time.
from __future__ import annotations

import collections
import itertools
import operator
import os
from typing import Iterable, TYPE_CHECKING

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from .datatype import DataType, TYPE_PRIORITY_ORDER
from . import MeasurementSet

if TYPE_CHECKING:
    from datetime import datetime
    from pipeline.domain.field import Field

LOG = infrastructure.get_logger(__name__)


def sort_measurement_set(ms: MeasurementSet) -> tuple[int, datetime]:
    """Return sort key for measurement set object.

    Sort key consists of data priority and observation start time.
    Data priority is determined from the data types of given
    measurement set. Sort order of the data types is defined in
    DATA_TYPE_PRIORITY list. If no DataType is set to the MS,
    the lowest priority is assigned to the MS.

    Returns:
        Data priority and observation start time
    """
    data_types = set(itertools.chain(*ms.data_types_per_source_and_spw.values()))
    assert all([t in TYPE_PRIORITY_ORDER for t in data_types])
    if len(data_types) > 0:
        data_priority = max([TYPE_PRIORITY_ORDER.index(t) for t in data_types])
    else:
        # no data type assignments, assign lowest priority (largest value)
        data_priority = len(TYPE_PRIORITY_ORDER)
    observation_start_time = utils.get_epoch_as_datetime(ms.start_time)
    return data_priority, observation_start_time


class ObservingRun(object):
    """
    ObservingRun is a logical representation of an observing run.

    Attributes:
        measurement_sets: List of measurementSet objects associated with run.
        org_directions: Dictionary with Direction objects of the origin (ALMA
            Single-Dish only).
        virtual_science_spw_ids: Dictionary mapping each virtual science
            spectral window ID (key) to corresponding science spectral window
            name (value) that is shared across all measurement sets in the
            observing run.
        virtual_science_spw_names: Dictionary mapping each science spectral
            window name (key) to corresponding virtual spectral window ID (value).
        virtual_science_spw_shortnames: Dictionary mapping each science spectral
            window name (key) to a shortened version of the name (value).
    """
    def __init__(self) -> None:
        """
        Initialize an ObservingRun object.
        """
        self.measurement_sets: list[MeasurementSet] = []
        self.org_directions = {}
        self.virtual_science_spw_ids: dict[int, str] = {}  # PIPE-123
        self.virtual_science_spw_names: dict[str, int] = {}  # PIPE-123
        self.virtual_science_spw_shortnames: dict[str, str] = {}  # PIPE-123

    def add_measurement_set(self, ms: MeasurementSet) -> None:
        """Add a MeasurementSet to the ObservingRun."""
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
        self.measurement_sets.sort(key=sort_measurement_set)

    def get_ms(self, name: str = None, intent: str = None) -> MeasurementSet:
        """
        Returns the first measurement set matching the given identifier.

        Identifier precedence is name then intent.

        Args:
            name: Name to find matching measurement set for.
            intent: Intent to find matching measurement set for.

        Returns:
            MeasurementSet object for first match found.

        Raises:
            KeyError, if no measurement set is found for given name or intent.
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

    def get_measurement_sets(self, names: str | None = None, intents: Iterable | str | None = None,
                             fields: Iterable | str | None = None) -> list[MeasurementSet]:
        """
        Returns measurement sets matching the given arguments.

        Args:
            names: Name(s) to find matching measurement set(s) for.
            intents: Intent(s) to find matching measurement set(s) for.
            fields: Field name(s) to find matching measurement set(s) for.

        Returns:
            List of MeasurementSet objects for measurement sets matching given
            arguments.
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
                          if not fields_to_match.isdisjoint({field.name for field in ms.fields})]

        return candidates

    def get_measurement_sets_of_type(self, dtypes: list[DataType], msonly: bool = True, source: str | None = None,
                                     spw: str | None = None, vis: list[str] | None = None) \
            -> list[MeasurementSet] | tuple[collections.OrderedDict, DataType | None]:
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
            source: Filter for particular source name selection (comma
                separated list of names).
            spw: Filter for particular virtual spw specification (comma
                separated list of virtual IDs).
            vis: Filter for list of MS names (list of strings)

        Returns:
            When msonly is True, a list of MeasurementSet domain objects of
            a matching DataType (and optionally sources and spws) is returned.
            Otherwise, a tuple of an ordered dictionary and a matched DataType
            (and optionally sources and spws) is returned. The ordered
            dictionary stores matching MS domain objects as keys and matching
            data column names as values. The order of elements is that appears
            in measurement_sets list attribute.
        """
        found = []
        column = []
        for dtype in dtypes:
            for ms in self.measurement_sets:
                # Filter for vis list
                if isinstance(vis, list):
                    if ms.name not in vis:
                        continue
                if spw is not None:
                    real_spw_ids = ','.join(str(self.virtual2real_spw_id(spw_id, ms)) for spw_id in spw.split(',') if self.virtual2real_spw_id(spw_id, ms) is not None)
                    if not real_spw_ids:
                        real_spw_ids = None
                else:
                    real_spw_ids = None
                dcol = ms.get_data_column(dtype, source, real_spw_ids)
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
            if len(found) > 0:
                ms_dict = collections.OrderedDict( (m, c) for m, c in zip(found, column))
                return ms_dict, dtype
            else:
                return collections.OrderedDict(), None

    # TODO: appears unused, remove?
    def get_fields(self, names: str | None = None) -> list[Field]:
        """
        Returns fields matching the given arguments from all measurement sets in
        this observing run.

        Args:
            names: Field name(s) to match. If None, it will match all field(s) found.

        Returns:
            List of Field objects for fields in all measurements sets in this
            observing run, filtered by given field names.
        """
        match = [ms.fields for ms in self.measurement_sets]
        # flatten the fields lists to one sequence
        match = itertools.chain(*match)

        if names is not None:
            if isinstance(names, str):
                names = utils.safe_split(names)
            names = set(names)
            match = [f for f in match if f.name in names]
        else:
            match = list(match)

        return match

    @staticmethod
    def get_real_spw_id_by_name(spw_name: str | None, target_ms: MeasurementSet) -> int | None:
        """
        Translate a (science) spw name to the real spw ID for a given MS.

        Args:
            spw_name: The spectral window name to convert to ID.
            target_ms: The MS for which to map name to real spectral window ID.

        Returns:
            Real spectral window ID for given spectral window name in given MS,
            or None if name was not found in the given MS.
        """
        spw_id = None
        for spw in target_ms.get_spectral_windows(science_windows_only=True):
            if spw.name == spw_name:
                spw_id = spw.id
        return spw_id

    def get_virtual_spw_id_by_name(self, spw_name: str) -> int | None:
        """
        Translate a (science) spw name to the virtual spw ID for this pipeline run.

        Args:
            spw_name: The spectral window name to convert.

        Returns:
            Virtual spectral window ID for given name, or None if no virtual
            spw was found with given name.
        """
        return self.virtual_science_spw_names.get(spw_name, None)

    def virtual2real_spw_id(self, spw_id: int | str, target_ms: MeasurementSet) -> int | None:
        """
        Translate a virtual (science) spw ID to the real one for a given MS.

        Args:
            spw_id: The virtual spectral window ID (as integer or string) to convert.
            target_ms: The MS for which to map virtual spw ID to real spw ID.

        Returns:
            Real spectral window ID matching given virtual spectral ID in given
            MS. Returns None if either the given virtual spw ID does not exist
            or the given virtual spw ID does not appear in given MS.
        """
        return self.get_real_spw_id_by_name(self.virtual_science_spw_ids.get(int(spw_id), None), target_ms)

    def real2virtual_spw_id(self, spw_id: int | str, target_ms: MeasurementSet) -> int | None:
        """
        Translate a real (science) spw ID of a given MS to the virtual one for this pipeline run.

        Args:
            spw_id: The real spectral window ID (as integer or string) to convert.
            target_ms: The MS for which to map its real spw ID to the virtual spw ID.

        Returns:
            Virtual spectral window ID in the observing run corresponding to the
            name of the given real spectral window ID in the given MS, or None
            if the spectral window name was not found.
        """
        return self.get_virtual_spw_id_by_name(target_ms.get_spectral_window(int(spw_id)).name)

    def get_real_spwsel(self, spwsel: list[str], vis: list[str]) -> list[str]:
        """
        Translate a virtual (science) spw selection to the real one for a given MS.

        Args:
            spwsel: The list of spw selections to convert.
            vis: The list of MS names for which to convert the virtual spw
                selection to a real spw selection.

        Returns:
            List of real spw selections for given virtual spw selections and
            MS names.
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
    def start_time(self) -> dict | None:
        """
        Return start time of earliest measurement set in this observing run.

        Returns earliest MS start time as a CASA 'epoch' measure dictionary, or
        None if there are no measurement sets registered in this observing run.
        """
        if not self.measurement_sets:
            return None
        earliest, _ = min([(ms, utils.get_epoch_as_datetime(ms.start_time)) for ms in self.measurement_sets],
                          key=operator.itemgetter(1))
        return earliest.start_time

    @property
    def start_datetime(self) -> datetime | None:
        """
        Return start date and time of earliest measurement set in this observing run.

        Returns earliest MS start time as a datetime object, or None if there
        are no measurement sets registered in this observing run.
        """
        if not self.start_time:
            return None
        return utils.get_epoch_as_datetime(self.start_time)

    @property
    def end_time(self) -> dict | None:
        """
        Return end time of latest measurement set in this observing run.

        Returns latest MS end time as a CASA 'epoch' measure dictionary, or
        None if there are no measurement sets registered in this observing run.
        """
        if not self.measurement_sets:
            return None
        latest, _ = max([(ms, utils.get_epoch_as_datetime(ms.end_time)) for ms in self.measurement_sets],
                        key=operator.itemgetter(1))
        return latest.end_time

    @property
    def end_datetime(self) -> datetime | None:
        """
        Return end date and time of latest measurement set in this observing run.

        Returns latest MS end time as a datetime object, or None if there
        are no measurement sets registered in this observing run.
        """
        if not self.end_time:
            return None
        return utils.get_epoch_as_datetime(self.end_time)

    @property
    def project_ids(self) -> set[str]:
        """Return unique project ID(s) associated with the MSes in this observing run."""
        return {ms.project_id for ms in self.measurement_sets}

    @property
    def schedblock_ids(self) -> set[str]:
        """Return unique scheduling block ID(s) associated with the MSes in this observing run."""
        return {ms.schedblock_id for ms in self.measurement_sets}

    @property
    def execblock_ids(self) -> set[str]:
        """Return unique execution block ID(s) associated with the MSes in this observing run."""
        return {ms.execblock_id for ms in self.measurement_sets}

    @property
    def observers(self) -> set[str]:
        """Return unique observer(s) associated with the MSes in this observing run."""
        return {ms.observer for ms in self.measurement_sets}
