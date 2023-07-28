"""Provide a class to store logical representation of field."""
import pprint

import numpy as np

from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import utils

_pprinter = pprint.PrettyPrinter(width=1e99)


class Field(object):
    """
    A class to store logical representation of a field.

    Attributes:
        id: The numerical identifier of this field within the
            FIELD subtable of the MeasurementSet
        source_id: A source ID associated with this field
        time: A list of the unique times for this field
        name: Field name
        intents: A list of unique scan intents associated with this field
        states: A list of unique State objects associated with this field
        valid_spws: A list of unique SpectralWindow objects associated with
            this field
        flux_densities: A list of unique flux measurments from setjy
    """

    def __init__(self, field_id: int, name: str, source_id: int,
                 time: np.ndarray, direction: dict):
        """
        Initialize Field class.

        Args:
            field_id: Field ID
            name: Field name
            source_id: A source ID associated with this field
            time: A list of the unique times for this field
            direction: A direction measures for the phasecenter of this field
        """
        self.id = field_id
        self.source_id = source_id
        self.time = time
        self.name = name

        self._mdirection = direction

        self.intents = set()
        self.states = set()
        self.valid_spws = set()
        self.flux_densities = set()

    def __repr__(self):
        name = self.name
        if '"' in name:
            name = name[1:-1]

        return 'Field({0}, {1!r}, {2}, {3}, {4})'.format(
            self.id,
            name,
            self.source_id,
            'numpy.array(%r)' % self.time.tolist(),
            _pprinter.pformat(self._mdirection)
        )

    @property
    def clean_name(self):
        """
        Get the field name with illegal characters replaced with underscores.

        This property is used to determine whether the field name, when given
        as a CASA argument, should be enclosed in quotes.
        """
        return utils.fieldname_clean(self._name)

    @property
    def dec(self):
        return casa_tools.quanta.formxxx(self.latitude, format='dms', prec=2)

    @property
    def frame(self):
        return self._mdirection['refer']

    @property
    def identifier(self):
        """
        A human-readable identifier for this Field.
        """
        return self.name if self.name else '#{0}'.format(self.id)

    @property
    def latitude(self):
        return self._mdirection['m1']

    @property
    def longitude(self):
        return self._mdirection['m0']

    @property
    def mdirection(self):
        return self._mdirection

    @property
    def name(self):
        # SCOPS-1666
        # work around CASA data selection problems with names consisting
        # entirely of digits
        return utils.fieldname_for_casa(self._name)

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def ra(self):
        return casa_tools.quanta.formxxx(self.longitude, format='hms', prec=3)

    # Galactic Longitude: it is usually expressed in DMS format
    @property
    def gl(self):
        return casa_tools.quanta.formxxx(self.longitude, format='dms', prec=2)

    # Galactic Latitude
    @property
    def gb(self):
        return self.dec

    # Time on source
    @property
    def time_on_source(self):
       """
       Return the time on source in seconds. The implementation relies
       on the msmd.timesforfield() time stamps that are stored in
       self.time. These appear to be in general on the 16 ms granularity
       for ALMA. There are gaps between the observations when other fields
       are observed and when other scans are done. The simple algorithm
       here tries to exclude these gaps to sum up the deltas. It does not
       correct for edge effects (half integrations before the first and
       after the last time stamp per observation). This is negligible for
       the 16 ms granularity and the intended use of this time for the
       heuristic requested in PIPE-1782. A fully fledged solution would
       involve reading the INTERVAL column of the MS for a given field
       based selection. This is more time consuming and should only be
       considered if the current method is not accurate enough.
       """

       delta_times = self.time[1:]-self.time[:-1]
       median_delta_time = np.median(delta_times)
       return np.sum(delta_times[delta_times <= 3 * median_delta_time])

    def set_source_type(self, source_type):
        source_type = source_type.strip().upper()

        # replace any VLA source_type with pipeline/ALMA intents
        source_type = source_type.replace('SOURCE', 'TARGET')
        source_type = source_type.replace('GAIN', 'PHASE')
        source_type = source_type.replace('FLUX', 'AMPLITUDE')

        for intent in ['BANDPASS', 'PHASE', 'AMPLITUDE', 'TARGET', 'POINTING',
                       'WVR', 'ATMOSPHERE', 'SIDEBAND', 'POLARIZATION',
                       'POLANGLE', 'POLLEAKAGE', 'CHECK', 'DIFFGAIN', 'UNKNOWN',
                       'SYSTEM_CONFIGURATION']:
            if source_type.find(intent) != -1:
                self.intents.add(intent)

    def __str__(self):
        return '<Field {id}: name=\'{name}\' intents=\'{intents}\'>'.format(
            id=self.identifier, name=self.name,
            intents=','.join(self.intents))
