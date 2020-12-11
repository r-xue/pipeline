import pprint

from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import utils

_pprinter = pprint.PrettyPrinter(width=1e99)


class Field(object):
    def __init__(self, field_id, name, source_id, time, direction):
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

    def set_source_type(self, source_type):
        source_type = source_type.strip().upper()

        # replace any VLA source_type with pipeline/ALMA intents
        source_type = source_type.replace('SOURCE', 'TARGET')
        source_type = source_type.replace('GAIN', 'PHASE')
        source_type = source_type.replace('FLUX', 'AMPLITUDE')

        for intent in ['BANDPASS', 'PHASE', 'AMPLITUDE', 'TARGET', 'POINTING', 
                       'WVR', 'ATMOSPHERE', 'SIDEBAND', 'POLARIZATION',
                       'POLANGLE', 'POLLEAKAGE', 'CHECK', 'UNKNOWN',
                       'SYSTEM_CONFIGURATION']:
            if source_type.find(intent) != -1:
                self.intents.add(intent)

    def __str__(self):
        return '<Field {id}: name=\'{name}\' intents=\'{intents}\'>'.format(
            id=self.identifier, name=self.name, 
            intents=','.join(self.intents))
