"""Module to access Jy/K DB (REST API)."""
import certifi
import collections
import datetime
import json
import os
import re
import ssl
import string
import urllib

from typing import TYPE_CHECKING, Any, Dict, Generator, Iterable, List, NewType, NoReturn, Optional, Union

import numpy

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.domain.measurementset import MeasurementSet as MS
    from pipeline.domain.spectralwindow import SpectralWindow


LOG = infrastructure.get_logger(__name__)

CasaQuantity = NewType('CasaQuantity', Dict[str, Union[str, float]])
MEpoch = NewType('MEpoch', Dict[str, Union[str, CasaQuantity]])

QueryStruct = collections.namedtuple('QueryStruct', ['param', 'subparam'])
ResponseStruct = collections.namedtuple('ResponseStruct', ['response', 'subparam'])


class ALMAJyPerKDatabaseAccessBase(object):
    """Base class for DB access."""

    BASE_URL = 'https://asa.alma.cl/science/jy-kelvins'
    ENDPOINT_TYPE = None

    @property
    def url(self) -> str:
        """Construct URL to access.

        ENDPOINT_TYPE property must be defined in each subclass.

        Returns:
            URL for DB (REST API)
        """
        assert self.ENDPOINT_TYPE is not None, \
            '{} cannot be instantiated. Please use subclasses.'.format(self.__class__.__name__)

        s = '/'.join([self.BASE_URL, self.ENDPOINT_TYPE])
        if not s.endswith('/'):
            s += '/'
        return s

    def __init__(self, context: Optional['Context'] = None) -> None:
        """Initialize ALMAJyPerKDatabaseAccessBase class.

        ALMAJyPerKDatabaseAccessBase is a base class for accessing Jy/K
        DB to retrieve conversion factor for ALMA TP data.
        ALMAJyPerKDatabaseAccessBase is kind of a template class that
        only provides a standard workflow to get a list of conversion
        factors. Each access class must inherit this class and
        implement/override some methods according to the target API.
        Subclasses must implement properties and methods listed below:

            ENDPOINT_TYPE (property): Must be a string representing the API
            access (method): Receive a list of queries as a generator,
                             access the DB through the generator, and
                             return the formatted response. Return value
                             should be a dictionary with 'query', 'data',
                             and 'total' fields. The 'query' field holds
                             the query whil the 'data' field stores the
                             response. The 'total' fields is the number of
                             response. Each item of the 'data' field should
                             consist of single conversion factor ('Factor')
                             with the meta-data, 'MS', 'Antenna', 'Spwid',
                             'Polarization'.
            get_params (method): Receive a name of the MS and generate
                                 a dictionary containing a list of query
                                 parameters. Required parameters depend on
                                 the API.

        Args:
            context: Pipeline Context (default: None)
        """
        self.context = context

    def _get_observing_band(self, ms: 'MS') -> numpy.ndarray:
        """Extract observing band from MS domain object.

        Args:
            ms: MS domain object

        Returns:
            List of observing band name for science spws
        """
        if self.context is None:
            return 'Unknown'

        spws = ms.get_spectral_windows(science_windows_only=True)
        bands = [spw.band for spw in spws]
        return numpy.unique(bands)

    def _generate_query(self, url: str, params: List[Dict[str, Any]]) -> Generator[ResponseStruct, None, None]:
        """Generate query and access DB (REST API).

        Args:
            url (): Base URL for DB access
            params (): List of parameters for DB (REST API).

        Raises:
            RuntimeError: DB access succeeded but its return value was invalid
            urllib.error.HTTPError: DB access failed
            urllib.error.URLError: DB access failed

        Yields:
            Response from DB
        """
        try:
            for p in params:
                # encode params
                encoded = urllib.parse.urlencode(p.param)

                # try opening url
                query = '?'.join([url, encoded])
                LOG.info('Accessing Jy/K DB: query is "{}"'.format(query))
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                # set timeout to 3min (=180sec)
                response = urllib.request.urlopen(query, context=ssl_context, timeout=180)
                retval = json.load(response)
                if not retval['success']:
                    msg = 'Failed to get a Jy/K factor from DB: {}'.format(retval['error'])
                    LOG.warning(msg)
                    raise RuntimeError(msg)
                yield ResponseStruct(response=retval, subparam=p.subparam)
        except urllib.error.HTTPError as e:
            msg = 'Failed to load URL: {0}\n'.format(url) \
                + 'Error Message: HTTPError(code={0}, Reason="{1}")\n'.format(e.code, e.reason)
            LOG.warning(msg)
            raise e
        except urllib.error.URLError as e:
            msg = 'Failed to load URL: {0}\n'.format(url) \
                + 'Error Message: URLError(Reason="{0}")\n'.format(e.reason)
            LOG.warning(msg)
            raise e

    def validate(self, vis: str) -> None:
        """Check if provided MS is valid or not.

        The method raises exception if,

            - MS is not registered to Pipeline context, or,
            - MS is not ALMA data.

        Args:
            vis: Name of MS

        Raises:
            KeyError: MS is not registered to Pipeline context
            RuntimeError: MS is not ALMA data
        """
        basename = os.path.basename(vis.rstrip('/'))
        try:
            ms = self.context.observing_run.get_ms(vis)
        except KeyError:
            LOG.error('{} is not registered to context'.format(basename))
            raise

        array_name = ms.antenna_array.name
        if array_name != 'ALMA':
            raise RuntimeError('{} is not ALMA data'.format(basename))

    def getJyPerK(self, vis: str) -> dict:
        """Return list of Jy/K conversion factors with their meta data.

        Args:
            vis: Name of MS

        Returns:
            Dictionary consists of list of Jy/K conversion factors with meta data and allsuccess (True or False).
        """
        # sanity check
        self.validate(vis)

        # get Jy/K value from DB
        jyperk = self.get(vis)
        allsuccess = jyperk['allsuccess']

        # convert to pipeline-friendly format
        formatted = self.format_jyperk(vis, jyperk)
        filtered = self.filter_jyperk(vis, formatted)
        return {'filtered': filtered, 'allsuccess': allsuccess}

    def get_params(self, vis: str) -> NoReturn:
        """Construct query parameter from MS.

        This must be implemented in each subclass.

        Args:
            vis: Name of MS

        Raises:
            NotImplementedError: always raise an exception
        """
        raise NotImplementedError

    def access(self, queries: Iterable[ResponseStruct]) -> NoReturn:
        """Access Jy/K DB.

        This must be implemented in each subclass.

        Args:
            queries: Queries to DB

        Raises:
            NotImplementedError: always raise an exception
        """
        raise NotImplementedError

    def get(self, vis: str) -> Dict[str, Any]:
        """Access Jy/K DB and return its response.

        Args:
            vis: Name of MS

        Raises:
            urllib2.HTTPError: DB access failed
            urllib2.URLError: DB access failed

        Returns:
            Response from the DB as a dictionary. It should contain
            the following keys:
                'query' -- query data
                'total' -- number of data
                'data'  -- data
        """
        # set URL
        url = self.url

        params = self.get_params(vis)

        queries = self._generate_query(url, params)

        retval = self.access(queries)
        # retval should be a dict that consists of
        # 'query': query data
        # 'total': number of data
        # 'data': response data
        return retval

    def format_jyperk(self, vis: str, jyperk: Dict[str, Any]) -> List[List[str]]:
        """Format jyperk dictionary.

        Format given dictionary to the formatted list as below.

            [['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor'],
             ['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor'],
             ...
             ['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor']]

        Args:
            vis: Name of MS
            jyperk: Dictionary containing Jy/K factors with meta data

        Returns:
            Formatted list of Jy/K factors
        """
        template = string.Template('$vis $Antenna $Spwid I $factor')
        data = jyperk['data']
        basename = os.path.basename(vis.rstrip('/'))
        factors = [list(map(str, template.safe_substitute(vis=basename, **d).split())) for d in data]
        return factors

    def filter_jyperk(self, vis: str, factors: List[List[str]]) -> List[List[str]]:
        """Perform filtering of Jy/K DB response.

        Returned list only contains the items for science spectral windows
        in the given MS. Other items are discarded by the method.

        Args:
            vis: Name of MS
            factors: List of Jy/K factors with meta data

        Returns:
            Filtered list of Jy/K factors with meta data
        """
        ms = self.context.observing_run.get_ms(vis)
        science_windows = [x.id for x in ms.get_spectral_windows(science_windows_only=True)]
        filtered = [i for i in factors if (len(i) == 5) and (i[0] == ms.basename) and (int(i[2]) in science_windows)]
        return filtered


class JyPerKAbstractEndPoint(ALMAJyPerKDatabaseAccessBase):
    """Base class for some query classes."""

    def get_params(self, vis: str) -> Generator[QueryStruct, None, None]:
        """Construct query parameter from MS.

        Args:
            vis: Name of MS

        Yields:
            Query parameter as QueryStruct instance
        """
        ms = self.context.observing_run.get_ms(vis)

        # parameter dictionary
        params = {}

        # date
        params['date'] = mjd_to_datestring(ms.start_time)

        # temperature
        params['temperature'] = get_mean_temperature(vis)

        # other
        params.update(self._aux_params())

        # loop over antennas and spws
        for ant in ms.antennas:
            # antenna name
            params['antenna'] = ant.name

            # elevation
            params['elevation'] = get_mean_elevation(self.context, vis, ant.id)

            for spw in ms.get_spectral_windows(science_windows_only=True):
                # observing band is taken from the string spw.band
                # whose format should be "ALMA Band X"
                params['band'] = int(spw.band.split()[-1])

                # baseband
                params['baseband'] = int(spw.baseband)

                # mean frequency
                params['frequency'] = get_mean_frequency(spw)

                # subparam is dictionary holding vis and spw id
                subparam = {'vis': vis, 'spwid': spw.id}
                yield QueryStruct(param=params, subparam=subparam)

    def access(self, queries: Iterable[ResponseStruct]) -> Dict[str, Any]:
        """Convert queries to response.

        Args:
            queries: Queries to DB

        Returns:
            Dictionary including response from DB
        """
        data = []
        allsuccess = True
        for result in queries:
            # response from DB
            response = result.response

            # subparam is dictionary holding vis and spw id
            subparam = result.subparam
            assert isinstance(subparam, dict)
            assert ('vis' in subparam) and ('spwid' in subparam)
            spwid = subparam['spwid']
            assert isinstance(spwid, int)
            vis = subparam['vis']
            assert isinstance(vis, str)
            basename = os.path.basename(vis.rstrip('/'))

            factor = self._extract_factor(response)
            polarization = 'I'
            antenna = response['query']['antenna']
            data.append({'MS': basename, 'Antenna': antenna, 'Spwid': spwid,
                         'Polarization': polarization, 'factor': factor})
            allsuccess = allsuccess and response['success']

        return {'query': '', 'data': data, 'total': len(data), 'allsuccess': allsuccess}

    def _aux_params(self) -> Dict[str, Any]:
        """Return endpoint-specific parameters.

        This returns empty dictionary. But it may be overridden by the subclasses.

        Returns:
            Endpoint-specific parameters
        """
        return {}

    def _extract_factor(self, response: Dict[str, Any]) -> NoReturn:
        """Extract Jy/K factor from the response.

        This must be implemented in each subclass.

        Args:
            response: Response from DB.

        Raises:
            NotImplementedError: always raise an exception
        """
        raise NotImplementedError


class JyPerKAsdmEndPoint(ALMAJyPerKDatabaseAccessBase):
    """Class to access 'asdm' endpoint of Jy/K DB."""

    ENDPOINT_TYPE = 'asdm'

    def get_params(self, vis: str) -> Generator[QueryStruct, None, None]:
        """Construct query parameter from MS.

        ASDM endpoint only requires ASDM uid.

        Args:
            vis: Name of MS

        Yields:
            Query paramter as QueryStruct instance
        """
        # subparam is vis
        yield QueryStruct(param={'uid': vis_to_uid(vis)}, subparam=vis)

    def access(self, queries: Iterable[ResponseStruct]) -> Dict[str, Any]:
        """Convert queries to response.

        Args:
            queries: Queries to DB

        Returns:
            response: Dictionary consists of response from DB
        """
        responses = list(queries)

        # there should be only one query
        assert len(responses) == 1

        response = responses[0].response
        response['total'] = response['data']['length']
        response['data'] = response['data']['factors']
        response['allsuccess'] = response['success']
        return response


class JyPerKModelFitEndPoint(JyPerKAbstractEndPoint):
    """Class to access 'model-fit' endpoint of Jy/K DB."""

    ENDPOINT_TYPE = 'model-fit'

    def _extract_factor(self, response: Dict[str, Any]) -> str:
        """Extract Jy/K factor from the response.

        Args:
            response: Response from DB.

        Returns:
            Jy/K conversion factor
        """
        return response['data']['factor']


class JyPerKInterpolationEndPoint(JyPerKAbstractEndPoint):
    """Class to access 'interpolation' endpoint of Jy/K DB."""

    ENDPOINT_TYPE = 'interpolation'

    def _aux_params(self) -> Dict[str, Any]:
        """Return endpoint-specific parameter.

        Appends 'delta_days' value to the parameter.

        Returns:
            Endpoint-specific parameter
        """
        return {'delta_days': 1000}

    def _extract_factor(self, response: Dict[str, Any]) -> str:
        """Extract Jy/K factor from the response.

        Args:
            response: Response from DB.

        Returns:
            Jy/K conversion factor
        """
        return response['data']['factor']['mean']


def vis_to_uid(vis: str) -> str:
    """Convert MS name into ASDM uid.

    This converts MS name like uid___A002_Xabcd_X012 into uid://A002/Xabcd/X012.

    Args:
        vis: Name of MS

    Raises:
        RuntimeError: MS name is incompatible with ASDM uid

    Returns:
        Corresponding ASDM uid
    """
    basename = os.path.basename(vis.rstrip('/'))
    pattern = r'^uid___A[0-9][0-9][0-9]_X[0-9a-f]+_X[0-9a-f]+\.ms$'
    if re.match(pattern, basename):
        return basename.replace('___', '://').replace('_', '/').replace('.ms', '')
    else:
        raise RuntimeError('MS name is not appropriate for DB query: {}'.format(basename))


def mjd_to_datestring(epoch: MEpoch) -> str:
    """Return string representation of MJD.

    Args:
        epoch: MEpoch dictionary created by measures.epoch

    Returns:
        MJD string
    """
    # casa_tools
    me = casa_tools.measures
    qa = casa_tools.quanta

    if epoch['refer'] != 'UTC':
        try:
            epoch = me.measure(epoch, 'UTC')
        finally:
            me.done()

    t = qa.splitdate(epoch['m0'])
    dd = datetime.datetime(t['year'], t['month'], t['monthday'], t['hour'], t['min'], t['sec'], t['usec'])
    #datestring = dd.strftime('%Y-%m-%dT%H:%M:%S.%f')
    datestring = dd.strftime('%Y-%m-%dT%H:%M:%S')
    return datestring


def get_mean_frequency(spw: 'SpectralWindow') -> float:
    """Return mean frequency of the spectral window.

    Args:
        spw: Spectral window domain object

    Returns:
        Mean frequency of spectral window in Hz
    """
    return float(spw.mean_frequency.convert_to(measures.FrequencyUnits.HERTZ).value)


def get_mean_temperature(vis: str) -> float:
    """Return mean temperature for MS.

    Take mean of the temperature measurement stored in the WEATHER subtable.

    Args:
        vis: Name of MS

    Returns:
        Mean temperature during observation, usually in Kelvin
    """
    with casa_tools.TableReader(os.path.join(vis, 'WEATHER')) as tb:
        valid_temperatures = numpy.ma.masked_array(
            tb.getcol('TEMPERATURE'),
            tb.getcol('TEMPERATURE_FLAG')
        )

    return valid_temperatures.mean()


def get_mean_elevation(context: 'Context', vis: str, antenna_id: int) -> float:
    """Return mean elevation for given antenna in the MS.

    Read elevation value from Datatable corresponding to MS and
    take its mean.

    Args:
        context: Pipeline context
        vis: Name of MS
        antenna_id: Antenna id

    Returns:
        Mean elevation in degree
    """
    dt_name = context.observing_run.ms_datatable_name
    ms = context.observing_run.get_ms(vis)
    basename = os.path.basename(ms.origin_ms)
    with casa_tools.TableReader(os.path.join(dt_name, basename, 'RO')) as tb:
        try:
            t = tb.query('ANTENNA=={}&&SRCTYPE==0'.format(antenna_id))
            assert t.nrows() > 0
            elevations = t.getcol('EL')
        finally:
            t.close()

    return elevations.mean()
