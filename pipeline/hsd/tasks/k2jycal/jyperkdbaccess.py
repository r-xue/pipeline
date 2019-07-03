from __future__ import absolute_import

import urllib
import urllib2
import json
import numpy
import os
import re
import string

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools

LOG = infrastructure.get_logger(__name__)


class ALMAJyPerKDatabaseAccessBase(object):
    BASE_URL = 'https://asa.alma.cl/science/jy-kelvins'
    ENDPOINT_TYPE = None

    @property
    def url(self):
        assert self.ENDPOINT_TYPE is not None
        s = '/'.join([self.BASE_URL, self.ENDPOINT_TYPE])
        if not s.endswith('/'):
            s += '/'
        return s

    def __init__(self, context=None):
        self.context = context

    def _get_observing_band(self, ms):
        if self.context is None:
            return 'Unknown'

        spws = ms.get_spectral_windows(science_windows_only=True)
        bands = [spw.band for spw in spws]
        return numpy.unique(bands)

    def validate(self, vis):
        basename = os.path.basename(vis.rstrip('/'))
        try:
            ms = self.context.observing_run.get_ms(vis)
        except KeyError:
            LOG.error('{} is not registered to context'.format(basename))
            raise

        array_name = ms.antenna_array.name
        if array_name != 'ALMA':
            raise RuntimeError('{} is not ALMA data'.format(basename))

    def getJyPerK(self, vis):
        """
        getJyPerK returns list of Jy/K conversion factors with their
        meta data (MS name, antenna name, spwid, and pol string).

        Arguments:
            vis {str} -- Name of MS

        Returns:
            [list] -- List of Jy/K conversion factors with meta data
        """
        # sanity check
        self.validate(vis)

        # get Jy/K value from DB
        jyperk = self.get(vis)

        # convert to pipeline-friendly format
        basename = os.path.basename(re.sub('\.ms$', '', vis.rstrip('/')))
        formatted = self.format_jyperk(basename, jyperk)

        return formatted

    def get_params(self, vis):
        raise NotImplementedError

    def get(self, vis):
        """
        Access Jy/K DB and return its response.

        Arguments:
            vis {str} -- Name of MS

        Raises:
            e: urllib2.HTTPError
            e: urllib2.URLError

        Returns:
            [dict] -- Response from the DB as a dictionary. It should contain
                      the following keys:
                          'query' -- query data
                          'total' -- number of data
                          'data'  -- data
        """
        # set URL
        url = self.url

        params = self.get_params(vis)
        encoded = urllib.urlencode(params)

        try:
            # try opening url
            query = '?'.join([url, encoded])
            LOG.info('Accessing Jy/K DB: query is "{}"'.format(query))
            response = urllib2.urlopen(query)
        except urllib2.HTTPError as e:
            msg = 'Failed to load URL: {0}\n'.format(url) \
                + 'Error Message: HTTPError(code={0}, Reason="{1}")\n'.format(e.code, e.reason)
            LOG.error(msg)
            raise e
        except urllib2.URLError as e:
            msg = 'Failed to load URL: {0}\n'.format(url) \
                + 'Error Message: URLError(Reason="{0}")\n'.format(e.reason)
            LOG.error(msg)
            raise e

        LOG.info('URL="{0}"'.format(response.geturl()))

        retval = json.load(response)
        # retval should be a dict that consists of
        # 'query': query data
        # 'total': number of data
        # 'data': data
        return retval

    def format_jyperk(self, basename, jyperk):
        """
        Format given dictionary to the formatted list as below.

            [['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor'],
             ['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor'],
             ...
             ['MS_name', 'antenna_name', 'spwid', 'pol string', 'factor']]

        Arguments:
            basename {str} -- Basename of MS
            jyperk {dict} -- Dictionary containing Jy/K factors with meta data

        Returns:
            [list] -- Formatted list of Jy/K factors
        """
        template = string.Template('$vis $Antenna $Spwid I $Factor')
        data = jyperk['data']
        factors = [map(str, template.safe_substitute(vis=basename, **d).split()) for d in data]
        return factors


class JyPerKAsdmEndPoint(ALMAJyPerKDatabaseAccessBase):
    ENDPOINT_TYPE = 'asdm'

    def get_params(self, vis):
        return {'uid': vis_to_uid(vis)}


class JyPerKModelFitEndPoint(ALMAJyPerKDatabaseAccessBase):
    ENDPOINT_TYPE = 'model-fit'


def vis_to_uid(vis):
    basename = os.path.basename(vis.rstrip('/'))
    pattern = '^uid___A[0-9][0-9][0-9]_X[0-9a-f]+_X[0-9a-f]+\.ms$'
    if re.match(pattern, basename):
        return basename.replace('___', '://').replace('_', '/').replace('.ms', '')
    else:
        raise RuntimeError('MS name is not appropriate for DB query: {}'.format(basename))
