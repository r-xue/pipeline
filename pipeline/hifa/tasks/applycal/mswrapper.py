import numpy

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.logging as logging

LOG = logging.get_logger(__name__)


class MSWrapper(object):
    """
    MSWrapper is a wrapper around a NumPy array populated with measurement set
    data for a specified scan and spectral window. The MSWrapper can be
    filtered on various criteria, e.g, spw, scan, antenna, etc., to narrow the
    data to a particular data selection.

    The static method MSWrapper.create_from_ms should be used to instantiate
    MSWrapper objects.
    """

    @staticmethod
    def create_from_ms(filename, scan, spw):
        """
        Create a new MSWrapper for the specified scan and spw.

        Reading in raw measurement set data can be a very memory-intensive
        process, so data selection is deliberately limited to one scan and one
        spw at a time.

        :param filename: measurement set filename
        :param scan: integer scan ID
        :param spw: integer spw ID
        :return:
        """
        LOG.trace('MSWrapperFactory.from_ms(%r, %r, %r)', filename, scan, spw)

        data_selection = dict(scan=str(scan), spw=str(spw))
        colnames = ['antenna1', 'antenna2', 'flag', 'time', 'corrected_amplitude', 'corrected_data', 'corrected_phase']

        with casatools.MSReader(filename) as openms:
            openms.msselect(data_selection)
            raw_data = openms.getdata(colnames)
            axis_info = openms.getdata(['axis_info'])
            num_rows = openms.nrow(selected=True)

        corr_axis = axis_info['axis_info']['corr_axis']
        freq_axis = axis_info['axis_info']['freq_axis']

        scalar_cols = [c for c in colnames if len(raw_data[c].shape) == 1]
        var_cols = [c for c in colnames if c not in scalar_cols]

        # data has axis order pol->channel->time. Swap order to a more natural time->pol->channel
        for c in var_cols:
            raw_data[c] = raw_data[c].swapaxes(0, 2).swapaxes(1, 2)

        dtypes = {c: get_dtype(raw_data, c) for c in colnames}

        col_dtypes = [dtypes[c] for c in dtypes if dtypes[c] is not None]
        data = numpy.ma.empty(num_rows, dtype=col_dtypes)

        for c in scalar_cols:
            data[c] = raw_data[c]

        # convert to NumPy MaskedArray if FLAG column is present
        mask = raw_data['flag']
        var_cols_to_mask = [c for c in var_cols if c != 'flag']
        for c in var_cols_to_mask:
            data[c] = numpy.ma.MaskedArray(data=raw_data[c], dtype=raw_data[c].dtype, mask=mask)

        return MSWrapper(filename, scan, spw, data, corr_axis, freq_axis)

    def filter(self, antenna1=None, antenna2=None, **kwargs):
        """
        Return a new MSWrapper containing rows matching the column selection
        criteria.

        Data for rows meeting all the column criteria will be funnelled into
        the new MSWrapper return object. A boolean AND is effectively
        performed: e.g., antenna1=3 AND antenna2=5 will only return rows for
        one baseline.

        Data can be filtered on any column listed in the wrapper.data.dtype.
        """
        mask_args = dict(kwargs)

        # create a mask that lets all data through for columns that are not
        # specified as arguments, or just the specified values through for
        # columns that are specified as arguments
        def passthrough(k, column_name):
            if k is None:
                if column_name not in kwargs:
                    mask_args[column_name] = numpy.ma.unique(self[column_name])
            else:
                mask_args[column_name] = k

        for arg, column_name in [(antenna1, 'antenna1'), (antenna2, 'antenna2')]:
            passthrough(arg, column_name)

        # combine masks to create final data selection mask
        mask = numpy.ones(len(self))
        for k, v in mask_args.items():
            mask = (mask == 1) & (self._get_mask(v, k) == 1)

        # find data for the selection mask
        data = self[mask]

        # create new object for the filtered data
        return MSWrapper(self.filename, data, self.corr_axis, self.freq_axis)

    def xor_filter(self, antenna1=None, antenna2=None, **kwargs):
        """
        Return a new MSWrapper containing rows matching the column selection
        criteria.

        Data for rows meeting any column criteria will be funnelled into
        the new MSWrapper return object. A boolean AND is effectively
        performed: e.g., antenna1=3 AND antenna2=5 will only return rows for
        one baseline.

        Data can be filtered on any column listed in the wrapper.data.dtype.

        DANGER! DANGER! DANGER!

        NOTE! This class has only been tested for baseline selection! Using
        this method for other use cases could be dangerous. Use at your own
        risk!
        """

        # TODO this method could probably be refactored to use numpy xor, but
        # I don't have time right now...

        mask_args = dict(kwargs)

        # create a mask that lets all data through for columns that are not
        # specified as arguments, or just the specified values through for
        # columns that are specified as arguments
        def passthrough(k, column_name):
            if k is None:
                if column_name not in kwargs:
                    mask_args[column_name] = numpy.unique(self[column_name])
            else:
                mask_args[column_name] = k

        for arg, column_name in [(antenna1, 'antenna1'), (antenna2, 'antenna2')]:
            passthrough(arg, column_name)

        # combine masks to create final data selection mask
        mask = numpy.zeros(len(self))
        for k, v in mask_args.items():
            mask = (mask == 1) | (self._get_mask(v, k) == 1)

        # remove autocorrelations
        mask = (mask == 1) & (self['antenna1'] != self['antenna2'])

        # find data for the selection mask
        data = self[mask]

        # create new object for the filtered data
        return MSWrapper(self.filename, self.scan, self.spw, data, self.corr_axis, self.freq_axis)

    def __init__(self, scan, spw, filename, data, corr_axis, freq_axis):
        self.scan = scan
        self.spw = spw
        self.filename = filename
        self.data = data
        self.corr_axis = corr_axis
        self.freq_axis = freq_axis

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data.dtype.names

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return (i for i in self.data)

    def _get_mask(self, allowed, column):
        try:
            iter(allowed)
        except TypeError:
            allowed = [allowed]
        mask = numpy.zeros(len(self))
        for a in allowed:
            if a not in self.data[column]:
                raise KeyError('{} column {} value not found: {}'.format(self.filename, column, a))
            mask = (mask == 1) | (self[column] == a)
        return mask


def get_dtype(data, column_name):
    """
    Get the numpy data type for a CASA caltable column.

    :param tb: CASA table tool with caltable open.
    :param column_name: name of column to process
    :return: 3-tuple of column name, NumPy dtype, column shape
    """
    column_data = data[column_name]
    column_dtype = column_data.dtype
    column_shape = column_data.shape

    if len(column_shape) == 1:
        return column_name, column_dtype

    return column_name, column_dtype, column_shape[1:]


