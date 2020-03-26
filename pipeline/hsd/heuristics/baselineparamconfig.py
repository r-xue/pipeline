import os
import numpy
import collections
import abc

import pipeline.infrastructure.api as api
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.logging as logging
# from pipeline.domain.datatable import DataTableImpl as DataTable
from . import fitorder
from . import fragmentation

LOG = infrastructure.get_logger(__name__)


def TRACE():
    return LOG.isEnabledFor(logging.LOGGING_LEVELS['trace'])


def DEBUG():
    return LOG.isEnabledFor(logging.LOGGING_LEVELS['debug'])


class BaselineParamKeys(object):
    ROW = 'row'
    POL = 'pol'
    MASK = 'mask'
    CLIPNITER = 'clipniter'
    CLIPTHRESH = 'clipthresh'
    USELF = 'use_linefinder'
    LFTHRESH = 'thresh'
    LEDGE = 'Ledge'
    REDGE = 'Redge'
    AVG_LIMIT = 'avg_limit'
    FUNC = 'blfunc'
    ORDER = 'order'
    NPIECE = 'npiece'
    NWAVE = 'nwave'
    ORDERED_KEY = [ROW, POL, MASK, CLIPNITER, CLIPTHRESH, USELF, LFTHRESH,
                   LEDGE, REDGE, AVG_LIMIT, FUNC, ORDER, NPIECE, NWAVE]


BLP = BaselineParamKeys


# @sdutils.profiler
def write_blparam(fileobj, param):
    param_values = collections.defaultdict(str)
    for key in BLP.ORDERED_KEY:
        if key in param:
            param_values[key] = param[key]
    line = ','.join(map(str, [param_values[k] for k in BLP.ORDERED_KEY]))
    #line = ','.join((str(param[k]) if k in param.keys() else '' for k in BLP.ORDERED_KEY))
    fileobj.write(line+'\n')


def as_maskstring(masklist):
    return ';'.join(['%s~%s' % (x[0], x[1]) for x in masklist])


def no_switching(engine, nchan, edge, num_pieces, masklist):
    return 'cspline', 0


def do_switching(engine, nchan, edge, num_pieces, masklist):
    return engine(nchan, edge, num_pieces, masklist)


class BaselineFitParamConfig(api.Heuristic, metaclass=abc.ABCMeta):
    """
    Generate/update BLParam file according to the input parameters.
    """

    ApplicableDuration = 'raster'  # 'raster' | 'subscan'
    MaxPolynomialOrder = 'none'  # 'none', 0, 1, 2,...
    PolynomialOrder = 'automatic'  # 'automatic', 0, 1, 2, ...

    def __init__(self, switchpoly=True):
        super(BaselineFitParamConfig, self).__init__()
        self.paramdict = {}
        self.heuristics_engine = fitorder.SwitchPolynomialWhenLargeMaskAtEdgeHeuristic()
        if switchpoly == True:
            self.switching_heuristic = do_switching
        else:
            self.switching_heuristic = no_switching

    # readonly attributes
    @property
    def ClipCycle(self):
        return 1

    is_multi_vis_task = False

    def __datacolumn(self, vis):
        colname = ''
        if isinstance(vis, str):
            with casatools.TableReader(vis) as tb:
                candidate_names = ['CORRECTED_DATA',
                                   'DATA',
                                   'FLOAT_DATA']
                for name in candidate_names:
                    if name in tb.colnames():
                        colname = name
                        break
        return colname

    def calculate(self, datatable, ms, antenna_id, field_id, spw_id, fit_order, edge, deviation_mask, blparam):
        """
        Generate/update BLParam file, which will be an input to sdbaseline,
        according to the input parameters.

        Inputs:
            datatable -- DataTable instance
            ms -- MeasurementSet domain object
            antenna_id -- Antenna ID to process
            field_id -- Field ID to process
            spw_id -- Spw ID to process
            fit_order -- Fiting order ('automatic' or number)
            edge -- Number of edge channels to be excluded from the heuristics
                    (format: [L, R])
            deviation_mask -- Deviation mask
            blparam -- Name of the BLParam file
                       File contents will be updated by this heuristics

        Returns:
            Name of the BLParam file
        """
        LOG.debug('Starting BaselineFitParamConfig')
        fragmentation_heuristic = fragmentation.FragmentationHeuristics()

        # fitting order
        if fit_order == 'automatic':
            # fit order heuristics
            LOG.info('Baseline-Fitting order was automatically determined')
            self.fitorder_heuristic = fitorder.FitOrderHeuristics()
        else:
            LOG.info('Baseline-Fitting order was fixed to {}'.format(fit_order))
            self.fitorder_heuristic = lambda *args, **kwargs: fit_order #self.inputs.fit_order

        vis = ms.name
        if DEBUG() or TRACE():
            LOG.debug('MS "{}" ant {} field {} spw {}'.format(os.path.basename(vis), antenna_id, field_id, spw_id))

        nchan = ms.spectral_windows[spw_id].num_channels
        data_desc = ms.get_data_description(spw=spw_id)
        npol = data_desc.num_polarizations
        # edge must be formatted to [L, R]
        assert isinstance(edge, list) and len(edge) == 2, 'edge must be a list [L, R]. "{0}" was given.'.format(edge)

        if DEBUG() or TRACE():
            LOG.debug('nchan={nchan} edge={edge}'.format(nchan=nchan, edge=edge))

        if self.ApplicableDuration == 'subscan':
            timetable_index = 1
        else:
            timetable_index = 0

        index_list_total = []

        # prepare mask arrays
        mask_array = numpy.ones(nchan, dtype=int)
        mask_array[:edge[0]] = 0
        mask_array[nchan-edge[1]:] = 0

        # deviation mask
        if DEBUG() or TRACE():
            LOG.debug('Deviation mask for field {} antenna {} spw {}: {}'.format(
                field_id, antenna_id, spw_id, deviation_mask))
        if deviation_mask is not None:
            for mask_range in deviation_mask:
                mask_array[mask_range[0]:mask_range[1]] = 0

        base_mask_array = mask_array.copy()

        #LOG.info('base_mask_array = {}'.format(''.join(map(str, base_mask_array))))

        time_table = datatable.get_timetable(antenna_id, spw_id, None, os.path.basename(vis), field_id)
        member_list = time_table[timetable_index]

        # working with spectral data in scantable
        nrow_total = sum((len(x[0]) for x in member_list))

        LOG.info('Calculating Baseline Fitting Parameter...')
        LOG.info('Processing {} spectra...'.format(nrow_total))

        #colname = self.inputs.colname
        datacolumn = self.__datacolumn(vis)

        if DEBUG() or TRACE():
            LOG.debug('data column name is "{}"'.format(datacolumn))

        # open blparam file (append mode)
        with open(blparam, 'a') as blparamfileobj:

            with casatools.TableReader(vis) as tb:
                for y in range(len(member_list)):
                    rows = member_list[y][0]
                    idxs = member_list[y][1]

                    #spectra = numpy.fromiter((tb.getcell(colname,row)
                    #                          for row in rows),
                    #                         dtype=numpy.float64)
                    #tsel = tb.query('ROWNUMBER() IN [%s]'%((','.join(map(str, rows)))), style='python')
                    #spectra = tsel.getcol()
                    #tsel.close()
                    #spectra = numpy.asarray([tb.getcell(colname, row).real for row in rows])
                    spectra = numpy.zeros((len(rows), npol, nchan,), dtype=numpy.float32)
                    for (i, row) in enumerate(rows):
                        spectra[i] = tb.getcell(datacolumn, row).real
                    #get_mask_from_flagtra: 1 valid 0 invalid
                    #arg for mask_to_masklist: 0 valid 1 invalid
                    #flaglist = [self._mask_to_masklist(-sdutils.get_mask_from_flagtra(tb.getcell('FLAGTRA', row))+1 )
                    flaglist = [self._mask_to_masklist(tb.getcell('FLAG', row).astype(int))
                                for row in rows]

                    #LOG.trace("Flag Mask = %s" % str(flaglist))

                    spectra[:, :edge[0], :] = 0.0
                    spectra[:, nchan-edge[1]:, :] = 0.0

                    # here we assume that masklist is polarization-independent
                    # (this is because that line detection/validation process accumulates
                    # polarization components together
                    masklist = [datatable.getcell('MASKLIST', idx) for idx in idxs]
                    #masklist = [datatable.getcell('MASKLIST',idxs[i]) + flaglist[i]
                    #            for i in range(len(idxs))]
                    #LOG.debug('DONE {}'.format(y))

                    npol = spectra.shape[1]
                    for pol in range(npol):
                        # fit order determination
                        polyorder = self.fitorder_heuristic(
                            spectra[:, pol, :], [list(masklist[i]) + flaglist[i][pol] for i in range(len(idxs))], edge)
                        #del spectra
                        if fit_order == 'automatic' and self.MaxPolynomialOrder != 'none':
                            polyorder = min(polyorder, self.MaxPolynomialOrder)
                        #LOG.debug('time group {} pol {}: fitting order={}'.format(
                        #            y, pol, polyorder))

                        # calculate fragmentation
                        (fragment, nwindow, win_polyorder) = fragmentation_heuristic(polyorder, nchan, edge)

                        nrow = len(rows)
                        if DEBUG() or TRACE():
                            LOG.debug('nrow = {}'.format(nrow))
                            LOG.debug('len(idxs) = {}'.format(len(idxs)))

                        for i in range(nrow):
                            row = rows[i]
                            idx = idxs[i]
                            if TRACE():
                                LOG.trace('===== Processing at row = {} ====='.format(row))
                            #nochange = datatable.getcell('NOCHANGE',idx)
                            #LOG.trace('row = %s, Flag = %s'%(row, nochange))

                            # mask lines
                            maxwidth = 1
    #                       _masklist = masklist[i]
                            _masklist = list(masklist[i]) + flaglist[i][pol]
                            #LOG.info('_masklist = {}'.format(_masklist))
                            #LOG.info('masklist[{}] = {}'.format(i, masklist[i]))
                            #LOG.info('flaglist[{}][{}] = {}'.format(i, pol, flaglist[i][pol]))
                            #LOG.info('FLAG[{}][{}] = {}'.format(i, pol, ''.join(map(str, numpy.array(tb.getcell('FLAG', row)[pol], dtype=numpy.uint8)))))
                            for [chan0, chan1] in _masklist:
                                if chan1 - chan0 >= maxwidth:
                                    maxwidth = int((chan1 - chan0 + 1) / 1.4)
                                    # allowance in Process3 is 1/5:
                                    #    (1 + 1/5 + 1/5)^(-1) = (5/7)^(-1)
                                    #                         = 7/5 = 1.4
                            max_polyorder = int((nchan - sum(edge)) // maxwidth + 1)
                            if TRACE():
                                LOG.trace('Masked Region from previous processes = {}'.format(
                                    _masklist))
                                LOG.trace('edge parameters= {}'.format(edge))
                                LOG.trace('Polynomial order = {}  Max Polynomial order = {}'.format(polyorder, max_polyorder))

                            # fitting
                            polyorder = min(polyorder, max_polyorder)
                            mask_array[:] = base_mask_array
                            #LOG.info('mask_array = {}'.format(''.join(map(str, mask_array))))
                            #irow = len(row_list_total)+len(row_list)
                            #irow = len(index_list_total) + i
                            irow = row
                            param = self._calc_baseline_param(irow, pol, polyorder, nchan, 0, edge, _masklist,
                                                              win_polyorder, fragment, nwindow, mask_array)
                            # definition of masklist differs in pipeline and ASAP
                            # (masklist = [a, b+1] in pipeline masks a channel range a ~ b-1)
                            param[BLP.MASK] = [[start, end-1] for [start, end] in param[BLP.MASK]]
                            param[BLP.MASK] = as_maskstring(param[BLP.MASK])
                            if TRACE():
                                LOG.trace('Row {}: param={}'.format(row, param))
                            write_blparam(blparamfileobj, param)

                        # MS rows contain npol spectra
                        if pol == 0:
                            index_list_total.extend(idxs)

        return blparam

    #@sdutils.profiler
    def _calc_baseline_param(self, row_idx, pol, polyorder, nchan, modification, edge, masklist, win_polyorder,
                             fragment, nwindow, mask):
        # Create mask for line protection
        nchan_without_edge = nchan - sum(edge)
        #LOG.info('__ mask (before) = {}'.format(''.join(map(str, mask))))
        if isinstance(masklist, (list, numpy.ndarray)):
            for [m0, m1] in masklist:
                mask[m0:m1] = 0
        else:
            LOG.critical('Invalid masklist')
        #LOG.info('__ mask (after)  = {}'.format(''.join(map(str, mask))))
        num_mask = int(nchan_without_edge - numpy.sum(mask[edge[0]:nchan-edge[1]] * 1.0))
        # here meaning of "masklist" is changed
        #     masklist: list of channel ranges to be *excluded* from the fit
        # masklist_all: list of channel ranges to be *included* in the fit
        masklist_all = self.__mask_to_masklist(mask)
        #LOG.info('__ masklist (before)= {}'.format(masklist))
        #LOG.info('__ masklist (after) = {}'.format(masklist_all))

        if TRACE():
            LOG.trace('nchan_without_edge, num_mask, diff={}, {}'.format(
                nchan_without_edge, num_mask))

        outdata = self._get_param(row_idx, pol, polyorder, nchan, mask, edge, nchan_without_edge, num_mask, fragment,
                                  nwindow, win_polyorder, masklist_all)

        if TRACE():
            LOG.trace('outdata={}'.format(outdata))

        return outdata

    def _mask_to_masklist(self, mask):
        return [self.__mask_to_masklist(m) for m in mask]

    def __mask_to_masklist(self, mask):
        """
        Converts mask array to masklist
        Resulting masklist is a list of channel ranges whose values are 1

        Argument
            mask : an array of channel mask in values 0 or 1
        """
        # get indices of clump boundaries
        idx = (mask[1:] ^ mask[:-1]).nonzero()
        idx = (idx[0] + 1)
        # idx now contains pairs of start-end indices, edges need handling
        # depending on first and last mask value
        if mask[0]:
            if len(idx) == 0:
                return [[0, len(mask)]]
            r = [[0, idx[0]]]
            if len(idx) % 2 == 1:
                r.extend(idx[1:].reshape(-1, 2).tolist())
            else:
                r.extend(idx[1:-1].reshape(-1, 2).tolist())
        else:
            if len(idx) == 0:
                return []
            if len(idx) % 2 == 1:
                r = (idx[:-1].reshape(-1, 2).tolist())
            else:
                r = (idx.reshape(-1, 2).tolist())
        if mask[-1]:
            r.append([idx[-1], len(mask)])
        return r

    @abc.abstractmethod
    def _get_param(self, idx, pol, polyorder, nchan, mask, edge, nchan_without_edge, nchan_masked, fragment, nwindow,
                   win_polyorder, masklist):
        raise NotImplementedError


class CubicSplineFitParamConfig(BaselineFitParamConfig):

    def __init__(self, switchpoly=True):
        super(CubicSplineFitParamConfig, self).__init__(switchpoly)

        # constant stuff
        #self.paramdict[BLP.FUNC] = 'cspline'
        self.paramdict[BLP.CLIPNITER] = self.ClipCycle
        self.paramdict[BLP.CLIPTHRESH] = 5.0

    def _get_param(self, idx, pol, polyorder, nchan, mask, edge, nchan_without_edge, nchan_masked, fragment, nwindow,
                   win_polyorder, masklist):
        num_nomask = nchan_without_edge - nchan_masked
        num_pieces = max(int(min(polyorder * num_nomask / float(nchan_without_edge) + 0.5, 0.1 * num_nomask)), 1)
        if TRACE():
            LOG.trace('Cubic Spline Fit: Number of Sections = {}'.format(num_pieces))
        self.paramdict[BLP.ROW] = idx
        self.paramdict[BLP.POL] = pol
        self.paramdict[BLP.MASK] = masklist
        self.paramdict[BLP.NPIECE] = num_pieces

        # # fit function heuristics
        # # nchan: total number of channels
        # # nchan_segment: number of channels in one segment
        # # edge: number of channels from the edges to be excluded from the fit [C0, C1]
        # # mask: mask array (0->rejected, 1->adopted)
        # # masklist: list of fit ranges (included in the fit) [[C0, C1], [C2, C3], ...]
        # # nchan_edge: max number of consecutive masked channels from edges
        # # if nchan_edge >= nchan/2:
        # #     fitfunc='poly'
        # #     order=1
        # # elif nchan_edge >= nchan_segment:
        # #     fitfunc='poly'
        # #     order=2
        # # else:
        # #     fitfunc='cspline'
        # if len(masklist) == 0:
        #     # special case: all channels are excluded from the fit
        #     nchan_edge0 = nchan
        #     nchan_edge1 = nchan
        # else:
        #     # number of masked edge channels: Left side
        #     edge_mask0 = list(map(min, masklist))
        #     assert edge[0] >= 0
        #     nchan_edge0 = max(min(edge_mask0), edge[0]) if len(edge_mask0) > 0 else edge[0]
        #     # number of masked edge channels: Right side
        #     edge_mask1 = list(map(max, masklist))
        #     assert edge[1] >= 0
        #     nchan_edge1 = max(nchan - 1 - max(edge_mask1), edge[1]) if len(edge_mask1) > 0 else edge[1]
        #     # merge result
        # nchan_edge = max(nchan_edge0, nchan_edge1)
        # nchan_segment = int(round(float(nchan) / num_pieces))
        # nchan_half = nchan // 2 + nchan % 2
        # nchan_quarter = nchan // 4 + (3 + nchan % 4) // 4
        # if nchan_edge >= nchan_half:
        #     fitfunc = 'poly'
        #     order = 1
        # elif nchan_edge >= nchan_quarter:
        #     fitfunc = 'poly'
        #     order = 2
        # else:
        #     fitfunc = 'cspline'
        #     order = 0  # not used
        fitfunc, order = self.switching_heuristic(
            self.heuristics_engine,
            nchan,
            edge,
            num_pieces,
            masklist
        )
        self.paramdict[BLP.FUNC] = fitfunc
        self.paramdict[BLP.ORDER] = order

        return self.paramdict
