import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api

LOG = infrastructure.get_logger(__name__)


class FitOrderHeuristics(api.Heuristic):
    """
    Determine fitting order from a set of spectral data.
    """
    MaxDominantFreq = 15

    def calculate(self, data, mask=None, edge=(0, 0)):
        """
        Determine fitting order from a set of spectral data, data,
        with masks for each spectral data, mask, and number of edge
        channels to be excluded, edge.

        First, manipulate each spectral data by the following procedure:

           1) mask regions specified by mask and edge,
           2) subtract average from spectral data,
           3) compute one-dimensional discrete Fourier Transform.

        Then, Fourier power spectrum is averaged and averaged power
        spectrum is analyzed to determine optimal polynomial order
        for input data array. The heuristics returns one representative
        polynomial order per input data array.

        Args:
            data: two-dimensional data array with shape (nrow, nchan).
            mask: list of mask regions. Value should be a list of
                  [[start0,end0],[start1,end1],...] for each spectrum.
                  [[-1,-1]] indicates no mask. Default is None.
            edge: number of edge channels to be dropped. Default is (0,0).
        Returns:
            Representative polynomial order, None if all data are masked
        """
        (nrow, nchan) = data.shape
        effective_nchan = nchan - sum(edge)
        power_spectrum = []
        if mask is not None:
            mask_maker = MaskMaker(nchan, mask, edge)
        else:
            mask_maker = MaskMakerNoLine(nchan, edge)
        for irow in range(nrow):
            spectrum = data[irow]
            flag = mask_maker.get_mask(irow)
            if numpy.any( flag == 1 ):
                average = numpy.average( spectrum, weights=flag )
                spectrum = (spectrum - average) * flag

                # Apply FFT to the spectrum
                power_spectrum.append(numpy.abs(numpy.fft.rfft(spectrum)))

        # return None if all rows are completely masked
        if len(power_spectrum) == 0:
            return None

        # Average seems to be better than median
        #power = numpy.median(power_spectrum, axis=0)
        power = numpy.average(power_spectrum, axis=0)

        max_freq = max(int(self.MaxDominantFreq * effective_nchan / 2048.0), 1)

        # 2007/09/01 Absolute value of power should be taken into account
        # If the power is low, it should be ignored
        # Normalize the power
        power2 = power / power.mean()
        max_power = power2[:max_freq].max()
        if max_power < 3.0:
            poly_order = 1.0
        elif max_power < 5.0:
            poly_order = 1.5
        elif max_power < 10.0:
            poly_order = 2.0
        else:
            flag = False
            for i in range(max_freq, -1, -1):
                if power2[i] > 10.0:
                    break
                if power2[i] > 5.0:
                    flag = True
            if flag is True:
                poly_order = float(max(2.0, i)) + 0.5
            else:
                poly_order = float(max(2.0, i))

        # Finally, convert to polynomial order
        #poly_order = int(poly_order * 3.0)
        #poly_order = int(poly_order + 1) * 2.0 + 0.5)
        poly_order = int(poly_order * 2.0 + 1.5)

        return poly_order


class MaskMakerNoLine(object):
    def __init__(self, nchan, edge):
        self.flag = numpy.ones( nchan, dtype=numpy.int8 )
        self.flag[:edge[0]] = 0
        self.flag[(nchan-edge[1]):] = 0

    def get_mask(self, row):
        return self.flag


class MaskMaker(MaskMakerNoLine):
    def __init__(self, nchan, lines, edge):
        super(MaskMaker, self).__init__(nchan, edge)
        self.lines = lines

    def get_mask(self, row):
        flag = self.flag.copy()
        for line in self.lines[row]:
            if line[0] != -1:
                flag[line[0]:line[1] + 1] = 0  # line[1] is an index of the end of the line
        return flag


class SwitchPolynomialWhenLargeMaskAtEdgeHeuristic(api.Heuristic):
    def calculate(self, nchan, edge, num_pieces, masklist):
        # fit function heuristics
        # nchan: total number of channels
        # nchan_segment: number of channels in one segment
        # edge: number of channels from the edges to be excluded from the fit [C0, C1]
        # mask: mask array (0->rejected, 1->adopted)
        # masklist: list of fit ranges (included in the fit) [[C0, C1], [C2, C3], ...]
        # nchan_edge: max number of consecutive masked channels from edges
        # if nchan_edge >= nchan/2:
        #     fitfunc='poly'
        #     order=1
        # elif nchan_edge >= nchan_segment:
        #     fitfunc='poly'
        #     order=2
        # else:
        #     fitfunc='cspline'
        if len(masklist) == 0:
            # special case: all channels are excluded from the fit
            nchan_edge0 = nchan
            nchan_edge1 = nchan
        else:
            # number of masked edge channels: Left side
            edge_mask0 = list(map(min, masklist))
            assert edge[0] >= 0
            nchan_edge0 = max(min(edge_mask0), edge[0]) if len(edge_mask0) > 0 else edge[0]
            # number of masked edge channels: Right side
            edge_mask1 = list(map(max, masklist))
            assert edge[1] >= 0
            nchan_edge1 = max(nchan - 1 - max(edge_mask1), edge[1]) if len(edge_mask1) > 0 else edge[1]
            # merge result
        nchan_edge = max(nchan_edge0, nchan_edge1)
        nchan_segment = int(round(float(nchan) / num_pieces))
        nchan_half = nchan // 2 + nchan % 2
        nchan_quarter = nchan // 4 + (3 + nchan % 4) // 4
        if nchan_edge >= nchan_half:
            fitfunc = 'poly'
            order = 1
        elif nchan_edge >= nchan_quarter:
            fitfunc = 'poly'
            order = 2
        else:
            fitfunc = 'cspline'
            order = 0  # not used

        LOG.debug('DEBUGGING INFORMATION:')
        LOG.debug('inclusive masklist={}'.format(masklist))
        LOG.debug('edge = {}'.format(list(edge)))
        LOG.debug('nchan_edge = {} (left {} right {})'.format(nchan_edge, nchan_edge0, nchan_edge1))
        LOG.debug('nchan = {}, num_pieces = {} => nchan_segment = {}, nchan_half = {}'.format(nchan, num_pieces, nchan_segment, nchan_half))
        LOG.debug('---> RESULT: fitfunc "{}" order "{}"'.format(fitfunc, order))

        return fitfunc, order
