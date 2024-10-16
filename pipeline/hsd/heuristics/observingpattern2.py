import math

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api

LOG = infrastructure.get_logger(__name__)


class ObservingPattern2(api.Heuristic):
    """
    Analyze pointing pattern
    """
    def calculate(self, pos_dict):
        """
        Analyze pointing pattern from pos_dict which is calculated by
        GroupByPosition2 heuristic.
        Return (ret)
            ret: 'RASTER', 'SINGLE-POINT', or 'MULTI-POINT'
        # PosDict[row]: index
        """
        LOG.info('Analyze Scan Pattern by Positions...')

        rows = list(pos_dict.keys())
        nrows = len(pos_dict)
        nPos = 0
        for row in rows:
            if pos_dict[row][0] != -1:
                nPos += 1
        if nPos == 0: nPos = 1
        LOG.debug('Number of Spectra: %d,   Number of independent position > %d' % (nrows, nPos))
        #if nPos > math.sqrt(len(rows)) or nPos > 10: ret = 'RASTER'
        if nPos > math.sqrt(nrows) or nPos > 3: ret = 'RASTER'
        elif nPos == 1: ret = 'SINGLE-POINT'
        else: ret = 'MULTI-POINT'
        LOG.info('Pattern is %s' % (ret))
        return ret
