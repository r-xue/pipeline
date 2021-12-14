import os
import numpy as np

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils

from . import renorm

LOG = logging.get_logger(__name__)


class RenormQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained RenormResults.
    """
    result_cls = renorm.RenormResults
    child_cls = None

    def handle(self, context, result):
        # Get task threshold value. In the future one might rely on
        # the automatic defaults in almarenorm. In that case the threshold
        # per spw needs to be fetched from the stats dictionary in the spw
        # loop below (result.stats[source][spw]['threshold']).
        threshold = result.threshold 
        if result.exception is not None:
            score = 0.0
            shortmsg = 'Failure in renormalization'
            longmsg = 'Failure in running renormalization heuristic: {}'.format(result.exception)
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))
            return

        if result.apply and result.corrApplied:
            # Request for correcting data that has already been corrected
            score = 0.0
            shortmsg = 'Corrections already applied'
            longmsg = 'EB {}: Corrections already applied to data'.format(os.path.basename(result.vis))
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))

        if not result.corrColExists:
            # CORRECTED_DATA column does not exist
            score = 0.0
            shortmsg = 'No corrected data column'
            longmsg = 'EB {}: Corrected data column does not exist'.format(os.path.basename(result.vis))
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))

        for source in result.stats: 
            for spw in result.stats[source]:
                try:
                    max_factor = result.stats[source][spw]['max_rn']
                    origin = pqa.QAOrigin(metric_name='MaxRenormFactor',
                                          metric_score=max_factor,
                                          metric_units='')

                    if (max_factor < 1.0) or np.isnan(max_factor):
                        # These values should never occur
                        score = 0.0
                        shortmsg = 'Unexpected values.'
                        longmsg = 'EB {} source {} spw {}: Error calculating corrections. Maximum factor is {}.'.format( \
                                  os.path.basename(result.vis), source, spw, max_factor)
                    elif np.isnan(result.rnstats['N'][source][spw]).any():
                        # Any NaNs apart from max_factor?
                        score = 0.0
                        shortmsg = 'Unexpected values.'
                        longmsg = 'EB {} source {} spw {}: Error calculating corrections. NaNs encountered.'.format( \
                                  os.path.basename(result.vis), source, spw)
                    elif 1.0 <= max_factor <= threshold:
                        score = 1.0
                        shortmsg = 'Renormalization factor within threshold'
                        longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                  'is within threshold of {:.1%}'.format( \
                                  os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)
                    elif max_factor > threshold:
                        if result.apply:
                            score = 0.9
                            shortmsg = 'Renormalization applied'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%} so corrections were applied to data'.format( \
                                      os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)
                        else:
                            score = max(0.66+threshold-max_factor, 0.34)
                            shortmsg = 'Renormalization factor outside threshold'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%}'.format( \
                                      os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)

                    result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))
                except:
                    # No factors for this spw. Just skip it.
                    pass


class RenormListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing RenormResults.
    """
    result_cls = basetask.ResultsList
    child_cls = renorm.RenormResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated