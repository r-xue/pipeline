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
        threshold = result.threshold 
        threshold_factor = 1.0 + threshold 
        for source in result.stats: 
            for spw in result.stats[source]:
                try:
                    max_factor = result.stats[source][spw]['max_rn']
                    origin = pqa.QAOrigin(metric_name='MaxRenormFactor',
                                          metric_score=max_factor,
                                          metric_units='')

                    if (max_factor < 1.0) or (max_factor is np.nan):
                        # These values should never occur
                        score = 0.0
                        shortmsg = 'Unexpected values.'
                        longmsg = 'EB {} source {} spw {}: Error calculating corrections. Maximum factor is {}.'.format( \
                                  os.path.basename(result.vis), source, spw, max_factor)
                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))
                    elif 1.0 <= max_factor <= threshold_factor:
                        score = 1.0
                        shortmsg = 'Renormalization factor within threshold'
                        longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                  'is within threshold of {:.1%}'.format( \
                                  os.path.basename(result.vis), source, spw, max_factor, threshold)
                    elif max_factor > threshold_factor:
                        if result.apply:
                            score = 0.9
                            shortmsg = 'Renormalization applied'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%} so corrections were applied to data'.format( \
                                      os.path.basename(result.vis), source, spw, max_factor, threshold)
                        else:
                            score = max(0.66+threshold_factor-max_factor, 0.34)
                            shortmsg = 'Renormalization factor outside threshold'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%}'.format( \
                                      os.path.basename(result.vis), source, spw, max_factor, threshold)

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
