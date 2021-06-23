import os

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
        for source in result.stats: 
            for spw in result.stats[source]: 
                if result.stats[source][spw]['max_rn'] < 1.0 + result.threshold:
                    score = 1.0
                    shortmsg = 'No renormalization'
                    longmsg = 'No renormalization necessary for EB {} source {} spw {}.'.format( \
                              os.path.basename(result.vis), source, spw)
                else:
                    score = 0.5
                    shortmsg = 'Renormalization applied'
                    longmsg = 'Renormalization applied for EB {} source {} spw {}. ' \
                              'Maximum factor {:.3f} for field {}.'.format(os.path.basename(result.vis), \
                              source, spw, result.stats[source][spw]['max_rn'], \
                              result.stats[source][spw]['max_rn_field'])

                origin = pqa.QAOrigin(metric_name='MaxRenormFactor',
                                      metric_score=result.stats[source][spw]['max_rn'],
                                      metric_units='')

                result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))


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
