import collections
from functools import reduce

import numpy as np

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils

from . import resultobjects

LOG = logging.get_logger(__name__)


class MakeImagesQAHandler(pqa.QAPlugin):
    result_cls = resultobjects.MakeImagesResult
    child_cls = None

    def handle(self, context, result):
        # calculate QA score as minimum of all sub-scores
        if result.mitigation_error:
            result.qa.pool[:] = [pqa.QAScore(0.0, longmsg='Size mitigation error. No targets were processed.',
                                             shortmsg='Size mitigation error')]
        elif len(result.results) > 0:
            score_objects = reduce(lambda x, y: x+y, [item.qa.pool for item in result.results])
            result.qa.pool[:] = score_objects
            summary_scores = self._add_summary_scores(context, result)
            if summary_scores:
                result.qa.pool.extend(summary_scores)
                result.qa.representative = summary_scores[0]
        else:
            if len(result.targets) == 0:
                result.qa.pool[:] = [pqa.QAScore(None, longmsg='No imaging targets were defined',
                                                 shortmsg='Nothing to image')]
            else:
                result.qa.pool[:] = [
                    pqa.QAScore(0.0,
                                longmsg='No imaging results found. Expected %d.' % (len(result.targets)),
                                shortmsg='No imaging results')
                ]

    def _add_summary_scores(self, context, result):

        summary_scores = []
        if context.imaging_mode == 'VLASS-SE-CUBE':
            summary_scores.extend(_add_vlass_cube_imaging_scores(result))
        return summary_scores


class MakeImagesListQAHandler(pqa.QAPlugin):
    result_cls = collections.Iterable
    child_cls = resultobjects.MakeImagesResult

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated


def _add_vlass_cube_imaging_scores(result):
    """Custom implementation of a summary QAscore list for VLASS-SE-CUBE."""

    vlass_cube_qascores = []
    result_metadata = result.metadata
    if 'vlass_cube_metadata' in result_metadata:
        plane_keep = result_metadata['vlass_cube_metadata']['plane_keep']
        nplane_expected = float(plane_keep.size)
        nplane_kept = np.sum(plane_keep)
        score_value = nplane_kept/nplane_expected
        is_or_are = 'are' if nplane_expected-nplane_kept > 1 else 'is'
        score_msg = f'{int(nplane_expected-nplane_kept)} of {int(nplane_expected)} imaged planes {is_or_are} rejected.'
        vlass_cube_qascores = [pqa.QAScore(score_value, longmsg=score_msg, shortmsg=score_msg)]

    return vlass_cube_qascores
