from __future__ import absolute_import

import pipeline.infrastructure.logging as logging
import pipeline.qa.scorecalculator as qacalc
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.pipelineqa as pqa
from . import imaging

LOG = logging.get_logger(__name__)


class SDImagingQAHandler(pqa.QAPlugin):
    """
    SDImagingQAHandler is qa handler for each image product represented
    as the SDImagingResultItem instance.
    """
    result_cls = imaging.SDImagingResultItem
    child_cls = None
    generating_task = imaging.SDImaging

    def handle(self, context, result):
        """
        This handles single SDImagingResultItem.
        """
        if 'image' not in result.outcome:
            return

        image_item = result.outcome['image']
        imagename = image_item.imagename
        score_masked = qacalc.score_sdimage_masked_pixels(imagename)
        result.qa.pool.append(score_masked)


class SDImagingListQAHandler(pqa.QAPlugin):
    """
    SDImagingListQAHandler is qa handler for a list of image products
    represented as the SDImagingResults. SDImagingResults is a subclass
    of ResultsList and contains SDImagingResultsItem instances.
    """
    result_cls = imaging.SDImagingResults
    child_cls = imaging.SDImagingResultItem

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated
