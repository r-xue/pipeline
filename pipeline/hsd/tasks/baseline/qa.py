import numpy
import os
from typing import List, Optional, Union

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc

from ..common import compress
from . import baseline

LOG = logging.get_logger(__name__)


class SDBaselineQAHandler(pqa.QAPlugin):    
    result_cls = baseline.SDBaselineResults
    child_cls = None

    def handle(self, context, result):
        scores = []
        lines_list = []
        group_id_list = []
        spw_id_list = []
        field_id_list = []
        reduction_group = context.observing_run.ms_reduction_group
        baselined = result.outcome['baselined']
        for b in baselined:
            reduction_group_id = b['group_id']
            members = b['members']
            group_desc = reduction_group[reduction_group_id]
            spw_id = numpy.fromiter((group_desc[m].spw_id for m in members), dtype=numpy.int32)  # b['spw']
            field_id = numpy.fromiter((group_desc[m].field_id for m in members), dtype=numpy.int32)  # b['field']
            lines = b['lines']
            lines_list.append(lines)
            group_id_list.append(reduction_group_id)
            spw_id_list.append(spw_id)
            field_id_list.append(field_id)
        scores.append(qacalc.score_sd_line_detection_for_ms(group_id_list, 
                                                            field_id_list, 
                                                            spw_id_list, 
                                                            lines_list))
        for figfile, stat in result.outcome['baseline_quality_stat'].items():
            plot = _get_plot(result.outcome['plots'], figfile)
            if plot is None:
                LOG.warning(f'Unable to find plot instance for {figfile}')
                continue
            p = plot.parameters
            scores.append(qacalc.score_sd_baseline_quality(p['vis'], plot.field, p['ant'], p['spw'], p['pol'], stat))
            del plot
        result.qa.pool.extend(scores)

def _get_plot(plots: List[logger.Plot], figfile: str) -> Optional[Union[compress.CompressedObj, logger.Plot]]:
    """
    Return Plot instance that matches figure file name.

    Args:
        plots: A list of plot objects
        figfile: The name of figure file
    Returns:
        Plot instance. Returns None if no match is found.
    """
    for p in plots:
        if isinstance(p, compress.CompressedObj):
            p = p.decompress()
        if p.basename == os.path.basename(figfile):
            return p
    return None


class SDBaselineListQAHandler(pqa.QAPlugin):
    result_cls = basetask.ResultsList
    child_cls = baseline.SDBaselineResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated
