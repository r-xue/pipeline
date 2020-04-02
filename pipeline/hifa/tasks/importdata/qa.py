from typing import List

import pipeline.h.tasks.importdata.importdata as importdata
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.qa.scorecalculator as qacalc
from . import almaimportdata

LOG = logging.get_logger(__name__)


class ALMAImportDataQAHandler(pqa.QAPlugin):
    result_cls = importdata.ImportDataResults
    child_cls = None
    generating_task = almaimportdata.ALMAImportData

    def handle(self, context, result):
        # Check for the presense of polarization intents
        recipe_name = context.project_structure.recipe_name
        polcal_scores = _check_polintents(recipe_name, result.mses)

        # Check for the presence of receiver bands with calibration issues
        score2 = _check_bands(result.mses)

        # Check for the presence of bandwidth switching
        score3 = _check_bwswitching(result.mses)

        # Check for science spw names matching the virtual spw ID lookup table
        score4 = _check_science_spw_names(result.mses,
                                          context.observing_run.virtual_science_spw_names)

        # Flux service usage
        score5 = _check_fluxservice(result)

        result.qa.pool.extend(polcal_scores)
        result.qa.pool.extend([score2, score3, score4, score5])


def _check_polintents(recipe_name, mses) -> List[pqa.QAScore]:
    """
    Check each measurement set for polarization intents
    """
    return qacalc.score_polintents(recipe_name, mses)


def _check_bands(mses) -> pqa.QAScore:
    """
    Check each measurement set for bands with calibration issues
    """
    return qacalc.score_bands(mses)


def _check_bwswitching(mses) -> pqa.QAScore:
    """
    Check each measurement set for bandwidth switching calibration issues
    """
    return qacalc.score_bwswitching(mses)


def _check_science_spw_names(mses, virtual_science_spw_names) -> pqa.QAScore:
    """
    Check science spw names
    """
    return qacalc.score_science_spw_names(mses, virtual_science_spw_names)


def _check_fluxservice(result) -> pqa.QAScore:
    """
    Check flux service usage
    """
    return qacalc.score_fluxservice(result)
