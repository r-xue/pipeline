# import pipeline.hif.tasks.importdata.importdata as importdata
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
        # replace this with results of calls to ALMA-specific functions in qacalc
        # score = pqa.QAScore(0.1, longmsg='Hello from ALMA-specific QA', shortmsg='ALMA QA')
        # scores = [score]

        # Check for the presense of polarization intents
        score1 = self._check_polintents(result.mses)

        # Check for the presence of receiver bands with calibration issues
        score2 = self._check_bands(result.mses)

        # Check for the presence of bandwidth switching
        score3 = self._check_bwswitching(result.mses)

        # Check for science spw names matching the virtual spw ID lookup table
        score4 = self._check_science_spw_names(result.mses, context.observing_run.virtual_science_spw_names)

        # Flux service usage
        score5 = self._check_fluxservice(result)

        scores = [score1, score2, score3, score4, score5]

        result.qa.pool.extend(scores)

    def _check_polintents(self, mses):
        """
        Check each measurement set for polarization intents
        """
        return qacalc.score_polintents(mses)

    def _check_bands(self, mses):
        """
        Check each measurement set for bands with calibration issues
        """
        return qacalc.score_bands(mses)

    def _check_bwswitching(self, mses):
        """
        Check each measurement set for bandwidth switching calibration issues
        """
        return qacalc.score_bwswitching(mses)

    def _check_science_spw_names(self, mses, virtual_science_spw_names):
        """
        Check science spw names
        """
        return qacalc.score_science_spw_names(mses, virtual_science_spw_names)

    def _check_fluxservice(self, result):
        """
        Check flux service usage
        """
        return qacalc.score_fluxservice(result)
