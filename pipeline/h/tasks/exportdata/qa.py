import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import exportdata

LOG = logging.get_logger(__name__)


class ExportDataQAHandler(pqa.QAPlugin):
    result_cls = exportdata.ExportDataResults
    child_cls = None

    def handle(self, context, result):
        # Check for existence of core pipeline products
        score1 = self._ppr_exists(result.inputs['products_dir'], result.pprequest)
        score2 = self._weblog_exists(result.inputs['products_dir'], result.weblog)
        score3 = self._pipescript_exists(result.inputs['products_dir'], result.pipescript)
        score4 = self._commandslog_exists(result.inputs['products_dir'], result.commandslog)

        if result.inputs['exportmses']:
            score5 = self._mses_exist(result.inputs['products_dir'], result.msvisdict)
            scores = [score1, score2, score3, score4, score5]
        elif result.inputs['imaging_products_only']:
            score5 = self._images_exist(result.inputs['products_dir'], result.inputs['imaging_products_only'],
                                        result.calimages[1], result.targetimages[1])
            scores = [score1, score2, score3, score4, score5]
        else:
            score5 = self._restorescript_exists(result.inputs['products_dir'], result.restorescript)
            score6 = self._flags_exist(result.inputs['products_dir'], result.calvisdict)
            score7 = self._applycmds_exist(result.inputs['products_dir'], result.calvisdict)
            score8 = self._caltables_exist(result.inputs['products_dir'], result.sessiondict)
            score9 = self._images_exist(result.inputs['products_dir'], result.inputs['imaging_products_only'],
                                        result.calimages[1], result.targetimages[1])
            scores = [score1, score2, score3, score4, score5, score6, score7, score8, score9]

        result.qa.pool[:] = scores
        result.qa.all_unity_longmsg = "All expected pipeline products have been exported"

    def _ppr_exists(self, products_dir, ppr_file):
        """Check for the existence of the PPR"""
        return qacalc.score_file_exists(products_dir, ppr_file, 'pipeline processing request')

    def _weblog_exists(self, products_dir, weblog_file):
        """Check for the existence of the web log"""
        return qacalc.score_file_exists(products_dir, weblog_file, 'pipeline web log')

    def _pipescript_exists(self, products_dir, pipescript_file):
        """Check for the existence of the pipeline script"""
        return qacalc.score_file_exists(products_dir, pipescript_file, 'pipeline script')

    def _restorescript_exists(self, products_dir, restorescript_file):
        """Check for the existence of the pipeline restore script"""
        return qacalc.score_file_exists(products_dir, restorescript_file, 'pipeline restore script')

    def _commandslog_exists(self, products_dir, commandslog_file):
        """Check for the existence of the commands log file"""
        return qacalc.score_file_exists(products_dir, commandslog_file, 'pipeline commands log')

    def _flags_exist(self, products_dir, visdict):
        """Check for the existence of the final flagging version files"""
        return qacalc.score_flags_exist(products_dir, visdict)

    def _mses_exist(self, products_dir, visdict):
        """Check for the existence of the final mses"""
        return qacalc.score_mses_exist(products_dir, visdict)

    def _applycmds_exist(self, products_dir, visdict):
        """Check for the existence of the applycal commands files"""
        return qacalc.score_applycmds_exist(products_dir, visdict)

    def _caltables_exist(self, products_dir, sessiondict):
        """Check for the existence of the session / caltables files"""
        return qacalc.score_caltables_exist(products_dir, sessiondict)

    def _images_exist(self, products_dir, imaging_products_only, calimages, targetimages):
        """Check for the existence of the calibrator and / or target images"""
        return qacalc.score_images_exist(products_dir, imaging_products_only, calimages, targetimages)


class ExportDataListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing ExportDataResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = exportdata.ExportDataResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
        result.qa.all_unity_longmsg = "All expected pipeline products have been exported"
