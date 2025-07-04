"""QA score module for applycal task."""
import os
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import applycal
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.hsd.tasks.applycal.applycal import SDApplycalResults

LOG = logging.get_logger(__name__)


class SDApplyCalQAHandler(pqa.QAPlugin):
    """Class to handle QA score for skycal result."""

    result_cls = applycal.SDApplycalResults
    child_cls = None

    def handle(self, context: 'Context', result: 'SDApplycalResults') -> None:
        """Evaluate QA score for applycal result.

        Args:
            context: Pipeline context.
            result: SDApplycalResults instance.
        """

        vis = os.path.basename(result.inputs['vis'])
        ms = context.observing_run.get_ms(vis)
        spwids = [spws.id for spws in ms.get_spectral_windows()]
        ants = [str(i.name) for i in ms.get_antenna()]
        figroot = os.path.join(context.report_dir,
                               'stage%s' % result.stage_number)

        list_is_figfile_exists = []
        for spw in spwids:
            for ant in ants:
                prefix = '{vis}-{y}_vs_{x}-{ant}-spw{spw}'.format(
                    vis=vis, y='amp', x='time', ant=ant, spw=spw)
                figfile = os.path.join(figroot, '{prefix}.png'.format(prefix=prefix))
                is_figfile_exists = os.path.exists(figfile)
                list_is_figfile_exists.append(is_figfile_exists)
        is_successful = all(list_is_figfile_exists)

        if is_successful:
            shortmsg = 'Generating amp vs time plot was successful.'
            longmsg = 'Generating amp vs time plot was successful.'
            score = 1.0
        else:
            shortmsg = 'Generating amp vs time plot was failed.'
            longmsg = 'Generating amp vs time plot was failed.'
            score = 0.65
        scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]

        result.qa.pool.extend(scores)
        result.qa.pool.append(scores)


class SDApplyCalListQAHandler(pqa.QAPlugin):

    """Class to handle QA score for a list of applycal results."""

    result_cls = basetask.ResultsList
    child_cls = applycal.SDApplycalResults

    def handle(self, context: 'Context', result: 'SDApplycalResults') -> None:
        """Evaluate QA score for a list of applycal results.

        Args:
            context: Pipeline context (not used).
            result: List of SDApplyCalResults instances.
        """
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
