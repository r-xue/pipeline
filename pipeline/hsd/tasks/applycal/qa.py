"""QA score module for applycal task."""
import os
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
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
        ants = ['all']
        ants.extend([str(i.name) for i in ms.get_antenna()])
        figroot = os.path.join(context.report_dir,
                               'stage%s' % result.stage_number)
        score = 1.0
        scores = []
        for spw in spwids:
            for ant in ants:
                prefix = '{vis}-{y}_vs_{x}-{ant}-spw{spw}'.format(
                    vis=vis, y='real', x='time', ant=ant, spw=spw)
                figfile = os.path.join(figroot, '{prefix}.png'.format(prefix=prefix))
                is_figfile_exists = os.path.exists(figfile)
                if is_figfile_exists:
                    shortmsg = 'Generating amp vs time plot was successful.'
                    longmsg = 'Generating amp vs time plot for Spw{0} and Antenna={1} of {2} was successful.'.format(spw, ant, vis)
                else:
                    shortmsg = 'Generating amp vs time plot was failed.'
                    longmsg = 'Generating amp vs time plot for Spw{0} and Antenna={1} of {2} was failed.'.format(spw, ant, vis)
                    score = 0.65
                scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]
                result.qa.pool.extend(scores)
