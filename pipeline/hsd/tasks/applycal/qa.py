"""QA score module for applycal task."""
import os
import numpy as np
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
from . import applycal
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.callibrary import CalApplication, CalFrom
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
        antmap = dict((a.id, a.name) for a in ms.get_antenna())
        antname = list(antmap.values())
        ants.extend(antname)
        figroot = os.path.join(context.report_dir,
                               'stage%s' % result.stage_number)

        for calapp in result.applied:
            gaintable = calapp.gaintable
            if type(gaintable) is list:
                gaintable = gaintable[0]
            gaintable = os.path.basename(gaintable)

            score = 1.0
            scores = []
            with casa_tools.TableReader(gaintable) as tb:
                for spw in spwids:
                    for ant in ants:
                        prefix = '{vis}-{y}_vs_{x}-{ant}-spw{spw}'.format(
                            vis=vis, y='real', x='time', ant=ant, spw=spw)
                        figfile = os.path.join(figroot, '{prefix}.png'.format(prefix=prefix))
                        is_figfile_exists = os.path.exists(figfile)
                        if is_figfile_exists:
                            flagged_data = np.empty(1)
                            if ant == "all":
                                selected = tb.query("SPECTRAL_WINDOW_ID=={}".format(spw))
                            else:
                                antid = [str(k) for k, v in antmap.items() if v == ant][0]
                                selected = tb.query("SPECTRAL_WINDOW_ID=={} && ANTENNA1=={}".format(spw, antid))
                            flagged_data = selected.getcol('FLAG')
                            if not all(flagged_data.reshape(-1)):
                                LOG.info("Not all flagged_data are True for Spw{0} and Antenna={1} of {2}.".format(spw, ant, vis))
                                shortmsg = 'Generating amp vs time plot was successful.'
                                longmsg = 'Generating amp vs time plot for Spw{0} and Antenna={1} of {2} was successful.'.format(spw, ant, vis)
                            else:
                                LOG.info("All flagged_data are True for Spw{0} and Antenna={1} of {2}.".format(spw, ant, vis))
                                shortmsg = 'Generating amp vs time plot was successful but empty.'
                                longmsg = 'Generating amp vs time plot for Spw{0} and Antenna={1} of {2} was successful but empty.'.format(spw, ant, vis)
                                score = 0.8
                        else:
                            shortmsg = 'Generating amp vs time plot was failed.'
                            longmsg = 'Generating amp vs time plot for Spw{0} and Antenna={1} of {2} was failed.'.format(spw, ant, vis)
                            score = 0.65

                        scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]
                        result.qa.pool.extend(scores)
