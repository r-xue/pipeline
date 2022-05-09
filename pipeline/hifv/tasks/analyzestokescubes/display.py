import os

import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.hifv.heuristics.rfi import RflagDevHeuristic
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal

LOG = infrastructure.get_logger(__name__)


class VlassCubeStokesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plot_wrappers = []
        roi_list = ['peak_stokesi', 'peak_linpolint']

        for roi_name in roi_list:

            label = self.result.stats['spw']

            roi_stats = self.result.stats[roi_name]
            figfile = os.path.join(stage_dir, f'stokes_summary_u_vs_q_{roi_name}.png')

            x = np.array(roi_stats['stokesq'])/np.array(roi_stats['stokesi'])
            y = np.array(roi_stats['stokesu'])/np.array(roi_stats['stokesi'])

            LOG.debug(f'Creating the ROI={roi_name} Stokes U vs. Q plot.')

            try:
                fig, ax = plt.subplots(figsize=(10, 8))
                cmap = cm.get_cmap('rainbow_r')
                for idx in range(len(x)):
                    color_idx = idx/len(x)
                    ax.scatter(x[idx], y[idx], color=cmap(color_idx),
                               label=label[idx], edgecolors='black', alpha=0.7, s=300.)
                    text = ax.annotate(label[idx], (x[idx], y[idx]), ha='center', va='center', fontsize=9.)
                    text.set_alpha(.7)

                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize=12, labelspacing=0.75)
                ax.set_xlabel('Frac. Stokes $Q$')
                ax.set_ylabel('Frac. Stokes $U$')
                peak_loc = roi_stats['world']
                ax.set_title(f"Intensity Peak at \n {peak_loc}")
                ax.set_aspect('equal')
                ax.axhline(0, linestyle='-', color='lightgray')
                ax.axvline(0, linestyle='-', color='lightgray')

                amp_max = np.max(np.abs(np.array([ax.get_xlim(), ax.get_ylim()])))
                amp_scale = 1.2
                ax.set_xlim(-amp_max*amp_scale, amp_max*amp_scale)
                ax.set_ylim(-amp_max*amp_scale, amp_max*amp_scale)

                fig.savefig(figfile)

                plt.close(fig)

                if roi_name == 'peak_stokesi':
                    desc = 'Peak of the Stokes-I map'
                if roi_name == 'peak_linpolint':
                    desc = 'Peak of the linearly polarized intensity map'
                plot = logger.Plot(figfile,
                                   x_axis='Frac. Stokes-Q',
                                   y_axis='Frac. Stokes-U',
                                   parameters={'desc': desc})
                plot_wrappers.append(plot)

            except Exception as ex:
                LOG.warning("Could not create plot {}".format(figfile))
                LOG.warning(ex)

        return plot_wrappers
