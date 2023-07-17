import os

import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal

LOG = infrastructure.get_logger(__name__)


class VlassFlagSummary(object):
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

        vlass_flag_stats = self.result.vlass_flag_stats

        spwgroup_list = vlass_flag_stats['spwgroup_list']
        scan_list = vlass_flag_stats['scan_list']
        fname_list = vlass_flag_stats['fname_list']
        flagpct_field_spwgroup = vlass_flag_stats['flagpct_field_spwgroup']

        scan_idx = []
        scan_label = []
        scan_edge = []
        for scan_unique in np.unique(scan_list):
            field_idxs = np.where(scan_list == scan_unique)[0]
            scan_idx.append(np.min(field_idxs))
            scan_edge.append((np.min(field_idxs)-0.5, np.max(field_idxs)+0.5))
            scan_desc = [fname_list[field_idxs[0]],
                         f'scan no.: {int(scan_list[field_idxs[0]])}']
            scan_label.append('\n'.join(scan_desc))

        figfile = os.path.join(stage_dir, 'vlass_flagsummary_field_spwgroup.png')
        n_field, n_spwgroup = flagpct_field_spwgroup.shape

        try:

            cmap = mpl.colors.ListedColormap(['green', 'blue', 'red'])
            norm = mpl.colors.BoundaryNorm([-1, 0.7, 0.9, 1.2], cmap.N)

            fig, ax = plt.subplots(figsize=(10, 10))

            ax.imshow(flagpct_field_spwgroup, origin='lower', aspect='auto',
                      cmap=cmap, norm=norm,
                      extent=(-0.5, n_spwgroup-0.5, -0.5, n_field-0.5))

            ax.set_xticks(np.arange(n_spwgroup))
            ax.set_xticklabels(spwgroup_list)

            ax.set_xlabel('Spw Selection')
            ax.set_ylabel('VLASS Image Row: 1st field name')
            ax.tick_params(which='minor', bottom=False, left=False)

            ax.set_xticks(np.arange(n_spwgroup+1)-0.5, minor=True)
            ax.set_yticks(np.arange(n_field+1)-0.5, minor=True)

            ax.set_yticks(np.unique(scan_idx), minor=False)
            ax.set_yticklabels(scan_label, rotation=45, ma='left', va='center', rotation_mode="anchor")

            ax.grid(which='minor', axis='both', color='white', linestyle='-', linewidth=2)

            ax.set_title('Flagged fraction')

            lg_colors = {'<70%': 'green', '70%<flagged<90%': 'blue', '>90%': 'red'}
            lg_patch = [mpatches.Patch(color=lg_colors[lg_label], label=lg_label) for lg_label in lg_colors]
            ax.legend(handles=lg_patch, bbox_to_anchor=(0.5, -0.1), loc='upper center', ncol=len(lg_patch))

            fig.tight_layout()
            fig.savefig(figfile, bbox_inches='tight')
            plt.close(fig)

            plot = logger.Plot(figfile,
                               x_axis='Spw Group',
                               y_axis='Field',
                               parameters={})

            plot_wrappers.append(plot)

        except Exception as ex:
            LOG.warning('Could not create plot %s', figfile)
            LOG.warning(ex)

        return plot_wrappers
