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

        figfile = os.path.join(stage_dir, 'image_q_vs_u.png')

        x = np.array(self.result.stats['peak_q'])/np.array(self.result.stats['peak_i'])
        y = np.array(self.result.stats['peak_u'])/np.array(self.result.stats['peak_i'])
        label = self.result.stats['spw']
        LOG.debug('Creating the MADrms vs. spw plot.')
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
            peak_loc = self.result.stats['peak_radec'][0]+' ' + \
                self.result.stats['peak_radec'][1]+' '+self.result.stats['peak_radec'][3]
            ax.set_title(f"Stokes $I$ Peak at \n {peak_loc}")
            ax.set_aspect('equal')
            ax.axhline(0, linestyle='-', color='lightgray')
            ax.axvline(0, linestyle='-', color='lightgray')

            amp_max = np.max(np.abs(np.array([ax.get_xlim(), ax.get_ylim()])))
            amp_scale = 1.2
            ax.set_xlim(-amp_max*amp_scale, amp_max*amp_scale)
            ax.set_ylim(-amp_max*amp_scale, amp_max*amp_scale)

            fig.tight_layout()
            fig.savefig(figfile)

            plt.close(fig)
            plot = logger.Plot(figfile,
                               x_axis='U_peak',
                               y_axis='Q_peak',
                               parameters={})
            return plot
        except Exception as ex:
            LOG.warning("Could not create plot {}".format(figfile))
            LOG.warning(ex)
            return None


class VlassCubeRmsSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        figfile = os.path.join(stage_dir, 'image_rms_vs_spw.png')

        x = np.array(self.result.stats['spw'])
        x = np.array(self.result.stats['reffreq'])/1e9
        y = np.array(self.result.stats['rms'])*1e3
        bm = np.array(self.result.stats['beamarea'])

        LOG.debug('Creating the RMS_median vs. Frequency plot.')
        try:
            fig, ax = plt.subplots(figsize=(8,6))

            for idx, stokes in enumerate(['I', 'Q', 'U', 'V']):
                ax.plot(x, y[:, idx], marker="o", label=f'$\it{stokes}$')

            ax.set_xlabel('Frequency [GHz]')
            ax.set_ylabel('RMS$_{median}$ [mJy/beam]')
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            ax.set_xlim(xlim)
            ax.set_ylim(ylim)

            sefd = RflagDevHeuristic._get_vla_sefd()

            for band, sefd_per_band in sefd.items():
                sefd_x = sefd_per_band[:, 0]/1e3
                sefd_y = sefd_per_band[:, 1]
                if np.mean(x) > np.min(sefd_x) and np.mean(x) < np.max(sefd_x):
                    LOG.info(f'Selecting Band {band} for the SEFD-based rms prediction.')
                    sefd_spw = np.interp(x, sefd_x, sefd_y)
                    scale = np.median(y[:, 1]/sefd_spw)
                    ax.plot(sefd_x, sefd_y*scale, color='gray', label='Expected')
            # ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            yrange = ax.get_ylim()

            freq_min = []
            freq_max = []
            baseband_spws = None
            if baseband_spws is not None:
                for band, basebands_per_band in baseband_spws.items():
                    for baseband, spws_per_baseband in basebands_per_band.items():
                        for idx, spw_dict in enumerate(spws_per_baseband):
                            for spw_id, spw_freq_range in spw_dict.items():
                                dlog10 = np.log10(yrange[1]/yrange[0])
                                yhline = yrange[0]*10**(dlog10*(0.75+0.01*idx))
                                ax.hlines(yhline, float(spw_freq_range[0].value)/1e6,
                                          float(spw_freq_range[1].value)/1e6, color='k')
                                freq_min.append(float(spw_freq_range[0].value)/1e6)
                                freq_max.append(float(spw_freq_range[1].value)/1e6)
                ax.set_xlim([np.min(freq_min)/1.01, np.max(freq_max)*1.01])

            ax.legend(fontsize=12, labelspacing=0.5)

            fig.tight_layout()
            fig.savefig(figfile)
            plt.close(fig)
            plot = logger.Plot(figfile,
                               x_axis='Frequency',
                               y_axis='RMS_median',
                               parameters={})
            return plot

        except Exception as ex:
           LOG.warning("Could not create plot {}".format(figfile))
           LOG.warning(ex)
           return None
