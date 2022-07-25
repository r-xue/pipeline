import math
import os

import matplotlib.cbook as cbook
import matplotlib.pyplot as plt
import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)


class weightboxChart(object):

    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.result.weight_stats = {}

    @staticmethod
    def _get_weight_from_wtable(tbl, this_ant='', this_spw='', this_scan=''):
        if (this_ant != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_spw, this_ant)
        elif (this_ant != ''):
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0'.format(this_ant)
        elif (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ntrue(FLAG)==0'.format(this_spw)
        elif (this_scan !=''): #TODO: Add other options including scan later. They don't happen in the current code. 
            query_str = 'SCAN_NUMBER=={0} && ntrue(FLAG)==0'.format(this_scan)
        else:
            query_str = ''

        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query_str)
            weights = stb.getcol('CPARAM').ravel()
            stb.done()

        return weights.real

    def _create_plot_from_wtable(self, suffix):

        tbl = self.result.wtables[suffix]
        figfile = self._get_figfile(suffix)
        fig_title = os.path.basename(tbl)

        LOG.info('making antenna-based weight plot: {}'.format(figfile))

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))
            scans = np.sort(np.unique(tb.getcol('SCAN_NUMBER')))

        with casa_tools.TableReader(tbl+'/ANTENNA') as tb:
            ant_names = tb.getcol('NAME')
            ant_idxs = range(len(ant_names))

        whis = 3.944

        bxpstats_per_ant = list()
        for this_ant in ant_idxs:
            dat = self._get_weight_from_wtable(tbl, this_ant=this_ant)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats[0]['min'] = np.min(dat)
                bxpstats[0]['max'] = np.max(dat)
                bxpstats_per_ant.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats[0]['min'] = None
                bxpstats[0]['max'] = None
                bxpstats_per_ant.extend(bxpstats)
            bxpstats_per_ant[-1]['ant'] = ant_names[this_ant]

        bxpstats_per_spw = list()
        for this_spw in spws:
            dat = self._get_weight_from_wtable(tbl, this_spw=this_spw)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats[0]['min'] = np.min(dat)
                bxpstats[0]['max'] = np.max(dat)
                bxpstats_per_spw.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats[0]['min'] = None
                bxpstats[0]['max'] = None
                bxpstats_per_spw.extend(bxpstats)
            bxpstats_per_spw[-1]['spw'] = this_spw
        

        # Initial default plot sizes
        plot_len = 15
        number_of_plots = 3 
        number_of_scan_plots = 1

        max_scans_per_plot = 75 # 5 for quick tests
        if (len(scans) > max_scans_per_plot): 
            number_of_scan_plots = math.ceil(len(scans)/max_scans_per_plot)
            number_of_plots = number_of_scan_plots + 2 

        plot_height = number_of_plots * 2 # was fixed at 6. 

        # this needs to be if vla
        bxpstats_per_scan = list()
        for this_scan in scans: 
            dat = self._get_weight_from_wtable(tbl, this_scan=this_scan)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats[0]['min'] = np.min(dat)
                bxpstats[0]['max'] = np.max(dat)
                bxpstats_per_scan.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats[0]['min'] = None
                bxpstats[0]['max'] = None
                bxpstats_per_scan.extend(bxpstats)
            bxpstats_per_scan[-1]['scan'] = this_scan

        # this needs to be different if vlass
        # Make sure y-axis is fixed if plots are split up. 
        fig, subplots = plt.subplots(number_of_plots, 1, figsize=(plot_len, plot_height))

        # This is copied from SO and should be re-worked
        def split(a, n):
            k, m = divmod(len(a), n)
            return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

        if number_of_scan_plots == 1:
            ax1, ax2, ax3 = subplots
        else: 
            ax1 = subplots[0]
            ax2 = subplots[1]
            ax3 = subplots[2]
            ax_scans = subplots[2:]

        flierprops = dict(marker='+', markerfacecolor='royalblue', markeredgecolor='royalblue')

        ax1.bxp(bxpstats_per_ant, flierprops=flierprops)
        ax1.axes.set_xticklabels(ant_names, rotation=45, ha='center')
        ax1.set_ylabel('$Wt_{i}$')
        ax1.set_title('Antenna-based weights')
        ax1.get_yaxis().get_major_formatter().set_useOffset(False)

        ax2.bxp(bxpstats_per_spw, flierprops=flierprops)
        ax2.axes.set_xticklabels(spws)
        ax2.set_xlabel('SPW ID')
        ax2.set_ylabel('$Wt_{i}$')
        ax2.get_yaxis().get_major_formatter().set_useOffset(False)

        if number_of_scan_plots <= 1: 
            ax3.bxp(bxpstats_per_scan, flierprops=flierprops)
            ax3.axes.set_xticklabels(scans)
            ax3.set_xlabel('Scan Number')
            ax3.set_ylabel('$Wt_{i}$')
            ax3.get_yaxis().get_major_formatter().set_useOffset(False)
        else: 
#            print(bxpstats_per_scan)
            # ax3.bxp(bxpstats_per_scan, flierprops=flierprops)
            # ax3.axes.set_xticklabels(scans)
            # ax3.set_xlabel('Scan Number')
            # ax3.set_ylabel('$Wt_{i}$')
            # ax3.get_yaxis().get_major_formatter().set_useOffset(False)
#            y_min, y_max = ax3.get_ylim()
            # remove this subplot if needed 
#            fig.delaxes(ax3)
            bxpstats_per_scan_split = list(split(bxpstats_per_scan, number_of_scan_plots))
            y_min = np.min([dat['min'] for dat in bxpstats_per_scan])
            y_max = np.min([dat['max'] for dat in bxpstats_per_scan])
            y_min = y_min - 0.1*(y_max - y_min)
            y_max = y_max + 0.1*(y_max - y_min)

            scans_split = list(split(scans, number_of_scan_plots))
            for i, axis in enumerate(ax_scans): 
                axis.bxp(bxpstats_per_scan_split[i], flierprops=flierprops)
                axis.axes.set_xticklabels(scans_split[i], rotation=45, ha='center')
                axis.set_xlabel('Scan Number')
                axis.set_ylabel('$Wt_{i}$')
#                axis.set_ymargin(0.15) 
                axis.set_ylim([y_min, y_max])
                axis.get_yaxis().get_major_formatter().set_useOffset(False) 

        fig.tight_layout()
        fig.savefig(figfile)
        plt.close(fig)

        self.result.weight_stats[suffix] = {'per_spw': bxpstats_per_spw,
                                            'per_ant': bxpstats_per_ant,
                                            'per_scan': bxpstats_per_scan} # only add this if vla, not vlass

        return

    def _get_figfile(self, suffix):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['statwt',
                                                   self.ms.basename, 'summary', suffix])))+'.png'
        return os.path.join(stage_dir, fig_basename)

    def _get_plot_wrapper(self, suffix=''):
        figfile = self._get_figfile(suffix)
        wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                              parameters={'vis': self.ms.basename,
                                          'x_axis': 'ant/spw/scan', #only add if via not vlass
                                          'y_axis': 'weight',
                                          'type': suffix})

        if not os.path.exists(figfile):
            LOG.trace('Statwt summary plot not found. Creating new plot.')
            try:
                self._create_plot_from_wtable(suffix)
            except Exception as ex:
                LOG.error('Could not create ' + suffix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper

    def plot(self):
        plots = []
        for k, t in self.result.wtables.items():
            plots.append(self._get_plot_wrapper(suffix=k))

        return [p for p in plots if p is not None]
