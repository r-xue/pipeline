import collections
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
    def _get_weight_from_wtable(tbl, this_ant='', this_spw='', this_scan='', spw_list=[]):
        if (this_ant != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_spw, this_ant)
        elif (this_ant != '') and spw_list: 
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {1}'.format(this_ant, list(map(int, spw_list)))
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

    # Get the scans from the weight table with spws in the input list
    # This is used to get the scans for a band, and is only used for the VLA-PI code
    def _get_scans_with_spws(self, tbl, spws=[]):
        query = 'ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {0}'.format(list(map(int, spws)))
        
        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query)
            scans = np.sort(np.unique(stb.getcol('SCAN_NUMBER')))
            stb.done()
        return scans


    def _create_plot_from_wtable(self, suffix):
        tbl = self.result.wtables[suffix]

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))
            scans = np.sort(np.unique(tb.getcol('SCAN_NUMBER')))

        with casa_tools.TableReader(tbl+'/ANTENNA') as tb:
            ant_names = tb.getcol('NAME')
            ant_idxs = range(len(ant_names))

        # This has gotten so different that I think I need to separate it out into VLASS and
        # VLA-PI

        spw2band = self.ms.get_vla_spw2band()
        # Format: {'1':'C', '2':'K' }
        band2spw = collections.defaultdict(list)
        listspws = [spw for spw in spws]
        for spw, band in spw2band.items():
            if spw in listspws:
                band2spw[band].append(str(spw))
        print(band2spw)
        self.band2spw = band2spw
        # Format: {'C':[1,2,3], 'K':[4,5,6]}

        # filter scans or ants to only those with spws for one band

        whis = 3.944
        bands_return = []
        for band in band2spw: 
            # Needs to be updated to actually be per-band
            figfile = self._get_figfile(suffix, band)
            LOG.info('Making antenna-based weight plot: {0} for band: {1}'.format(figfile, band))
            bxpstats_per_ant = list()
            #ants, ant_names = get_ants_with_spw #TODO: needed? 
            for this_ant in ant_idxs:
                dat = self._get_weight_from_wtable(tbl, this_ant=this_ant, spw_list=band2spw[band]) # needs to get updated ticks at the same time?
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
            for this_spw in band2spw[band]:
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
            #number_of_plots = 3 
            number_of_scan_plots = 1
            number_of_spw_plots = 1
            number_of_antenna_plots = 1
            number_of_plots = number_of_scan_plots + number_of_spw_plots + number_of_antenna_plots

            scans = self._get_scans_with_spws(tbl, band2spw[band])

            max_scans_per_plot = 75 # 5 for quick tests
            if (len(scans) > max_scans_per_plot): 
                number_of_scan_plots = math.ceil(len(scans)/max_scans_per_plot)
                number_of_plots = number_of_scan_plots + number_of_spw_plots + number_of_antenna_plots

            plot_height = number_of_plots * 2 # was prevously fixed at 6. 

            # this needs to be if vla only
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
            fig, subplots = plt.subplots(number_of_plots, 1, figsize=(plot_len, plot_height))

            # This is copied from SO and should be re-worked, or at least not dropped in the middle of a giant
            # function definition
            def split(a, n):
                k, m = divmod(len(a), n)
                return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

            if number_of_scan_plots == 1:
                ax1, ax2, ax3 = subplots
            else: 
                ax1 = subplots[0]
                ax2 = subplots[1]
                ax_scans = subplots[2:]

            flierprops = dict(marker='+', markerfacecolor='royalblue', markeredgecolor='royalblue')

            # Create per-antenna plots
            ax1.bxp(bxpstats_per_ant, flierprops=flierprops)
            ax1.axes.set_xticklabels(ant_names, rotation=45, ha='center')
            ax1.set_ylabel('$Wt_{i}$')
            ax1.set_title('Antenna-based weights, {}-band'.format(band))
            ax1.get_yaxis().get_major_formatter().set_useOffset(False)

            # Create per-spw plots
            ax2.bxp(bxpstats_per_spw, flierprops=flierprops)
            ax2.axes.set_xticklabels(band2spw[band])
            ax2.set_xlabel('SPW ID')
            ax2.set_ylabel('$Wt_{i}$')
            ax2.get_yaxis().get_major_formatter().set_useOffset(False)
            
            # Save-off y-axis limits so they can be re-used for the scans plots, which 
            # can be split up if there are more than 75 scans, to keep y-axes consistent
            y_min, y_max = ax2.get_ylim()

            # Create per-scan sub-plots
            if number_of_scan_plots <= 1: 
                ax3.bxp(bxpstats_per_scan, flierprops=flierprops)
                ax3.axes.set_xticklabels(scans)
                ax3.set_xlabel('Scan Number')
                ax3.set_ylabel('$Wt_{i}$')
                ax3.get_yaxis().get_major_formatter().set_useOffset(False)
            else: 
                bxpstats_per_scan_split = list(split(bxpstats_per_scan, number_of_scan_plots))

                scans_split = list(split(scans, number_of_scan_plots))
                for i, axis in enumerate(ax_scans): 
                    axis.bxp(bxpstats_per_scan_split[i], flierprops=flierprops)
                    axis.axes.set_xticklabels(scans_split[i], rotation=45, ha='center')
                    axis.set_xlabel('Scan Number')
                    axis.set_ylabel('$Wt_{i}$')
                    axis.set_ylim([y_min, y_max])
                    axis.get_yaxis().get_major_formatter().set_useOffset(False) 

            fig.tight_layout()
            fig.savefig(figfile)
            plt.close(fig)

            if not self.result.weight_stats:
                self.result.weight_stats[suffix] = {}
            # it might make sense to re-order these (band, then suffix, since bands will be grouped together.)
            self.result.weight_stats[suffix][band] =  {'per_spw': bxpstats_per_spw,
                                                      'per_ant': bxpstats_per_ant,
                                                      'per_scan': bxpstats_per_scan} # only add this if vla, not vlass
            bands_return.append(band)
                                                    
        return bands_return


    def _get_figfile(self, suffix, band=''):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['statwt',
                                                   self.ms.basename, 'summary', band, suffix])))+'.png'
        return os.path.join(stage_dir, fig_basename)


    def _get_plot_wrapper(self, suffix='', band=''):
        figfile = self._get_figfile(suffix, band)
        wrappers = []
        wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                              parameters={'vis': self.ms.basename,
                                          'x_axis': 'ant/spw/scan', #only add "scan" if via not vlass
                                          'y_axis': 'weight',
                                          'type': suffix, 
                                          'band': band})
# TODO:       if not os.path.exists(figfile):
        LOG.trace('Statwt summary plot not found. Creating new plot.')
        try:
            bands = self._create_plot_from_wtable(suffix)
            for band in bands:
                figfile = self._get_figfile(suffix, band)
                wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                        parameters={'vis': self.ms.basename,
                                    'x_axis': 'ant/spw/scan', # Only add "scan" if via not vlass
                                    'y_axis': 'weight',
                                    'type': suffix, 
                                    'band': band})
                wrappers.append(wrapper)
        except Exception as ex:
            LOG.error('Could not create ' + suffix + ' plot.')
            LOG.exception(ex)
            return None

        return wrappers

    def plot(self):
        plots = []
        for k, t in self.result.wtables.items():
            plots.extend(self._get_plot_wrapper(suffix=k)) # this doesn't handle the per-band directly/well
        return [p for p in plots if p is not None]
