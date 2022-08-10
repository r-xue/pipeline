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
    whis = 3.944

    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.result.weight_stats = {}
        self.band2spw = None

    @staticmethod
    def _get_weight_from_wtable(tbl, this_ant='', this_spw='', this_scan=''): #, spw_list=[]):
        if (this_ant != '') and (this_spw != '') and (this_scan!=''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && SCAN_NUMBER=={2} ntrue(FLAG)==0'.format(this_spw, this_ant, this_scan)
        elif (this_ant != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_spw, this_ant)
        #elif (this_ant != '') and spw_list: 
        #    query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {1}'.format(this_ant, list(map(int, spw_list)))
        elif (this_ant != '') and (this_scan != ''):
            query_str = 'SCAN_NUMBER=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_scan, this_ant)
        elif (this_scan != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && SCAN_NUMBER=={1} && ntrue(FLAG)==0'.format(this_spw, this_scan)
        elif (this_ant != ''):
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0'.format(this_ant)
        elif (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ntrue(FLAG)==0'.format(this_spw)
        elif (this_scan !=''):
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
        scans = None        
        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query)
            scans = np.sort(np.unique(stb.getcol('SCAN_NUMBER')))
            stb.done()
        return scans


    # get ants with provided spws used to determine scans for a band
    def _get_ants_with_spws(self, tbl, spws=[]):
        query = 'ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {0}'.format(list(map(int, spws)))
        ants = None
        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query)
            ants = np.sort(np.unique(stb.getcol('ANTENNA1')))
            stb.done()
        return ants


    def _create_plot_vla_pi(self, suffix, tbl, spws, scans, ant_names, ant_idxs): 
        # Create plots for each band
        bands_return = []
        for band in self.band2spw: 
            figfile = self._get_figfile(suffix, band)
            LOG.info('Making antenna-based weight plot: {0} for band: {1}'.format(figfile, band))
            bxpstats_per_ant = list()
            # TODO: handle this section, both with the query and with the "correct ticks" better
            # antenna names should come from the full MS passed in
            ant_idxs = self._get_ants_with_spws(tbl, self.band2spw[band]) #TODO: works? indices come from the wts table ANTENNA1 is for example 0,1,2,3
            antname_labels = []
            for this_ant in ant_idxs:
                print("Populating ant: {}".format(this_ant))
                dat = self._get_weight_from_wtable(tbl, this_ant=this_ant)#, spw_list=band2spw[band]) # needs to get updated ticks at the same time?
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_ant.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_ant.extend(bxpstats)
                bxpstats_per_ant[-1]['ant'] = ant_names[this_ant]
                antname_labels.append(ant_names[this_ant])

            bxpstats_per_spw = list()
            for this_spw in self.band2spw[band]:
                print("Populatin spw: {}".format(this_spw))
                dat = self._get_weight_from_wtable(tbl, this_spw=this_spw)
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_spw.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_spw.extend(bxpstats)
                bxpstats_per_spw[-1]['spw'] = this_spw
           
            scans = self._get_scans_with_spws(tbl, self.band2spw[band])

            bxpstats_per_scan = list()
            for this_scan in scans: 
                print("Populating scan: {}".format(this_scan))
                dat = self._get_weight_from_wtable(tbl, this_scan=this_scan)
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_scan.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_scan.extend(bxpstats)
                bxpstats_per_scan[-1]['scan'] = this_scan

            # Setup plot sizes and number of plots
            max_scans_per_plot = 75 # 5 for quick tests Scan plots are split every 75 for plot readibility
            number_of_scan_plots = math.ceil(len(scans)/max_scans_per_plot)
            # There is always one spw plot and one antenna plot.
            number_of_plots = number_of_scan_plots + 2 
            plot_len = 15
            plot_height = number_of_plots * 2

            fig, subplots = plt.subplots(number_of_plots, 1, figsize=(plot_len, plot_height))

            if number_of_scan_plots == 1:
                ax1, ax2, ax3 = subplots
            else: 
                ax1 = subplots[0]
                ax2 = subplots[1]
                ax_scans = subplots[2:]

            flierprops = dict(marker='+', markerfacecolor='royalblue', markeredgecolor='royalblue')

            # Create per-antenna plots
            ax1.bxp(bxpstats_per_ant, flierprops=flierprops)
            ax1.axes.set_xticklabels(antname_labels, rotation=45, ha='center')
            ax1.set_ylabel('$Wt_{i}$')
            ax1.set_title('Antenna-based weights, {}-band'.format(band))
            ax1.get_yaxis().get_major_formatter().set_useOffset(False)

            # Create per-spw plots
            ax2.bxp(bxpstats_per_spw, flierprops=flierprops)
            ax2.axes.set_xticklabels(self.band2spw[band], rotation=45, ha='center')
            ax2.set_xlabel('SPW ID')
            ax2.set_ylabel('$Wt_{i}$')
            ax2.get_yaxis().get_major_formatter().set_useOffset(False)
            
            # Save-off y-axis limits so they can be re-used for the scans plots, which 
            # can be split up if there are more than max_scans_per_plot scans, to keep y-axes consistent
            y_min, y_max = ax2.get_ylim()

            # Create per-scan sub-plots
            if number_of_scan_plots == 1: 
                ax3.bxp(bxpstats_per_scan, flierprops=flierprops)
                ax3.axes.set_xticklabels(scans, rotation=45, ha='center')
                ax3.set_xlabel('Scan Number')
                ax3.set_ylabel('$Wt_{i}$')
                ax3.get_yaxis().get_major_formatter().set_useOffset(False)
            else: 
                bxpstats_per_scan_split = list(self._split(bxpstats_per_scan, number_of_scan_plots))
                scans_split = list(self._split(scans, number_of_scan_plots))
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
                                                       'per_scan': bxpstats_per_scan}
            bands_return.append(band)
                                                    
        return bands_return

    def _create_plot_vlass(self, suffix, tbl, spws, ant_names, ant_idxs):
        figfile = self._get_figfile(suffix)

        bxpstats_per_ant = list()
        for this_ant in ant_idxs:
            dat = self._get_weight_from_wtable(tbl, this_ant=this_ant)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats_per_ant.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats_per_ant.extend(bxpstats)
            bxpstats_per_ant[-1]['ant'] = ant_names[this_ant]

        bxpstats_per_spw = list()
        for this_spw in spws:
            dat = self._get_weight_from_wtable(tbl, this_spw=this_spw)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats_per_spw.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats_per_spw.extend(bxpstats)
            bxpstats_per_spw[-1]['spw'] = this_spw

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6))
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

        fig.tight_layout()
        fig.savefig(figfile)
        plt.close(fig)

        self.result.weight_stats[suffix] = {'per_spw': bxpstats_per_spw,
                                            'per_ant': bxpstats_per_ant}
        return

    def _create_plot_from_wtable(self, suffix):
        tbl = self.result.wtables[suffix]

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))
            scans = np.sort(np.unique(tb.getcol('SCAN_NUMBER')))

        with casa_tools.TableReader(tbl+'/ANTENNA') as tb:
            ant_names = tb.getcol('NAME')
            ant_idxs = range(len(ant_names))

        # Plots are per-band for VLA and not for VLASS (there are also other differences)
        plots = None
        if self.result.inputs['statwtmode'] == "VLA": 
            # VLA PI plots are separated out by band, so determine the spws 
            # for each band.  
            # Could this be different for before/after? 
            spw2band = self.ms.get_vla_spw2band()
            # Format: {'1':'C', '2':'K' }
            band2spw = collections.defaultdict(list)
            listspws = [spw for spw in spws]
            for spw, band in spw2band.items():
                if spw in listspws:
                    band2spw[band].append(str(spw))
            print(band2spw)
            self.band2spw = band2spw #TODO: set for whole class, or pass in? It might be more appropriate to pass in.
            # Format: {'C':[1,2,3], 'K':[4,5,6]}
            plots =  self._create_plot_vla_pi(suffix, tbl, spws, scans, ant_names, ant_idxs)
        else: 
            plots = self._create_plot_vlass(suffix, tbl, spws, ant_names, ant_idxs)

        return plots

    def _get_figfile(self, suffix, band=''):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        
        if self.result.inputs['statwtmode'] == "VLA": 
            fig_basename = '-'.join(list(filter(None, ['statwt',
                                                   self.ms.basename, 'summary', band, suffix])))+'.png'
        else:
            fig_basename = '-'.join(list(filter(None, ['statwt',
                                                   self.ms.basename, 'summary', suffix])))+'.png'
        
        return os.path.join(stage_dir, fig_basename)

# TODO: everything from here down is kind of poorly split into vla vs vlass. potentially give this some more thought.
    def _get_plot_wrapper(self, suffix='', band=''):
        figfile = self._get_figfile(suffix, band)
        wrappers = []

        # only generate a plot wrapper for VLASS-SE
        if not (self.result.inputs['statwtmode'] == "VLA"): 
            wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                    parameters={'vis': self.ms.basename,
                                'x_axis': 'ant/spw',
                                'y_axis': 'weight',
                                'type': suffix})
            wrappers.append(wrapper)

        if not os.path.exists(figfile):
            LOG.trace('Statwt summary plot not found. Creating new plot.')
            try:
                if self.result.inputs['statwtmode'] == "VLA": 
                    bands = self._create_plot_from_wtable(suffix)
                    for band in bands:
                        figfile = self._get_figfile(suffix, band)
                        wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                                parameters={'vis': self.ms.basename,
                                            'x_axis': 'ant/spw/scan',
                                            'y_axis': 'weight',
                                            'type': suffix, 
                                            'band': band})
                        wrappers.append(wrapper)
                else: 
                    self._create_plot_from_wtable(suffix)
            except Exception as ex:
                LOG.error('Could not create ' + suffix + ' plot.')
                LOG.exception(ex)
                return None

        return wrappers

    def plot(self):
        plots = []
        for k, _ in self.result.wtables.items():
            print("making a plot for stage {}".format(k))
            plots.extend(self._get_plot_wrapper(suffix=k))
        return [p for p in plots if p is not None]

    # -------------
    # This is copied from SO and should be re-worked, or at least not dropped in the middle of a giant
    # function definition
    def _split(self, a, n):
        k, m = divmod(len(a), n)
        return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))
