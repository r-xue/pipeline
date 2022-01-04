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
        print("in init")
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.result.weight_stats = {}

    @staticmethod
    def _get_weight_from_wtable(tbl, this_ant='', this_spw=''):
        if (this_ant != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_spw, this_ant)
        elif (this_ant != ''):
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0'.format(this_ant)
        elif (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ntrue(FLAG)==0'.format(this_spw)
        else:
            query_str = ''

        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query_str)
            weights = stb.getcol('CPARAM').ravel()
            stb.done()

        return weights.real

    def _create_plot_from_wtable(self, suffix):
        print("Creating plot from wtable")
        tbl = self.result.wtables[suffix]
        figfile = self._get_figfile(suffix)
        fig_title = os.path.basename(tbl)

        LOG.info('making antenna-based weight plot: {}'.format(figfile))

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))

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
                bxpstats_per_ant.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
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
                bxpstats_per_spw.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats_per_spw.extend(bxpstats)
            bxpstats_per_spw[-1]['spw'] = this_spw

        # HERE: might need to add a bxpstats_per_scan 

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
                                          'x_axis': 'ant/spw',
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
        print("in plot")
        print(len(self.result.wtables.items())) #Okay, so my test dataset has absolutely no results :/
        plots = [] 
        for k, t in self.result.wtables.items():
            print("suffix is: ")
            print(k)
            plots.append(self._get_plot_wrapper(suffix=k))

        return [p for p in plots if p is not None]
