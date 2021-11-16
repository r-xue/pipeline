import os

import numpy as np
import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

LOG = infrastructure.get_logger(__name__)


class syspowerBoxChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable
        # results[4].read()[0].rq_result[0].final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('syspower_box')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antenna_names = [a.name for a in self.ms.antennas]

        # box plot of Pdiff template
        dat_common = self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower box chart...")
        plt.clf()
        dshape = dat_common.shape
        ant_dat = np.reshape(dat_common, newshape=(dshape[0], np.product(dshape[1:])))
        ant_dat = np.ma.array(ant_dat)
        ant_dat.mask = np.ma.getmaskarray(ant_dat)
        ant_dat = np.ma.masked_outside(ant_dat, clip_sp_template[0], clip_sp_template[1])
        ant_dat_filtered = [ant_dat[i][~ant_dat.mask[i]] for i in range(dshape[0])]
        plt.boxplot(ant_dat_filtered, whis=10, sym='.')
        plt.xticks(rotation=45)
        plt.ylim(clip_sp_template[0], clip_sp_template[1])
        plt.ylabel('Template Pdiff')
        plt.xlabel('Antenna')
        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower' + prefix + '-%s-box.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('syspower summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class syspowerBarChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable
        # results[4].read()[0].rq_result[0].final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('syspower_bar')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antenna_names = [a.name for a in self.ms.antennas]

        # box plot of Pdiff template
        dat_common = self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower bar chart...")
        plt.clf()
        dshape = dat_common.shape
        ant_dat = np.reshape(dat_common, newshape=(dshape[0], np.product(dshape[1:])))
        ant_dat = np.ma.array(ant_dat)
        ant_dat.mask = np.ma.getmaskarray(ant_dat)
        ant_dat = np.ma.masked_outside(ant_dat, clip_sp_template[0], clip_sp_template[1])

        # fraction of flagged data in Pdiff template
        percent_flagged_by_antenna = [100. * np.sum(ant_dat.mask[i]) / ant_dat.mask[i].size for i in range(dshape[0])]
        plt.bar(list(range(dshape[0])), percent_flagged_by_antenna, color='red')
        plt.xticks(rotation=45)
        plt.ylabel('Fraction of Flagged Solutions (%)')
        plt.xlabel('Antenna')

        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower' + prefix + '-%s-bar.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('syspower summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class syspowerPerAntennaChart(object):
    def __init__(self, context, result, yaxis, caltable, fileprefix, tabletype):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.yaxis = yaxis
        self.caltable = caltable
        self.fileprefix = fileprefix
        self.tabletype = tabletype

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          yaxis + fileprefix + tabletype + '-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        numAntenna = len(m.antennas)
        plots = []

        LOG.info("Plotting syspower " + self.tabletype + " charts for " + self.yaxis)
        nplots = numAntenna

        for ii in range(nplots):

            filename = self.fileprefix + '_' + self.tabletype + '_' + self.yaxis + str(ii) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % result.stage_number
            stage_dir = os.path.join(context.report_dir, stage)

            figfile = os.path.join(stage_dir, filename)

            plotrange = []

            if self.yaxis == 'spgain':
                plotrange = [0, 0, 0, 0.1]
            if self.yaxis == 'tsys':
                plotrange = [0, 0, 0, 100]
                spws = m.get_all_spectral_windows()
                freqs = sorted({spw.max_frequency.value for spw in spws})
                if float(max(freqs)) >= 18000000000.0:
                    plotrange = [0, 0, 0, 200]
            if self.tabletype == 'pdiff':
                clip_sp_template = self.result.clip_sp_template
                plotrange = [-1, -1, clip_sp_template[0], clip_sp_template[1]]

            if not os.path.exists(figfile):
                try:
                    # Get antenna name
                    antName = antPlot
                    if antPlot != '':
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                    LOG.debug("Sys Power Plot, using antenna={!s}".format(antName))

                    job = casa_tasks.plotms(vis=self.caltable, xaxis='time', yaxis=self.yaxis, field='',
                                            antenna=antPlot, spw='6,14', timerange='',
                                            plotrange=plotrange, coloraxis='spw',
                                            title='Sys Power ' + self.tabletype + '.tbl  Antenna: {!s}'.format(antName),
                                            titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile)

                    job.execute(dry_run=False)

                except Exception as ex:
                    LOG.warning("Unable to plot " + filename)
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis=self.yaxis.title(), field='',
                                   parameters={'spw': '',
                                               'pol': '',
                                               'ant': antName,
                                               'type': self.tabletype,
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warning("Unable to add plot to stack")
                plots.append(None)

        return [p for p in plots if p is not None]
