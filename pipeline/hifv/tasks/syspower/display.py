import os

import numpy as np
import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

LOG = infrastructure.get_logger(__name__)


class syspowerBoxChart(object):
    def __init__(self, context, result, dat_common, band):
        self.context = context
        self.result = result
        self.dat_common = dat_common
        self.band = band
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
        dat_common = self.dat_common # self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower box chart for {!s}-band...".format(self.band))
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
        plt.ylabel('Template Pdiff   {!s}-band'.format(self.band))
        plt.xlabel('Antenna')
        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-box.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'caption': 'Template pdiff',
                                                                                'spw': '',
                                                                                'band': self.band})

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
    def __init__(self, context, result,  dat_common, band):
        self.context = context
        self.result = result
        self.dat_common = dat_common
        self.band = band
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
        dat_common = self.dat_common  # self.result.dat_common
        clip_sp_template = self.result.clip_sp_template

        LOG.info("Creating syspower bar chart for {!s}-band...".format(self.band))
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
        plt.ylabel('Fraction of Flagged Solutions (%)     {!s}-band'.format(self.band))
        plt.xlabel('Antenna')

        plt.savefig(figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-bar.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'caption': 'Fraction of flagged solutions',
                                                                                'spw': '',
                                                                                'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class compressionSummary(object):
    def __init__(self, context, result, spowerdict, band):
        self.context = context
        self.result = result
        self.spowerdict = spowerdict
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('compressionSummary')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        LOG.info("Creating syspower compression summary chart for {!s}-band...".format(self.band))
        pdiff = self.spowerdict['spower_common']  # self.result.spowerdict['spower_common']
        pdiff_ma = np.ma.masked_equal(pdiff, 0)

        fig0, axes = plt.subplots(4, 1, sharex='col')
        this_alpha = 1.0
        axes[0].plot(np.ma.max(pdiff_ma, axis=0)[0, 0], 'o', mfc='blue', mew=0, ms=3, alpha=this_alpha, label='max')
        axes[0].plot(np.ma.median(pdiff_ma, axis=0)[0, 0], 'o', mfc='green', mew=0, ms=3, alpha=this_alpha,
                     label='median')
        axes[0].plot(np.ma.min(pdiff_ma, axis=0)[0, 0], 'o', mfc='brown', mew=0, ms=3, alpha=this_alpha, label='min')
        axes[0].set_ylim(0.5, 1.2)
        axes[0].set_ylabel('Bband 0R')
        leg = axes[0].legend(loc='lower center', ncol=3, bbox_to_anchor=(0.5, 1.0), frameon=True, numpoints=1,
                             fancybox=False)
        title = axes[0].set_title('P_diff template summary    {!s}-band'.format(self.band))
        title.set_position([.5, 1.225])
        axes[1].plot(np.ma.max(pdiff_ma, axis=0)[0, 1], 'o', mfc='blue', mew=0, ms=3, alpha=this_alpha)
        axes[1].plot(np.ma.median(pdiff_ma, axis=0)[0, 1], 'o', mfc='green', mew=0, ms=3, alpha=this_alpha)
        axes[1].plot(np.ma.min(pdiff_ma, axis=0)[0, 1], 'o', mfc='brown', mew=0, ms=3, alpha=this_alpha)
        axes[1].set_ylim(0.5, 1.2)
        axes[1].set_ylabel('Bband 0L')

        try:
            axes[2].plot(np.ma.max(pdiff_ma, axis=0)[1, 0], 'o', mfc='blue', mew=0, ms=3, alpha=this_alpha)
            axes[2].plot(np.ma.median(pdiff_ma, axis=0)[1, 0], 'o', mfc='green', mew=0, ms=3, alpha=this_alpha)
            axes[2].plot(np.ma.min(pdiff_ma, axis=0)[1, 0], 'o', mfc='brown', mew=0, ms=3, alpha=this_alpha)
            axes[2].set_ylim(0.5, 1.2)
            axes[2].set_ylabel('Bband 1R')

            axes[3].plot(np.ma.max(pdiff_ma, axis=0)[1, 1], 'o', mfc='blue', mew=0, ms=3, alpha=this_alpha)
            axes[3].plot(np.ma.median(pdiff_ma, axis=0)[1, 1], 'o', mfc='green', mew=0, ms=3, alpha=this_alpha)
            axes[3].plot(np.ma.min(pdiff_ma, axis=0)[1, 1], 'o', mfc='brown', mew=0, ms=3, alpha=this_alpha)
            axes[3].set_ylim(0.5, 1.2)
            axes[3].set_ylabel('Bband 1L')
            axes[3].set_xlabel('Time (seconds)')
        except IndexError as ex:
            LOG.debug("Only one baseband to plot.")
        fig0.set_size_inches(8, 10)

        plt.savefig(figfile)
        leg.set_bbox_to_anchor((0.5, 0.85))
        # mpld3.save_html(fig0, figname.replace('.png', '.html'))
        plt.close(fig0)
        plt.close()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-compressionSummary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='time', y_axis='pdiff', parameters={'vis': self.ms.basename,
                                                                                   'type': prefix,
                                                                                   'caption': 'Compression summary',
                                                                                   'spw': '',
                                                                                   'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower compressionSummary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class medianSummary(object):
    def __init__(self, context, result, spowerdict, band):
        self.context = context
        self.result = result
        self.spowerdict = spowerdict
        self.band = band
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = result.gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('medianSummary')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        # Note that the original script needs the following variables (named differently)
        # Original pdiff from spower_common
        # New variable determined via np.ma.masked_equal(pdiff, 0)
        # Second variable determined via np.ma.masked_where(<2nd variable>== 0, <2nd variable>)

        LOG.info("Creating syspower compression median pdiff summary chart for {!s}-band...".format(self.band))
        pd = self.spowerdict['spower_common']  # self.result.spowerdict['spower_common']
        pdiff = np.ma.masked_equal(pd, 0)

        pdiff_ma = np.ma.masked_where(pdiff == 0, pdiff)

        xrange = np.array(range(pdiff.shape[3]))
        pdiff_ma.data[pdiff_ma > 1.1] = 1.1
        pdiff_ma.data[pdiff_ma < 0.8] = 0.7
        n_blank = 100

        # https://github.com/numpy/numpy/issues/14650

        try:
            pdiff_ma.mask[:, :, :, :n_blank] = True
        except Exception as e:
            if type(pdiff_ma.mask) == np.bool_:
                pdiff_ma.mask = np.ma.getmaskarray(pdiff_ma)
            pdiff_ma.mask[:, :, :, :n_blank] = True
            LOG.debug("Issue with the array mask {!s}".format(e))
            LOG.info("No zero values in pdiff - mask value set to a scalar boolean.  Reset to a matrix. ")

        ma_medians = np.ma.median(pdiff_ma, axis=0)

        fig0 = plt.figure()

        these_medians = ma_medians[0, 0, :]
        hits = np.logical_not(these_medians.mask)
        plt.plot(xrange[hits], these_medians[hits], 'o', mew=0, ms=5, alpha=1.0, label='Bband 0R')

        these_medians = ma_medians[0, 1, :]
        hits = np.logical_not(these_medians.mask)
        plt.plot(xrange[hits], these_medians[hits], 'o', mew=0, ms=5, alpha=1.0, label='Bband 0L')

        try:
            these_medians = ma_medians[1, 0, :]
            hits = np.logical_not(these_medians.mask)
            plt.plot(xrange[hits], these_medians[hits], 'o', mew=0, ms=5, alpha=1.0, label='Bband 1R')

            these_medians = ma_medians[1, 1, :]
            hits = np.logical_not(these_medians.mask)
            plt.plot(xrange[hits], these_medians[hits], 'o', mew=0, ms=5, alpha=1.0, label='Bband 1L')
        except IndexError as ex:
            LOG.debug("Only one baseband to plot.")

        plt.xlim(0, pdiff.shape[3])
        leg = plt.legend(loc='upper center', ncol=4, bbox_to_anchor=(0.5, 1.1))
        plt.xlabel('Time (seconds)')
        plt.ylabel('median P_diff     {!s}-band'.format(self.band))
        plt.ticklabel_format(useOffset=False)
        plt.gcf().set_size_inches(8, 7)
        plt.savefig(figfile)
        leg.set_bbox_to_anchor((0.5, 0.99))
        # mpld3.save_html(fig0, figname.replace('.png', '.html'))
        plt.close(fig0)
        plt.close()

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'syspower-{!s}-'.format(self.band) + prefix + '-%s-medianSummary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='time', y_axis='pdiff', parameters={'vis': self.ms.basename,
                                                                                  'type': prefix,
                                                                                  'caption': 'Median pdiff summary',
                                                                                  'spw': '',
                                                                                  'band': self.band})

        if not os.path.exists(figfile):
            LOG.trace('syspower medianSummary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class syspowerPerAntennaChart(object):
    def __init__(self, context, result, yaxis, caltable, fileprefix, tabletype, band, spw):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.yaxis = yaxis
        self.caltable = caltable
        self.fileprefix = fileprefix
        self.tabletype = tabletype
        self.band = band
        self.spw = spw

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          yaxis + fileprefix + tabletype + '-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        numAntenna = len(m.antennas)
        plots = []

        LOG.info("Plotting syspower " + self.tabletype + " charts for " + self.yaxis +
                 " and {!s}-band".format(self.band))
        nplots = numAntenna

        for ii in range(nplots):

            filename = self.fileprefix + '_' + self.tabletype + '_' + '{!s}_'.format(self.band) + self.yaxis + str(ii) + '.png'
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

                    tabletype = self.tabletype
                    if self.tabletype == 'pdiff':
                        tabletype = 'pdfif_{!s}'.format(self.band)

                    pindexlist = [0, 1]
                    cplots = [True, False]

                    numspws = len(self.spw.split(','))
                    if numspws == 1:
                        pindexlist = [0]

                    for pindex in pindexlist:

                        spwtouse = self.spw.split(',')[pindex]
                        job = casa_tasks.plotms(vis=self.caltable, xaxis='time', yaxis=self.yaxis, field='',
                                                antenna=antPlot, spw=spwtouse, timerange='',
                                                plotindex=pindex, gridrows=2, gridcols=1, rowindex=pindex, colindex=0,
                                                plotrange=plotrange, coloraxis='corr', overwrite=True,
                                                clearplots=cplots[pindex],
                                                title='Sys Power ' + tabletype +
                                                      '.tbl  Antenna: {!s}  {!s}-band  spw: {!s}'.format(antName,
                                                                                                         self.band,
                                                                                                         spwtouse),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile)

                        job.execute(dry_run=False)

                except Exception as ex:
                    LOG.warning("Unable to plot " + filename)
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis=self.yaxis.title(), field='',
                                   parameters={'spw': self.spw,
                                               'pol': '',
                                               'ant': antName,
                                               'type': self.tabletype,
                                               'band': self.band,
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warning("Unable to add plot to stack")
                plots.append(None)

        return [p for p in plots if p is not None]
