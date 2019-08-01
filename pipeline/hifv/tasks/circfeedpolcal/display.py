from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import casa
import shutil

LOG = infrastructure.get_logger(__name__)


class PolarizationPlotCalChart(object):
    def __init__(self, context, result, caltable='', yaxis='delay', xaxis='freq', antenna='', caption=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.measurement_sets[0]
        self.yaxis = yaxis
        self.xaxis = xaxis
        self.antenna = antenna
        self.caltable = caltable
        self.caption = caption

        # self.caltable = result.final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper('plotsummary')]

        # Create a dummy plot to release the cal table

        '''
        scratchfile = 'scratchpol.g'
        shutil.copytree(self.caltable, scratchfile)
        casa.plotcal(caltable=scratchfile,
                     xaxis='time', yaxis='phase', poln='', field='',
                     antenna=str(0), spw='', timerange='',
                     subplot=111, overplot=False, clearpanel='Auto',
                     iteration='antenna', plotrange=[0, 0, -180, 180],
                     showflags=False, plotsymbol='o-', plotcolor='blue',
                     markersize=5.0, fontsize=10.0, showgui=False, figfile="scratchpol.png")
        shutil.rmtree(scratchfile)
        '''

        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        LOG.info("Creating PlotSummary Polarization plot")
        # casa.plotcal(caltable=self.caltable, xaxis=self.xaxis, yaxis=self.yaxis, poln='',
        #             field='', antenna=self.antenna, spw='', timerange='', subplot=111, overplot=False,
        #             clearpanel='Auto', iteration='', plotrange=[], showflags=False,
        #             plotsymbol='o', plotcolor='blue', markersize=5.0, fontsize=10.0,
        #             showgui=False, figfile=figfile)

        casa.plotms(vis=self.caltable, xaxis=self.xaxis, yaxis=self.yaxis, field='', antenna=self.antenna,
                    spw='', timerange='', plotrange=[], coloraxis='spw',
                    title='{!s}  Antenna: {!s}'.format(self.caltable, self.antenna),
                    titlefont=7, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            prefix + '_' + self.yaxis + '_vs_' + self.xaxis + '-%s-plotcal.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        typeentry = ''
        if self.antenna:
            typeentry = ' for RefAnt {!s}'.format(self.antenna)

        wrapper = logger.Plot(figfile, x_axis=self.xaxis, y_axis=self.yaxis,
                              parameters={'vis': self.ms.basename,
                                          'caltable': self.caltable,
                                          'type': typeentry,
                                          'antenna': self.antenna,
                                          'spw': '',
                                          'figfile': figfile,
                                          'caption': self.caption + typeentry})

        if not os.path.exists(figfile):
            LOG.trace('plotsummary Plotcal plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class ampfreqPerAntennaChart(object):
    def __init__(self, context, result, caltable):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.caltable = caltable

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'ampfreq-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        numAntenna = len(m.antennas)

        plots = []

        LOG.info("Plotting amp vs. freq charts for " + self.caltable)

        nplots = numAntenna

        for ii in range(nplots):

            filename = 'ampfreq_' + str(ii) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % result.stage_number
            stage_dir = os.path.join(context.report_dir, stage)
            # construct the relative filename, eg. 'stageX/testdelay0.png'

            figfile = os.path.join(stage_dir, filename)

            plotrange = []

            if not os.path.exists(figfile):
                try:
                    # Get antenna name
                    antName = antPlot
                    if antPlot != '':
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                    casa.plotms(vis=self.caltable, xaxis='freq', yaxis='amp', field='',
                                antenna=antPlot, spw='', timerange='', plotrange=plotrange, coloraxis='',
                                title='POL table: {!s}   Antenna: {!s}'.format(self.caltable, antName),
                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                xconnector='step')

                except Exception as ex:
                    LOG.warn("Unable to plot " + filename + str(ex))
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Frequency', y_axis='Amplitude', field='',
                                   parameters={'spw': '',
                                               'pol': '',
                                               'ant': antName,
                                               'type': 'ampfreq',
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warn("Unable to add plot to stack. " + str(ex))
                plots.append(None)

        return [p for p in plots if p is not None]
