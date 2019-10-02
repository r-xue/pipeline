from __future__ import absolute_import
import os

import numpy as np

import casatasks

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)


class testgainsSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        # self.caltable = result.final[0].gaintable

    def plot(self):
        # science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        plots = [self.get_plot_wrapper('testgains_sample')]
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        antplot = '0~2'

        plotmax = 100

        # Dummy plot
        casatasks.plotms(vis=self.result.bpdgain_touse, xaxis='time', yaxis='amp', field='',
                         antenna=antplot, spw='', timerange='', plotrange=[0, 0, 0, plotmax], coloraxis='spw',
                         title='testgains Temp table', titlefont=8, xaxisfont=7, yaxisfont=7,
                         showgui=False, plotfile=figfile)

    def get_figfile(self, prefix):
        return os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number,
                            'testgains'+prefix+'-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': prefix,
                                                                                'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('testgains summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + prefix + ' plot.')
                LOG.exception(ex)
                return None

        return wrapper


class testgainsPerAntennaChart(object):
    def __init__(self, context, result, yaxis):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.yaxis = yaxis

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          yaxis + 'testgains-%s.json' % self.ms)

    def plot(self):
        m = self.context.observing_run.measurement_sets[0]
        numAntenna = len(m.antennas)
        plots = []

        LOG.info("Plotting testgain solutions")

        with casatools.TableReader(self.result.bpdgain_touse) as tb:
            cpar = tb.getcol('CPARAM')
            flgs = tb.getcol('FLAG')
        amps = np.abs(cpar)
        good = np.logical_not(flgs)
        maxamp = np.max(amps[good])
        plotmax = maxamp

        nplots = numAntenna

        for ii in range(nplots):

            filename = 'testgaincal_' + self.yaxis + str(ii) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % self.result.stage_number
            stage_dir = os.path.join(self.context.report_dir, stage)
            # construct the relative filename, eg. 'stageX/testdelay0.png'

            figfile = os.path.join(stage_dir, filename)

            xconnector = 'step'

            if self.yaxis == 'amp':
                plotrange = [0, 0, 0, plotmax]
                plotsymbol = 'o'
                xconnector = 'line'

            if self.yaxis == 'phase':
                plotrange = [0, 0, -180, 180]
                plotsymbol = 'o-'
                xconnector = 'line'

            if not os.path.exists(figfile):
                try:

                    # Get antenna name
                    antname = antPlot
                    if antPlot != '':
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antname = ','.join(idents)

                    casatasks.plotms(vis=self.result.bpdgain_touse, xaxis='time', yaxis=self.yaxis, field='',
                                     antenna=antPlot, spw='', timerange='', plotrange=plotrange, coloraxis='',
                                     title='G table: {!s}   Antenna: {!s}'.format(self.result.bpdgain_touse, antname),
                                     titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                     xconnector=xconnector)

                except Exception as ex:
                    LOG.warn("Unable to plot " + filename + str(ex))
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis=self.yaxis.title(), field='',
                                   parameters={'spw': '',
                                               'pol': '',
                                               'ant': antname,
                                               'type': self.yaxis,
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warn("Unable to add plot to stack. " + str(ex))
                plots.append(None)

        return [p for p in plots if p is not None]
