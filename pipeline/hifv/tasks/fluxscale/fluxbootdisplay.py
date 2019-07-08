from __future__ import absolute_import
import os
import numpy as np
import pylab as pb

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger

import casa

LOG = infrastructure.get_logger(__name__)


class fluxbootSummaryChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        # self.caltable = result.final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        context = self.context
        m = context.observing_run.measurement_sets[0]
        corrstring = m.get_vla_corrstring()
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string
        ms_active = m.name

        casa.plotms(vis=ms_active, xaxis='freq', yaxis='amp', ydatacolumn='model', selectdata=True,
                    scan=calibrator_scan_select_string, correlation=corrstring, averagedata=True,
                    avgtime='1e8', avgscan=True, transform=False,    extendflag=False, iteraxis='',
                    coloraxis='field', plotrange=[], title='', xlabel='', ylabel='',  showmajorgrid=False,
                    showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

    def get_figfile(self):
        return os.path.join(self.context.report_dir, 
                            'stage%s' % self.result.stage_number, 
                            'bootstrappedFluxDensities-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename,
                                                                                'type': 'fluxboot',
                                                                                'spw': '',
                                                                                'figurecaption':'Model calibrator.  Plot of amp vs. freq.'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting model calibrator flux densities. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot plot.')
                LOG.exception(ex)
                return None

        return wrapper


class fluxgaincalSummaryChart(object):
    def __init__(self, context, result, caltable):
        self.context = context
        self.result = result
        self.caltable = caltable
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        # self.caltable = result.final[0].gaintable

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        casa.plotms(vis=self.caltable, xaxis='freq', yaxis='amp', ydatacolumn='', selectdata=True,
                    scan='', correlation='', averagedata=True,
                    avgtime='', avgscan=False, transform=False, extendflag=False, iteraxis='',
                    coloraxis='field', plotrange=[], title='', xlabel='', ylabel='', showmajorgrid=False,
                    showminorgrid=False, plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'fluxgaincalFluxDensities-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxgaincal',
                                          'spw': '',
                                          'figurecaption': 'Caltable: {!s}. Plot of amp vs. freq.'.format(self.caltable)})

        if not os.path.exists(figfile):
            LOG.trace('Plotting amp vs. freq for fluxgaincal. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxgaincal plot.')
                LOG.exception(ex)
                return None

        return wrapper


class modelfitSummaryChart(object):
    def __init__(self, context, result, webdicts):
        self.context = context
        self.result = result
        self.webdicts = webdicts
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        webdicts = self.webdicts
        pb.clf()

        mysize = 'small'
        colors = ['red', 'blue', 'green', 'cyan', 'yellow', 'orange', 'purple']
        colorcount = 0
        title = ''

        for source, datadicts in webdicts.iteritems():
            try:
                frequencies = []
                data = []
                model = []
                for datadict in datadicts:
                    data.append(float(datadict['data']))
                    model.append(float(datadict['fitteddata']))
                    frequencies.append(float(datadict['freq']))
                pb.plot(frequencies, data, 'o', label=source, color=colors[colorcount])
                pb.plot(frequencies, model, '-', color=colors[colorcount])
                pb.ylabel('Flux Density [Jy]', size=mysize)
                pb.xlabel('Frequency [GHz]', size=mysize)
                pb.legend()
                # title = title + '   ' + str(source) + '({!s})'.format(colors[colorcount])
                colorcount += 1

            except Exception as e:
                continue

        pb.title('Flux (Data and Fit) vs. Frequency')
        pb.savefig(figfile)
        pb.close()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'modelfit-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='flux',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxbootmodelfit',
                                          'figurecaption': 'Flux vs. frequency'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting fluxboot fit vs. freq. Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot fitting plot.')
                LOG.exception(ex)
                return None

        return wrapper


class residualsSummaryChart(object):
    def __init__(self, context, result, webdicts):
        self.context = context
        self.result = result
        self.webdicts = webdicts
        self.ms = context.observing_run.get_ms(result.inputs['vis'])

    def plot(self):
        plots = [self.get_plot_wrapper()]
        return [p for p in plots if p is not None]

    def create_plot(self):
        figfile = self.get_figfile()

        webdicts = self.webdicts
        pb.clf()

        mysize = 'small'
        colors = ['red', 'blue', 'green', 'cyan', 'yellow', 'orange', 'purple']
        colorcount = 0
        title = ''

        for source, datadicts in webdicts.iteritems():
            try:
                frequencies = []
                residuals = []
                for datadict in datadicts:
                    residuals.append(float(datadict['data']) - float(datadict['fitteddata']))
                    frequencies.append(float(datadict['freq']))
                pb.plot(frequencies, residuals, 'o', label=source, color=colors[colorcount])
                pb.plot(np.linspace(np.min(frequencies),
                                    np.max(frequencies),
                                    10),
                        np.zeros(10) + np.mean(residuals), linestyle='--', label='Mean', color=colors[colorcount])
                pb.ylabel('Residuals (data - fit) [Jy]', size=mysize)
                pb.xlabel('Frequency [GHz]', size=mysize)
                pb.legend()
                # title = title + '   ' + str(source) + '({!s})'.format(colors[colorcount])
                colorcount += 1

            except Exception as e:
                continue

        pb.title('Residuals vs. Frequency')
        pb.savefig(figfile)
        pb.close()

    def get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'residuals-%s-summary.png' % self.ms.basename)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()

        wrapper = logger.Plot(figfile, x_axis='freq', y_axis='residuals',
                              parameters={'vis': self.ms.basename,
                                          'type': 'fluxbootresiduals',
                                          'figurecaption': 'Fluxboot residuals vs. frequency'})

        if not os.path.exists(figfile):
            LOG.trace('Plotting fluxboot residuals vs. freq Creating new plot.')
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create fluxboot residuals plot.')
                LOG.exception(ex)
                return None

        return wrapper
