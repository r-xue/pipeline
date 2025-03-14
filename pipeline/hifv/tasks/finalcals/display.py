import math
import os
import collections

import numpy as np

from pipeline.hifv.tasks.testBPdcals import baseDisplay
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


class finalcalsSummaryChart(baseDisplay.SummaryChart):
    def __init__(self, context, result, spw='', suffix='', taskname=None):
        super().__init__(context, result, spw=spw, suffix=suffix, taskname=taskname)

    def plot(self):
        plots = super().plot()
        return plots

    def create_plot(self,  prefix=''):
        super().create_plot(prefix)

    def get_figfile(self, prefix=''):
        filename = super().get_figfile(prefix)
        return filename

    def get_plot_wrapper(self, prefix=''):
        wrapper = super().get_plot_wrapper(prefix)
        return wrapper


class AntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        super().__init__(context, result, suffix, taskname, plottype, perSpwChart)

    def plot(self):
        plots = super().plot()
        return plots


class finalbpSolAmpPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolamp-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    _, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
                    ampplotmax = maxmaxamp

                    LOG.info("Plotting amp bandpass solutions")

                    for ii in range(nplots):

                        filename = 'finalBPcal_amp' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False
                        # Get antenna name
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting amp bandpass solutions " + antName)
                                job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='amp', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'finalBPcal.tbl', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                real_figfile = figfile

                                plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'BP Amp solution',
                                                            'file': os.path.basename(real_figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalbpSolPhasePerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalbpsolphase-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    maxmaxphase, maxmaxamp = get_maxphase_maxamp(self.result.bpcaltable)
                    phaseplotmax = maxmaxphase

                    LOG.info("Plotting phase bandpass solutions")

                    for ii in range(nplots):

                        filename = 'finalBPcal_phase' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False

                        # Get antenna name
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)
                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting phase bandpass solutions " + antName)
                                job = casa_tasks.plotms(vis=self.result.bpcaltable, xaxis='freq', yaxis='phase', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                        symbolshape='circle',
                                                        title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            'finalBPcal.tbl', antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='step')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                real_figfile = figfile

                                plot = logger.Plot(real_figfile, x_axis='Freq', y_axis='Phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'BP Phase solution',
                                                            'file': os.path.basename(real_figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalAmpTimeCalPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalamptimecal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    with casa_tools.TableReader(self.result.finalampgaincaltable) as tb:
                        cpar = tb.getcol('CPARAM')
                        flgs = tb.getcol('FLAG')
                    amps = np.abs(cpar)
                    good = np.logical_not(flgs)
                    maxamp = np.max(amps[good])
                    plotmax = max(2.0, maxamp)

                    LOG.info("Plotting final amp timecal")

                    for ii in range(nplots):

                        filename = 'finalamptimecal' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False

                        # Get antenna name
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting final amp timecal " + antName)
                                job = casa_tasks.plotms(vis=self.result.finalampgaincaltable, xaxis='time', yaxis='amp', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[mintime, maxtime, 0, plotmax], symbolshape='circle',
                                                        title='G table: finalampgaincal.tbl   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='line')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='Time', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Final amp time cal',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class finalPhaseGainCalPerAntennaPerSpwChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'finalphasegaincal-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        spws = m.get_spectral_windows(science_windows_only=True)
        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting final phase freqcal per spw for spectral window spws")

        spw2band = self.ms.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = self.ms.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        with casa_tools.TableReader(self.result.finalphasegaincaltable) as tb:
            times = tb.getcol('TIME')
        mintime = np.min(times)
        maxtime = np.max(times)

        for spw in spwobjlist:
            if spw.specline_window:
                for bandname, spwlist in band2spw.items():
                    if spw2band[spw.id] != bandname:
                        continue
                    for ii in range(nplots):

                        filename = 'finalphasegaincal' + str(ii) + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False

                        # Get antenna name
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                        if not os.path.exists(figfile):
                            try:

                                LOG.debug("Plotting final phase freqcal " + antName)
                                job = casa_tasks.plotms(vis=self.result.finalphasegaincaltable, xaxis='time', yaxis='phase', field='',
                                                        antenna=antPlot, spw=str(spw.id), timerange='',
                                                        coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                                        title='G table: finalphasegaincal.tbl   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format(
                                                            antName, bandname, str(spw.id)),
                                                        titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                        xconnector='line')

                                job.execute()

                            except Exception as ex:
                                plot_failed = True
                                LOG.warning("Problem with plotting " + filename + str(ex))
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='time', y_axis='phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'Final phase gain cal',
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except Exception as ex:
                                LOG.warning("Unable to add plot to stack.  " + str(ex))
                                plots.append(None)
                        else: 
                            plots.append(None)

        return [p for p in plots if p is not None]


def get_maxphase_maxamp(bpcaltable):
    """
    Get the max amplitude and max phase for a given bpcaltable
    """
    with casa_tools.TableReader(bpcaltable) as tb:
        dataVarCol = tb.getvarcol('CPARAM')
        flagVarCol = tb.getvarcol('FLAG')

    rowlist = list(dataVarCol.keys())
    maxmaxamp = 0.0
    maxmaxphase = 0.0
    for rrow in rowlist:
        dataArr = dataVarCol[rrow]
        flagArr = flagVarCol[rrow]
        amps = np.abs(dataArr)
        phases = np.arctan2(np.imag(dataArr), np.real(dataArr))
        good = np.logical_not(flagArr)
        tmparr = amps[good]
        if len(tmparr) > 0:
            maxamp = np.max(amps[good])
            if maxamp > maxmaxamp:
                maxmaxamp = maxamp
        tmparr = np.abs(phases[good])
        if len(tmparr) > 0:
            maxphase = np.max(np.abs(phases[good])) * 180. / math.pi
            if maxphase > maxmaxphase:
                maxmaxphase = maxphase
    return maxmaxphase, maxmaxamp
