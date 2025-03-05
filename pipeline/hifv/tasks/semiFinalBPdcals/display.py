import math
import os

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.hifv.tasks.testBPdcals import baseDisplay

LOG = infrastructure.get_logger(__name__)


class semifinalBPdcalsSummaryChart(baseDisplay.SummaryChart):
    def __init__(self, context, result, suffix='', taskname=None):
        super().__init__(context, result, suffix, taskname)

    def plot(self):
        plots = super().plot()
        return plots

    def create_plot(self, prefix=''):
        super().create_plot(prefix)

    def get_figfile(self, prefix=''):
        filename = super().get_figfile(prefix)
        return filename

    def get_plot_wrapper(self, prefix=''):
        wrapper = super().get_plot_wrapper(prefix)
        return wrapper


class semifinalBPdcalsSpwSummaryChart(baseDisplay.PerSpwSummaryChart):
    def __init__(self, context, result, spw=None, suffix='', taskname=None):
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


class DelaysPerAntennaChart(baseDisplay.PerAntennaChart):
    def __init__(self, context, result, suffix='', taskname=None):
        super().__init__(context, result, suffix, taskname)

    def plot(self):
        plots = super().plot()
        return plots


class semifinalphaseGainPerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'phasegain-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        times = []
        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():
            with casa_tools.TableReader(bpdgain_touse) as tb:
                times.extend(tb.getcol('TIME'))
        mintime = np.min(times)
        maxtime = np.max(times)

        LOG.info("Plotting phase gain solutions")

        for bandname, bpdgain_touse in self.result.bpdgain_touse.items():

            for ii in range(nplots):
                filename = 'BPinitialgainphase' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                if not os.path.exists(figfile):
                    try:
                        # Get antenna name
                        antName = antPlot
                        if antPlot != '':
                            domain_antennas = self.ms.get_antenna(antPlot)
                            idents = [a.name if a.name else a.id for a in domain_antennas]
                            antName = ','.join(idents)

                        LOG.debug("Plotting phase gain solutions " + antName)

                        job = casa_tasks.plotms(vis=bpdgain_touse, xaxis='time', yaxis='phase', field='',
                                         antenna=antPlot, spw='', timerange='',
                                         coloraxis='', plotrange=[mintime, maxtime, -180, 180], symbolshape='circle',
                                         title='G table: {!s}   Antenna: {!s}  Band: {!s}'.format(bpdgain_touse, antName, bandname),
                                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                         xconnector='line')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:

                    plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'phasegain' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolAmpPerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting amp bandpass solutions")

        for bandname, bpcaltablename in self.result.bpcaltable.items():

            with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
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
            ampplotmax = maxmaxamp

            for ii in range(nplots):
                filename = 'BPcal_amp' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                if not os.path.exists(figfile):
                    try:
                        # Get antenna name
                        antName = antPlot
                        if antPlot != '':
                            domain_antennas = self.ms.get_antenna(antPlot)
                            idents = [a.name if a.name else a.id for a in domain_antennas]
                            antName = ','.join(idents)

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                         antenna=antPlot, spw='', timerange='',
                                         coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                         title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format('BPcal.b', antName, bandname),
                                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                         xconnector='step')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Amp', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'bpsolamp' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolAmpPerAntennaPerSpwChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolamp-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]
        spws = m.get_spectral_windows(science_windows_only=True)

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting amp bandpass solutions per spw for spectral window spws")

        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():

                    with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
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
                    ampplotmax = maxmaxamp

                    for ii in range(nplots):
                        filename = 'BPcal_amp' + str(ii) + '_' + self.suffix + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False
                        if not os.path.exists(figfile):
                            try:
                                # Get antenna name
                                antName = antPlot
                                if antPlot != '':
                                    domain_antennas = self.ms.get_antenna(antPlot)
                                    idents = [a.name if a.name else a.id for a in domain_antennas]
                                    antName = ','.join(idents)

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='amp', field='',
                                                antenna=antPlot,timerange='',
                                                spw=str(spw.id),
                                                coloraxis='', plotrange=[0, 0, 0, ampplotmax], symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format('BPcal.b', antName, bandname, str(spw.id)),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                                job.execute()

                            except:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename)
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='Freq', y_axis='Amp', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'bpsolamp' + self.suffix,
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

        return [p for p in plots if p is not None]


class semifinalbpSolPhasePerAntennaChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting phase bandpass solutions")

        for bandname, bpcaltablename in self.result.bpcaltable.items():

            with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
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
            phaseplotmax = maxmaxphase

            for ii in range(nplots):
                filename = 'BPcal_phase' + str(ii) + '_' + self.suffix + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % result.stage_number
                stage_dir = os.path.join(context.report_dir, stage)
                # construct the relative filename, eg. 'stageX/testdelay0.png'

                figfile = os.path.join(stage_dir, filename)

                if not os.path.exists(figfile):
                    try:
                        # Get antenna name
                        antName = antPlot
                        if antPlot != '':
                            domain_antennas = self.ms.get_antenna(antPlot)
                            idents = [a.name if a.name else a.id for a in domain_antennas]
                            antName = ','.join(idents)

                        job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                         antenna=antPlot, spw='', timerange='',
                                         coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                         symbolshape='circle',
                                         title='B table: {!s}   Antenna: {!s}  Band: {!s}'.format('BPcal.tbl', antName, bandname),
                                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                         xconnector='step')

                        job.execute()

                    except:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Freq', y_axis='Phase', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': 'bpsolphase' + self.suffix,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

            # Get BPcal.b to close...

        return [p for p in plots if p is not None]


class semifinalbpSolPhasePerAntennaPerSpwChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        ms = self.ms

        self.json = {}
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          'bpsolphase-' + self.suffix + '%s.json' % ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)

        plots = []

        LOG.info("Plotting phase bandpass solutions for spectral windows spws")
        spws = m.get_spectral_windows(science_windows_only=True)
        for spw in spws:
            if spw.specline_window:
                for bandname, bpcaltablename in self.result.bpcaltable.items():

                    with casa_tools.TableReader(self.result.bpcaltable[bandname]) as tb:
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
                    phaseplotmax = maxmaxphase

                    for ii in range(nplots):
                        filename = 'BPcal_phase' + str(ii) + '_' + self.suffix + '_' + bandname + '_' + str(spw.id) + '.png'
                        antPlot = str(ii)

                        stage = 'stage%s' % result.stage_number
                        stage_dir = os.path.join(context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        plot_failed = False
                        if not os.path.exists(figfile):
                            try:
                                # Get antenna name
                                antName = antPlot
                                if antPlot != '':
                                    domain_antennas = self.ms.get_antenna(antPlot)
                                    idents = [a.name if a.name else a.id for a in domain_antennas]
                                    antName = ','.join(idents)

                                job = casa_tasks.plotms(vis=bpcaltablename, xaxis='freq', yaxis='phase', field='',
                                                antenna=antPlot, timerange='',
                                                spw=str(spw.id),
                                                coloraxis='', plotrange=[0, 0, -phaseplotmax, phaseplotmax],
                                                symbolshape='circle',
                                                title='B table: {!s}   Antenna: {!s}  Band: {!s}  Spw: {!s}'.format('BPcal.tbl', antName, bandname, str(spw.id)),
                                                titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                                xconnector='step')

                                job.execute()

                            except:
                                plot_failed = True
                                LOG.warning("Unable to plot " + filename)
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        if not plot_failed:
                            try:
                                plot = logger.Plot(figfile, x_axis='Freq', y_axis='Phase', field='',
                                                parameters={'spw': str(spw.id),
                                                            'pol': '',
                                                            'ant': antName,
                                                            'bandname': bandname,
                                                            'type': 'bpsolphase' + self.suffix,
                                                            'file': os.path.basename(figfile)})
                                plots.append(plot)
                            except:
                                LOG.warning("Unable to add plot to stack")
                                plots.append(None)
                        else:
                            plots.append(None)

                # Get BPcal.b to close...

        return [p for p in plots if p is not None]
