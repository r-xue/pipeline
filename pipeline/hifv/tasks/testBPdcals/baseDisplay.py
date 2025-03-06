import math
import os
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
from pipeline.infrastructure import casa_tools
LOG = infrastructure.get_logger(__name__)


class Chart(object):
    def plot(self):
        task_plots = {
            "testBPdcals": [self.get_plot_wrapper('BPcal'), self.get_plot_wrapper('delaycal')],
            "semiFinalBPdcals": [self.get_plot_wrapper()]
        }
        plots = task_plots.get(self.taskname, [])
        return [p for p in plots if p is not None]

    def get_plot_params(self, figfile):
        corrstring = self.ms.get_vla_corrstring()
        plot_params = {
                'vis': self.ms.name,
                'xaxis': 'freq', 'yaxis': 'amp', 'ydatacolumn': 'corrected',
                'selectdata': True, 'averagedata': True, 'avgtime': '1e8', 'avgscan': True,
                'transform': False, 'extendflag': False, 'iteraxis': '', 'coloraxis': 'antenna2',
                'plotrange': [], 'title': '', 'xlabel': '', 'ylabel': '',
                'showmajorgrid': False, 'showminorgrid': False, 'plotfile': figfile,
                'overwrite': True, 'clearplots': True, 'showgui': False, 'correlation': corrstring
                }
        return plot_params

    def get_maxphase_maxamp(self, bpdgain_touse, bpcaltablename):
        """
        Calculates the maximum amplitude and phase values from the bandpass calibration table.

        Args:
            bpdgain_touse (str): The path to the bandpass calibration table.
            bpcaltablename (str): The name of the bandpass calibration table.

        Returns:
            tuple: The maximum amplitude and phase values.
        """
        with casa_tools.TableReader(bpcaltablename) as tb:
            dataVarCol = tb.getvarcol('CPARAM')
            flagVarCol = tb.getvarcol('FLAG')

        rowlist = list(dataVarCol.keys())
        nrows = len(rowlist)
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
        phaseplotmax = maxmaxphase
        return phaseplotmax, ampplotmax


class SummaryChart(Chart):
    def __init__(self, context, result, suffix='', taskname=None):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        self.taskname = taskname

    def create_plot(self, prefix=''):
        figfile = self.get_figfile(prefix)
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string

        plot_params = super().get_plot_params(figfile)
        plot_scan = None
        if self.taskname == "testBPdcals":
            if prefix == 'BPcal':
                plot_scan = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
            elif (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
                plot_scan = delay_scan_select_string
        elif self.taskname == "semiFinalBPdcals":
            plot_scan = self.context.evla['msinfo'][self.ms.name].calibrator_scan_select_string
            plot_params['avgscan'] = False

        if plot_scan is not None:
            job = casa_tasks.plotms(**{**plot_params, 'scan': plot_scan})
            job.execute()

    def get_figfile(self, prefix=''):

        filename = ''
        base_path = os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number)
        if self.taskname == "testBPdcals":
            filename = 'testcalibrated' + prefix
        elif self.taskname == "semiFinalBPdcals":
            filename = 'semifinalcalibrated_' + self.suffix
        if filename:
            filename = os.path.join(base_path, (filename + '-%s-summary.png' % self.ms.basename))
        return filename

    def get_plot_wrapper(self, prefix=''):
        figfile = self.get_figfile(prefix=prefix)

        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
        plot_type = ''
        if self.taskname == "testBPdcals":
            if prefix == 'BPcal' or ((delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal'):
                plot_type = prefix
        elif self.taskname == "semiFinalBPdcals":
            plot_type = 'semifinalcalibratedcals' + self.suffix

        if plot_type:
            wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename, 'type': plot_type, 'spw': ''})

        if not os.path.exists(figfile):
            LOG.trace('Summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create ' + self.suffix + ' plot.')
                LOG.exception(ex)
                return None
        if plot_type:
            return wrapper
        return None


class PerSpwSummaryChart(Chart):
    def __init__(self, context, result, spw=None, suffix='', taskname=None):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.spw = str(spw)
        self.suffix = suffix
        self.taskname = taskname

    def create_plot(self, prefix=''):
        figfile = self.get_figfile(prefix)

        bandpass_field_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string
        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
        calibrator_scan_select_string = self.context.evla['msinfo'][self.ms.name].calibrator_scan_select_string
        plot_params = super().get_plot_params(figfile)
        plot_params['spw'] = self.spw
        plot_scan = None
        if self.taskname == "testBPdcals":
            if prefix == 'BPcal':
                plot_scan = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
                plot_params['field'] = bandpass_field_select_string

            if (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
                plot_scan = delay_scan_select_string

        elif self.taskname == "semiFinalBPdcals":
            plot_scan = calibrator_scan_select_string
            plot_params['avgscan'] = False

        if plot_scan is not None:
            job = casa_tasks.plotms(**{**plot_params, 'scan': plot_scan})
            job.execute()

    def get_figfile(self, prefix):
        filename = ''
        base_path = os.path.join(self.context.report_dir, 'stage%s' % self.result.stage_number)
        if self.taskname == "testBPdcals":
            filename = 'testcalibrated_per_spw_' + self.spw + '_' + prefix
        elif self.taskname == "semiFinalBPdcals":
            filename = 'semifinalcalibrated_per_spw_' + self.spw + '_' + self.suffix
        if filename:
            filename = os.path.join(base_path, (filename + '-%s-summary.png' % self.ms.basename))
        return filename

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix=prefix)

        bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
        delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
        plot_type = ''
        if self.taskname == "testBPdcals":
            if prefix == 'BPcal' or ((delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal'):
                plot_type = prefix
        elif self.taskname == "semiFinalBPdcals":
            plot_type = 'semifinalcalibratedcals per spw' + self.suffix

        if plot_type:
            wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp', parameters={'vis': self.ms.basename, 'type': plot_type, 'spw': self.spw})

        if not os.path.exists(figfile):
            LOG.trace('Summary plot not found. Creating new plot.')
            try:
                self.create_plot(prefix)
            except Exception as ex:
                LOG.error('Could not create plot.')
                LOG.exception(ex)
                return None

        if plot_type:
            return wrapper
        return None


class PerAntennaChart(Chart):
    def __init__(self, context, result, suffix='', taskname=None, plottype=None, perSpwChart=False):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.taskname = taskname
        self.suffix = suffix
        self.plottype = plottype
        self.perSpwChart = perSpwChart
        self.json = {}
        plottext = ""
        if self.plottype == "delay":
            plottext = "testdelays" if self.taskname == "testBPdcals" else "delays" if self.taskname == "semiFinalBPdcals" else ''
        elif self.plottype == "ampgain":
            plottext = "ampgain" if self.taskname == "testBPdcals" else ''
        elif self.plottype == "phasegain":
            plottext = "phasegain" if self.taskname == "testBPdcals" else "phasegain-" + self.suffix if self.taskname == "semiFinalBPdcals" else ''
        elif self.plottype == "bpsolamp":
            plottext = "bpsolamp" if self.taskname == "testBPdcals" else "bpsolamp-" + self.suffix if self.taskname == "semiFinalBPdcals" else ''
        elif self.plottype == "bpsolphase":
            plottext = "bpsolphase" if self.taskname == "testBPdcals" else "bpsolphase-" + self.suffix if self.taskname == "semiFinalBPdcals" else ''
        elif self.plottype == "bpsolamp_perspw":
            plottext = "bpsolamp" if self.taskname == "testBPdcals" else "bpsolamp_perspw-" + self.suffix if self.taskname == "semiFinalBPdcals" else ''
        elif self.plottype == "bpsolphase_perspw":
            plottext = "bpsolphase" if self.taskname == "testBPdcals" else "bpsolphase_perspw-" + self.suffix if self.taskname == "semiFinalBPdcals" else ''
        if plottext:
            self.json_filename = os.path.join(context.report_dir,
                                            f'stage{result.stage_number}',
                                            f'{plottext}-{self.ms}.json')

    def plot(self):
        nplots = len(self.ms.antennas)
        plots = []
        result_table = None
        times = []
        xconnector = ''
        plot_title = ''
        xaxis = ''
        yaxis = ''
        filenametext = ''
        type = None
        plot_params = {
            'vis': '',
            'xaxis': 'time',
            'yaxis': 'phase',
            'field': '',
            'antenna': '',
            'spw': '',
            'timerange': '',
            'coloraxis': '',
            'plotrange': [],
            'symbolshape': 'circle',
            'title': '',
            'titlefont': 8,
            'xaxisfont': 7,
            'yaxisfont': 7,
            'showgui': False,
            'plotfile': '',
            'xconnector': 'line'
        }

        LOG.info("Plotting {!s} {!s}".format(self.taskname, self.plottype))

        if self.plottype == "delay":
            filenametext = "testdelay" if self.taskname == "testBPdcals" else "delay" if self.taskname == "semiFinalBPdcals" else ''
            type = "Test Delay" if self.taskname == "testBPdcals" else 'delay' + self.suffix if self.taskname == "semiFinalBPdcals" else None
            result_table = self.result.ktypecaltable
            xconnector = 'step'
            xaxis = 'Frequency'
            yaxis = 'Delay'
        elif self.plottype == "ampgain":
            filenametext = "testBPdinitialgainamp" if self.taskname == "testBPdcals" else ''
            type = "Amp Gain" if self.taskname == "testBPdcals" else None
            result_table = self.result.bpdgain_touse
            xconnector = 'line'
            xaxis = 'Time'
            yaxis = 'Amp'
        elif self.plottype == "phasegain":
            result_table = self.result.bpdgain_touse
            filenametext = "testBPdinitialgainphase" if self.taskname == "testBPdcals" else "BPinitialgainphase" if self.taskname == "semiFinalBPdcals" else ''
            xconnector = 'line'
            xaxis = 'Time'
            yaxis = 'phase'
            plot_params['symbolshape'] = 'circle'
            type = "Phase Gain" if self.taskname == "testBPdcals" else 'phasegain' + self.suffix if self.taskname == "semiFinalBPdcals" else None
        elif self.plottype == "bpsolamp":
            result_table = self.result.bpcaltable
            filenametext = "testBPcal_amp" if self.taskname == "testBPdcals" else "BPcal_amp" if self.taskname == "semiFinalBPdcals" else ''
            xconnector = 'step'
            xaxis = 'Frequency'
            yaxis = 'Amp'
            plot_params['symbolshape'] = 'circle'
            type = "Bandpass Amp Solution" if self.taskname == "testBPdcals" else 'bpsolamp' + self.suffix if self.taskname == "semiFinalBPdcals" else None
        elif self.plottype == "bpsolphase":
            result_table = self.result.bpcaltable
            filenametext = "testBPcal_phase" if self.taskname == "testBPdcals" else "BPcal_phase" if self.taskname == "semiFinalBPdcals" else ''
            xconnector = 'step'
            xaxis = 'Frequency'
            yaxis = 'Phase'
            plot_params['symbolshape'] = 'circle'
            type = "Bandpass Phase Solution" if self.taskname == "testBPdcals" else 'bpsolphase' + self.suffix if self.taskname == "semiFinalBPdcals" else None
        elif self.plottype == "bpsolamp_perspw":
            result_table = self.result.bpcaltable
            filenametext = "testBPcal_amp" if self.taskname == "testBPdcals" else "BPcal_amp" if self.taskname == "semiFinalBPdcals" else ''
            xconnector = 'step'
            xaxis = 'Frequency'
            yaxis = 'Amp'
            plot_params['symbolshape'] = 'circle'
            type = "Bandpass Amp Solution" if self.taskname == "testBPdcals" else 'bpsolamp' + self.suffix if self.taskname == "semiFinalBPdcals" else None
        elif self.plottype == "bpsolphase_perspw":
            result_table = self.result.bpcaltable
            filenametext = "testBPcal_phase" if self.taskname == "testBPdcals" else "BPcal_phase" if self.taskname == "semiFinalBPdcals" else ''
            xconnector = 'step'
            xaxis = 'Frequency'
            yaxis = 'Phase'
            plot_params['symbolshape'] = 'circle'
            type = "Bandpass Phase Solution" if self.taskname == "testBPdcals" else 'bpsolphase' + self.suffix if self.taskname == "semiFinalBPdcals" else None

        plot_params['xaxis'] = xaxis
        plot_params['yaxis'] = yaxis
        plot_params['xconnector'] = xconnector

        for bandname, tabitem in result_table.items():
            with casa_tools.TableReader(tabitem) as tb:
                times.extend(tb.getcol('TIME'))
        mintime = np.min(times)
        maxtime = np.max(times)
        plotrange = []
        spws = []
        spw2band = []
        if self.perSpwChart:
            spws = self.ms.get_spectral_windows(science_windows_only=True)
            spw2band = self.ms.get_vla_spw2band()
        else:
            # adding a place holder if perSpwChart is false
            # This is just to ensure that loop executes atleast once if
            # perSpwChart is false
            spws.append(-1)
        for spw in spws:
            if not self.perSpwChart or spw.specline_window:
                for bandname, tabitem in result_table.items():
                    if self.perSpwChart and spw2band[spw.id] != bandname:
                        continue
                    if self.plottype == "bpsolamp" or self.plottype == "bpsolphase" or self.plottype == "bpsolamp_perspw" or self.plottype == "bpsolphase_perspw":
                        maxmaxphase, maxmaxamp = self.get_maxphase_maxamp(self.result.bpdgain_touse[bandname], tabitem)
                        ampplotmax = maxmaxamp
                        phaseplotmax = maxmaxphase

                    if self.plottype == "delay":
                        plot_title_prefix = 'K table: {!s}'.format(tabitem)
                    elif self.plottype == "ampgain":
                        with casa_tools.TableReader(tabitem) as tb:
                            cpar = tb.getcol('CPARAM')
                            flgs = tb.getcol('FLAG')
                        amps = np.abs(cpar)
                        good = np.logical_not(flgs)
                        maxamp = np.max(amps[good])
                        plotmax = maxamp
                        plotrange = [mintime, maxtime, 0.0, plotmax]
                        plot_title_prefix = 'G table: {!s}'.format(tabitem)
                    elif self.plottype == "phasegain":
                        plot_title_prefix = 'G table: {!s}'.format(tabitem)
                        plotrange = [mintime, maxtime, -180, 180]
                    elif self.plottype == "bpsolamp":
                        plot_title_prefix = 'B table: {!s}'.format(tabitem)
                        plotrange = [0, 0, 0, ampplotmax]
                    elif self.plottype == "bpsolphase":
                        plot_title_prefix = 'B table: {!s}'.format(tabitem)
                        plotrange = [0, 0, -phaseplotmax, phaseplotmax]
                    elif self.plottype == "bpsolamp_perspw":
                        plot_title_prefix = 'B table: {!s}'.format(tabitem)
                        plotrange = [0, 0, 0, ampplotmax]
                    elif self.plottype == "bpsolphase_perspw":
                        plot_title_prefix = 'B table: {!s}'.format(tabitem)
                        plotrange = [0, 0, -phaseplotmax, phaseplotmax]

                    plot_params['plotrange'] = plotrange

                    for ii in range(nplots):
                        if self.taskname == "testBPdcals":
                            filename = filenametext + str(ii) + '_' + bandname
                        elif self.taskname == "semiFinalBPdcals":
                            filename = filenametext + str(ii) + '_' + self.suffix + '_' + bandname
                        if self.perSpwChart:
                            filename = "{!s}_{!s}.png".format(filename, str(spw.id))
                            plot_params['spw'] = str(spw.id)
                        else:
                            filename = "{!s}.png".format(filename)

                        antPlot = str(ii)
                        # Get antenna name
                        domain_antennas = self.ms.get_antenna(antPlot)
                        idents = [a.name if a.name else a.id for a in domain_antennas]
                        antName = ','.join(idents)

                        stage = 'stage%s' % self.result.stage_number
                        stage_dir = os.path.join(self.context.report_dir, stage)
                        # construct the relative filename, eg. 'stageX/testdelay0.png'

                        figfile = os.path.join(stage_dir, filename)

                        if not os.path.exists(figfile):
                            try:
                                LOG.debug("Plotting {!s} {!s} {!s}".format(self.taskname, self.plottype, antName))
                                plot_title = "{!s} Antenna: {!s} Band: {!s}".format(plot_title_prefix, antName, bandname)
                                plot_params['vis'] = tabitem
                                plot_params['antenna'] = antPlot
                                plot_params['title'] = plot_title
                                plot_params['plotfile'] = figfile

                                job = casa_tasks.plotms(**plot_params)

                                job.execute()

                            except Exception as ex:
                                LOG.warning("Unable to plot " + filename)
                        else:
                            LOG.debug('Using existing ' + filename + ' plot.')

                        try:
                            plot = logger.Plot(figfile, x_axis=xaxis, y_axis=yaxis, field='',
                                            parameters={'spw': plot_params['spw'],
                                                        'pol': '',
                                                        'ant': antName,
                                                        'bandname': bandname,
                                                        'type': type,
                                                        'file': os.path.basename(figfile)})
                            plots.append(plot)
                        except Exception as ex:
                            LOG.warning("Unable to add plot to stack ", ex)
                            plots.append(None)

        return [p for p in plots if p is not None]
