import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks

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
    def __init__(self, context, result, suffix='', taskname=None):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.taskname = taskname
        self.suffix = suffix

        self.json = {}

        delaytext = "testdelays" if self.taskname == "testBPdcals" else "delays" if self.taskname == "semiFinalBPdcals" else None
        """
        self.json_filename = os.path.join(context.report_dir,
                                          'stage%s' % result.stage_number,
                                          '%s-%s.json' % delaytext, self.ms)
        """
        self.json_filename = os.path.join(context.report_dir,
                                          f'stage{result.stage_number}',
                                          f'{delaytext}-{self.ms}.json')

    def plot(self):

        nplots = len(self.ms.antennas)
        plots = []

        LOG.info("Plotting {!s} delays".format(self.taskname))
        # Note: ideally delaytext variable defined in __init__ can be used here
        # but the text there is "textdelays" and here it is "testdelay"
        # Check if both text can be made consistent.
        delaytext = "testdelay" if self.taskname == "testBPdcals" else "delay" if self.taskname == "semiFinalBPdcals" else None
        type = "Test Delay" if self.taskname == "testBPdcals" else 'delay' + self.suffix if self.taskname == "semiFinalBPdcals" else None
        if delaytext is None or type is None:
            return []
        for bandname, ktypecaltablename in self.result.ktypecaltable.items():
            for ii in range(nplots):

                filename = delaytext + str(ii) + '_' + bandname + '.png'
                antPlot = str(ii)

                stage = 'stage%s' % self.result.stage_number
                stage_dir = os.path.join(self.context.report_dir, stage)
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

                        LOG.debug("Plotting {!s} delays {!s}".format(self.taskname, antName))

                        job = casa_tasks.plotms(vis=ktypecaltablename, xaxis='freq', yaxis='amp', field='',
                                         antenna=antPlot, spw='', timerange='', plotrange=[], coloraxis='',
                                         title='K table: {!s}   Antenna: {!s} Band: {!s}'.format(ktypecaltablename, antName, bandname),
                                         titlefont=8, xaxisfont=7, yaxisfont=7, showgui=False, plotfile=figfile,
                                         xconnector='step')

                        job.execute()

                    except Exception as ex:
                        LOG.warning("Unable to plot " + filename)
                else:
                    LOG.debug('Using existing ' + filename + ' plot.')

                try:
                    plot = logger.Plot(figfile, x_axis='Frequency', y_axis='Delay', field='',
                                       parameters={'spw': '',
                                                   'pol': '',
                                                   'ant': antName,
                                                   'bandname': bandname,
                                                   'type': type,
                                                   'file': os.path.basename(figfile)})
                    plots.append(plot)
                except Exception as ex:
                    LOG.warning("Unable to add plot to stack")
                    plots.append(None)

        return [p for p in plots if p is not None]
