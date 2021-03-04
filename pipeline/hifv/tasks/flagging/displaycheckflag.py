import os

import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import griddata

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools

LOG = infrastructure.get_logger(__name__)


class checkflagSummaryChart(object):
    def __init__(self, context, result, suffix=''):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        # self.caltable = result.final[0].gaintable

    def plot(self):
        # Default
        plots = [None]

        if self.result.inputs['checkflagmode'] == 'bpd' or self.result.inputs['checkflagmode'] == 'bpd-vlass':
            plots = [self.get_plot_wrapper('BPcal'), self.get_plot_wrapper('delaycal')]
        if self.result.inputs['checkflagmode'] == 'allcals' or self.result.inputs['checkflagmode'] == 'allcals-vlass':
            plots = [self.get_plot_wrapper('allcals')]
        if self.result.inputs['checkflagmode'] == 'vlass-imaging':
            plots = [self.get_plot_wrapper('targets-vlass')]            
        return [p for p in plots if p is not None]

    def create_plot(self, prefix):
        figfile = self.get_figfile(prefix)

        corrstring = self.ms.get_vla_corrstring()

        if self.result.inputs['checkflagmode'] == 'bpd' or self.result.inputs['checkflagmode'] == 'bpd-vlass':
            bandpass_field_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string
            bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
            delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
            if prefix == 'BPcal':
                job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                        selectdata=True, field=bandpass_field_select_string,
                                        scan=bandpass_scan_select_string, correlation=corrstring, averagedata=True,
                                        avgtime='1e8', avgscan=True, transform=False, extendflag=False, iteraxis='',
                                        coloraxis='antenna2', plotrange=[], title='', xlabel='', ylabel='',
                                        showmajorgrid=False, showminorgrid=False, plotfile=figfile,
                                        overwrite=True, clearplots=True, showgui=False)

            if (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
                job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                        selectdata=True, scan=delay_scan_select_string, correlation=corrstring,
                                        averagedata=True, avgtime='1e8', avgscan=True, transform=False,
                                        extendflag=False, iteraxis='', coloraxis='antenna2', plotrange=[], title='',
                                        xlabel='', ylabel='', showmajorgrid=False, showminorgrid=False,
                                        plotfile=figfile, overwrite=True, clearplots=True, showgui=False)

            job.execute(dry_run=False)

        if self.result.inputs['checkflagmode'] == 'allcals' or self.result.inputs['checkflagmode'] == 'allcals-vlass':
            calibrator_scan_select_string = self.context.evla['msinfo'][self.ms.name].calibrator_scan_select_string

            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='corrected',
                                    selectdata=True, scan=calibrator_scan_select_string, correlation=corrstring,
                                    averagedata=True, avgtime='1e8', avgscan=False, transform=False, extendflag=False,
                                    iteraxis='', coloraxis='antenna2', plotrange=[], title='', xlabel='', ylabel='',
                                    showmajorgrid=False, showminorgrid=False, plotfile=figfile, overwrite=True,
                                    clearplots=True, showgui=False)

            job.execute(dry_run=False)

        if self.result.inputs['checkflagmode'] == 'vlass-imaging':
            job = casa_tasks.plotms(vis=self.ms.name, xaxis='freq', yaxis='amp', ydatacolumn='data',
                                    selectdata=True, scan='', correlation=corrstring,
                                    averagedata=True, avgtime='1e8', avgscan=False, transform=False, extendflag=False,
                                    iteraxis='', coloraxis='antenna2', plotrange=[], title='', xlabel='', ylabel='',
                                    showmajorgrid=False, showminorgrid=False, plotfile=figfile, overwrite=True,
                                    clearplots=True, showgui=False)

            job.execute(dry_run=False)

    def get_figfile(self, prefix):
        fig_basename = '-'.join(list(filter(None, ['checkflag', prefix,
                                                   self.ms.basename, 'summary', self.suffix])))+'.png'
        return os.path.join(self.context.report_dir,
                            'stage{}'.format(self.result.stage_number),
                            fig_basename)

    def get_plot_wrapper(self, prefix):
        figfile = self.get_figfile(prefix)

        if prefix == 'delaycal':
            bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
            delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
            plot_delaycal = bandpass_scan_select_string != delay_scan_select_string
        else:
            plot_delaycal = False

        if (prefix == 'BPcal' or plot_delaycal or prefix == 'allcals' or prefix == 'allcals-vlass' or prefix == 'targets-vlass'):
            wrapper = logger.Plot(figfile, x_axis='freq', y_axis='amp',
                                  parameters={'vis': self.ms.basename,
                                              'type': prefix,
                                              'version': self.suffix,
                                              'spw': ''})

            if not os.path.exists(figfile):
                LOG.trace('Checkflag summary plot not found. Creating new plot.')
                try:
                    self.create_plot(prefix)
                except Exception as ex:
                    LOG.error('Could not create ' + prefix + ' plot.')
                    LOG.exception(ex)
                    return None

            return wrapper

        return None


class checkflagPercentageMap(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing checkflag percentage map plot')
            return self._get_plot_object()

        LOG.debug('Creating new checkflag percentage map plot')
        try:

            fig_title = self.ms.basename

            fig, ax = plt.subplots()
            fields_name, fields_ra, fields_dec = self._fields_to_ra_dec()

            fieldflags = np.zeros((len(fields_name), 3))
            flags_by_field = self.result.summaries[-1]['field']

            for idx in range(len(fields_name)):
                field_name = fields_name[idx]
                fieldflags[idx, 0] = 100.0 * flags_by_field[field_name]['flagged']/flags_by_field[field_name]['total']
                fieldflags[idx, 1] = np.degrees(float(fields_ra[idx]))
                fieldflags[idx, 2] = np.degrees(float(fields_dec[idx]))

            self._plot_grid(fieldflags[:, 1], fieldflags[:, 2], fieldflags[:, 0])
            ax.set_xlabel('R.A. [deg]')
            ax.set_ylabel('Dec. [deg]')

            fig.savefig(self.figfile)

        except:
            return None

        return self._get_plot_object()

    def _get_figfile(self):
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            'checkflag-%s-percentagemap.png' % self.ms.basename)

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='R.A.',
                           y_axis='Dec.',
                           parameters={'vis': self.ms.basename})

    def _fields_to_ra_dec(self):

        with casa_tools.TableReader(self.ms.name + '/FIELD') as table:
            phase_dir = table.getcol('PHASE_DIR')
            field_names = table.getcol('NAME')

        if len(phase_dir.shape) > 2:
            phase_dir = phase_dir.squeeze()

        return field_names, phase_dir[0, :], phase_dir[1, :]

    def _plot_grid(self, x, y, z, nx=100, ny=100):

        x[x < 0] = x[x < 0] + 360.

        xi = np.linspace(np.max(x), np.min(x), nx)
        yi = np.linspace(np.min(y), np.max(y), ny)

        zi = griddata((x, y), z, (xi[None, :], yi[:, None]), method='cubic')
        zi[zi > 100] = 100.

        dx = (np.max(x)-np.min(x))*0.1
        dy = (np.max(y)-np.min(y))*0.1
        plt.imshow(zi, origin='lower', extent=[np.max(x)+dx, np.min(x)-dx, np.min(y)-dy, np.max(y)+dy], aspect='equal')
        plt.plot(x, y, 'k+')

        plt.gca().get_xaxis().get_major_formatter().set_useOffset(False)
        cba = plt.colorbar()
        cba.set_label('percent flagged [%]')
