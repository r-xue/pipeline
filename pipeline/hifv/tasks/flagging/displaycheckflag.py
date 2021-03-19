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
    def __init__(self, context, result, suffix='', plotms_args={}):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.suffix = suffix
        self.plotms_args = plotms_args
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

        plotms_args = {'vis': self.ms.name,
                       'xaxis': 'freq', 'yaxis': 'amp',
                       'xdatacolumn': '', 'ydatacolumn': 'corrected',
                       'selectdata': True, 'field': '', 'scan': '', 'correlation': corrstring,
                       'averagedata': True, 'avgtime': '1e8', 'avgscan': False,
                       'transform': False, 'extendflag': False,
                       'coloraxis': 'antenna2',
                       'plotfile': figfile, 'overwrite': True, 'clearplots': True, 'showgui': False}

        plotms_args.update(self.plotms_args)

        if self.result.inputs['checkflagmode'] == 'bpd' or self.result.inputs['checkflagmode'] == 'bpd-vlass':
            bandpass_field_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_field_select_string
            bandpass_scan_select_string = self.context.evla['msinfo'][self.ms.name].bandpass_scan_select_string
            delay_scan_select_string = self.context.evla['msinfo'][self.ms.name].delay_scan_select_string
            if prefix == 'BPcal':
                plotms_args.update(field=bandpass_field_select_string,
                                   scan=bandpass_scan_select_string, avgscan=True)
                job = casa_tasks.plotms(**plotms_args)
            if (delay_scan_select_string != bandpass_scan_select_string) and prefix == 'delaycal':
                plotms_args.update(scan=delay_scan_select_string, avgscan=True)
                job = casa_tasks.plotms(**plotms_args)

        if self.result.inputs['checkflagmode'] == 'allcals' or self.result.inputs['checkflagmode'] == 'allcals-vlass':
            calibrator_scan_select_string = self.context.evla['msinfo'][self.ms.name].calibrator_scan_select_string
            plotms_args.update(scan=calibrator_scan_select_string, avgscan=False)
            job = casa_tasks.plotms(**plotms_args)

        if self.result.inputs['checkflagmode'] == 'vlass-imaging':
            plotms_args.update(ydatacolumn='data', avgscan=False)
            job = casa_tasks.plotms(**plotms_args)

        job.execute(dry_run=False)

    def get_figfile(self, prefix):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['checkflag', prefix,
                                                   self.ms.basename, 'summary', self.suffix])))+'.png'
        return os.path.join(stage_dir, fig_basename)

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

        LOG.info('Creating new checkflag percentage map plot')
        try:

            fig_title = self.ms.basename

            fig, ax = plt.subplots()
            fields_name, fields_ra, fields_dec = self._fields_to_ra_dec()

            flags_by_field = self.result.summaries[-1]['field']
            flagpct_per_field = np.zeros((len(flags_by_field), 3))

            for idx, (field_name, field_flag) in enumerate(flags_by_field.items()):
                field_id = fields_name.index(field_name)
                flagpct_per_field[idx, 0] = 100.0 * field_flag['flagged']/field_flag['total']
                flagpct_per_field[idx, 1] = np.degrees(float(fields_ra[field_id]))
                flagpct_per_field[idx, 2] = np.degrees(float(fields_dec[field_id]))

            self._plot_grid(flagpct_per_field[:, 1], flagpct_per_field[:, 2], flagpct_per_field[:, 0])
            ax.set_xlabel('R.A. [deg]')
            ax.set_ylabel('Dec. [deg]')

            fig.savefig(self.figfile)
            plt.close(fig)

        except:
            LOG.warn('Could not create the flagging percentage map plot')
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

        return list(field_names), phase_dir[0, :], phase_dir[1, :]

    def _plot_grid(self, x, y, z, nx=100, ny=100):

        x[x < 0] = x[x < 0] + 360.

        xi = np.linspace(np.max(x), np.min(x), nx)
        yi = np.linspace(np.min(y), np.max(y), ny)

        zi = griddata((x, y), z, (xi[None, :], yi[:, None]), method='cubic')
        with np.errstate(invalid='ignore'):
            zi[zi > 100] = 100.

        dx = (np.max(x)-np.min(x))*0.1
        dy = (np.max(y)-np.min(y))*0.1
        plt.imshow(zi, origin='lower', extent=[np.max(x)+dx, np.min(x)-dx, np.min(y)-dy, np.max(y)+dy], aspect='equal')
        plt.plot(x, y, 'k+')

        plt.gca().get_xaxis().get_major_formatter().set_useOffset(False)
        cba = plt.colorbar()
        cba.set_label('percent flagged [%]')
