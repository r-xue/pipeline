import os

import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.renderer.logger as logger

from astropy import units as u
from astropy.coordinates import SkyCoord

LOG = infrastructure.get_logger(__name__)


class selfcalphaseGainPerAntennaChart(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.basevis = os.path.basename(result.inputs['vis'])

        self.json = {}
        self.json_filename = os.path.join(context.report_dir, 'stage%s' % result.stage_number,
                                          'selfcalphasegain-%s.json' % self.ms)

    def plot(self):
        context = self.context
        result = self.result
        m = context.observing_run.measurement_sets[0]

        nplots = len(m.antennas)
        plots = []

        LOG.info("Plotting phase vs. time after running hifv_selfcal")

        for ii in range(nplots):

            filename = 'selfcalgainphase' + str(ii) + '.png'
            antPlot = str(ii)

            stage = 'stage%s' % result.stage_number
            stage_dir = os.path.join(context.report_dir, stage)
            # construct the relative filename, eg. 'stageX/testdelay0.png'

            figfile = os.path.join(stage_dir, filename)

            # Get antenna name
            antName = antPlot
            if antPlot != '':
                domain_antennas = self.ms.get_antenna(antPlot)
                idents = [a.name if a.name else a.id for a in domain_antennas]
                antName = ','.join(idents)

            if not os.path.exists(figfile):
                try:
                    LOG.debug("Plotting phase/SNR vs. time... {!s}".format(antName))
                    job = casa_tasks.plotms(vis=result.caltable, xaxis='time', yaxis='phase', field='',
                                            antenna=antPlot, spw='', timerange='',
                                            gridrows=2, gridcols=1, rowindex=0, colindex=0, plotindex=0,
                                            coloraxis='', plotrange=[0, 0, -180, 180], symbolshape='circle',
                                            title='G table: {!s}   Antenna: {!s}'.format(result.caltable, antName), xlabel=' ',
                                            showlegend=True,
                                            showgui=False, plotfile=figfile, clearplots=True, overwrite=True,
                                            titlefont=8, xaxisfont=7, yaxisfont=7, xconnector='line')
                    job.execute(dry_run=False)

                    job = casa_tasks.plotms(vis=result.caltable, xaxis='time', yaxis='snr', field='',
                                            antenna=antPlot, spw='', timerange='',
                                            gridrows=2, gridcols=1, rowindex=1, colindex=0, plotindex=1,
                                            coloraxis='', plotrange=[], symbolshape='circle',
                                            title=' ',
                                            showgui=False, plotfile=figfile, clearplots=False, overwrite=True,
                                            showlegend=True,
                                            titlefont=8, xaxisfont=7, yaxisfont=7, xconnector='line')
                    job.execute(dry_run=False)

                except Exception as ex:
                    LOG.warn("Unable to plot " + filename + str(ex))
            else:
                LOG.debug('Using existing ' + filename + ' plot.')

            try:
                plot = logger.Plot(figfile, x_axis='Time', y_axis='Phase', field='',
                                   parameters={'spw': '',
                                               'pol': '',
                                               'ant': antName,
                                               'type': 'selfcalphasegain',
                                               'file': os.path.basename(figfile)})
                plots.append(plot)
            except Exception as ex:
                LOG.warn("Unable to add plot to stack.  " + str(ex))
                plots.append(None)

        return [p for p in plots if p is not None]


class selfcalSolutionNumPerFieldChart(object):
    """
    likely only work for the self-cal gain table from the 'VLASS-SE' mode (see PIPE-1010), i.e.
        one solution per polarization per image-row (unique source id) for each antenna
    """

    def __init__(self, context, result):
        self.context = context
        self.caltable = result.caltable
        self.reportdir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        self.figfile = self._get_figfile()
        self.x_axis = 'No. of Gain Solutions'
        self.y_axis = 'VLASS image row '

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing selfcal solution summary plot')
            return self._get_plot_object()

        selfcal_stats = self._calstat_from_caltable()
        LOG.debug('Creating new selfcal solution summary plot')
        try:

            fig, ax = plt.subplots(figsize=(10, 8))
            row_label = []
            nsol = []
            nsol_flagged = []

            for idx, field_ids in enumerate(selfcal_stats['field']):

                row_desc = []
                row_desc.append(selfcal_stats['field_desc']['name'][field_ids[0]])
                #c = selfcal_stats['field_desc']['position'][field_ids[0]]
                #row_desc.append(c.ra.to_string(unit=u.hour, pad=True, precision=1))
                #row_desc.append(c.dec.to_string(unit=u.degree, pad=True, precision=0, alwayssign=True))

                row_label.append('\n'.join(row_desc))
                nsol.append(selfcal_stats['flag'][idx].size)
                nsol_flagged.append(np.sum(selfcal_stats['flag'][idx]))

            ypos = np.arange(len(row_label))
            ax.barh(ypos, nsol, color='lightgray', align='center')
            ax.barh(ypos, nsol_flagged, color='red', align='center')
            ax.set_yticks(ypos)
            ax.set_yticklabels(row_label, rotation=45)

            lg_colors = {'flagged': 'red', 'total': 'lightgray'}
            lg_handles = [plt.Rectangle((0, 0), 1, 1, color=lg_colors[lg_label]) for lg_label in lg_colors]
            ax.legend(lg_handles, list(lg_colors.keys()))

            #ax.set_ylabel('VLASS Image Row: 1st field name, R.A./Dec.')
            ax.set_ylabel('VLASS Image Row: 1st field name')
            ax.set_xlabel('No. of Gain Solutions')
            title = 'Number of Self-Cal Gain Solutions per Image Row'
            ax.set_title(title)

            fig.tight_layout()
            fig.savefig(self.figfile)
            plt.close(fig)

            LOG.debug('Saving new iselfcal solution summary plot to {}'.format(self.figfile))

        except:

            LOG.debug('Unable to create {}'.format(self.figfile))
            return None

        return self._get_plot_object()

    def _get_figfile(self):

        return os.path.join(self.reportdir,
                            self.caltable+'.nsols.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis=self.x_axis,
                           y_axis=self.y_axis)

    def _calstat_from_caltable(self):
        """get selfcal solution statistics from caltable
        """
        with casa_tools.TableReader(self.caltable) as table:
            time = table.getcol('TIME')  # (n_ant x n_row)
            field_id = table.getcol('FIELD_ID')
            flag = table.getcol('FLAG')
            ant_id = table.getcol('ANTENNA1')

        with casa_tools.TableReader(self.caltable+'/FIELD') as table:
            field_name = table.getcol('NAME')
            phasedir = table.getcol('PHASE_DIR')

        with casa_tools.TableReader(self.caltable+'/ANTENNA') as table:
            ant_name = table.getcol('NAME')

        field_unique, field_unique_idx1st = np.unique(field_id, return_index=True)

        field_list = np.split(field_id, field_unique_idx1st[1:], axis=-1)
        flag_list = np.split(flag, field_unique_idx1st[1:], axis=-1)
        ant_list = np.split(ant_id, field_unique_idx1st[1:], axis=-1)
        time_list = np.split(time, field_unique_idx1st[1:], axis=-1)

        if len(phasedir.shape) > 2:
            phasedir = phasedir.squeeze()

        selfcal_stats = {'field': field_list,
                         'flag': flag_list,
                         'antenna': ant_list,
                         'time': time_list,
                         'field_unique': field_unique,
                         'field_desc': {'name': field_name, 'position': SkyCoord(ra=phasedir[0, :]*u.rad, dec=phasedir[1, :]*u.rad)},
                         'ant_desc': {'name': ant_name}}

        return selfcal_stats
