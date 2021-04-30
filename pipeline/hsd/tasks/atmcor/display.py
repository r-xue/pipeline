import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)


class PlotmsRealVsFreqPlotter(object):
    def __init__(self, ms, atmvis, atmtype, datacolumn='data', output_dir='.'):
        self.ms = ms
        self.atmvis = atmvis.rstrip('/')
        self.atmtype = atmtype
        self.spw = ''
        self.antenna = ''
        self.field = ''
        self.clean_field_name = ''
        self.datacolumn = datacolumn
        self.output_dir = output_dir

    @property
    def name(self):
        return self.ms.name

    @property
    def basename(self):
        return self.ms.basename

    @property
    def original_field_name(self):
        return self.field.strip('"')

    def set_spw(self, spw=''):
        self.spw = spw

    def set_antenna(self, antenna=''):
        if isinstance(antenna, int):
            self.antenna = self.ms.antennas[antenna].name
        else:
            self.antenna = antenna

    def set_field(self, field=''):
        if isinstance(field, int):
            self.field = self.ms.fields[field].name
            self.clean_field_name = self.ms.fields[field].clean_name
        else:
            self.field = field
            self.clean_field_name = field

    def get_antenna_selection(self):
        if len(self.antenna) == 0:
            antenna = self.antenna
        else:
            antenna = '{}&&&'.format(self.antenna)
        return antenna

    def get_color_axis(self):
        if len(self.antenna) == 0:
            coloraxis = 'antenna1'
        else:
            coloraxis = 'corr'
        return coloraxis

    def get_title(self):
        title = '\n'.join(
            [
                'ATM Corrected Real vs Frequency',
                '{} ATMType {} {} Spw {} Antenna {}'.format(
                    self.basename,
                    self.atmtype,
                    ('all' if self.field == '' else self.original_field_name),
                    ('all' if self.spw == '' else self.spw),
                    ('all' if self.antenna == '' else self.antenna),
                ),
                'Coloraxis {}'.format(self.get_color_axis())
            ]
        )
        return title

    def get_plotfile_name(self):
        plotfile = '{}-atmtype_{}-{}-spw_{}-antenna_{}-atmcor-TARGET-real_vs_freq.png'.format(
            self.basename,
            self.atmtype,
            ('all' if self.field == '' else self.clean_field_name),
            ('all' if self.spw == '' else self.spw),
            ('all' if self.antenna == '' else self.antenna),
        )
        return os.path.join(self.output_dir, plotfile)

    def get_plot(self, plotfile):
        parameters = {
            'vis': self.name,
            'ant': self.antenna,
            'spw': self.spw,
            'field': self.original_field_name,
        }
        plot = logger.Plot(
            plotfile,
            x_axis='Frequency',
            y_axis='Real',
            parameters=parameters,
            command='hsd_atmcor'
        )
        return plot

    def plot(self):
        title = self.get_title()
        plotfile = self.get_plotfile_name()
        antenna = self.get_antenna_selection()
        coloraxis = self.get_color_axis()
        task_args = {
            'vis': self.atmvis,
            'xaxis': 'freq',
            'yaxis': 'real',
            'ydatacolumn': self.datacolumn,
            'spw': self.spw,
            'antenna': antenna,
            'field': self.field,
            'intent': 'OBSERVE_TARGET#ON_SOURCE',
            'coloraxis': coloraxis,
            'showgui': False,
            'title': title,
            'plotfile': plotfile,
            'showlegend': True,
            'averagedata': True,
            'avgtime': '1e8',
            'avgscan': True,
            'showatm': True,
        }
        task = casa_tasks.plotms(**task_args)
        if os.path.exists(plotfile):
            LOG.debug('Returning existing plot')
        else:
            task.execute()

        plot = self.get_plot(plotfile)
        return plot
