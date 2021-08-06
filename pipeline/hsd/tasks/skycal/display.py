import os
import matplotlib.pyplot as plt
import numpy
import traceback

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import common as common
from pipeline.h.tasks.common.displays import bandpass as bandpass
from ..common import display as sd_display
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.plotstyle import casa5style_plot

LOG = logging.get_logger(__name__)


class SingleDishSkyCalDisplayBase(object):
    def init_with_field(self, context, result, field):
        vis = self._vis
        ms = context.observing_run.get_ms(vis)
        num_fields = len(ms.fields)
        if field.isdigit() and int(field) < num_fields:
            self.field_id = int(field)
            self.field_name = ms.get_fields(self.field_id)[0].clean_name
        else:
            self.field_name = field
            fields = ms.get_fields(name=field)
            assert len(fields) == 1
            self.field_id = fields[0].id
        LOG.debug('field: ID %s Name \'%s\''%(self.field_id, self.field_name))
        old_prefix = self._figroot.replace('.png', '')
        self._figroot = self._figroot.replace('.png', '-%s.png' % (self.field_name))
        new_prefix = self._figroot.replace('.png', '')

        self._update_figfile(old_prefix, new_prefix)

        if 'field' not in self._kwargs:
            self._kwargs['field'] = self.field_id

    def add_field_identifier(self, plots):
        for plot in plots:
            if 'field' not in plot.parameters:
                plot.parameters['field'] = self.field_name

    def _update_figfile(self):
        raise NotImplementedError()


class SingleDishSkyCalAmpVsFreqSummaryChart(common.PlotbandpassDetailBase, SingleDishSkyCalDisplayBase):
    def __init__(self, context, result, field):
        super(SingleDishSkyCalAmpVsFreqSummaryChart, self).__init__(context, result,
                                                                    'freq', 'amp',
                                                                    showatm=True,
                                                                    overlay='antenna',
                                                                    solutionTimeThresholdSeconds=3600.)

        # self._figfile structure: {spw_id: {antenna_id: filename}}
        self.spw_ids = list(self._figfile.keys())
        # take any value from the dictionary
        self._figfile = dict((spw_id, list(self._figfile[spw_id].values())[0]) for spw_id in self.spw_ids)
        self.init_with_field(context, result, field)

    def plot(self):
        missing = [spw_id
                   for spw_id in self.spw_ids
                   if not os.path.exists(self._figfile[spw_id])]
        if missing:
            LOG.trace('Executing new plotbandpass job for missing figures')
            for spw_id in missing:
                # PIPE-110: show image sideband for DSB receivers.
                showimage = self._rxmap.get(spw_id, "") == "DSB"
                try:
                    task = self.create_task(spw_id, '', showimage=showimage)
                    task.execute(dry_run=False)
                except Exception as ex:
                    LOG.error('Could not create plotbandpass summary plots')
                    LOG.exception(ex)

        wrappers = []
        for spw_id, figfile in self._figfile.items():
            # PIPE-110: show image sideband for DSB receivers.
            showimage = self._rxmap.get(spw_id, "") == "DSB"
            if os.path.exists(figfile):
                task = self.create_task(spw_id, '', showimage=showimage)
                wrapper = logger.Plot(figfile,
                                      x_axis=self._xaxis,
                                      y_axis=self._yaxis,
                                      parameters={'vis': self._vis_basename,
                                                  'spw': spw_id,
                                                  'field': self.field_name},
                                      command=str(task))
                wrappers.append(wrapper)
            else:
                LOG.trace('No plotbandpass summary plot found for spw '
                          '%s' % spw_id)

        return wrappers

    def _update_figfile(self, old_prefix, new_prefix):
        for spw_id, figfile in self._figfile.items():
            self._figfile[spw_id] = figfile.replace(old_prefix, new_prefix)
            spw_indicator = 'spw{}'.format(spw_id)
            pieces = self._figfile[spw_id].split('.')
            try:
                spw_index = pieces.index(spw_indicator)
            except:
                spw_index = -3
            # remove antenna name from the filename
            pieces.pop(spw_index - 1)
            self._figfile[spw_id] = '.'.join(pieces)


class SingleDishSkyCalAmpVsFreqDetailChart(bandpass.BandpassDetailChart, SingleDishSkyCalDisplayBase):
    def __init__(self, context, result, field):
        super(SingleDishSkyCalAmpVsFreqDetailChart, self).__init__(
            context, result, xaxis='freq', yaxis='amp', showatm=True, overlay='time')

        self.init_with_field(context, result, field)

    def plot(self):
        wrappers = super(SingleDishSkyCalAmpVsFreqDetailChart, self).plot()

        self.add_field_identifier(wrappers)

        return wrappers

    def _update_figfile(self, old_prefix, new_prefix):
        for spw_id in self._figfile:
            for antenna_id, figfile in self._figfile[spw_id].items():
                new_figfile = figfile.replace(old_prefix, new_prefix)
                self._figfile[spw_id][antenna_id] = new_figfile


class SingleDishPlotmsLeaf(object):
    """
    Class to execute plotms and return a plot wrapper. Task arguments for plotms
    is customized for single dish usecase.
    """
    def __init__(self, context, result, calapp, xaxis, yaxis, spw='', ant='', coloraxis='', **kwargs):
        LOG.debug('__init__(caltable={caltable}, spw={spw}, ant={ant})'.format(caltable=calapp.gaintable, spw=spw,
                                                                               ant=ant))
        self.xaxis = xaxis
        self.yaxis = yaxis
        self.field = calapp.gainfield
        self.caltable = calapp.gaintable
        self.vis = calapp.vis
        self.spw = str(spw)
        self.antenna = str(ant)
        self.coloraxis = coloraxis

        ms = context.observing_run.get_ms(self.vis)

        num_fields = len(ms.fields)
        if self.field.isdigit() and int(self.field) < num_fields:
            self.field_id = int(self.field)
            self.field_name = ms.get_fields(self.field_id)[0].clean_name
        else:
            self.field_name = self.field
            fields = ms.get_fields(name=self.field)
            assert len(fields) == 1
            self.field_id = fields[0].id

        LOG.debug('field: ID %s Name \'%s\'' % (self.field_id, self.field_name))

        self.antmap = dict((a.id, a.name) for a in ms.antennas)
        if len(self.antenna) == 0:
            self.antenna_selection = 'summary'
        else:
            self.antenna_selection = list(self.antmap.values())[int(self.antenna)]
        LOG.info('antenna: ID %s Name \'%s\'' % (self.antenna, self.antenna_selection))
#        self.antenna_selection = '*&&&'

        self._figroot = os.path.join(context.report_dir,
                                     'stage%s' % result.stage_number)

    def plot(self):

        prefix = '{caltable}-{y}_vs_{x}-{field}-{ant}-spw{spw}'.format(
            caltable=os.path.basename(self.caltable), y=self.yaxis, x=self.xaxis, field=self.field_name,
            ant=self.antenna_selection, spw=self.spw)

        title = '{caltable} \nField "{field}" Antenna {ant} Spw {spw} \ncoloraxis={caxis}'.format(
            caltable=os.path.basename(self.caltable), field=self.field_name, ant=self.antenna_selection, spw=self.spw,
            caxis=self.coloraxis)

        figfile = os.path.join(self._figroot, '{prefix}.png'.format(prefix=prefix))

        task = self._create_task(title, figfile)
        try:
            if os.path.exists(figfile):
                LOG.debug('Returning existing plot')
            else:
                task.execute()

            plot_objects = [self._get_plot_object(figfile, task)]
        except Exception as e:
            LOG.error(str(e))
            LOG.debug(traceback.format_exc())
            LOG.error('Failed to generate plot "{}"'.format(figfile))
            plot_objects = []

        return plot_objects

    def _create_task(self, title, figfile):
        task_args = {'vis': self.caltable,
                     'xaxis': self.xaxis,
                     'yaxis': self.yaxis,
                     'plotfile': figfile,
                     'coloraxis': self.coloraxis,
                     'showgui': False,
                     'field': self.field,
                     'spw': self.spw,
                     'antenna': self.antenna,
                     'title': title,
                     'showlegend': True,
                     'averagedata': True,
                     'avgchannel': '1e8'}

        return casa_tasks.plotms(**task_args)

    def _get_plot_object(self, figfile, task):
        parameters = {'vis': os.path.basename(self.vis),
                      'ant': self.antenna_selection,
                      'spw': self.spw,
                      'field': self.field_name}

        return logger.Plot(figfile,
                           x_axis='Time',
                           y_axis='Amplitude',
                           parameters=parameters,
                           command=str(task))


class SingleDishPlotmsAntComposite(common.AntComposite):
    leaf_class = SingleDishPlotmsLeaf


class SingleDishPlotmsSpwComposite(common.SpwComposite):
    leaf_class = SingleDishPlotmsLeaf


class SingleDishPlotmsAntSpwComposite(common.AntSpwComposite):
    leaf_class = SingleDishPlotmsSpwComposite


class SingleDishSkyCalAmpVsTimeSummaryChart(SingleDishPlotmsSpwComposite):
    def __init__(self, context, result, calapp):
        super(SingleDishSkyCalAmpVsTimeSummaryChart, self).__init__(context, result, calapp,
                                                                    xaxis='time', yaxis='amp',
                                                                    coloraxis='ant1')


class SingleDishSkyCalAmpVsTimeDetailChart(SingleDishPlotmsAntSpwComposite):
    def __init__(self, context, result, calapp):
        super(SingleDishSkyCalAmpVsTimeDetailChart, self).__init__(context, result, calapp,
                                                                   xaxis='time', yaxis='amp',
                                                                   coloraxis='corr')


class SingleDishSkyCalIntervalVsTimeDisplay(common.PlotbandpassDetailBase, SingleDishSkyCalDisplayBase):
    """
    Class to execute pyplot and return a plot (figure) of Interval vs. Time.
    If figtype='summary', the first spw is used, while all spw are used if figtype='detail'.
    """
    def __init__(self, context, result, calapp, figtype=''):
        self.context = context
        self.result = result
        self.calapp = calapp
        self.figtype = str(figtype)
        LOG.info('figtype = {0}'.format(self.figtype))

    @casa5style_plot
    def plot(self):
        context = self.context
        result = self.result
        calapp = self.calapp
        figtype = self.figtype
        vis = os.path.basename(result.inputs['vis'])
        ms = context.observing_run.get_ms(vis)
        antennas = ms.antennas
        fields = ms.fields
        science_spw = ms.get_spectral_windows(science_windows_only=True)
        if figtype == 'detail':
            spw_ids = [spw.id for spw in science_spw]
        elif figtype == 'summary':
            spw_ids = [science_spw[0].id]
        else:
            pass
        plot = None
        plots = []
        antenna_ids = [ant.id for ant in antennas]
        field_strategy = ms.calibration_strategy['field_strategy']

        for spw in spw_ids:
            spw_id = spw
            LOG.debug('spw_id={0}'.format(spw_id))
            for antenna_id in antenna_ids:
                LOG.debug('antenna_id={0}'.format(antenna_id))
                n = 0
                for field_id_target, field_id_reference in field_strategy.items():
                    LOG.debug('field_id_target = {0}, field_id_reference = {1}'.format(field_id_target, field_id_reference))
                    field = fields[field_id_target]
                    # make plots for the interval ratio (off-source/on-source) vs time;
                    with casa_tools.TableReader(calapp.gaintable) as tb:
                        t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}&&FIELD_ID=={}'.format(spw_id, antenna_id, field_id_reference), sortlist='TIME', columns='TIME, SPECTRAL_WINDOW_ID, INTERVAL')
                        mjd = t.getcol('TIME')
                        ms_target = ms.get_scans(scan_intent='TARGET', field=field_id_target)
                        ms_target0 = ms_target[0]
                        interval_unit = ms_target0.mean_interval(spw_id=spw_id)
                        interval_unit = interval_unit.total_seconds()
                        interval = t.getcol('INTERVAL') / interval_unit
                        t.close()
                        if len(mjd) == 0:
                            pass
                        else:
                            mjd_list = mjd.tolist()
                            mjd_list = [v/86400.0 for v in mjd_list]
                            mjd_list = sd_display.mjd_to_plotval(mjd_list)
                            start_time = numpy.min([numpy.min(x) for x in mjd_list if len(mjd_list) > 0])
                            end_time = numpy.max([numpy.max(x) for x in mjd_list if len(mjd_list) > 0])
                            fig = plt.figure()
                            ax = fig.add_subplot(1,1,1)
                            ax.xaxis.set_major_locator(sd_display.utc_locator(start_time=start_time, end_time=end_time))
                            ax.xaxis.set_major_formatter(sd_display.utc_formatter())
                            ax.tick_params( axis='both', labelsize=10 )
                            antenna_name = antennas[antenna_id].name
                            field_name = field.clean_name
                            plt.title('Interval vs. Time Plot\n{} Field:{} Antenna:{} Spw:{}'.format(vis, field_name, antenna_name, spw_id), fontsize=12)
                            plt.ylabel('Interval of Off-Source / Interval of On-Source', fontsize=10)
                            plt.xlabel("UTC", fontsize=10)
                            ax.plot(mjd_list, interval, linestyle='None', marker=".", label="Interval of Off-Source\nUnit: {} seconds (Interval of On-Source)".format(interval_unit))
                            min_interval = numpy.min(interval)
                            max_interval = numpy.max(interval)
                            ax.set_ylim([min_interval-3.0, max_interval+3.0])
                            plt.legend(bbox_to_anchor=(1, 1), loc='upper right', borderaxespad=1, fontsize=10)
                            if figtype == "summary":
                                prefix = vis + "_" + '{}'.format(antenna_name)+ "_" + '{}'.format(field_name) + "_summary_hsd_skycal_offinterval"
                            else:
                                prefix = vis + "_" + '{}'.format(antenna_name)+ "_" + '{}'.format(field_name) + "_spw" + '{}'.format(spw_id) + "_hsd_skycal_offinterval"
                            figroot = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
                            figpath = os.path.join(figroot, '{prefix}.png'.format(prefix=prefix))
                            LOG.info('Plot of Interval vs Time: figpath = {0}'.format(figpath))
                            plt.savefig(figpath)
                            plt.close()
                            if os.path.exists(figpath):
                                parameters = {
                                'spw': spw_id,
                                'ant': antenna_name,
                                'vis': vis,
                                'type': 'Plot of Interval vs. Time',
                                'file': vis}
                                plot = logger.Plot(figpath,
                                    x_axis='Time',
                                    y_axis='Off-Source Interval / On-Source Interval',
                                    field=field_name,
                                    parameters=parameters)
                                plots.append(plot)
                    n += 1
        return plots


@casa5style_plot
def plot_elevation_difference(context, result, eldiff, threshold=3.0):
    """
    context
    result
    eldiff -- dictionary whose value is ElevationDifference named tuple instance that holds
                  'timeon': timestamp for ON-SOURCE pointings
                  'elon': ON-SOURCE elevation
                  'timecal': timestamp for OFF-SOURCE pointings
                  'elcal': OFF-SOURCE elevation
                  'time0': timestamp for preceding OFF-SOURCE pointings
                  'eldiff0': elevation difference between ON-SOURCE and preceding OFF-SOURCE
                  'time1': timestamp for subsequent OFF-SOURCE pointings
                  'eldiff1': elevation difference between ON-SOURCE and subsequent OFF-SOURCE
              eldiff is a nested dictionary whose first key is FIELD_ID for target, the second one
              is ANTENNA_ID, and the third one is SPW_ID.
    threhshold -- Elevation threshold for QA (default 3deg)
    """
    calapp = result.final[0]
    vis = calapp.calto.vis
    ms = context.observing_run.get_ms(vis)

    figroot = os.path.join(context.report_dir,
                           'stage%s' % result.stage_number)

    figure0 = 'PERANTENNA_PLOT'
    figure1 = 'ALLANTENNA_PLOT'
    start_time = numpy.min([numpy.min(x.timeon) for z in eldiff.values() for y in z.values()
                            for x in y.values() if len(x.timeon) > 0])
    end_time = numpy.max([numpy.max(x.timeon) for z in eldiff.values() for y in z.values()
                          for x in y.values() if len(x.timeon) > 0])

    def init_figure(figure_id):
        plt.figure(figure_id)
        plt.clf()
        a0 = plt.axes([0.125, 0.51, 0.775, 0.39])
        a0.xaxis.set_major_locator(sd_display.utc_locator(start_time=start_time, end_time=end_time))
        a0.xaxis.set_major_formatter(plt.NullFormatter())
        a0.tick_params( axis='both', labelsize=10 )
        plt.ylabel('Elevation [deg]', fontsize=10)

        a1 = plt.axes([0.125, 0.11, 0.775, 0.39])
        a1.xaxis.set_major_locator(sd_display.utc_locator(start_time=start_time, end_time=end_time))
        a1.xaxis.set_major_formatter(sd_display.utc_formatter())
        a1.tick_params( axis='both', labelsize=10 )
        plt.ylabel('Elevation Difference [deg]', fontsize=10)
        plt.xlabel('UTC', fontsize=10)
        return a0, a1

    def finalize_figure(figure_id, vis, field_name, antenna_name):
        figure = plt.figure(figure_id)
        axes = figure.axes
        assert len(axes) == 2
        a0 = axes[0]
        a1 = axes[1]
        plt.gcf().sca(a1)
        ymin, ymax = plt.ylim()
        dy = ymax - ymin
        plt.ylim([0, max(ymax + 0.1 * dy, threshold + 0.1)])

        plt.axhline(threshold, color='red')
        xmin, xmax = plt.xlim()
        dx = xmax - xmin
        x = xmax - 0.01 * dx
        y = threshold - 0.05
        plt.text(x, y, 'QA threshold', ha='right', va='top', color='red', size='small')

        plt.gcf().sca(a0)
        labelon = False
        labeloff = False
        for l in a0.lines:
            if (labelon is False) and (l.get_color() == 'black'):
                l.set_label('ON')
                labelon = True
            if (labeloff is False) and (l.get_color() == 'blue'):
                l.set_label('OFF')
                labeloff = True
            if labelon and labeloff:
                break
        plt.legend(loc='best', numpoints=1, prop={'size': 'small'})
        plt.title('Elevation Difference between ON and OFF\n{} Field {} Antenna {}'.format(vis,
                                                                                           field_name,
                                                                                           antenna_name),
                  fontsize=12)

    def generate_plot(figure_id, vis, field_name, antenna_name):
        plt.figure(figure_id)
        vis_prefix = '.'.join(vis.split('.')[:-1])
        figfile = 'elevation_difference_{}_{}_{}.png'.format(vis_prefix, field_name, antenna_name)
        figpath = os.path.join(figroot, figfile)
        #LOG.info('figpath={}'.format(figpath))
        plt.savefig(figpath)
        plot = None
        if os.path.exists(figpath):
            parameters = {'intent': 'TARGET',
                          'spw': '',
                          'pol': '',
                          'ant': antenna_name,
                          'vis': vis,
                          'type': 'Elevation Difference vs. Time',
                          'file': vis}
            plot = logger.Plot(figpath,
                               x_axis='Time',
                               y_axis='Elevation Difference',
                               field=field_name,
                               parameters=parameters)
        return plot

    def close_figure( figure_id ):
        plt.close(figure_id)

    plots = []
    for field_id, eldiff_field in eldiff.items():
        # figure for summary plot
        a2, a3 = init_figure(figure1)

        field = ms.fields[field_id]
        field_name = field.clean_name

        plots_per_field = []

        for antenna_id, eldant in eldiff_field.items():
            # figure for per-antenna plots
            a0, a1 = init_figure(figure0)

            antenna_name = ms.antennas[antenna_id].name

            for spw_id, eld in eldant.items():
                if len(eld.timeon) == 0 or len(eld.timecal) == 0:
                    continue

                # Elevation vs. Time
                xon = sd_display.mjd_to_plotval(eld.timeon)
                xoff = sd_display.mjd_to_plotval(eld.timecal)
                for a in [a0, a2]:
                    a.plot(xon, eld.elon, '.', color='black', mew=0)
                    a.plot(xoff, eld.elcal, '.-', color='blue', mew=0)

                # Elevation Difference vs. Time
                time0 = eld.time0
                eldiff0 = eld.eldiff0
                time1 = eld.time1
                eldiff1 = eld.eldiff1
                io0 = numpy.where(numpy.abs(eldiff0) < threshold)[0]
                ix0 = numpy.where(numpy.abs(eldiff0) >= threshold)[0]
                io1 = numpy.where(numpy.abs(eldiff1) < threshold)[0]
                ix1 = numpy.where(numpy.abs(eldiff1) >= threshold)[0]
                index_list = [io0, io1, ix0, ix1]
                time_list = [time0, time1, time0, time1]
                eldiff_list = [eldiff0, eldiff1, eldiff0, eldiff1]
                style_list = ['g.', 'g.', 'rs', 'rs']
                for idx, t, ed, ls in zip(index_list, time_list, eldiff_list, style_list):
                    if len(idx) > 0:
                        x = sd_display.mjd_to_plotval(t[idx])
                        y = numpy.abs(ed[idx])
                        for a in [a1, a3]:
                            a.plot(x, y, ls, mew=0)

            # finalize figure for per-antenna plot
            finalize_figure(figure0, ms.basename, field_name, antenna_name)

            # generate plot object
            plot = generate_plot(figure0, ms.basename, field_name, antenna_name)
            if plot is not None:
                plots_per_field.append(plot)

        # finalize figure for summary plot
        finalize_figure(figure1, ms.basename, field_name, 'ALL')

        # generate plot object
        plot = generate_plot(figure1, ms.basename, field_name, '')
        if plot is not None:
            plots_per_field.append(plot)

        plots.extend(plots_per_field)

    close_figure( figure0 )
    close_figure( figure1 )

    return plots
