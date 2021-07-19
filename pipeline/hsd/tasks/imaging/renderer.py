import collections
import os

import pipeline.domain.measures as measures
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from . import resultobjects
from . import display
from ..common import utils as sdutils

LOG = logging.get_logger(__name__)

ImageRMSTR = collections.namedtuple('ImageRMSTR', 'name estimate range width theoretical_rms observed_rms')


class T2_4MDetailsSingleDishImagingRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='hsd_imaging.mako',
                 description='Image single dish data',
                 always_rerender=False):
        super(T2_4MDetailsSingleDishImagingRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        LOG.info('ctx = {}'.format(ctx))
        LOG.info('context = {}'.format(context))
        LOG.info('results = {}'.format(results))
        # whether or not virtual spw id handling is necessary
        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        sorted_fields = sdutils.sort_fields(context)
        ctx.update({
            'dovirtual': dovirtual
        })

        cqa = casa_tools.quanta
        plots = []
        image_rms_reps = []
        image_rms_others = []
        image_rms = []
        for r in results:
            LOG.info('r = {}'.format(r))
            if isinstance(r, resultobjects.SDImagingResultItem):
                image_item = r.outcome['image']
                msid_list = r.outcome['file_index']
                imagemode = r.outcome['imagemode']
                LOG.info('image_item = {}'.format(image_item))
                LOG.info('msid_list = {}'.format(msid_list))
                LOG.info('imagemode = {}'.format(imagemode))
                v_spwid = image_item.spwlist
                mses = context.observing_run.measurement_sets
                spwid = [context.observing_run.virtual2real_spw_id(s, mses[i]) for s, i in zip(v_spwid, msid_list)]
                ref_ms = mses[msid_list[0]]
                ref_spw = spwid[0]
                LOG.info('v_spwid = {}'.format(v_spwid))
                LOG.info('mses = {}'.format(mses))
                LOG.info('spwid = {}'.format(spwid))
                LOG.info('ref_ms = {}'.format(ref_ms))
                LOG.info('ref_spw = {}'.format(ref_spw))
                #rep = ref_ms.representative_target[0]
                LOG.info('ref_ms.representative_target[0] = {}'.format(ref_ms.representative_target[0]))
                spw_type = 'TP' if imagemode.upper() == 'AMPCAL' else ref_ms.spectral_windows[ref_spw].type
                task_cls = display.SDImageDisplayFactory(spw_type)
                inputs = task_cls.Inputs(context, result=r)
                task = task_cls(inputs)
                plots.append(task.plot())
                # RMS of combined image
                if r.sensitivity_info is not None:
                    rms_info = r.sensitivity_info
                    LOG.info('rms_info = {}'.format(rms_info))
                    sensitivity = rms_info.sensitivity
                    LOG.info('sensitivity = {}'.format(sensitivity))
                    theoretical_rms = r.theoretical_rms['sensitivity']
                    LOG.info('theoretical_rms = {}'.format(theoretical_rms))
                    trms = cqa.tos(theoretical_rms) if theoretical_rms['value'] >= 0 else 'n/a'
                    LOG.info('trms = {}'.format(trms))
                    icon = '<span class="glyphicon glyphicon-ok"></span>' if rms_info.representative else ''
                    LOG.info('icon = {}'.format(icon))
                    if ref_ms.representative_target[0] is not None:
                        tr_rep = ImageRMSTR(image_item.imagename, icon, rms_info.frequency_range,
                                        cqa.getvalue(cqa.convert(sensitivity['bandwidth'], 'kHz'))[0],
                                        trms, cqa.tos(sensitivity['sensitivity']))
                        LOG.info('tr_rep = {}'.format(tr_rep))
                        image_rms.append(tr_rep)
                        LOG.info('image_rms = {}'.format(image_rms))
                    else:
                        tr_other = ImageRMSTR(image_item.imagename, icon, rms_info.frequency_range,
                                        cqa.getvalue(cqa.convert(sensitivity['bandwidth'], 'kHz'))[0],
                                        trms, cqa.tos(sensitivity['sensitivity']))
                        LOG.info('tr_other = {}'.format(tr_other))
                        image_rms_others.append(tr_other)                    
                        LOG.info('image_rms_others = {}'.format(image_rms_others))
                    if len(image_rms) > 0 and len(image_rms_others) > 0:
                        image_rms.extend(image_rms_others)
                        LOG.info('image_rms = {}'.format(image_rms))

        rms_table = utils.merge_td_columns(image_rms, num_to_merge=0)
        LOG.info('rms_table = {}'.format(rms_table))

        map_types = {'sparsemap': {'type': 'sd_sparse_map',
                                   'plot_title': 'Sparse Profile Map'},
                     'profilemap': {'type': 'sd_spectral_map',
                                    'plot_title': 'Detailed Profile Map'},
                     'channelmap': {'type': 'channel_map',
                                    'plot_title': 'Channel Map'},
                     'rmsmap': {'type': 'rms_map',
                                'plot_title': 'Baseline RMS Map'},
                     'momentmap': {'type': 'sd_moment_map',
                                   'plot_title': 'Maximum Intensity Map'},
                     'integratedmap': {'type': 'sd_integrated_map',
                                       'plot_title': 'Integrated Intensity Map'},
                     'contaminationmap': {'type': 'sd_contamination_map',
                                          'plot_title': 'Contamination Plots'}}

        for key, value in map_types.items():
            plot_list = self._plots_per_field_with_type(plots, value['type'])
            LOG.debug('plot_list=%s'%((plot_list)))

            # plot_list can be empty
            # typical case is spectral map for NRO
            if len(plot_list) == 0:
                ctx.update({'%s_subpage' % key: None,
                            '%s_plots' % key: None})
                continue

            flattened = []
            for inner in plot_list.values():
                for plot in inner:
                    flattened.append(plot)
            LOG.debug('flattened=%s'%((flattened)))
            #summary = self._summary_plots(plot_list)
            if key == 'channelmap':
                summary = self._summary_plots_channelmap(context, plot_list)
            else:
                summary = self._summary_plots(plot_list)

            # contamination plots
            if key == 'contaminationmap':
                ctx.update({f'{key}_subpage': None,
                            f'{key}_plots': summary})
                continue

            subpage = collections.OrderedDict()
            plot_title = value['plot_title']
            LOG.debug('plot_title=%s'%(plot_title))
            renderer = basetemplates.JsonPlotRenderer('generic_x_vs_y_ant_field_spw_pol_plots.mako',
                                                      context,
                                                      results,
                                                      flattened,
                                                      plot_title,
                                                      filenamer.sanitize('%s.html' % (plot_title.lower())))
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
            for fieldobj in sorted_fields:
                field = self.get_field_key(plot_list, fieldobj)
                if field is None:
                    LOG.info('No "{}" plots for field "{}"'.format(key, fieldobj.name))
                    continue
                subpage[field] = os.path.basename(renderer.path)
            ctx.update({'%s_subpage' % key: subpage,
                        '%s_plots' % key: summary})
            if key == 'sparsemap':
                profilemap_entries = {}
                for field, _plots in plot_list.items():
                    _ap = {}
                    for p in _plots:
                        ant = p.parameters['ant']
                        pol = p.parameters['pol']
                        field = p.parameters['field']
                        if ant not in _ap:
                            _ap[ant] = [pol]
                        elif pol not in _ap[ant]:
                            _ap[ant].append(pol)
                    profilemap_entries[field] = _ap
                ctx.update({'profilemap_entries': profilemap_entries, 'rms_table': rms_table})

        if 'rms_table' not in ctx:
            ctx.update({'rms_table': None})

    @staticmethod
    def _plots_per_field_with_type(plots, type_string):
        plot_group = {}
        for p in [p for _p in plots for p in _p]:
            if p.parameters['type'] == type_string:
                key = p.field
                if key in plot_group:
                    plot_group[key].append(p)
                else:
                    plot_group[key] = [p]
        return plot_group

    @staticmethod
    def _summary_plots(plot_group):
        summary_plots = {}
        for field_name, plots in plot_group.items():
            spw_list = []
            summary_plots[field_name] = []
            for plot in plots:
                spw = plot.parameters['spw']
                if spw not in spw_list:
                    spw_list.append(spw)
                    summary_plots[field_name].append(plot)
                if plot.parameters['ant'] == 'COMBINED':
                    idx = spw_list.index(spw)
                    summary_plots[field_name][idx] = plot
        return summary_plots

    @staticmethod
    def _summary_plots_channelmap(context, plot_group):
        # take the ms having the largest number of fields
        nfields = [len(ms.fields) for ms in context.observing_run.measurement_sets]
        repid = nfields.index(max(nfields))
        ms = context.observing_run.measurement_sets[repid]
        source_names = [filenamer.sanitize(s.name) for s in ms.sources]

        summary_plots = {}
        for field_name, plots in plot_group.items():
            spw_list = []
            summary_plots[field_name] = []
            best_plot = {}
            min_separation = {}

            for plot in plots:
                if plot.parameters['ant'] != 'COMBINED':
                    continue

                spw_id = plot.parameters['spw']
                if spw_id not in spw_list:
                    spw_list.append(spw_id)
                source_name = plot.field
                source_index = source_names.index(source_name)
                source_id = ms.sources[source_index].id
                # center frequency
                spw = ms.get_spectral_window(spw_id)
                cf = spw.centre_frequency
                center_freq = float(cf.convert_to(measures.FrequencyUnits.HERTZ).value)
                # first item of rest frequencies
                rest_frequency = sdutils.get_restfrequency(ms.name, spw_id, source_id)
                if rest_frequency is None:
                    # center frequency of the spw (TOPO)
                    # the result may be wrong due to the difference of frequency reference
                    LOG.debug('rest frequency is not available for {} spw {}. Using center frequency instead.'.format(source_name, spw_id))
                    rest_frequency = center_freq

                # line window in LSRK frequency
                line_window = plot.parameters['line']
                if line_window[0] > line_window[1]:
                    tmp = line_window[0]
                    line_window[1] = line_window[0]
                    line_window[0] = tmp
                line_center = sum(line_window) / 2
                LOG.debug('line_center = {}'.format(line_center))

                penalty = center_freq
                if line_window[0] <= rest_frequency and rest_frequency <= line_window[1]:
                    separation = abs(line_center - rest_frequency)
                    LOG.debug('line window brackets rest frequency')
                    LOG.debug('FIELD {} SPW {} rest frequency {} separation {}'.format(field_name, spw_id, rest_frequency, separation))
                else:
                    # add penalty term to the separation
                    separation = penalty + abs(line_center - rest_frequency)
                    LOG.debug('FIELD {} SPW {} rest frequency {} separation {} (w/o penalty {})'.format(field_name,
                                                                                                        spw_id,
                                                                                                        rest_frequency,
                                                                                                        separation,
                                                                                                        separation - penalty))

                if spw_id not in best_plot or separation < min_separation[spw_id]:
                    LOG.debug('updating best_plot for SPW {} (min_separation {} separation {})'.format(spw_id,
                                                                                                       min_separation.get(spw_id, None),
                                                                                                       separation))
                    best_plot[spw_id] = plot
                    min_separation[spw_id] = separation

            LOG.debug('FIELD {}'.format(field_name))
            LOG.debug('spw_list {}'.format(spw_list))
            for spw_id in spw_list:
                summary_plots[field_name].append(best_plot[spw_id])

        return summary_plots

    @staticmethod
    def get_field_key(plot_dict, field_domain):
        field_candidates = filter(
            lambda x: x in plot_dict,
            set([field_domain.name, field_domain.name.strip('"'), field_domain.clean_name]))
        try:
            field_key = next(field_candidates)
        except StopIteration:
            field_key = None
        return field_key

