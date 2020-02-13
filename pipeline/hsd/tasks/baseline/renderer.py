import collections
import os

import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.filenamer as filenamer

from . import display

from ..common import compress
from ..common import utils

LOG = logging.get_logger(__name__)


class T2_4MDetailsSingleDishBaselineRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    # Renderer class for stage summary
    def __init__(self, template='hsd_baseline.mako',
                 description='Generate Baseline tables and subtract spectral baseline',
                 always_rerender=False):
        super(T2_4MDetailsSingleDishBaselineRenderer, self).__init__(template,
                                                                     description,
                                                                     always_rerender)

    def update_mako_context(self, ctx, context, results):
        plots = []
        sparsemap_plots = []
        for r in results:
            inputs = display.ClusterDisplay.Inputs(context, result=r)
            task = display.ClusterDisplay(inputs)
            plots.append(task.plot())
            sparsemap_plots.extend(r.outcome['plots'])

        plot_group = self._group_by_axes(plots)
        plot_detail = collections.OrderedDict()  # key is field name, subkeys are 'title', 'html', 'cover_plots'
        plot_cover = collections.OrderedDict()  # key is field name, subkeys are 'title', 'cover_plots'
        # Render stage details pages
        details_title = ["R.A. vs Dec."]
        name_list = ['R.A. vs Dec.', 'Line Center vs Line Width', 'Number of Clusters vs Score']
        sorted_fields = utils.sort_fields(context)
        for name in name_list:
            if name not in plot_group:
                # no plots available. probably no lines are detected.
                continue

            _plots = plot_group[name]
            perfield_plots = self._plots_per_field(_plots)
            renderer = SingleDishClusterPlotsRenderer(context, results, name, _plots)
            for fieldobj in sorted_fields:
                group_desc = {'title': name,
                              'html': os.path.basename(renderer.path)}
                field = self.get_field_key(perfield_plots, fieldobj)
                if field is None:
                    raise RuntimeError('No plots for field "{}"'.format(fieldobj.name))
                pfplots = perfield_plots[field]
                if name in details_title:
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())
                    _plots = plot_detail.setdefault(field, [])
                    group_desc['cover_plots'] = self._get_a_plot_per_spw(pfplots)
                else:
                    _plots = plot_cover.setdefault(field, [])
                    group_desc['cover_plots'] = pfplots
                _plots.append(group_desc)
            LOG.info('plot_detail {}'.format(plot_detail))
            LOG.info('plot_cover {}'.format(plot_cover))

        # whether or not virtual spw id is effective
        dovirtual = utils.require_virtual_spw_id_handling(context.observing_run)
        ctx.update({'detail': plot_detail,
                    'cover_only': plot_cover,
                    'dovirtual': dovirtual})

        # profile map before and after baseline subtracton
        maptype_list = ['before', 'after', 'before']
        subtype_list = ['raw', 'raw', 'avg']
        for maptype, subtype in zip(maptype_list, subtype_list):
            plot_list = self._plots_per_field_with_type(sparsemap_plots, maptype, subtype)
            summary = self._summary_plots(plot_list)
            subpage = collections.OrderedDict()
            # flattened = [plot for inner in plot_list.values() for plot in inner]
            flattened = compress.CompressedList()
            for inner in plot_list.values():
                for plot in inner:
                    flattened.append(plot)
            datatype = 'Raw' if subtype == 'raw' else 'Averaged'
            plot_title = '{} Sparse Profile Map {} Baseline Subtraction'.format(datatype, maptype.lower())
            renderer = basetemplates.JsonPlotRenderer('generic_x_vs_y_ant_field_spw_pol_plots.mako',
                                                      context,
                                                      results,
                                                      flattened,
                                                      plot_title,
                                                      filenamer.sanitize('%s.html' % (plot_title.lower())))
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())

            for fieldobj in sorted_fields:
                name = self.get_field_key(plot_list, fieldobj)
                assert name is not None
                subpage[name] = os.path.basename(renderer.path)
            ctx.update({'sparsemap_subpage_{}_{}'.format(maptype.lower(), subtype.lower()): subpage,
                        'sparsemap_{}_{}'.format(maptype.lower(), subtype.lower()): summary})

    @staticmethod
    def _group_by_axes(plots):
        plot_group = {}
        for p in [p for _p in plots for p in _p]:
            key = "%s vs %s" % (p.x_axis, p.y_axis)
            if key in plot_group:
                plot_group[key].append(p)
            else:
                plot_group[key] = [p]
        return plot_group

    @staticmethod
    def _get_a_plot_per_spw(plots):
        known_spw = []
        plot_list = []
        for p in plots:
            if p.parameters['type'] == 'clustering_final' and p.parameters['spw'] not in known_spw:
                known_spw.append(p.parameters['spw'])
                plot_list.append(p)
        return plot_list

    @staticmethod
    def _plots_per_field(plots):
        plot_group = {}
        for p in plots:
            key = p.field
            if key in plot_group:
                plot_group[key].append(p)
            else:
                plot_group[key] = [p]
        return plot_group

    @staticmethod
    def _plots_per_field_with_type(plots, type_string, subtype_string):
        plot_group = {}
        for x in plots:
            if isinstance(x, compress.CompressedObj):
                p = x.decompress()
            else:
                p = x
            if p.parameters['type'].find(type_string) != -1 and p.parameters['type'].find(subtype_string) != -1:
                key = p.field
                if key in plot_group:
                    plot_group[key].append(x)
                else:
                    plot_group[key] = [x]
            del p
        return plot_group

    @staticmethod
    def _summary_plots(plot_group):
        summary_plots = {}
        for field_name, plots in plot_group.items():
            spw_list = []
            summary_plots[field_name] = compress.CompressedList()
            for xplot in plots:
                if isinstance(xplot, compress.CompressedObj):
                    plot = xplot.decompress()
                else:
                    plot = xplot
                spw = plot.parameters['spw']
                if spw not in spw_list:
                    spw_list.append(spw)
                    summary_plots[field_name].append(xplot)
                del plot
        return summary_plots

    @staticmethod
    def get_field_key(plot_dict, field_domain):
        field_candidates = filter(
            lambda x: x in plot_dict,
            set([field_domain.name, field_domain.name.strip('"'), field_domain.clean_name]))
        try:
            field_key = next(field_candidates)
        except StopIteration:
            LOG.warn('No plots for field "{}"'.format(field_domain.name))
            field_key = None
        return field_key


class SingleDishClusterPlotsRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, xytitle, plots):
        outfile = filenamer.sanitize('%s.html' % (xytitle.lower().replace(" ", "_")))
        new_title = "Clustering: %s" % xytitle

        super(SingleDishClusterPlotsRenderer, self).__init__(
            'hsd_cluster_plots.mako', context, result, plots, new_title, outfile)

    def update_json_dict(self, d, plot):
        d['type'] = plot.parameters['type']
