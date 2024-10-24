"""Weblog renderer for baseline subtraction task."""
import collections
import os
from typing import TYPE_CHECKING, List, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.filenamer as filenamer

from . import display

from ..common import compress
from ..common import utils

if TYPE_CHECKING:
    from pipeline.domain.field import Field
    from pipeline.infrastructure.api import Results
    from pipeline.infrastructure.basetask import ResultsList
    from pipeline.infrastructure.launcher import Context
    from pipeline.infrastructure.renderer.logger import Plot

LOG = logging.get_logger(__name__)


class T2_4MDetailsSingleDishBaselineRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """Generates task detail page for baseline subtraction task."""

    # Renderer class for stage summary
    def __init__(self,
                 template: str = 'hsd_baseline.mako',
                 description: str = 'Generate Baseline tables and subtract spectral baseline',
                 always_rerender: bool = False) -> None:
        """Create T2_4MDetailsSingleDishBaselineRenderer instance.

        Args:
            template: Name of Mako template file. Defaults to 'hsd_baseline.mako'.
            description: Description of the task. This is embedded into the task detail page.
                         Defaults to 'Generate Baseline tables and subtract spectral baseline'.
            always_rerender: Always rerender the page if True. Defaults to False.
        """
        super(T2_4MDetailsSingleDishBaselineRenderer, self).__init__(template,
                                                                     description,
                                                                     always_rerender)

    def update_mako_context(self, ctx: dict, context: 'Context', results: 'ResultsList') -> None:
        """Update context object for Mako template in place.

        Clustering analysis plots are generated and included in the Mako context.
        Detail plot pages for clustering analysis plots as well as sparse profile maps
        are created and included in the context as well.

        Args:
            ctx: dict for mako context
            context: Pipeline context
            results: ResultsList instance generated by task execution
        """
        # clustering plot is created only when plotlevel is 'all'
        plots = []
        plot_detail = None
        plot_cover = None

        sorted_fields = utils.sort_fields(context)

        # clustering plots are generated only when plotlevel is 'all'
        if infrastructure.generate_detail_plots(results):
            # to capture warning message
            handler = logging.CapturingHandler(level=logging.WARNING)
            logging.add_handler(handler)

            try:
                for r in results:
                    inputs = display.ClusterDisplay.Inputs(context, result=r)
                    task = display.ClusterDisplay(inputs)
                    plots.append(task.plot())
            finally:
                logging.remove_handler(handler)
                # add the log records to the result
                if not hasattr(results, 'logrecords'):
                    results.logrecords = handler.buffer
                else:
                    results.logrecords.extend(handler.buffer)

            plot_detail = collections.OrderedDict()  # key is field name, subkeys are 'title', 'html', 'cover_plots'
            plot_cover = collections.OrderedDict()  # key is field name, subkeys are 'title', 'cover_plots'

            plot_group = self._group_by_axes(plots)
            # Render stage details pages
            details_title = ["R.A. vs Dec."]
            name_list = ['R.A. vs Dec.', 'Line Center vs Line Width']
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
                        LOG.info('No "{}" plots for field "{}"'.format(name, fieldobj.name))
                        plot_detail[fieldobj.name] = []
                        plot_cover[fieldobj.name] = []
                        continue
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

        sparsemap_plots = []
        for r in results:
            sparsemap_plots.extend(r.outcome['plots'])

        # whether or not virtual spw id is effective
        dovirtual = utils.require_virtual_spw_id_handling(context.observing_run)
        ctx.update({'detail': plot_detail,
                    'cover_only': plot_cover,
                    'dovirtual': dovirtual})

        # profile map before and after baseline subtracton
        maptype_list = ['before', 'after', 'before', 'after']
        subtype_list = ['raw', 'raw', 'avg', 'flatness']
        for maptype, subtype in zip(maptype_list, subtype_list):
            plot_list = self._plots_per_field_with_type(sparsemap_plots, maptype, subtype)
            summary = self._summary_plots(plot_list)
            subpage = collections.OrderedDict()
            # flattened = [plot for inner in plot_list.values() for plot in inner]
            flattened = compress.CompressedList()
            for inner in plot_list.values():
                for plot in inner:
                    flattened.append(plot)
            if subtype != 'flatness':
                datatype = 'Raw' if subtype == 'raw' else 'Averaged'
                plot_title = '{} Sparse Profile Map {} Baseline Subtraction'.format(datatype, maptype.lower())
            else:
                plot_title = 'Baseline Flatness {} Baseline Subtraction'.format(maptype.lower())
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
    def _group_by_axes(plots: List[List['Plot']]) -> dict:
        """Group Plot objects by axis labels.

        Plot objects must contain two-dimensional figure, and its
        axis types must be described on x_axis and y_axis attributes.

        Args:
            plots (): List of Plot objects.

        Returns:
            Dictionary of Plot objects classified by axis types
        """
        plot_group = {}
        for p in [p for _p in plots for p in _p]:
            key = "%s vs %s" % (p.x_axis, p.y_axis)
            if key in plot_group:
                plot_group[key].append(p)
            else:
                plot_group[key] = [p]
        return plot_group

    @staticmethod
    def _get_a_plot_per_spw(plots: List['Plot']) -> List['Plot']:
        """Return list of representative Plot object per spw.

        Pick up "final" clustering plot for each spw and return
        them as a new list.

        Args:
            plots: List of Plot objects

        Returns:
            List of representative Plot objects per spw
        """
        known_spw = []
        plot_list = []
        for p in plots:
            if p.parameters['type'] == 'clustering_final' and p.parameters['spw'] not in known_spw:
                known_spw.append(p.parameters['spw'])
                plot_list.append(p)
        return plot_list

    @staticmethod
    def _plots_per_field(plots: List['Plot']) -> dict:
        """Classify Plot objects by field.

        Args:
            plots: List of Plot objects

        Returns:
            List of Plot objects classified by field
        """
        plot_group = {}
        for p in plots:
            key = p.field
            if key in plot_group:
                plot_group[key].append(p)
            else:
                plot_group[key] = [p]
        return plot_group

    @staticmethod
    def _plots_per_field_with_type(plots: List['Plot'], type_string: str, subtype_string: str) -> dict:
        """Classify Plot objects by field with filtering by plot types.

        Filtering by plot types is based on "in" operator for the type string.
        If type_string is 'foo' and subtype_string is 'bar', Plot objects whose
        type contains 'foo' and 'bar', such as 'foo_bar' or 'foo_bar_baz', will
        be included in the returned dictionary.

        Args:
            plots: List of Plot objects
            type_string: Plot type
            subtype_string: Plot subtype

        Returns:
            List of Plot objects with desired type classfied by field
        """
        plot_group = {}
        for x in plots:
            if isinstance(x, compress.CompressedObj):
                p = x.decompress()
            else:
                p = x
            if type_string in p.parameters['type'] and subtype_string in p.parameters['type']:
                key = p.field
                if key in plot_group:
                    plot_group[key].append(x)
                else:
                    plot_group[key] = [x]
            del p
        return plot_group

    @staticmethod
    def _summary_plots(plot_group: dict) -> dict:
        """Select summary Plot objects per field and spw.

        Selected plots are grouped by field and consolidated as a dictionary.

        Args:
            plot_group: List of Plot objects per field

        Returns:
            List of summary Plot objects per field and spw
        """
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
    def get_field_key(plot_dict: dict, field_domain: 'Field') -> Optional[str]:
        """Get field name consistent with dictionary key.

        This is used to check the connection between dictionary key (str)
        and Field domain object. If this returns any string, it means that
        dictionary key refers given field_domain object.

        There are several possibilities in the representation of field name
        when original field name contains some special character such as
        ' ' (whitespace). In that case, those characters may be replaced with
        '_' (underscore), or field name may be bracketed by '"' (double-quote).
        Since field key for the dictionary should be one of them, all the possible
        field names should be checked when we search field domain object that
        is referred by the field key. That is why this method exists.

        Args:
            plot_dict: List of Plot objects classified by field
            field_domain: Field domain object

        Returns:
            field name consistent with dictionary key or None
        """
        field_candidates = filter(
            lambda x: x in plot_dict,
            set([field_domain.name, field_domain.name.strip('"'), field_domain.clean_name]))
        try:
            field_key = next(field_candidates)
        except StopIteration:
            field_key = None
        return field_key


class SingleDishClusterPlotsRenderer(basetemplates.JsonPlotRenderer):
    """Custom JsonPlotRenderer for clustering plot."""

    def __init__(self, context: 'Context', result: 'Results', xytitle: str, plots: List['Plot']) -> None:
        """Construct SingleDishClusterPlotsRenderer instance.

        Args:
            context: Pipeline context
            result: Results instance of task execution
            xytitle: Description of x and y axis types
            plots: List of Plot objects to be included in the page
        """
        outfile = filenamer.sanitize('%s.html' % (xytitle.lower().replace(" ", "_")))
        new_title = "Clustering: %s" % xytitle

        super(SingleDishClusterPlotsRenderer, self).__init__(
            'hsd_cluster_plots.mako', context, result, plots, new_title, outfile)

    def update_json_dict(self, d: dict, plot: 'Plot') -> None:
        """Update JSON dictionary in place.

        Add plot type to the dictionary.

        Args:
            d: JSON dictionary for rendering
            plot: Plot object
        """
        d['type'] = plot.parameters['type']
