"""
Created on 24 Oct 2014

@author: sjw
"""
import collections
import itertools
import operator
import os
import shutil
from typing import Dict, Iterable, List, Optional, Type, Union


import pipeline.domain.measures as measures
import pipeline.infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.domain.measurementset import MeasurementSet
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure.basetask import ResultsList
from pipeline.infrastructure.displays.summary import UVChart
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.renderer.logger import Plot
from pipeline.h.tasks.applycal.applycal import ApplycalResults
from pipeline.infrastructure.renderer.basetemplates import JsonPlotRenderer
from ..common import flagging_renderer_utils as flagutils
from ..common.displays import applycal as applycal

LOG = logging.get_logger(__name__)

FlagTotal = collections.namedtuple('FlagSummary', 'flagged total')


class T2_4MDetailsApplycalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='applycal.mako', 
                 description='Apply calibrations from context',
                 always_rerender=False):
        super(T2_4MDetailsApplycalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, result):
        weblog_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)

        # calculate which intents to display in the flagging statistics table
        intents_to_summarise = flagutils.intents_to_summarise(context)
        flag_table_intents = ['TOTAL', 'SCIENCE SPWS']
        flag_table_intents.extend(intents_to_summarise)

        flag_totals = {}
        for r in result:
            if r.inputs['flagsum'] is True:
                flag_totals = utils.dict_merge(flag_totals,
                                               flagutils.flags_for_result(r, context, intents_to_summarise=intents_to_summarise))

        calapps = {}
        for r in result:
            calapps = utils.dict_merge(calapps, self.calapps_for_result(r))

        caltypes = {}
        for r in result:
            caltypes = utils.dict_merge(caltypes, self.caltypes_for_result(r))

        filesizes = {}
        for r in result:
            vis = r.inputs['vis']
            ms = context.observing_run.get_ms(vis)
            filesizes[ms.basename] = ms._calc_filesize()

        # return all agents so we get ticks and crosses against each one
        agents = ['before', 'applycal']

        ctx.update({
            'flags': flag_totals,
            'calapps': calapps,
            'caltypes': caltypes,
            'agents': agents,
            'dirname': weblog_dir,
            'filesizes': filesizes
        })

        # these dicts map vis to the hrefs of the detail pages
        amp_vs_freq_subpages = {}
        phase_vs_freq_subpages = {}
        amp_vs_uv_subpages = {}

        LOG.debug("creating amp vs. time summary plots in renderer")
        amp_vs_time_summary_plots, amp_vs_time_subpages = self.create_plots(
            context,
            result,
            applycal.AmpVsTimeSummaryChart,
            ['PHASE', 'BANDPASS', 'AMPLITUDE', 'CHECK', 'TARGET', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE']
        )

        LOG.debug("creating phase vs. time summary plots in renderer")
        phase_vs_time_summary_plots, phase_vs_time_subpages = self.create_plots(
            context,
            result,
            applycal.PhaseVsTimeSummaryChart,
            ['PHASE', 'BANDPASS', 'AMPLITUDE', 'CHECK', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE']
        )

        LOG.debug("creating amp vs. freq summary plots in renderer")
        amp_vs_freq_summary_plots = utils.OrderedDefaultdict(list)
        for intents in [['PHASE'], ['BANDPASS'], ['CHECK'], ['AMPLITUDE'],
                        ['POLARIZATION'], ['POLANGLE'], ['POLLEAKAGE']]:
            # it doesn't matter that the subpages dict is repeatedly redefined.
            # The only purpose of the returned dict is to map the vis to a
            # non-existing page, which will disable the link.
            plots, amp_vs_freq_subpages = self.create_plots(
                context,
                result,
                applycal.AmpVsFrequencySummaryChart,
                intents
            )

            for vis, vis_plots in plots.items():
                amp_vs_freq_summary_plots[vis].extend(vis_plots)

        LOG.debug("creating phase vs. freq summary plots in renderer")
        phase_vs_freq_summary_plots = utils.OrderedDefaultdict(list)
        for intents in [['PHASE'], ['BANDPASS'], ['CHECK'], ['POLARIZATION'], ['POLANGLE'], ['POLLEAKAGE']]:
            plots, phase_vs_freq_subpages = self.create_plots(
                context,
                result,
                applycal.PhaseVsFrequencyPerSpwSummaryChart,
                intents
            )

            for vis, vis_plots in plots.items():
                phase_vs_freq_summary_plots[vis].extend(vis_plots)

        # CAS-7659: Add plots of all calibrator calibrated amp vs uvdist to
        # the WebLog applycal page
        LOG.debug("creating amp vs. uv summary plots in renderer")
        amp_vs_uv_summary_plots = utils.OrderedDefaultdict(list)
        for intents in [['AMPLITUDE'], ['PHASE'], ['BANDPASS'], ['CHECK'],
                        ['POLARIZATION'], ['POLANGLE'], ['POLLEAKAGE']]:
            plots, amp_vs_uv_subpages = self.create_plots(
                context,
                result,
                applycal.AmpVsUVSummaryChart,
                intents
            )

            for vis, vis_plots in plots.items():
                amp_vs_uv_summary_plots[vis].extend(vis_plots)

        # CAS-5970: add science target plots to the applycal page
        LOG.debug("creating science summary plots in renderer")
        (science_amp_vs_freq_summary_plots,
         science_amp_vs_freq_subpages,
         science_amp_vs_uv_summary_plots,
         uv_max) = self.create_science_plots(context, result)

        LOG.debug("creating corrected ratio to antenna1 plots in renderer")
        corrected_ratio_to_antenna1_plots = utils.OrderedDefaultdict(list)
        corrected_ratio_to_uv_dist_plots = {}
        for r in result:
            vis = os.path.basename(r.inputs['vis'])
            uvrange_dist = uv_max.get(vis, None)
            in_m = str(uvrange_dist.to_units(measures.DistanceUnits.METRE))
            uvrange = '0~%sm' % in_m

            # CAS-9229: Add amp / model vs antenna id plots for other calibrators
            for intents, uv_cutoff in [(['AMPLITUDE'], uvrange),
                                       (['PHASE'], ''),
                                       (['BANDPASS'], ''),
                                       (['CHECK'], ''),
                                       (['POLARIZATION'], ''),
                                       (['POLANGLE'], ''),
                                       (['POLLEAKAGE'], '')]:
                p, _ = self.create_plots(
                    context,
                    [r],
                    applycal.CorrectedToModelRatioVsAntenna1SummaryChart,
                    intents,
                    uvrange=uv_cutoff
                )

                for vis, vis_plots in p.items():
                    corrected_ratio_to_antenna1_plots[vis].extend(vis_plots)

            p, _ = self.create_plots(
                context,
                [r],
                applycal.CorrectedToModelRatioVsUVDistanceSummaryChart,
                ['AMPLITUDE'],
                uvrange=uvrange,
                plotrange=[0, float(in_m), 0, 0]
            )
            corrected_ratio_to_uv_dist_plots[vis] = p[vis]

        # these dicts map vis to the list of plots
        amp_vs_freq_detail_plots = {}
        phase_vs_freq_detail_plots = {}
        phase_vs_time_detail_plots = {}

        # CAS-9154 Add per-antenna amplitude vs time plots for applycal stage
        #
        # Compromise to generate some antenna-specific plots to allow
        # bad antennas to be identified while keeping the overall number of
        # plots relatively unchanged.
        #
        LOG.debug("creating all detail plots in renderer")
        amp_vs_time_detail_plots, amp_vs_time_subpages = self.create_plots(
            context,
            result,
            applycal.CAS9154AmpVsTimeDetailChart,
            ['AMPLITUDE', 'PHASE', 'BANDPASS', 'CHECK', 'TARGET', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'],
            ApplycalAmpVsTimePlotRenderer,
            avgchannel='9000'
        )

        if pipeline.infrastructure.generate_detail_plots(result):
            # detail plots. Don't need the return dictionary, but make sure a
            # renderer is passed so the detail page is written to disk
            amp_vs_freq_detail_plots, amp_vs_freq_subpages = self.create_plots(
                context,
                result,
                applycal.AmpVsFrequencyDetailChart,
                ['BANDPASS', 'PHASE', 'CHECK', 'AMPLITUDE', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'],
                ApplycalAmpVsFreqPlotRenderer
            )

            phase_vs_freq_detail_plots, phase_vs_freq_subpages = self.create_plots(
                context,
                result,
                applycal.PhaseVsFrequencyDetailChart,
                ['BANDPASS', 'PHASE', 'CHECK', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'],
                ApplycalPhaseVsFreqPlotRenderer
            )

            phase_vs_time_detail_plots, phase_vs_time_subpages = self.create_plots(
                context,
                result,
                applycal.PhaseVsTimeDetailChart,
                ['AMPLITUDE', 'PHASE', 'BANDPASS', 'CHECK', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'],
                ApplycalPhaseVsTimePlotRenderer
            )

        # render plots for all EBs in one page
        for d, plotter_cls, subpages in (
                (amp_vs_freq_detail_plots, ApplycalAmpVsFreqPlotRenderer, amp_vs_freq_subpages),
                (phase_vs_freq_detail_plots, ApplycalPhaseVsFreqPlotRenderer, phase_vs_freq_subpages),
                (amp_vs_time_detail_plots, ApplycalAmpVsTimePlotRenderer, amp_vs_time_subpages),
                (phase_vs_time_detail_plots, ApplycalPhaseVsTimePlotRenderer, phase_vs_time_subpages)):
            if d:
                all_plots = list(utils.flatten([v for v in d.values()]))
                renderer = plotter_cls(context, result, all_plots)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                    # redirect subpage links to master page
                    for vis in subpages:
                        subpages[vis] = renderer.path

        # CAS-11511: add plots of UV coverage.
        if utils.contains_single_dish(context):
            uv_plots = None
        else:
            uv_plots = self.create_uv_plots(context, result, weblog_dir)

        # PIPE-615: Add links to the hif_applycal weblog for viewing each
        # callibrary table (and store all callibrary tables in the weblog
        # directory)
        callib_map = copy_callibrary(result, context.report_dir)

        # PIPE-396: Suppress redundant plots from hifa_applycal
        LOG.debug("deduplicating amp. vs freq")
        amp_vs_freq_summary_plots = deduplicate(context, amp_vs_freq_summary_plots)
        LOG.debug("deduplicating amp. vs UV")
        amp_vs_uv_summary_plots = deduplicate(context, amp_vs_uv_summary_plots)
        LOG.debug("deduplicating corrected ratio to antenna 1")
        corrected_ratio_to_antenna1_plots = deduplicate(context, corrected_ratio_to_antenna1_plots)

        ctx.update({
            'amp_vs_freq_plots': amp_vs_freq_summary_plots,
            'phase_vs_freq_plots': phase_vs_freq_summary_plots,
            'amp_vs_time_plots': amp_vs_time_summary_plots,
            'amp_vs_uv_plots': amp_vs_uv_summary_plots,
            'phase_vs_time_plots': phase_vs_time_summary_plots,
            'corrected_to_antenna1_plots': corrected_ratio_to_antenna1_plots,
            'corrected_to_model_vs_uvdist_plots': corrected_ratio_to_uv_dist_plots,
            'science_amp_vs_freq_plots': science_amp_vs_freq_summary_plots,
            'science_amp_vs_freq_subpages': science_amp_vs_freq_subpages,
            'science_amp_vs_uv_plots': science_amp_vs_uv_summary_plots,
            'uv_plots': uv_plots,
            'uv_max': uv_max,
            'amp_vs_freq_subpages': amp_vs_freq_subpages,
            'phase_vs_freq_subpages': phase_vs_freq_subpages,
            'amp_vs_time_subpages': amp_vs_time_subpages,
            'amp_vs_uv_subpages': amp_vs_uv_subpages,
            'phase_vs_time_subpages': phase_vs_time_subpages,
            'callib_map': callib_map,
            'flag_table_intents': flag_table_intents
        })

    @staticmethod
    def create_uv_plots(context, results, weblog_dir):
        uv_plots = collections.defaultdict(list)

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            plotter = UVChart(context, ms, customflagged=True, output_dir=weblog_dir, title_prefix="Post applycal: ")
            uv_plots[vis] = [plotter.plot()]

        return uv_plots

    def create_science_plots(self, context, results):
        """
        Create plots for the science targets, returning two dictionaries of 
        vis:[Plots], vis:[subpage paths], and vis:[max UV distances].

        Args:
            context: pipeline context
            results: ResultsList instance containing Applycal Results

        Returns:
            Three dictionaries of plot objects, subpage paths, and
            max UV distances for each vis.
        """
        amp_vs_freq_summary_plots = collections.defaultdict(dict)
        amp_vs_uv_summary_plots = collections.defaultdict(dict)
        max_uvs = collections.defaultdict(dict)

        amp_vs_freq_detail_plots = {}
        amp_vs_freq_subpages = collections.defaultdict(lambda: 'null')
        amp_vs_uv_detail_plots = {}

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            amp_vs_freq_summary_plots[vis] = []
            amp_vs_uv_summary_plots[vis] = []

            # Plot for 1 science field (either 1 science target or for a mosaic 1
            # pointing). The science field that should be chosen is the one with
            # the brightest average amplitude over all spws

            # Ideally, the uvmax of the spectrum (plots 1 and 2)
            # would be set by the appearance of plot 3; that is, if there is
            # no obvious drop in amplitude with uvdist, then use all the data.
            # A simpler compromise would be to use a uvrange that captures the
            # inner half the data.
            baselines = sorted(ms.antenna_array.baselines,
                               key=operator.attrgetter('length'))
            # take index as midpoint + 1 so we include the midpoint in the
            # constraint
            half_baselines = baselines[0:(len(baselines)//2)+1]
            uv_max = half_baselines[-1].length.to_units(measures.DistanceUnits.METRE)
            uv_range = '<%s' % uv_max
            LOG.debug('Setting UV range to %s for %s', uv_range, vis)
            max_uvs[vis] = half_baselines[-1].length

            # source to select
            representative_source_name, _ = ms.get_representative_source_spw()
            representative_source = {s for s in ms.sources if s.name == representative_source_name}
            if len(representative_source) >= 1:
                representative_source = representative_source.pop()

            brightest_field = get_brightest_field(ms, representative_source)
            plots = self.science_plots_for_result(context,
                                                  result,
                                                  applycal.AmpVsFrequencySummaryChart,
                                                  [brightest_field.id],
                                                  uv_range)
            for plot in plots:
                plot.parameters['source'] = representative_source
            amp_vs_freq_summary_plots[vis].extend(plots)

            plots = self.science_plots_for_result(context,
                                                  result,
                                                  applycal.AmpVsUVSummaryChart,
                                                  [brightest_field.id])
            for plot in plots:
                plot.parameters['source'] = representative_source
            amp_vs_uv_summary_plots[vis].extend(plots)

            if pipeline.infrastructure.generate_detail_plots(results):
                scans = ms.get_scans(scan_intent='TARGET')
                fields = {field.id for scan in scans for field in scan.fields}

                # Science target detail plots. Note that summary plots go onto the
                # detail pages; we don't create plots per spw or antenna
                plots = self.science_plots_for_result(context,
                                                      result,
                                                      applycal.AmpVsFrequencySummaryChart,
                                                      fields,
                                                      uv_range,
                                                      ApplycalAmpVsFreqSciencePlotRenderer)
                amp_vs_freq_detail_plots[vis] = plots

                plots = self.science_plots_for_result(context,
                                                      result,
                                                      applycal.AmpVsUVSummaryChart,
                                                      fields,
                                                      renderer_cls=ApplycalAmpVsUVSciencePlotRenderer)
                amp_vs_uv_detail_plots[vis] = plots

        for d, plotter_cls in (
                (amp_vs_freq_detail_plots, ApplycalAmpVsFreqSciencePlotRenderer),
                (amp_vs_uv_detail_plots, ApplycalAmpVsUVSciencePlotRenderer)):
            if d:
                all_plots = list(utils.flatten([v for v in d.values()]))
                renderer = plotter_cls(context, results, all_plots)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                if plotter_cls is ApplycalAmpVsFreqSciencePlotRenderer:
                    amp_vs_freq_subpages.update((vis, renderer.path) for vis in d.keys())

        return amp_vs_freq_summary_plots, amp_vs_freq_subpages, amp_vs_uv_summary_plots, max_uvs

    @staticmethod
    def science_plots_for_result(
            context: Context, 
            result: ApplycalResults,
            plotter_cls: Type[Union[applycal.PlotmsAntComposite, applycal.PlotmsSpwComposite, 
                                    applycal.PlotmsBasebandComposite, applycal.PlotmsFieldComposite, 
                                    applycal.PlotmsFieldSpwComposite, applycal.PlotmsSpwAntComposite, 
                                    applycal.PlotmsFieldSpwAntComposite]], 
            fields: Iterable[int], 
            uvrange: Optional[str]=None, 
            renderer_cls: Optional[Type[JsonPlotRenderer]]=None, 
            preserve_coloraxis: bool=False
    ) -> List[Plot]:
        """
        Create science plots for result
        
        Create science plots for result.
        Args:
            context:            Pipeline Context
            result:             Applycal Results
            plotter_cls:        Plotter class
            fields:             List of field_ids
            uvrange:            UV range
            renderer_cls:       Renderer class
            preserve_coloraxis: True to preserve predefined 'coloraxis' (for SD)
                                False to override 'coloraxis' with 'spw' (default)
        Returns:
            List[Plot]: List of Plot instances.
        """
        # preserve coloraxis if necessary (PIPE-710: SD needs to preserve 'coloraxis')
        overrides = {} if preserve_coloraxis else {'coloraxis': 'spw'}

        if uvrange is not None:
            overrides['uvrange'] = uvrange
            # CAS-9395: ALMA pipeline weblog plot of calibrated amp vs.
            # frequency with avgantenna=True and a uvrange upper limit leads
            # to misleading results and wrong conclusions
            overrides['avgantenna'] = False

        plots = []
        plot_output_dir = os.path.join(context.report_dir, 'stage{}'.format(result.stage_number))
        calto, _ = _get_data_selection_for_plot(context, result, ['TARGET'])

        for field in fields:
            # override field when plotting amp/phase vs frequency, as otherwise
            # the field is resolved to a list of all field IDs  
            overrides['field'] = field

            plotter = plotter_cls(context, plot_output_dir, calto, 'TARGET', **overrides)
            plots.extend(plotter.plot())

        for plot in plots:
            plot.parameters['intent'] = ['TARGET']

        if renderer_cls is not None:
            renderer = renderer_cls(context, result, plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())        

        return plots

    def create_plots(self, context, results, plotter_cls, intents, renderer_cls=None, **kwargs):
        """
        Create plots and return (dictionary of vis:[Plots], dict of vis:subpage URL).
        """
        d = {}
        subpages = {}

        for result in results:
            plots, href = self.plots_for_result(context, result, plotter_cls, intents, renderer_cls, **kwargs)
            d = utils.dict_merge(d, plots)

            vis = os.path.basename(result.inputs['vis'])
            subpages[vis] = href

        return d, subpages

    def plots_for_result(self, context, result, plotter_cls, intents, renderer_cls=None, **kwargs):
        vis = os.path.basename(result.inputs['vis'])
        output_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        calto, str_intents = _get_data_selection_for_plot(context, result, intents)

        plotter = plotter_cls(context, output_dir, calto, str_intents, **kwargs)
        plots = plotter.plot()
        for plot in plots:
            plot.parameters['intent'] = intents

        d = {vis: plots}

        path = None
        if renderer_cls is not None:
            renderer = renderer_cls(context, result, plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                path = renderer.path

        return d, path

    def calapps_for_result(self, result):
        calapps = collections.defaultdict(list)
        for calapp in result.applied:
            vis = os.path.basename(calapp.vis)
            calapps[vis].append(calapp)
        return calapps

    def caltypes_for_result(self, result):
        type_map = {
            'bandpass': 'Bandpass',
            'gaincal': 'Gain',
            'tsys': 'T<sub>sys</sub>',
            'wvr': 'WVR',
            'ps': 'Sky',
        }

        d = {}
        for calapp in result.applied:
            for calfrom in calapp.calfrom:
                caltype = type_map.get(calfrom.caltype, calfrom.caltype)

                if calfrom.caltype == 'gaincal':
                    # try heuristics to detect phase-only and amp-only 
                    # solutions 
                    caltype += self.get_gain_solution_type(calfrom.gaintable)

                d[calfrom.gaintable] = caltype

        return d

    def get_gain_solution_type(self, gaintable):
        # CAS-9835: hif_applycal() "type" descriptions are misleading /
        # incomplete in weblog table
        #
        # quick hack: match filenamer-generated file names
        #
        # TODO find a way to attach the originating task to the callibrary entries
        if gaintable.endswith('.gacal.tbl'):
            return ' (amplitude only)'
        if gaintable.endswith('.gpcal.tbl'):
            return ' (phase only)'
        if gaintable.endswith('.gcal.tbl'):
            return ''

        # resort to inspecting caltable values to infer what its type is

        # solve circular import problem by importing at run-time
        from pipeline.infrastructure import casa_tasks

        # get stats on amp solution of gaintable 
        calstat_job = casa_tasks.calstat(caltable=gaintable, axis='amp', 
                                         datacolumn='CPARAM', useflags=True)
        calstat_result = calstat_job.execute(dry_run=False)        
        stats = calstat_result['CPARAM']

        # amp solutions of unity imply phase-only was requested
        tol = 1e-3
        no_amp_soln = all([utils.approx_equal(stats['sum'], stats['npts'], tol),
                           utils.approx_equal(stats['min'], 1, tol),
                           utils.approx_equal(stats['max'], 1, tol)])

        # same again for phase solution
        calstat_job = casa_tasks.calstat(caltable=gaintable, axis='phase', 
                                         datacolumn='CPARAM', useflags=True)
        calstat_result = calstat_job.execute(dry_run=False)        
        stats = calstat_result['CPARAM']

        # phase solutions ~ 0 implies amp-only solution
        tol = 1e-5
        no_phase_soln = all([utils.approx_equal(stats['sum'], 0, tol),
                             utils.approx_equal(stats['min'], 0, tol),
                             utils.approx_equal(stats['max'], 0, tol)])

        if no_phase_soln and not no_amp_soln:
            return ' (amplitude only)'
        if no_amp_soln and not no_phase_soln:
            return ' (phase only)'
        return ''


class ApplycalAmpVsFreqPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs frequency for %s' % vis
        outfile = filenamer.sanitize('amp_vs_freq-%s.html' % vis)

        super(ApplycalAmpVsFreqPlotRenderer, self).__init__(
                'generic_x_vs_y_field_spw_ant_detail_plots.mako', context, 
                result, plots, title, outfile)


class ApplycalPhaseVsFreqPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated phase vs frequency for %s' % vis
        outfile = filenamer.sanitize('phase_vs_freq-%s.html' % vis)

        super(ApplycalPhaseVsFreqPlotRenderer, self).__init__(
                'generic_x_vs_y_field_spw_ant_detail_plots.mako', context, 
                result, plots, title, outfile)


class ApplycalAmpVsFreqSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs frequency for %s' % vis
        outfile = filenamer.sanitize('science_amp_vs_freq-%s.html' % vis)

        super(ApplycalAmpVsFreqSciencePlotRenderer, self).__init__(
                'generic_x_vs_y_spw_field_detail_plots.mako', context,
                result, plots, title, outfile)


class ApplycalAmpVsFreqPerAntSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    """
    Class to render 'per antenna' Amp vs Freq plots for applycal
    """
    def __init__(self, 
                 context: Context, 
                 result: ApplycalResults, 
                 plots: List[Plot]
    ) -> None:
        """
        Construct ApplycalAmpVsFreqPerAntSciencePlotRenderer instance

        Args:
            context: Pipeline context
            result:  Applycal Results
            plots:   Liost of Plot instances
        """
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs frequency for %s' % vis
        outfile = filenamer.sanitize('science_amp_vs_freq-%s.html' % vis)

        super(ApplycalAmpVsFreqPerAntSciencePlotRenderer, self).__init__(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
            result, plots, title, outfile)


class ApplycalAmpVsUVSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs UV distance for %s' % vis
        outfile = filenamer.sanitize('science_amp_vs_uv-%s.html' % vis)

        super(ApplycalAmpVsUVSciencePlotRenderer, self).__init__(
                'generic_x_vs_y_spw_field_detail_plots.mako', context,
                result, plots, title, outfile)


class ApplycalAmpVsUVPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs UV distance for %s' % vis
        outfile = filenamer.sanitize('amp_vs_uv-%s.html' % vis)

        super(ApplycalAmpVsUVPlotRenderer, self).__init__(
                'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
                result, plots, title, outfile)


class ApplycalPhaseVsUVPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated phase vs UV distance for %s' % vis
        outfile = filenamer.sanitize('phase_vs_uv-%s.html' % vis)

        super(ApplycalPhaseVsUVPlotRenderer, self).__init__(
                'generic_x_vs_y_spw_ant_plots.mako', context, 
                result, plots, title, outfile)


class ApplycalAmpVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated amplitude vs times for %s' % vis
        outfile = filenamer.sanitize('amp_vs_time-%s.html' % vis)

        super(ApplycalAmpVsTimePlotRenderer, self).__init__(
                'generic_x_vs_y_spw_ant_plots.mako', context,
                result, plots, title, outfile)


class ApplycalPhaseVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Calibrated phase vs times for %s' % vis
        outfile = filenamer.sanitize('phase_vs_time-%s.html' % vis)

        super(ApplycalPhaseVsTimePlotRenderer, self).__init__(
                'generic_x_vs_y_field_spw_ant_detail_plots.mako', context, 
                result, plots, title, outfile)


def _get_data_selection_for_plot(context, result, intent):
    """
    Inspect a result, returning a CalTo that matches the data selection of the
    applied calibration.

    Background: we don't want to create plots for an entire MS, only the data
    selection of interest. Rather than calculate and explicitly pass in the
    data selection of interest, this function calculates the data selection of
    interest by inspecting the results and extracting the data selection that 
    the calibration is applied to.

    :param context: pipeline Context
    :param result: a Result with an .applied property containing CalApplications
    :param intent: pipeline intent
    :return: 
    """
    spw = _get_calapp_arg(result, 'spw')
    field = _get_calapp_arg(result, 'field')
    antenna = _get_calapp_arg(result, 'antenna')
    intent = ','.join(intent).upper()

    vis = {calapp.vis for calapp in result.applied}
    assert (len(vis) is 1)
    vis = vis.pop()

    wanted = set(intent.split(','))
    fields_with_intent = set()
    for f in context.observing_run.get_ms(vis).get_fields(field):
        intersection = f.intents.intersection(wanted)
        if not intersection:
            continue
        fields_with_intent.add(f.name)
    field = ','.join(fields_with_intent)

    calto = callibrary.CalTo(vis, field, spw, antenna, intent)

    return calto, intent


def _get_calapp_arg(result, arg):
    s = set()
    for calapp in result.applied:
        s.update(utils.safe_split(getattr(calapp, arg)))
    return ','.join(s)


def get_brightest_field(ms, source, intent='TARGET'):
    """
    Analyse all fields associated with a source, identifying the brightest
    field as the field with highest median flux averaged over all spws.

    :param ms: measurementset to analyse
    :param source: representative source
    :param intent:
    :return:
    """
    # get IDs for all science spectral windows
    spw_ids = set()
    for scan in ms.get_scans(scan_intent=intent):
        scan_spw_ids = {dd.spw.id for dd in scan.data_descriptions}
        spw_ids.update(scan_spw_ids)

    if intent == 'TARGET':
        science_ids = {spw.id for spw in ms.get_spectral_windows()}
        spw_ids = spw_ids.intersection(science_ids)

    fields_for_source = [f for f in source.fields if intent in f.intents]

    # give the sole science target name if there's only one science target in this ms.
    if len(fields_for_source) == 1:
        LOG.info('Only one %s target for Source #%s. Bypassing brightest target selection.', intent, source.id)
        return fields_for_source[0]

    visstat_fields, visstat_spws = get_visstat_data_selection(ms, fields_for_source, spw_ids, intent)
    if not visstat_fields:
        LOG.info('All data flagged. Bypassing brightest target selection.')
        return fields_for_source[0]

    # a list of (field, field flux) tuples
    median_flux = []

    # defines the parameters for the visstat job
    job_params = {
        'vis': ms.name,
        'axis': 'amp',
        'datacolumn': 'corrected',
        'spw': ','.join((str(spw_id) for spw_id in sorted(visstat_spws))),
        'field': ','.join((str(field.id) for field in sorted(visstat_fields, key=operator.attrgetter('id')))),
        'intent': utils.to_CASA_intent(ms, intent),
        'reportingaxes': 'field',
        'useflags': True
    }

    # run visstat for each scan selection for the target
    LOG.info('Calculating which %s field has the highest median flux for Source #%s', intent, source.id)
    job = casa_tasks.visstat(**job_params)
    visstat_result = job.execute(dry_run=False)

    # representative visstat output:
    #  'FIELD_ID=6': {'median': 2.4579226970672607}
    for k, v in visstat_result.items():
        _, field_id = k.split('=')
        measurement_field = [f for f in fields_for_source if f.id == int(field_id)][0]
        median_flux.append((measurement_field, float(v['median'])))

    LOG.debug('Median flux for %s targets:', intent)
    for field, field_flux in median_flux:
        LOG.debug('\t{!r} ({}): {}'.format(field.name, field.id, field_flux))

    # find the ID of the field with the highest average flux
    sorted_by_flux = sorted(median_flux, key=operator.itemgetter(1), reverse=True)
    brightest_field, highest_flux = sorted_by_flux[0]

    LOG.info('{} field {!r} (#{}) has highest median flux ({})'.format(
        intent, brightest_field.name, brightest_field.id, highest_flux
    ))
    return brightest_field


def get_visstat_data_selection(ms, fields_for_source, spw_ids, intent):
    """
    Validate a visstat data selection, removing field/spw combinations that
    are completely flagged.

    :param ms: MeasurementSet object, used to get MS filename
    :param fields_for_source: iterable of Field domain objects
    :param spw_ids: iterable of integer spw IDs
    :return: ([Field, Field, ...], [int, int, ...])
    """
    LOG.info('Finding flagged data selections for {}'.format(ms.basename))

    #
    # PIPE-446: handle visstat exception
    #
    # Visstat raises an exception if a field is completely flagged.
    # Unfortunately it doesn't report which is the problem field, so we have
    # to find that by inspecting flagdata summaries, one for each field.
    #
    casa_intent = utils.to_CASA_intent(ms, intent)
    inpfile = ["mode='summary' name='field_{}_spw_{}' field='{}' spw='{}' intent='{}'".format(field.id, spw_id, field.id, spw_id, casa_intent)
               for field in sorted(fields_for_source, key=operator.attrgetter('id'))
               for spw_id in sorted(spw_ids)]
    flagdata_job = casa_tasks.flagdata(vis=ms.name, datacolumn='corrected', mode='list', inpfile=inpfile)
    flagdata_result = flagdata_job.execute(dry_run=False)

    spw_to_fields_for_visstat_job = {spw: set(fields_for_source) for spw in spw_ids}

    for flagdata_summary in flagdata_result.values():
        num_flagged_rows = flagdata_summary['flagged']
        num_rows = flagdata_summary['total']
        if num_flagged_rows == num_rows:
            _, flagged_field, _, flagged_spw = flagdata_summary['name'].split('_')
            LOG.info('Discarding field %s spw %s as a visstat candidate', flagged_field, flagged_spw)
            field_to_remove = ms.fields[int(flagged_field)]
            spw_to_fields_for_visstat_job[int(flagged_spw)].remove(field_to_remove)

    # Take the spw(s) with the largest number of <100% flagged fields.
    #
    # Different spws could have the same number of fields but a different
    # selection of fields (e.g., a different field in each spw has been flagged).
    # We don't care which field set is selected when multiple spws give the
    # same number of good fields.
    fields_for_job = max(spw_to_fields_for_visstat_job.values(), key=len)

    # Identify the spws that are good for the job fields we just identified.
    spws_for_job_fields = {spw_id for spw_id, fields_for_spw in spw_to_fields_for_visstat_job.items()
                           if fields_for_job.issubset(fields_for_spw)}

    return fields_for_job, spws_for_job_fields


def copy_callibrary(results: ResultsList, report_dir: str) -> Dict[str, str]:
    """
    Copy callibrary files across to the weblog stage directory, returning a
    Dict mapping MS name to the callibrary location on disk.
    """
    stage_dir = os.path.join(report_dir, f'stage{results.stage_number}')

    vis_to_callib = {}

    for result in results:
        if not result.callib_map:
            continue

        for vis, callib_src in result.callib_map.items():
            # copy callib file across to weblog directory
            callib_basename = os.path.basename(callib_src)
            callib_dst = os.path.join(stage_dir, os.path.basename(callib_basename))
            LOG.debug('Copying callibrary: src=%s dst=%s', callib_src, callib_dst)
            shutil.copyfile(callib_src, callib_dst)

            vis_to_callib[os.path.basename(vis)] = callib_dst

    return vis_to_callib


def deduplicate(context: Context, all_plots: Dict[str, List[Plot]]) -> Dict[str, List[Plot]]:
    """
    Process a dict mapping vis to plots, deduplicating the plot list for
    each MS.
    """
    result = {}
    for vis, vis_plots in all_plots.items():
        ms = context.observing_run.get_ms(name=vis)
        deduplicated = _deduplicate_plots(ms, vis_plots)
        result[vis] = deduplicated
    return result


def _deduplicate_plots(ms: MeasurementSet, plots: List[Plot]) -> List[Plot]:
    """
    Deduplicate plots by discarding extra plots created for the same scan.
    The remaining plot is relabelled as applicable to the discarded intents.
    """
    # holds the final deduplicated list of plots
    deduplicated: List[Plot] = []
    LOG.debug("deduplication started")
    # General algorithm is 'what scan does this spw and intent correspond to? 
    # Has this scan already been plotted? If so, discard the plot.'
    #
    # Plots are made per spw, per intent. Duplicate plots should be removed by
    # navigating down to the spw and filtering per spw. Just because spw 16 
    # BANDPASS cal is also spw 16 AMPLITUDE cal doesn't mean the same holds
    # for other spectral windows.

    # define functions to get spw and intent for a plot. These are used to
    # sort and group the plots.
    spw_fn = lambda plot: plot.parameters['spw']
    intent_fn = lambda plot: plot.parameters['intent']

    # First, group plots by spw
    plots_by_spw = sorted(plots, key=spw_fn)
    for spw, spw_plots in itertools.groupby(plots_by_spw, spw_fn):
        # Store group iterator as a list
        spw_plots = list(spw_plots)

        # dict to map scan ID to plots for that scan.
        plots_for_scan = {}

        # now group these plots by intent. The intent ordering used here will
        # be reflected in the order of the relabelled intents.
        spw_plots_by_intent = sorted(spw_plots, key=intent_fn)
        for intent, spw_intent_plots in itertools.groupby(spw_plots_by_intent, intent_fn):
            # Store group iterator as a list
            spw_intent_plots = list(spw_intent_plots)  

            # This should result in a single plot. If not, bail. Failsafe
            # behaviour is to return original list of plots. It's better to
            # have duplicates present than exceptions.
            if len(spw_intent_plots) != 1:
                LOG.warning('Plot deduplication cancelled. '
                            'Could not process ambiguous plot for spw %s intent %s', spw, intent)
                return plots
            plot = spw_intent_plots[0]

            scan_ids = sorted({(scan.id) for scan in ms.get_scans(scan_intent=intent, spw=spw)})
            scan_ids = ','.join(str(s) for s in scan_ids)
            if scan_ids not in plots_for_scan:
                LOG.debug('Retaining plot for scan %s (spw=%s intent=%s)', scan_ids, spw, intent)
                plots_for_scan[scan_ids] = plot
            else:
                LOG.debug('Discarding duplicate plot for scan %s (spw=%s intent=%s)', scan_ids, spw, intent)
                # intents are strings inside lists, e.g., ['BANDPASS']
                old_intent = intent_fn(plots_for_scan[scan_ids])
                new_intent = [','.join(sorted(set(old_intent).union(set(intent))))]
                LOG.info('Deduplicating plot: spw %s %s -> %s', spw, old_intent, new_intent)
                plots_for_scan[scan_ids].parameters['intent'] = new_intent

        deduplicated.extend(plots_for_scan.values())
    LOG.debug("deduplication complete")
    return deduplicated
