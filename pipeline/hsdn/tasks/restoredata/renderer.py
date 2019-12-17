import os
import collections
import shutil
import itertools
import operator

import pipeline.infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.casatools as casatools
from . import csvfilereader

from pipeline.h.tasks.applycal.renderer import *
from pipeline.h.tasks.common.displays import applycal as applycal
from pipeline.hsd.tasks.common import utils as sdutils

LOG = logging.get_logger(__name__)

JyperKTRV = collections.namedtuple('JyperKTRV', 'virtualspw msname realspw antenna pol factor')
JyperKTR  = collections.namedtuple('JyperKTR',  'spw msname antenna pol factor')
FlagTotal = collections.namedtuple('FlagSummary', 'flagged total')

class T2_4MDetailsNRORestoreDataRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='hsdn_restoredata.mako', 
                 description='Restoredata with scale adjustment between beams for NRO FOREST data.',
                 always_rerender=False):
        super(T2_4MDetailsNRORestoreDataRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):

        ctx_result = ctx['result']
        ctx_result0 = ctx_result[0]
        ctx_result0_inputs = ctx_result0.inputs
        inputs = {}
        for key, value in ctx_result0_inputs.items():
            if 'rawdata_dir' in key or 'products_dir' in key or 'vis' in key or 'output_dir' in key or 'reffile' in key:
                inputs[key] = value
        result_inputs = inputs
        LOG.debug('result_inputs = {0}'.format(result_inputs));
        ctx['result'].inputs = result_inputs
        reffile = None
        spw_factors = collections.defaultdict(lambda: [])
        valid_spw_factors = collections.defaultdict(lambda: collections.defaultdict(lambda: []))

        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        trfunc_r = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTR(_rspwid, _vis, _antenna, _pol, _factor)
        trfunc_v = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTRV(_vspwid, _vis, _rspwid, _antenna, _pol, _factor)
        trfunc = trfunc_v if dovirtual else trfunc_r

        res0 = results[0]

        ampcal_results = res0.ampcal_results
        applycal_results = res0.applycal_results

        for r in ampcal_results:
            metadata = None
            # Read reffile and insert the elements into a list "lines".
            reffile = r.reffile
            if not os.path.exists(reffile):
                LOG.warn('The factor file is invalid format: os.path.exists(reffile) = {0}'.format(os.path.exists(reffile)));
                metadata = ['No Data : No Data']
                break
            else:
                LOG.info('os.path.exists(reffile) = {0}'.format(os.path.exists(reffile)));
                with open(reffile, 'r') as f:
                    lines = f.readlines()
                # Count the line numbers for the beginning of metadata part and the end of it.
                if len(lines) == 0:
                    LOG.warn('The factor file is invalid format: size of reffile = {0}'.format(len(lines)));
                    metadata = ['No Data : No Data']
                    break
                else:
                    count = 0
                    beginpoint = 0
                    endpoint = 0
                    for elem in lines:
                        count += 1
                        if elem.startswith('#---Fill'):
                            beginpoint = count
                            LOG.debug('beginpoint = {0}'.format(beginpoint))
                        if elem.startswith('#---End'):
                            endpoint = count
                            LOG.debug('endpoint = {0}'.format(endpoint))
                            continue
                    # Insert the elements (from beginpoint to endpoint) into a list "metadata_tmp".
                    metadata_tmp = []
                    elem = ""
                    key = ""
                    value = ""
                    multivalue = ""
                    felem = ""
                    count = 0
                    for elem in lines:
                        count += 1
                        if count < beginpoint + 1:
                            continue
                        if count >= endpoint:
                            continue
                        elem = elem.replace('\r','')
                        elem = elem.replace('\n','')
                        elem = elem.replace('#','')
                        elem = elem.lstrip()
                        check = elem.split()
                        # The lines without "#" are regarded as all FreeMemo's values.
                        if len(elem) == 0:
                            LOG.debug('Skipped the blank line of the reffile.');
                            continue
                        else:
                            if not ":" in check[0]:
                                key = 'FreeMemo'
                                value = elem
                                elem = key + ':' + value
                            else:
                                onepair = elem.split(':', 1)
                                key = "".join(onepair[0])
                                value = "".join(onepair[1])
                                elem = key + ':' + value
                        metadata_tmp.append(elem)

                    if len(metadata_tmp) == 0:
                        LOG.info('The factor file is invalid format. [No Data : No Data] is inserted instead of blank.')
                        metadata = ['No Data : No Data']
                        break
                    else:
                        LOG.info('metadata_tmp: {0}'.format(metadata_tmp))
                        # Arrange "metadata_tmp" list to "metadata" list to connect FreeMemo values.
                        metadata = []
                        elem = ""
                        for elem in metadata_tmp:
                            onepair = elem.split(':', 1)
                            key = "".join(onepair[0])
                            value = "".join(onepair[1])
                            if 'FreeMemo' in key:
                                multivalue += value + '<br>'
                                elem = key + ':' + multivalue
                            else:
                                elem = key + ':' + value
                                metadata.append(elem)
                        felem = 'FreeMemo:' + multivalue
                        metadata.append(felem)
                        LOG.info('metadata: {0}'.format(metadata))
        ctx.update({'metadata': metadata})

        for r in ampcal_results:
            # rearrange scaling factors
            ms = context.observing_run.get_ms(name=r.vis)
            vis = ms.basename
            spw_band = {}
            for spw in ms.get_spectral_windows(science_windows_only=True):
                spwid = spw.id
                vspwid = context.observing_run.real2virtual_spw_id(spwid, ms)
                ddid = ms.get_data_description(spw=spwid)
                
                if vspwid not in spw_band:
                    spw_band[vspwid] = spw.band

                for ant in ms.get_antenna():
                    LOG.debug('ant = {0}'.format(ant));
                    ant_name = ant.name
                    mp = map(ddid.get_polarization_label, range(ddid.num_polarizations));
                    corrs = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))

                     # an attempt to collapse pol rows
                     # corr_collector[factor] = [corr0, corr1, ...]
                    corr_collector = collections.defaultdict(lambda: [])
                    for corr in corrs:
                        factor = self.__get_factor(r.factors, vis, spwid, ant_name, corr)

                        corr_collector[factor].append(corr)
                    for factor, corrlist in corr_collector.iteritems():
                        corr = str(', ').join(corrlist)
                        jyperk = factor if factor is not None else 'N/A (1.0)'

                        tr = trfunc(vspwid, vis, spwid, ant_name, corr, jyperk)
                        spw_factors[vspwid].append(tr)
                        if factor is not None:
                            valid_spw_factors[vspwid][corr].append(factor)
            reffile = r.reffile
            LOG.debug('reffile = {0}'.format(reffile));
        stage_dir = os.path.join(context.report_dir, 'stage%s' % ampcal_results.stage_number)

        # input file to correct relative amplitude 
        reffile_copied = None
        if reffile is not None and os.path.exists(reffile):
            shutil.copy2(reffile, stage_dir)
            reffile_copied = os.path.join(stage_dir, os.path.basename(reffile))
        # order table rows so that spw comes first
        row_values = []
        for factor_list in spw_factors.itervalues():
            row_values += list(factor_list)

        # set context
        ctx.update({'jyperk_rows': utils.merge_td_columns(row_values),
                    'reffile': reffile_copied,
                    'dovirtual': dovirtual})

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % applycal_results.stage_number)
        LOG.debug('weblog_dir = {0}'.format(weblog_dir));

        flag_totals = {}
        for r in applycal_results:
            LOG.debug('r in applycal_results = {0}'.format(r));

            if r.inputs['flagsum'] == True:
                flag_totals = utils.dict_merge(flag_totals, self.flags_for_result(r, context))

        calapps = {}
        for r in applycal_results:
            calapps = utils.dict_merge(calapps,
                                       self.calapps_for_result(r))
        LOG.debug('calapps = {0}'.format(calapps));

        caltypes = {}
        for r in applycal_results:
            caltypes = utils.dict_merge(caltypes,
                                        self.caltypes_for_result(r))
        LOG.debug('caltypes = {0}'.format(caltypes));

        filesizes = {}
        for r in applycal_results:
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

        # CAS-5970: add science target plots to the applycal page
        (science_amp_vs_freq_summary_plots, uv_max) = self.create_single_dish_science_plots(context, applycal_results)
        ctx.update({
            'science_amp_vs_freq_plots': science_amp_vs_freq_summary_plots,
            'uv_max': uv_max,
        })
        LOG.debug('ctx = {0}'.format(ctx));

    @staticmethod
    def science_plots_for_result(context, result, plotter_cls, fields, uvrange=None, renderer_cls=None):
        overrides = {'coloraxis': 'spw'}

        if uvrange is not None:
            overrides['uvrange'] = uvrange
            # CAS-9395: ALMA pipeline weblog plot of calibrated amp vs.
            # frequency with avgantenna=True and a uvrange upper limit leads
            # to misleading results and wrong conclusions
            overrides['avgantenna'] = False

        plots = []
        plot_output_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
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

    def create_single_dish_science_plots(self, context, results):
        """
        Create plots for the science targets, returning two dictionaries of 
        vis:[Plots].
        MODIFIED for single dish
        """
        amp_vs_freq_summary_plots = collections.defaultdict(dict)
        max_uvs = collections.defaultdict(dict)

        amp_vs_freq_detail_plots = {}
        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)
            max_uvs[vis] = measures.Distance(value=0.0, units=measures.DistanceUnits.METRE)
            amp_vs_freq_summary_plots[vis] = []

            # Plot for 1 science field (either 1 science target or for a mosaic 1
            # pointing). The science field that should be chosen is the one with
            # the brightest average amplitude over all spws
            representative_source_name, _ = ms.get_representative_source_spw()
            representative_source = {s for s in ms.sources if s.name == representative_source_name}
            if len(representative_source) >= 1:
                representative_source = representative_source.pop()

            brightest_field = get_brightest_field(ms, representative_source)
            plots = self.science_plots_for_result(context,
                                                  result,
                                                  applycal.RealVsFrequencySummaryChart,
                                                  [brightest_field.id], None)
            for plot in plots:
                plot.parameters['source'] = representative_source
            amp_vs_freq_summary_plots[vis].extend(plots)

            if pipeline.infrastructure.generate_detail_plots(result):
                fields = set()
                # scans = ms.get_scans(scan_intent='TARGET')
                # for scan in scans:
                #     fields.update([field.id for field in scan.fields])
                with casatools.MSMDReader(vis) as msmd:
                    fields.update(list(msmd.fieldsforintent("OBSERVE_TARGET#ON_SOURCE")))

                # Science target detail plots. Note that summary plots go onto the
                # detail pages; we don't create plots per spw or antenna
                plots = self.science_plots_for_result(context,
                                                      result,
                                                      applycal.RealVsFrequencySummaryChart,
                                                      fields, None,
                                                      ApplycalAmpVsFreqSciencePlotRenderer)
                amp_vs_freq_detail_plots[vis] = plots

        for d, plotter_cls in (
                (amp_vs_freq_detail_plots, ApplycalAmpVsFreqSciencePlotRenderer),):
            if d:
                all_plots = list(utils.flatten([v for v in d.itervalues()]))
                renderer = plotter_cls(context, result, all_plots)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())

        return amp_vs_freq_summary_plots, max_uvs

    @staticmethod
    def __get_factor(factor_dict, vis, spwid, ant_name, pol_name):
        """
        Returns a factor corresponding to vis, spwid, ant_name, and pol_name from
        a factor_dict[vis][spwid][ant_name][pol_name] = factor
        If factor_dict lack corresponding factor, the method returns None.
        """
        if (vis not in factor_dict or
                spwid not in factor_dict[vis] or
                ant_name not in factor_dict[vis][spwid] or
                pol_name not in factor_dict[vis][spwid][ant_name]):
            return None
        return factor_dict[vis][spwid][ant_name][pol_name]


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

    def flags_for_result(self, result, context):
        ms = context.observing_run.get_ms(result.inputs['vis'])
        summaries = result.summaries
        by_intent = self.flags_by_intent(ms, summaries)
        by_spw = self.flags_by_science_spws(ms, summaries)
        return {ms.basename: utils.dict_merge(by_intent, by_spw)}

    def flags_by_intent(self, ms, summaries):
        # create a dictionary of scans per observing intent, eg. 'PHASE':[1,2,7]
        intent_scans = {}
        for intent in ('BANDPASS', 'PHASE', 'AMPLITUDE', 'CHECK', 'TARGET'):
            # convert IDs to strings as they're used as summary dictionary keys
            intent_scans[intent] = [str(s.id) for s in ms.scans
                                    if intent in s.intents]

        # while we're looping, get the total flagged by looking in all scans 
        intent_scans['TOTAL'] = [str(s.id) for s in ms.scans]
        total = collections.defaultdict(dict)
        previous_summary = None
        for summary in summaries:

            for intent, scan_ids in intent_scans.iteritems():
                flagcount = 0
                totalcount = 0

                for i in scan_ids:
                    # workaround for KeyError exception when summary 
                    # dictionary doesn't contain the scan
                    if i not in summary['scan']:
                        continue

                    flagcount += int(summary['scan'][i]['flagged'])
                    totalcount += int(summary['scan'][i]['total'])

                    if previous_summary:
                        flagcount -= int(previous_summary['scan'][i]['flagged'])

                ft = FlagTotal(flagcount, totalcount)
                total[summary['name']][intent] = ft

            previous_summary = summary

        return total 

    def flags_by_science_spws(self, ms, summaries):
        science_spws = ms.get_spectral_windows(science_windows_only=True)
        total = collections.defaultdict(dict)
        previous_summary = None
        for summary in summaries:
            flagcount = 0
            totalcount = 0
            for spw in science_spws:
                spw_id = str(spw.id)
                flagcount += int(summary['spw'][spw_id]['flagged'])
                totalcount += int(summary['spw'][spw_id]['total'])
                if previous_summary:
                    flagcount -= int(previous_summary['spw'][spw_id]['flagged'])
            ft = FlagTotal(flagcount, totalcount)
            total[summary['name']]['SCIENCE SPWS'] = ft
            previous_summary = summary
        return total


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
