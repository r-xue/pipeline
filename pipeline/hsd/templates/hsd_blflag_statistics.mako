<%!
import collections
rsc_path = ""
SELECTORS = ['vis', 'type', 'spw', 'ant', 'field', 'pol']
HISTOGRAM_LABELS = collections.OrderedDict([
    ( 'outlier_Tsys',         'Outlier Tsys' ),
    ( 'rms_prefit',           'Baseline RMS pre-fit'   ),
    ( 'rms_postfit',          'Baseline RMS post-fit'  ),
    ( 'runmean_prefit',       'Running mean pre-fit'   ),
    ( 'runmean_postfit',      'Running mean post-fit'  ),
    ( 'expected_rms_prefit',  'Expected RMS pre-fit'   ),
    ( 'expected_rms_postfit', 'Expected RMS post-fit'  )
])

# set all X-axis labels to Kelvin
HISTOGRAM_AXES = collections.OrderedDict([
    ( 'outlier_Tsys',         'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'rms_prefit',           'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'rms_postfit',          'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'runmean_prefit',       'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'runmean_postfit',      'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'expected_rms_prefit',  'PLOTS.xAxisLabels["Flagged fraction"]' ),
    ( 'expected_rms_postfit', 'PLOTS.xAxisLabels["Flagged fraction"]' )
])
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    if len(pcontext.observing_run.measurement_sets)>1:
        multi_vis = True    
    else:
        multi_vis = False
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: p.parameters['spw'])}">
        <%def name="mouseover(plot)">Click to magnify plot for ${plot.parameters['vis']} field ${plot.parameters['field']} ${plot.parameters['ant']} Spw ${plot.parameters['spw']} pol ${plot.parameters['pol']} ${plot.parameters['type']}</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
		Field: ${plot.parameters['field']}<br>
		Antenna: ${plot.parameters['ant']}<br>
		Spectral Window: ${plot.parameters['spw']}<br>
        Polarization: ${plot.parameters['pol']}<br>
        ${plot.parameters['type']}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
		${plot.parameters['vis']}<br>
        % endif
        ${plot.parameters['field']}<br>
        ${plot.parameters['ant']}<br>
        Spw ${plot.parameters['spw']}<br>
        Pol ${plot.parameters['pol']}<br>
        ${plot.parameters['type']}<br>
    </%def>
</%self:render_plots>
