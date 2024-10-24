<%!
import html
rsc_path = ""
SELECTORS = ['vis', 'spw', 'field']
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    multi_vis = len({p.parameters['vis'] for p in plots}) > 1
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: html.escape(p.parameters['field'], True))}">
	<%def name="mouseover(plot)">Click to magnify plot for ${html.escape(plot.parameters['field'], True)} spw ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
		Field: ${html.escape(plot.parameters['field'], True)}<br>
		Spectral window: ${plot.parameters['spw']}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
		${html.escape(plot.parameters['field'], True)}<br>
		Spw ${plot.parameters['spw']}
	</%def>
</%self:render_plots>
