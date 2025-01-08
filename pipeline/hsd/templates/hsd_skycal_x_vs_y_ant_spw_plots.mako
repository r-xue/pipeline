<%!
rsc_path = ""
SELECTORS = ['vis', 'ant', 'field', 'spw']
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
def get_caltable_from_result(result):
    calapps = result.outcome
    gaintable = [calapp.gaintable for calapp in calapps]
    gaintable = ', '.join(gaintable)
    return gaintable
%>

<%
    multi_vis = True
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: p.parameters['spw'])}">
	<%def name="mouseover(plot)">Click to magnify plot for ${plot.parameters['vis']}, Antenna: ${plot.parameters['ant']}, Field: ${plot.parameters['field']}, Spw: ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
                ${plot.parameters['vis']}<br>
        % endif
                ${get_caltable_from_result(result)}<br>
		Field: ${plot.parameters['field']}<br>
		Antenna: ${plot.parameters['ant']}<br>
		Spw: ${plot.parameters['spw']}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
		${plot.parameters['vis']}<br>
        % endif
                ${get_caltable_from_result(result)}<br>
		Field: ${plot.parameters['field']}<br>
		Antenna: ${plot.parameters['ant']}<br>
		Spw: ${plot.parameters['spw']}
	</%def>
</%self:render_plots>
