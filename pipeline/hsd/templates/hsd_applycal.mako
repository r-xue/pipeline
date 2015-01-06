<%!
import os
import string
import types

agent_description = {
	'before'   : 'Before',
	'applycal' : 'After',
}

total_keys = {
	'TOTAL'        : 'All Data',
	'SCIENCE SPWS' : 'Science Spectral Windows',
	'BANDPASS'     : 'Bandpass',
	'AMPLITUDE'    : 'Flux',
	'PHASE'        : 'Phase',
	'TARGET'       : 'Target'
}

def template_agent_header1(agent):
	span = 'col' if agent in ('online','template') else 'row'
	return '<th %sspan=2>%s</th>' % (span, agent_description[agent])

def template_agent_header2(agent):
	if agent in ('online', 'template'):
		return '<th>File</th><th>Number of Statements</th>'
	else:
		return ''		

def get_template_agents(agents):
	return [a for a in agents if a in ('online', 'template')]


%>
<%inherit file="t2-4m_details-base.html"/>

<script>
$(document).ready(function(){
	$(".fancybox").fancybox();
	$('.caltable_popover').popover({
		  template: '<div class="popover gaintable_popover"><div class="arrow"></div><div class="popover-inner"><h3 class="popover-title"></h3><div class="popover-content"><p></p></div></div></div>'
	});
});
</script>

<%
def space_comma(s):
	return ', '.join(string.split(s, ','))
	
def ifmap_to_spwmap(num_spw, ifmap):
    spwmap = []
    for i in xrange(num_spw):
        j = i
        for (k,v) in ifmap.items():
            if i in v:
                j = k
        spwmap.append(j)
    return spwmap
%>

<%block name="title">Apply calibration tables</%block>

<p>This task applies all calibrations registered with the pipeline to their target scantables.<p>

<h2>Applied calibrations</h2>
<table class="table table-bordered table-striped table-condensed"
	   summary="Applied Calibrations">
	<caption>Applied Calibrations</caption>
	<thead>
		<tr>
            <th rowspan="2">Scantable</th>
			<th colspan="4">Target</th>
			<th colspan="6">Calibration</th>
		</tr>
		<tr>
			<th>Intent</th>
			<th>Field</th>
			<th>Spw</th>
			<th>Antenna</th>
			<th>Type</th>
			<th>spwmap</th>
			<th>interp</th>
			<th>table</th>
		</tr>
	</thead>
	<tbody>
% for vis in calapps:
	% for calapp in calapps[vis]:
		<% ca_rowspan = len(calapp.calfrom) %>
		<% num_spw = len(pcontext.observing_run.get_scantable(os.path.basename(calapp.infile)).spectral_window) %>
		<tr>
			<td rowspan="${ca_rowspan}">${vis}</td>
			<td rowspan="${ca_rowspan}">${space_comma(calapp.calto.intent)}</td>
			<td rowspan="${ca_rowspan}">${space_comma(calapp.calto.field)}</td>
			<td rowspan="${ca_rowspan}">${space_comma(calapp.calto.spw)}</td>
			<td rowspan="${ca_rowspan}">${space_comma(calapp.calto.antenna)}</td>
		% for calfrom in calapp.calfrom:
			<td>${caltypes[calfrom.gaintable]}</td>
			<td>${', '.join([str(i) for i in ifmap_to_spwmap(num_spw, calfrom.spwmap)])}</td>
			<td>${space_comma(calfrom.interp if len(calfrom.interp) > 0 else 'linear')}</td>
			<td><a href="#" class="btn btn-small caltable_popover" data-toggle="popover" data-placement="top" data-content="${os.path.basename(calfrom.gaintable)}" title="" data-original-title="">View...</a></td>
		</tr>
		% endfor
	% endfor
% endfor		
	</tbody>
</table>

