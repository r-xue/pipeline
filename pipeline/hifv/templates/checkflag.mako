<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">RFI Flagging</%block>

<p>Flag possible RFI using rflag and tfcrop; checkflagmode=${result[0].inputs['checkflagmode']}</p>


% if result[0].inputs['checkflagmode'] in ('bpd','allcals', 'bpd-vlass', 'allcals-vlass', 'vlass-imaging'):

<%
plot_caption = 'Calibrated bandpass after flagging'
if  result[0].inputs['checkflagmode'] == 'vlass-imaging':
    plot_caption = 'Calibrated targets after flagging'
%>

<%self:plot_group plot_dict="${summary_plots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Checkflag Summary Plot
        </%def>

        <%def name="preamble()">


        </%def>


        <%def name="mouseover(plot)">Summary window</%def>



        <%def name="fancybox_caption(plot)">
            ${plot_caption}
        </%def>


        <%def name="caption_title(plot)">
            ${plot_caption}
        </%def>
</%self:plot_group>

%endif





% if result[0].inputs['checkflagmode'] in ('vlass-imaging'):

<%

# these functions are defined in template scope so we have access to the flags
# and agents context objects

def percent_flagged(flagsummary):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total is 0:
        return 'N/A'
    else:
        return '%0.1f%%' % (100.0 * flagged / total)

def percent_flagged_diff(flagsummary1, flagsummary2):
    flagged1 = flagsummary1.flagged
    flagged2 = flagsummary2.flagged
    total = flagsummary1.total

    if total is 0:
        return 'N/A'
    else:
        return '%0.1f%%' % (100.0 * (flagged2-flagged1) / total)
    

%>

<h2 id="flagged_data_summary" class="jumptarget">Checkflag Summary</h2>

% for ms in flags.keys():

<h4>Measurement Set: ${os.path.basename(ms)}</h4>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption></caption>
	<thead>
		<tr>
			<th rowspan="2">Antenna Selection</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th rowspan="2">flagged after</th>
            <th rowspan="2">flagged additional</th>
		</tr>
	</thead>
	<tbody>
		% for k in sorted(flags[ms]['by_antenna']['before'].keys()):
		<tr>
			<th style="text-align:center">${k}</th>
            <td>${percent_flagged(flags[ms]['by_antenna']['before'][k])}</td>
            <td>${percent_flagged(flags[ms]['by_antenna']['after'][k])}</td>
            <td>${percent_flagged_diff(flags[ms]['by_antenna']['before'][k],flags[ms]['by_antenna']['after'][k])}</td>
		</tr>
		% endfor
	</tbody>
</table>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of flagged data. Each cell states the amount of data
		flagged as a fraction of the specified data selection.
	</caption>
	<thead>
		<tr>
			<th rowspan="2">Spw Selection</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th rowspan="2">flagged after</th>
            <th rowspan="2">flagged additional</th>
		</tr>
	</thead>
	<tbody>
		% for k in sorted(flags[ms]['by_spw']['before'].keys(),key=int):
		<tr>
			<th style="text-align:center">${k}</th>
            <td>${percent_flagged(flags[ms]['by_spw']['before'][k])}</td>
            <td>${percent_flagged(flags[ms]['by_spw']['after'][k])}</td>
            <td>${percent_flagged_diff(flags[ms]['by_spw']['before'][k],flags[ms]['by_spw']['after'][k])}</td>
		</tr>
		% endfor
	</tbody>
</table>

% endfor

%endif