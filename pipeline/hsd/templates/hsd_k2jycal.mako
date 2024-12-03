<%!
rsc_path = "../"
import os

import pipeline.infrastructure.utils as utils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Generate Kelvin to Jansky conversion caltables</%block>

<%
stage_dir = os.path.join(pcontext.report_dir, 'stage%s'%(result.stage_number))
observing_run = pcontext.observing_run
def id2name(spwid):
    return observing_run.virtual_science_spw_shortnames[observing_run.virtual_science_spw_ids[spwid]]
%>

<p>This task generates calibtation tables to convert the unit of single dish spectra from Kelvin to Jansky.</p>


<h3>Summary of Jy/K Conversion Factor</h3>
<p>
This plot shows the K/Jy factors across SPWs for different measurement sets.
</p>
% if jyperk_hist and len(jyperk_hist) > 0:
    <% plot = jyperk_hist[0] %>
    % if plot is not None and os.path.exists(plot.thumbnail):
        <%
            fullsize_relpath = os.path.relpath(plot.abspath, pcontext.report_dir)
            thumbnail_relpath = os.path.relpath(plot.thumbnail, pcontext.report_dir)
        %>

        <div class="thumbnail">
            <a href="${fullsize_relpath}"
               data-fancybox
               title='K/Jy Factors across SPWs'>
                <img class="lazyload"
                     data-src="${thumbnail_relpath}"
                     title="Click to show plot of K/Jy factors across SPWs">
            </a>

            <div class="caption">
                % if dovirtual:
                    <h4>K/Jy Factors across Virtual Spectral Windows</h4>
                    <p>Virtual SPWs included: ${', '.join(map(str, plot.parameters['spws']))}</p>
                % else:
                    <h4>K/Jy Factors across Spectral Windows</h4>
                    <p>SPWs included: ${', '.join(map(str, plot.parameters['spws']))}</p>
                % endif
                <h6>Receivers: ${', '.join(plot.parameters['receivers'])}</h6>
            </div>
        </div>
    % endif
% endif
<div class="clearfix"></div><!--  flush plots, break to next row -->

<h3>Jy/K Conversion Factors</h3>
The following table lists the Jy/K factors.
% if reffile_list is not None:
    Parameters can be found in:
    % for idx, reffile in enumerate(reffile_list):
        % if reffile is not None and len(reffile) > 0 and os.path.exists(os.path.join(stage_dir, os.path.basename(reffile))):
            <a class="replace-pre" href="${os.path.relpath(reffile, pcontext.report_dir)}">${os.path.basename(reffile)}</a>
            % if idx < len(reffile_list)-1:
                ,
            % else:
                .
            % endif
        % endif
    % endfor
% else:
    No Jy/K factors file is specified. 
% endif
<table class="table table-bordered table-striped" summary="Jy/K factors">
    <thead>
    % if dovirtual:
	<tr><th>Virtual Spw</th><th>MS</th><th>Real Spw</th><th>Antenna</th><th>Pol</th><th>Factor</th></tr>
	% else:
	<tr><th>Spw</th><th>MS</th><th>Antenna</th><th>Pol</th><th>Factor</th></tr>
	% endif
    </thead>
	<tbody>
	% for tr in jyperk_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
</table>
<p/>
