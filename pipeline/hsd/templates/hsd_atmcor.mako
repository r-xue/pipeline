<%!
rsc_path = ""
import os
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Apply correction for atmospheric effects</%block>

<%
def plot_title(plot):
    spw = plot.parameters['spw']
    field = plot.parameters['field']
    vis = plot.parameters['vis']
    return f'Spectra after ATM correction {vis} {field} Spw {spw}'

def get_spw_exp(spw):
    spw_exp = 'Spectral Window {}'.format(spw)
    #if dovirtual:
    #    spw_exp = 'Virtual ' + spw_exp
    return spw_exp

def get_spw_desc(spw):
    spw_exp = get_spw_exp(spw).replace('Window', 'Window:')
    #if dovirtual:
    #    spw_name = pcontext.observing_run.virtual_science_spw_ids[spw]
    #    spw_short_name = pcontext.observing_run.virtual_science_spw_shortnames[spw_name]
    #    spw_exp += '<br>({})'.format(spw_short_name)
    return spw_exp
%>

<style>
.atm-table-column-name {
    font-weight: bold;
}

.atm-application-status {
    font-style: italic;
}
</style>

<p>This task evaluates and applies the best model correction for atmospheric effects to science targets
in the calibrated measurement sets.
Four atmtypes are evaluated, for details see
<a href="https://iopscience.iop.org/article/10.1088/1538-3873/abe0ab">T. Sawada et al., PASP 133:034504, 2021</a>.</p>

<h2>Contents</h2>
<ul>
<li><a href="#applied_corrections">Applied Corrections</a></li>
<li><a href="#plots">Plots</a>
  <ul>
    <li><a href="#plots_amp_vs_freq">Science targets: ATM corrected amplitude vs frequency</a></li>
  </ul>
</li>
</ul>

<h2 id="applied_corrections" class="jumptarget">Applied Corrections</h2>

<p>The correction for atmospheric effects applies a model in each measurement set with the parameters
listed in the following table. The SPW chosen to evaluate the best model is based on the atmospheric
transmission model. The channels selected to perform the evaluation is marked in the plots
<span class="atm-table-column-name">'Attempted Models'</span>.
Note that only the <span class="atm-application-status">'Applied'</span> plots in
<span class="atm-table-column-name">'Attempted models'</span> is the actual correction, the
<span class="atm-application-status">'Discarded'</span>
plots in <span class="atm-table-column-name">'Attempted Models'</span> are for providing information
only and none of them are applied.</p>

<table class="table table-bordered table-striped table-condensed"
       summary="Applied Corrections">
    <caption>Applied Corrections</caption>
    <thead>
        <tr>
            <th>Measurement set</th>
            <th rowspan="2">Heuristics Applied</th>
            <th rowspan="2">Attempted Models</th>
            <th scope="col" colspan="3">Applied Model parameters</th>
        </tr>
        <tr>
            <th>Name</th>
            <th>atmType</th>
            <th>h0</th>
            <th>dTem_dh</th>
    </thead>
    <tbody>
        %for tr in heuristics_table:
            <tr>
                %for td in tr:
    				${td}
    			% endfor
            </tr>
        %endfor
    </tbody>
</table>


<h2 id="plots" class="jumptarget">Plots</h2>
<h2 id="plots_amp_vs_freq" class="jumptarget">Science targets: ATM corrected amplitude vs frequency</h2>

<p>Corrected by atmospheric effects amplitude vs frequency plots of each source in each
measurement set. The atmospheric transmission for each spectral window is overlayed
on each plot in pink.</p>

<p>Data are plotted for all antennas and correlations, with different antennas shown
in different colors.</p>

% for vis, plots_vis in summary_plots.items():
    <h3><a class="replace"
           href="${os.path.relpath(detail_page, pcontext.report_dir)}"
           data-vis="${vis}">${vis}</a>
    </h3>
    % for field, plots_fields in plots_vis.items():
        <h3><a class="replace"
               href="${os.path.relpath(detail_page, pcontext.report_dir)}"
               data-vis="${vis}"
               data-field="${field}">${field}</a>
        </h3>
        % for spw, plot in plots_fields.items():
            % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="${plot_title(plot)}">
	                </a>

	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.relpath(detail_page, pcontext.report_dir)}"
	                           class="replace"
                               data-vis="${vis}"
	                           data-field="${field}"
	                           data-spw="${spw}">
	                           ${get_spw_exp(spw)}
	                        </a>
	                    </h4>
	                    <p>Field: ${field}<br>
	                       ${get_spw_desc(spw)}
	                    </p>
	                </div>
	            </div>
	        </div>
            % endif
        % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
    % endfor
% endfor


