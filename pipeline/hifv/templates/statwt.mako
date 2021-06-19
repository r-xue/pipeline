<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Reweight visibilities</%block>

<p>Calculate data weights based on st. dev. within each spw.</p>


% if result[0].inputs['statwtmode'] == 'VLASS-SE' :

<%self:plot_group plot_dict="${summary_plots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Statwt Summary Plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Summary window</%def>

        <%def name="fancybox_caption(plot)">
            Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight)
        </%def>

        <%def name="caption_title(plot)">
            Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight)
        </%def>
</%self:plot_group>

<%
weight_stats=plotter.result.weight_stats
before_by_spw=weight_stats['before']['per_spw']
before_by_ant=weight_stats['before']['per_ant']
after_by_spw=weight_stats['after']['per_spw']
after_by_ant=weight_stats['after']['per_ant']
import numpy as np

def format_wt(wt):

    if wt is None:
        return 'N/A'
    else:
        return np.format_float_positional(wt, precision=4, fractional=False, trim='-')

%>

<h2 id="flagged_data_summary" class="jumptarget">Statwt Summary</h2>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of before/after-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each antenna. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>. 
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Antenna Selection</th>
			<!-- flags before task is always first agent -->
			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
			<th scope="col" colspan="3" style="text-align:center">statwt after</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_ant)):
		<tr>  
			<th style="text-align:center">${after_by_ant[i]['ant']}</th>  
            <td>${format_wt(before_by_ant[i]['med'])}</td>
            % if before_by_ant[i]['quartiles'] is not None:
                <td>${format_wt(before_by_ant[i]['q1'])}/${format_wt(before_by_ant[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif 
            <td>${format_wt(before_by_ant[i]['mean'])} &#177 ${format_wt(before_by_ant[i]['stdev'])}</td>         
            <td>${format_wt(after_by_ant[i]['med'])}</td>
            % if after_by_ant[i]['quartiles'] is not None:
                <td>${format_wt(after_by_ant[i]['q1'])}/${format_wt(after_by_ant[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${format_wt(after_by_ant[i]['mean'])} &#177 ${format_wt(after_by_ant[i]['stdev'])}</td>          
		</tr>
		% endfor
	</tbody>
</table>


<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of before/after-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each spectral window. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Spw Selection</th>
			<!-- flags before task is always first agent -->
			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
			<th scope="col" colspan="3" style="text-align:center">statwt after</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_spw)):
		<tr>
			<th style="text-align:center">${after_by_spw[i]['spw']}</th>  
            <td>${format_wt(before_by_spw[i]['med'])}</td>
            % if before_by_spw[i]['quartiles'] is not None:
                <td>${format_wt(before_by_spw[i]['q1'])}/${format_wt(before_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif            
            <td>${format_wt(before_by_spw[i]['mean'])} &#177 ${format_wt(before_by_spw[i]['stdev'])}</td>         
            <td>${format_wt(after_by_spw[i]['med'])}</td>
            % if after_by_spw[i]['quartiles'] is not None:
                <td>${format_wt(after_by_spw[i]['q1'])}/${format_wt(after_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${format_wt(after_by_spw[i]['mean'])} &#177 ${format_wt(after_by_spw[i]['stdev'])}</td>
           
		</tr>
		% endfor
	</tbody>
</table>

%endif