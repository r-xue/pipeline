<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Reweight visibilities</%block>

<p>Calculate data weights based on st. dev. within each spw.</p>

<%
mean =  result.jobs[0]['mean']
variance = result.jobs[0]['variance'] 
%>

<p>Mean: ${mean}</p>
<p>Variance: ${variance}</p>

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
after_by_spw=weight_stats['after']['per_spw']
after_by_ant=weight_stats['after']['per_ant']

if result[0].inputs['statwtmode'] == 'VLA':
#    before_by_scan=weight_stats['before']['per_scan']
    after_by_scan=weight_stats['after']['per_scan']
    description = "after"
    table_header = "Weight Properties"
    is_vlass = False
else: 
    before_by_spw=weight_stats['before']['per_spw']
    before_by_ant=weight_stats['before']['per_ant']
    description = "before/after"
    is_vlass = True
    table_header = "statwt after"


import numpy as np

def format_wt(wt):

    if wt is None:
        return 'N/A'
    else:
        return np.format_float_positional(wt, precision=4, fractional=False, trim='-')

%>

<h2 id="flagged_data_summary" class="jumptarget">Statwt Summary</h2>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each antenna. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>. 
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Antenna Selection</th>
			<!-- flags before task is always first agent -->
            % if is_vlass: 
    			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
            % endif
			<th scope="col" colspan="5" style="text-align:center">${table_header}</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            <!-- needs is_vla -->
            <th scope="col" >Minimum</th>
            <th scope="col" >Maximum</th>
            %if is_vlass:
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            %endif
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_ant)):
		<tr>  
			<th style="text-align:center">${after_by_ant[i]['ant']}</th>  
            % if is_vlass: 
                <td>${format_wt(before_by_ant[i]['med'])}</td>
                % if before_by_ant[i]['quartiles'] is not None:
                    <td>${format_wt(before_by_ant[i]['q1'])}/${format_wt(before_by_ant[i]['q3'])}</td>
                % else:
                    <td>N/A</td>
                % endif 
                <td>${format_wt(before_by_ant[i]['mean'])} &#177 ${format_wt(before_by_ant[i]['stdev'])}</td>  
            % endif 

            <td>${format_wt(after_by_ant[i]['med'])}</td>
            % if after_by_ant[i]['quartiles'] is not None:
                <td>${format_wt(after_by_ant[i]['q1'])}/${format_wt(after_by_ant[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${format_wt(after_by_ant[i]['mean'])} &#177 ${format_wt(after_by_ant[i]['stdev'])}</td>    
            <td>${format_wt(after_by_ant[i]['min'])}</td>
            <td>${format_wt(after_by_ant[i]['max'])}</td>      
		</tr>
		% endfor
	</tbody>
</table>


<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each spectral window. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Spw Selection</th>
			<!-- flags before task is always first agent -->
            % if is_vlass: 
			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
            % endif 
			<th scope="col" colspan="5" style="text-align:center">${table_header}</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
                        <!-- needs is_vla -->
            <th scope="col" >Minimum</th>
            <th scope="col" >Maximum</th>
            %if is_vlass:
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            %endif
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_spw)):
		<tr>
			<th style="text-align:center">${after_by_spw[i]['spw']}</th>  
            % if is_vlass: 
            <td>${format_wt(before_by_spw[i]['med'])}</td>
            % if before_by_spw[i]['quartiles'] is not None:
                <td>${format_wt(before_by_spw[i]['q1'])}/${format_wt(before_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif            
            <td>${format_wt(before_by_spw[i]['mean'])} &#177 ${format_wt(before_by_spw[i]['stdev'])}</td>     
            %endif 

            <td>${format_wt(after_by_spw[i]['med'])}</td>
            % if after_by_spw[i]['quartiles'] is not None:
                <td>${format_wt(after_by_spw[i]['q1'])}/${format_wt(after_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${format_wt(after_by_spw[i]['mean'])} &#177 ${format_wt(after_by_spw[i]['stdev'])}</td>
            <td>${format_wt(after_by_spw[i]['min'])}</td>
            <td>${format_wt(after_by_spw[i]['max'])}</td>      
		</tr>
		% endfor
	</tbody>
</table>

%if result[0].inputs['statwtmode'] == 'VLA' :
<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each scan. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Scan Selection</th>
			<!-- flags before task is always first agent -->
			<th scope="col" colspan="5" style="text-align:center">${table_header}</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            <!-- needs is_vla -->
            <th scope="col" >Minimum</th>
            <th scope="col" >Maximum</th>
            %if is_vlass:
            <th scope="col" >Median</th>
            <th scope="col" >1st/3rd Quartile</th>
            <th scope="col" >Mean &#177 S.Dev.</th>
            %endif
		</tr>        
	</thead>

	<tbody>
		% for i in range(len(after_by_scan)):
		<tr>
			<th style="text-align:center">${after_by_scan[i]['scan']}</th>  
            <td>${format_wt(after_by_scan[i]['med'])}</td>
            % if after_by_scan[i]['quartiles'] is not None:
                <td>${format_wt(after_by_scan[i]['q1'])}/${format_wt(after_by_scan[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${format_wt(after_by_scan[i]['mean'])} &#177 ${format_wt(after_by_scan[i]['stdev'])}</td>
            <td>${format_wt(after_by_scan[i]['min'])}</td>
            <td>${format_wt(after_by_scan[i]['max'])}</td>      
		</tr>
		% endfor
	</tbody>
</table>
%endif 
