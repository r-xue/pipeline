<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Reweight visibilities</%block>

<p>Calculate data weights based on st. dev. within each spw.</p>

<%
import numpy as np

def format_wt(wt): #TODO: consider consolidating with below
    if wt is None:
        return 'N/A'
    else:
        return np.format_float_positional(wt, precision=4, fractional=False, trim='-')

def format_wt_overall(wt): #TODO: see above
    if wt is None:
        return 'N/A'
    if wt >= 10**5:
        return np.format_float_scientific(wt, precision=6, trim='-')
    else:
        return np.format_float_positional(wt, precision=6, fractional=False, trim='-')

if result[0].inputs['statwtmode'] == 'VLA':
    mean =  result[0].jobs[0]['mean'] #TODO: double-check: can these have more than one value? Multiple MS output? 
    variance = result[0].jobs[0]['variance'] 
%>

% if result[0].inputs['statwtmode'] == 'VLA':
<h3>Overall results:</h3>
<b>Mean:</b> ${format_wt_overall(mean)} 
<br>
<b>Variance:</b> ${format_wt_overall(variance)}
% endif 

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

import collections
import numpy as np
from matplotlib.pyplot import cm
import matplotlib.colors as colors

def summarize_stats(input_stats):
    summary = collections.defaultdict(list)
    for i, row in enumerate(input_stats):
        for stat in row:
            val = input_stats[i][stat]
            summary[stat].append(val)
    return summary


def dev2shade(x, above_median=True):
    absx=abs(x)
    if above_median: 
        cmap=cm.get_cmap(name='Reds')
    else: 
        cmap=cm.get_cmap(name='Blues')
    if absx<4 and absx>=3:
        rgb_hex=colors.to_hex(cmap(0.2))
    elif absx<5 and absx>=4:
        rgb_hex=colors.to_hex(cmap(0.3))
    elif absx<6 and absx>=5:
        rgb_hex=colors.to_hex(cmap(0.4))
    elif absx>=6:
        rgb_hex=colors.to_hex(cmap(0.5))
    else: 
        rgb_hex=colors.to_hex(cmap(0.1))
    return rgb_hex  


def format_cell(whole, value, stat):
    if (value is None) or (whole is None) or (stat is None) or (value == 'N/A') or is_vlass: #TODO: can't even call for VLASS anyway. Remove this part of conditional
        return ''
    else:
        summary = np.array(whole[stat], dtype=np.float)
        median = np.nanmedian(summary)
        #sigma = np.nanstd(summary)
        sigma = 1.4826 * np.nanmedian(np.abs(summary - np.nanmedian(summary))) #TODO: double check -- correct for MAD? 
        dev = abs(float(value)) - median #TODO

        if len(summary) <= 1: 
            return f'debugging: only one entry'
        
        cell_title='{:.2f}'.format(dev/sigma)
        # if abs(dev) > 0.25*sigma: force for testing
        if abs(dev) > sigma*3.0: #TODO: ask if abs value is good here...
            bgcolor = dev2shade(dev/sigma, float(value) > median)
            # bgcolor = dev2shade(dev/(0.25*sigma)) force for testing
            return f'style="background-color: {bgcolor}", debugging: {cell_title}'
        else: 
            return f'debugging: {cell_title}'

if not is_vlass:
    summary_spw_stats = summarize_stats(after_by_spw)
    summary_ant_stats = summarize_stats(after_by_ant)
    summary_scan_stats = summarize_stats(after_by_scan)

    bgcolor_list=[dev2shade(3., True), dev2shade(4., True), dev2shade(5., True), dev2shade(6., True)]
    bgcolor_list_blue=[dev2shade(3., False), dev2shade(4., False), dev2shade(5., False), dev2shade(6., False)]
%>

<h2 id="flagged_data_summary" class="jumptarget">Statwt Summary</h2>

<p>The color background highlights spectral windows with a statistical property signficantly deviated from its median over all of the relevant group (spw, scan, antenna): 
<p> For values above the meidan, shades of red are used: </p>
<p style="background-color:${bgcolor_list[0]}; display:inline;">3&#963&le;dev&lt;4&#963</p>; <p style="background-color:${bgcolor_list[1]}; display:inline;">4&#963&le;dev&lt;5&#963</p>; 
<p style="background-color:${bgcolor_list[2]}; display:inline;">5&#963&le;dev&lt;6&#963</p>; <p style="background-color:${bgcolor_list[3]}; display:inline;">6&#963&le;dev,</p>
<p>where &#963 is defined as 1.4826*MAD.</p>
<p>For values below the median, shades of blue are used: </p> 
<p style="background-color:${bgcolor_list_blue[0]}; display:inline;">3&#963&le;dev&lt;4&#963</p>; <p style="background-color:${bgcolor_list_blue[1]}; display:inline;">4&#963&le;dev&lt;5&#963</p>; 
<p style="background-color:${bgcolor_list_blue[2]}; display:inline;">5&#963&le;dev&lt;6&#963</p>; <p style="background-color:${bgcolor_list_blue[3]}; display:inline;">
    6&#963&le;dev,</p> 
<p>where &#963 is defined as 1.4826*MAD.</p>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each antenna. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>. 
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Antenna Selection</th>
			<!-- flags before task is always first agent -->
            % if is_vlass: 
    			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
                <th scope="col" colspan="5" style="text-align:center">${table_header}</th>
            % else: 
                <th scope="col" colspan="7" style="text-align:center">${table_header}</th>
            % endif
			
		</tr>
		<tr>
            <th scope="col" >Median</th>
            % if is_vlass:
                <th scope="col" >1st/3rd Quartile</th>
                <th scope="col" >Mean &#177 S.Dev.</th>
            % else: 
                <th scope="col" >1st Quartile</th>
                <th scope="col" >3rd Quartile</th>
                <th scope="col" >Mean</th>
                <th scope="col" >S.Dev.</th>
                <th scope="col" >Minimum</th>
                <th scope="col" >Maximum</th>
            % endif

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

            %if is_vlass:
                <td>${format_wt(after_by_ant[i]['med'])}</td>
                % if after_by_ant[i]['quartiles'] is not None:
                    <td>${format_wt(after_by_ant[i]['q1'])}/${format_wt(after_by_ant[i]['q3'])}</td>
                % else:
                    <td>N/A</td>
                % endif
                <td>${format_wt(after_by_ant[i]['mean'])} &#177 ${format_wt(after_by_ant[i]['stdev'])}</td>    
            % else:
                <td ${format_cell(summary_ant_stats, after_by_ant[i]['med'], 'med')}>${format_wt(after_by_ant[i]['med'])}</td>
                % if after_by_ant[i]['quartiles'] is not None:
                    <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['q1']), 'q1')}>${format_wt(after_by_ant[i]['q1'])}</td>
                    <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['q3']), 'q3')}>${format_wt(after_by_ant[i]['q3'])}</td>
                % else:
                    <td>N/A</td><!-- might be able to get rid ov via format_wt-->
                    <td>N/A</td>
                % endif
                <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['mean']), 'mean')}>${format_wt(after_by_ant[i]['mean'])}</td>
                <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['stdev']), 'stdev')}>${format_wt(after_by_ant[i]['stdev'])}</td>
                <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['min']), 'min')}>${format_wt(after_by_ant[i]['min'])}</td>
                <td ${format_cell(summary_ant_stats, format_wt(after_by_ant[i]['max']), 'max')}>${format_wt(after_by_ant[i]['max'])}</td>
            % endif
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
                <th scope="col" colspan="5" style="text-align:center">${table_header}</th>
            % else: 
                <th scope="col" colspan="7" style="text-align:center">${table_header}</th>
            % endif 
		</tr>
		<tr>
            <th scope="col" >Median</th>

            % if is_vlass:
                <th scope="col" >1st/3rd Quartile</th>
                <th scope="col" >Mean &#177 S.Dev.</th>
            % else: 
                <th scope="col" >1st Quartile</th>
                <th scope="col" >3rd Quartile</th>
                <th scope="col" >Mean</th>
                <th scope="col" >S.Dev.</th>
                <th scope="col" >Minimum</th>
                <th scope="col" >Maximum</th>
            % endif

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


            %if is_vlass:
                <td>${format_wt(after_by_spw[i]['med'])}</td>
                % if after_by_spw[i]['quartiles'] is not None:
                    <td>${format_wt(after_by_spw[i]['q1'])}/${format_wt(after_by_spw[i]['q3'])}</td>
                % else:
                    <td>N/A</td>
                % endif
                <td>${format_wt(after_by_spw[i]['mean'])} &#177 ${format_wt(after_by_spw[i]['stdev'])}</td>
            %else: 
                <td ${format_cell(summary_spw_stats, after_by_spw[i]['med'], 'med')}>${format_wt(after_by_spw[i]['med'])}</td>
                % if after_by_spw[i]['quartiles'] is not None:
                    <td ${format_cell(summary_spw_stats, after_by_spw[i]['q1'], 'q1')}>${format_wt(after_by_spw[i]['q1'])}</td>
                    <td ${format_cell(summary_spw_stats, after_by_spw[i]['q3'], 'q3')}>${format_wt(after_by_spw[i]['q3'])}</td>
                % else:
                    <td>N/A</td>
                    <td>N/A</td>
                % endif
                <td ${format_cell(summary_spw_stats, after_by_spw[i]['mean'], 'mean')}>${format_wt(after_by_spw[i]['mean'])}</td>
                <td ${format_cell(summary_spw_stats, after_by_spw[i]['stdev'], 'stdev')}>${format_wt(after_by_spw[i]['stdev'])}</td>
                <td ${format_cell(summary_spw_stats, after_by_spw[i]['min'], 'min')}>${format_wt(after_by_spw[i]['min'])}</td>
                <td ${format_cell(summary_spw_stats, after_by_spw[i]['max'], 'max')}>${format_wt(after_by_spw[i]['max'])}</td>    
            %endif
  
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
			<th scope="col" colspan="8" style="text-align:center">${table_header}</th>
		</tr>
		<tr>
            <th scope="col" >Median</th>
            <th scope="col" >1st Quartile</th>
            <th scope="col" >3rd Quartile</th>
            <th scope="col" >Mean</th>
            <th scope="col" >S.Dev.</th>
            <th scope="col" >Minimum</th>
            <th scope="col" >Maximum</th>
		</tr>        
	</thead>

	<tbody>
		% for i in range(len(after_by_scan)):
		<tr>
			<th style="text-align:center">${after_by_scan[i]['scan']}</th>  
            <td ${format_cell(summary_scan_stats, after_by_scan[i]['med'], 'med')}>${format_wt(after_by_scan[i]['med'])}</td>
            % if after_by_scan[i]['quartiles'] is not None:
                <td ${format_cell(summary_scan_stats, after_by_scan[i]['q1'], 'q1')}>${format_wt(after_by_scan[i]['q1'])}</td>
                <td ${format_cell(summary_scan_stats, after_by_scan[i]['q3'], 'q3')}>${format_wt(after_by_scan[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td ${format_cell(summary_scan_stats, after_by_scan[i]['mean'], 'mean')}>${format_wt(after_by_scan[i]['mean'])}</td>
            <td ${format_cell(summary_scan_stats, after_by_scan[i]['stdev'], 'stdev')}>${format_wt(after_by_scan[i]['stdev'])}</td>
            <td ${format_cell(summary_scan_stats, after_by_scan[i]['min'], 'min')}>${format_wt(after_by_scan[i]['min'])}</td>
            <td ${format_cell(summary_scan_stats, after_by_scan[i]['max'], 'max')}>${format_wt(after_by_scan[i]['max'])}</td>      
		</tr>
		% endfor
	</tbody>
</table>
%endif 
