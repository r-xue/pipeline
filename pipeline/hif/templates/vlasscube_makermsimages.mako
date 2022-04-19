<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>

<%
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import matplotlib.colors as colors

def fmt_rms(rms,scale=1.e3):
    if rms is None:
        return 'N/A'
    else:
        #return np.format_float_positional(rms*scale, precision=3, fractional=False, trim='-')
        return np.format_float_positional(rms*scale, precision=3, fractional=False)

def val2color(x, cmap_name='Greys',vmin=None,vmax=None):
    """
    some cmap_name options: 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds'
    """
    norm=colors.Normalize(vmin, vmax)
    x_norm=0.05+0.5*(x-vmin) / (vmax-vmin)
    cmap=cm.get_cmap(name=cmap_name)
    rgb=cmap(x_norm)
    rgb_hex=colors.to_hex(rgb)
    return rgb_hex

def dev2color(x):
    color_list=['gainsboro','lightgreen','yellow','red']
    if x<=4 and x>3:
      rgb_hex='#D3D3D3'
    if x<=5 and x>4:
      rgb_hex=colors.cnames[color_list[1]]
    if x<=6 and x>5:
      rgb_hex=colors.cnames[color_list[2]]
    if x>6:
      rgb_hex=colors.cnames[color_list[3]]
    return rgb_hex            

border_line="2px solid #AAAAAA"
cell_line="1px solid #DDDDDD"

%>


<%inherit file="t2-4m_details-base.mako"/>

<style type="text/css">


.table {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;      
}

.table td {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;
}

.table th {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;
    
    border-bottom: ${border_line} !important;
    border-right: ${border_line} !important;
    border-top: ${border_line} !important;
    border-left: ${border_line}  !important;
    background-color: #F9F9F9;
    
}

</style>

<%block name="title">Make RMS Uncertainty Images</%block>

<p>RMS Images are meant to represent the root-mean-square deviation from the mean (rmsd)
   appropriate to measure the noise level in a Gaussian distribution.
</p>



<!-- <h3>Rms Image Stats</h3> -->

% for ms_name in rmsplots.keys():

    <%
    plots = rmsplots[ms_name]
    #spw_colname=[plot[0].parameters['virtspw'] for plot in plots]
    stats=plotter.result.stats
    stats_summary=plotter.result.stats_summary
    %>

    <h4>Rms Image Statistical Properties</h4>

    <!--
    <div style="width: 1200px; height: 250px; overflow: auto;">
    <div style="width: 1200px; height: 250px; overflow: auto;">
    <table class="table table-header-rotated">
    -->
    <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered">
    <caption>
        <li>
            Units in mJy/beam.
        </li>
        <li>
            The background of individual cells is color-coded by each image property after normalized by their respective ranges in all spw groups and Stokes planes.
        </li>
        <li>
            MADrms: the median absolute deviation from the median (i.e., 'medabsdevmed' defined in the CASA/imstat output), multiplied by 1.4826.
        </li>
    </caption>    
    
    <thead>
    </thead>

    <tbody>

        <tr>
            <th colspan="1"><b>Stokes</b></td>
            <th colspan="6"><b><i>I</i></b></td>
            <th colspan="6"><b><i>Q</i></b></td>
            <th colspan="6"><b><i>U</i></b></td>
            <th colspan="6"><b><i>V</i></b></td>
        </tr>
        <tr>
            <th colspan="1"><b>Spw</b></td>
            % for idx in range(4):
                % for item in ['Max','Min','Mean','Median','Sigma','MADrms']:
                    <th colspan="1"><b>${item}</b></td>
                % endfor
            % endfor  
        </tr>
       

        % for idx, stats_per_spw in enumerate(stats):
            <tr>
            <%
            cell_style=[f'border-left: {border_line}',f'border-right: {border_line}']
            if idx==len(stats)-1:
                cell_style.append('border-bottom: '+border_line)          
            cell_style='style="{}"'.format(('; ').join(cell_style))                    
            %> 

            <td ${cell_style}><b>${stats_per_spw['virtspw']}</b></td>
            % for idx_pol,name_pol in enumerate(['I','Q','U','V']):
                % for item, cmap in [('Max','Reds'),('Min','Oranges'),('Mean','Greens'),('Median','Blues'),('Sigma','Purples'),('MADrms','Greys')]:
                    <%
                    cell_style=[]
                    dev_in_madrms=abs(stats_summary[item.lower()]['spwwise_mean'][idx_pol]-stats_per_spw[item.lower()][idx_pol])
                    madrms=stats_summary[item.lower()]['spwwise_madrms'][idx_pol]
                    if dev_in_madrms>madrms*3.0:
                        #bgcolor=val2color(dev_in_madrms/madrms,cmap_name='Greys',vmin=3,vmax=10)
                        bgcolor=dev2color(dev_in_madrms/madrms)
                        cell_style.append(f'background-color: {bgcolor}')  
                    
                    if item=='MADrms':
                        cell_style.append('border-right: '+border_line)
                    if idx==len(stats)-1:
                        cell_style.append('border-bottom: '+border_line)          
                    cell_style='style="{}"'.format(('; ').join(cell_style))                    
                    
                    %>                    
                    
                    <td ${cell_style}>${fmt_rms(stats_per_spw[item.lower()][idx_pol])}</td>
                % endfor
            % endfor
            </tr>
        % endfor

    </tbody>
    </table>

% endfor

<div style="clear:both;"></div>

<%self:plot_group plot_dict="${rmsplots}"
                  url_fn="${lambda ms: 'noop'}"
                  break_rows_by="band"
                  sort_row_by='pol'>

        <%def name="title()">
        </%def>

        <%def name="fancybox_caption(plot)">
          Sky Image, Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']}
        </%def>

        <%def name="caption_title(plot)">
           Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']}
        </%def>

</%self:plot_group>
