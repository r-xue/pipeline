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
    print(x,norm(x),rgb)
    return rgb_hex

%>


<%inherit file="t2-4m_details-base.mako"/>

<style type="text/css">

.table-custom table {
  table-layout: fixed;
  width: 100px;
  border: 3px solid;
  border-collapse: collapse;
}

.table-custom tbody {
    display: block;
    overflow-x: auto;
}

.table-custom th {
  table-layout: fixed;
  width: 100px;
  height: 12px;
  border-top: 2px solid #dddddd;
  border-left: 2px solid #dddddd;
  border-right: 2px solid #dddddd;
  border-bottom: 2px solid #dddddd;
  vertical-align: middle;
  text-align: center;  
  font-size: 12px;
}

.table-custom td {
  table-layout: fixed;
  width: 100px;
  height: 12px;
  /*
  border-top: 1px solid #dddddd;
  border-left: 1px solid #dddddd;
  border-right: 1px solid #dddddd;
  border-bottom: 1px solid #dddddd;  
  */
  vertical-align: middle;
  text-align: center;  
  font-size: 12px;
}

.table-custom td.last{
  table-layout: fixed;
  width: 100px;
  height: 12px;
  /*
  border-top: 1px solid #dddddd;
  border-left: 1px solid #dddddd;
  border-right: 1px solid #dddddd;
  */
  border-bottom: 2px solid #dddddd;  
  vertical-align: middle;
  text-align: center;  
  font-size: 12px;
}


.table td {
  text-align: center;
  vertical-align: middle;
  font-size: 12px;
}
.table th {
  text-align: center;
  vertical-align: middle;
  font-size: 12px;
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
    print(plots[0].parameters)
    %>

    <h4>Rms Image Statistical Properties</h4>

    <!--
    <div style="width: 1200px; height: 250px; overflow: auto;">
    <div style="width: 1200px; height: 250px; overflow: auto;">
    <table class="table table-header-rotated">
    -->
    <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-striped table-bordered">
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
            <th><b>${stats_per_spw['virtspw']}</b></th>
            % for idx_pol,name_pol in enumerate(['I','Q','U','V']):
                % for item, cmap in [('Max','Reds'),('Min','Oranges'),('Mean','Greens'),('Median','Blues'),('Sigma','Purples'),('MADrms','Greys')]:
                    <td bgcolor="${val2color(stats_per_spw[item.lower()][idx_pol],cmap_name=cmap,
                                    vmin=stats_summary[item.lower()]['range'][0],
                                    vmax=stats_summary[item.lower()]['range'][1])}">
                        ${fmt_rms(stats_per_spw[item.lower()][idx_pol])}
                    </td>
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
