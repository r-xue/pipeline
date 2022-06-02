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

def fmt_cell(rms,scale=1.e3):
    if rms is None:
        return 'N/A'
    else:
        return np.format_float_positional(rms*scale, precision=3, fractional=False)

def fmt_model(p):
    label='I<sub>3GHz</sub>='+'{:.3f}'.format(p['model_amplitude'])+'mJy/bm'
    label+=', '
    label+='alpha='+'{:.3f}'.format(p['model_alpha'])
    return label
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
    font-size: 12px;
}

.table th {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;
    font-size: 12px;
    background-color: #F9F9F9;
}

.table caption {
    color: black;
}

</style>

<%block name="title">Analysis of Stokes Cubes</%block>

<p>This task performs analyses of Stokes cubes.</p>


<%self:plot_group plot_dict="${stokesplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.parameters['desc']}: ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.parameters['desc']}
        </%def>
</%self:plot_group>




<%self:plot_group plot_dict="${fluxplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.parameters['desc']}: ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.parameters['desc']}
        </%def>
</%self:plot_group>


<h4>Flux Properties</h4>

<div class="table-responsive">
<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-hover table-condensed">
<caption>     
    <li>
        Units in mJy/beam.
    </li>      
</caption>    

<thead>
</thead>

<tbody>

    <tr>
        <th colspan="1"><b>Region</b></td>
        <th colspan="4"><b>Stokes I Peak</b></td>
        <th colspan="4"><b>LinPol Peak</b></td>
    </tr>
    <tr>
        <th colspan="1"><b>Spw</b></td>
        <th colspan="1"><b><i>I</i></b></td>
        <th colspan="1"><b><i>Q</i></b></td>
        <th colspan="1"><b><i>U</i></b></td>
        <th colspan="1"><b><i>V</i></b></td>
        <th colspan="1"><b><i>I</i></b></td>
        <th colspan="1"><b><i>Q</i></b></td>
        <th colspan="1"><b><i>U</i></b></td>
        <th colspan="1"><b><i>V</i></b></td>            
    </tr>        

    % for idx in range(len(stats['peak_stokesi']['stokesi'])):
    <tr>
          <th colspan="1"><b>${stats['peak_stokesi']['spw'][idx]}</b></td>
          <td colspan="1">${fmt_cell(stats['peak_stokesi']['stokesi'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_stokesi']['stokesq'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_stokesi']['stokesu'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_stokesi']['stokesv'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_linpolint']['stokesi'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_linpolint']['stokesq'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_linpolint']['stokesu'][idx])}</td>
          <td colspan="1">${fmt_cell(stats['peak_linpolint']['stokesv'][idx])}</td>
    </tr>
    %endfor 

    <tr>
      <th colspan="1">Model</th>
      <td colspan="4">${fmt_model(fluxplots['Flux vs. Freq Plots'][0].parameters)}</td>
      <td colspan="4">${fmt_model(fluxplots['Flux vs. Freq Plots'][1].parameters)}</td>
    </tr>
                
</tbody>
</table>
</div>


<%self:plot_group plot_dict="${rmsplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
          ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

</%self:plot_group>

