<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>


<%

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import matplotlib.colors as colors

def fmt_val(rms,scale=1.):
    if rms is None:
        return 'N/A'
    else:
        #return np.format_float_positional(rms*scale, precision=3, fractional=False, trim='-')
        return np.format_float_positional(rms*scale, precision=3, fractional=False)

def colorcode(x, color='Greys',vmin=None,vmax=None):
    """Generate hex colorcode from a value.

    cmap_name options: 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds'
    colors
    """
    norm=colors.Normalize(vmin, vmax)
    x_norm=0.05+0.5*(x-vmin) / (vmax-vmin)

    cmap=cm.get_cmap(name=cmap_name)
    rgb=cmap(x_norm)
    rgb_hex=colors.to_hex(rgb)
    return rgb_hex

def val2color(x, cmap_name='Greys',vmin=None,vmax=None):
    """Generate hex colorcode from a value.

    some cmap_name options: 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds'
    """
    norm=colors.Normalize(vmin, vmax)
    x_norm=0.05+0.5*(x-vmin) / (vmax-vmin)
    
    cmap=cm.get_cmap(name=cmap_name)
    rgb=cmap(x_norm)
    rgb_hex=colors.to_hex(rgb)
    return rgb_hex

                                                                  

border_line="2px solid #AAAAAA"
cell_line="1px solid #DDDDDD"

%>


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
    border-bottom: ${cell_line};
    border-right: ${cell_line};  
    font-size: 12px;
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
    font-size: 12px;
}

</style>

<%block name="title">Make Cutout Images</%block>

% if not use_minified_js:
<link href="${self.attr.rsc_path}resources/css/select2.css" rel="stylesheet"/>
<link href="${self.attr.rsc_path}resources/css/select2-bootstrap.css" rel="stylesheet"/>
<script src="${self.attr.rsc_path}resources/js/select2.js"></script>
% endif

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
            $("#select-spw").val([spw]).trigger("change");
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        o.data("callback", createSpwSetter(spw));
    });
});
</script>

<p>Make cutouts of requested imaging products.</p>


<h4>Cutout Image Statistical Properties</h4>

<table class="table">

<caption>
  <li>
      Peak: the pixel value with the largest deviation from zero, which can be either the maximum or minimum value of each image. This is similar to the definition of "Peak" residual used in CASA/tclean.
  </li>
  <li>
      MADrms: the median absolute deviation from the median (i.e., 'medabsdevmed' defined in the CASA/imstat output), multiplied by 1.4826.
  </li>  
  <li>
      Px<sub><800&mu;Jy</sub> (pct.): the pct. of pixes (within the unmasked region) with value less than 800&mu;Jy in the Rms image.
  </li>
  <li>
      The gray-out cell highlights the spw with the largest deviation of an image property, among all spw selection groups.
  </li>        
</caption>

<thead>
</thead>

<tbody>

% for idx_pol, name_pol in enumerate(['I','Q','U','V']):

  <tr>
    <th>Stokes: <i>${name_pol}</i></th>
    <th colspan="3">Non-Pbcor Restored</th>
    <th colspan="3">Non-Pbcor Reidual</th>
    
    % if name_pol=='I':
      <th colspan="4">Rms Image</th>
      <th colspan="3">Beam</th>
      <th colspan="3">PB</th>
    % else:
      <th colspan="3">Rms Image</th>
    % endif
  </tr>

  <tr>
    <th rowspan="2" style="vertical-align : middle;text-align:center;">Spw</th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1" style="border-right: ${border_line}"><sup>Peak</sup>&frasl;<sub>MADrms</sub></th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1" style="border-right: ${border_line}"><sup>Peak</sup>/<sub>MADrms</sub></th>
    <th colspan="1">Max</th>
    <th colspan="1">Median</th>
    % if name_pol=='I':
       <th colspan="1">Px<sub><800&mu;Jy</th>
      <th colspan="1"  style="border-right: ${border_line}">Masked</th>
      <th colspan="1">Major</th>
      <th colspan="1">Minor</th>
      <th colspan="1"  style="border-right: ${border_line}">P.A.</th>      
      <th colspan="1">Max</th>
      <th colspan="1">Min</th>
      <th colspan="1"  style="border-right: ${border_line}">Median</th>
    % else:
      <th colspan="1"  style="border-right: ${border_line}">Px<sub><800&mu;Jy</sub></th>
    % endif

  </tr>

  <tr>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1"   style="border-right: ${border_line}" >N/A</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1"   style="border-right: ${border_line}">N/A</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    % if name_pol=='I':
      <th colspan="1" >Pct.</th>
      <th colspan="1"   style="border-right: ${border_line}">Pct.</th>
      <th colspan="1" >arcsec</th>
      <th colspan="1" >arcsec</th>
      <th colspan="1"   style="border-right: ${border_line}">deg.</th>
      <th colspan="1" >N/A</th>
      <th colspan="1" >N/A</th>
      <th colspan="1"   style="border-right: ${border_line}">N/A</th>      
    % else:
      <th colspan="1"  style="border-right: ${border_line}">Pct.</th>
    % endif
  </tr>  

  % for idx_spw, (spw,stats_spw) in enumerate(stats.items()):
    <tr>

      <%
      cell_style=[f'border-left: {border_line}',f'border-right: {border_line}']
      if idx_spw==len(stats)-1:
          cell_style.append('border-bottom: '+border_line)          
      cell_style='style="{}"'.format(('; ').join(cell_style))                    
      %> 
      <td ${cell_style}><b>${str(spw)}</b></td>
      
      <%
      type_item_scale=[('image','peak',1e3),
                  ('image','madrms',1e3),
                  ('image','max/madrms',1),
                  ('residual','peak',1e3),
                  ('residual','madrms',1e3),
                  ('residual','max/madrms',1),
                  ('rms','max',1e3),
                  ('rms','median',1e3),
                  ('rms','pct<800e-6',1e2),
                  ('rms','pct_masked',1e2),
                  ('beam','bmaj',1),
                  ('beam','bmin',1),
                  ('beam','bpa',1),                                    
                  ('pb','max',1.),
                  ('pb','min',1.),
                  ('pb','median',1.)]
      %>

      % for idx_item,(t,i,s) in enumerate(type_item_scale):
        % if name_pol!='I' and (t,i) in [('rms','pct_masked'),('pb','max'),('pb','min'),('pb','median'),('beam','bmaj'),('beam','bmin'),('beam','bpa')]:
          <% continue %> 
        % endif

        <%
        if 'pct' in i:
          suffix='&#37'
        else:
          suffix=''
        %>
        <%
        cell_style=[]
        if spw==stats_summary[t][i]['spw_outlier'][idx_pol]:
          cell_style.append('bgcolor: #D3D3D3')
        if idx_item in (2,5,9,12) or (name_pol!='I' and idx_item in ((2,5,8))):
          cell_style.append('border-right: '+border_line)
        if idx_spw==len(stats)-1:
          cell_style.append('border-bottom: '+border_line)          
        cell_style='style="{}"'.format(('; ').join(cell_style))
        %>
      
        % if t!='beam':
          <td ${cell_style}>${fmt_val(stats_spw[t][i][idx_pol],scale=s)}${suffix}</td>
        % else:
          <td ${cell_style}>${fmt_val(stats_spw[t][i],scale=s)}${suffix}</td>
        % endif
      % endfor

    </tr>

  % endfor
  <tr><td style="border-bottom: none; border-top: none; border-left: none; border-right: none;"></td></tr>

% endfor

</tbody>

</table>

<div style="clear:both;"></div>


<%self:plot_group plot_dict="${rms_plots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band,order_idx"
                  sort_row_by="band,order_idx">

        <%def name="title()">
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
        </%def>

        <%def name="caption_title(plot)">
        </%def>
</%self:plot_group>

<%self:plot_group plot_dict="${img_plots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band,order_idx"
                  sort_row_by="band,order_idx">

        <%def name="title()">
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']} Type: ${plot.parameters['type']}
        </%def>

        <%def name="caption_title(plot)">
            Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']} Type: ${plot.parameters['type']}
        </%def>
</%self:plot_group>
