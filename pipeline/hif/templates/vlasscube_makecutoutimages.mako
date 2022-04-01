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
    print(x,norm(x),rgb)
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
    print(x,norm(x),rgb)
    return rgb_hex


    #print(matplotlib.colors.cnames["lightgray"]                                                                         


%>


<style type="text/css">

.table-custom table {
  table-layout: fixed;
  width: 100px;
  border: 3px solid;
}

.table-custom tbody {
    display: block;
    overflow-x: auto;
}

.table-custom tr {
  margin:0;  
  padding: 0;
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
  padding:0; 
  margin:0;    
  line-height: 12px;
}

.table-custom td {
  table-layout: fixed;
  width: 100px;
  height: 12px;
  border-top: 1px solid #dddddd;
  border-left: 1px solid #dddddd;
  border-right: 1px solid #dddddd;
  border-bottom: 1px solid #dddddd;
  vertical-align: middle;
  text-align: center;  
  font-size: 12px;
  padding:0; 
  margin:0;  
  line-height: 12px;
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

<%
stats= plotter.result.stats
stats_summary= plotter.result.stats_summary
madrmsplots={}
madrmsplots['MADrms vs. Spw from Non-Pbcor Image']=[plotter.result.madrmsplots]
%>

<h4>Cutout Image Statistical Properties</h4>

<table style="float: left; margin:0 12px; width: auto; text-align:center" class="table table-striped table-custom">

<caption>
  <li>
      Peak: the pixel value with the largest deviation from zero, which can be either the maximum or minimum value of each image. This is similar to the definition of "Peak" residual used in CASA/tclean.
  </li>
  <li>
      The gray-out cell highlights the spw with the largest deviation of an image property, among all spw selection groups.
  </li>  
  <li>
      MADrms: the median absolute deviation from the median (i.e., 'medabsdevmed' defined in the CASA/imstat output), multiplied by 1.4826.
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
      <th colspan="3">PB</th>
    % else:
      <th colspan="3">Rms Image</th>
    % endif
  </tr>

  <tr>
    <th rowspan="2">Spw</th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1">Peak/MADrms</th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1">Peak/MADrms</th>
    <th colspan="1">Max</th>
    <th colspan="1">Median</th>
    <th colspan="1">Pix<612&mu;Jy</th>
    % if name_pol=='I':
      <th colspan="1">Masked</th>
      <th colspan="1">Max</th>
      <th colspan="1">Min</th>
      <th colspan="1">Median</th>
    % endif
  </tr>

  <tr>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>N/A</td>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>N/A</td>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>mJy/beam</td>
    <td colspan="1" class='last'>Percentage</td>
    % if name_pol=='I':
      <td colspan="1" class='last'>Percentage</td>
      <td colspan="1" class='last'>N/A</td>
      <td colspan="1" class='last'>N/A</td>
      <td colspan="1" class='last'>N/A</td>
    % endif
  </tr>  

  % for spw,stats_spw in stats.items():
    <tr>
      <th>${str(spw)}</th>
      
      <%
      type_item_scale=[('image','peak',1e3),
                  ('image','madrms',1e3),
                  ('image','max/madrms',1),
                  ('residual','peak',1e3),
                  ('residual','madrms',1e3),
                  ('residual','max/madrms',1),
                  ('rms','max',1e3),
                  ('rms','median',1e3),
                  ('rms','pct<6.12e-6',1e2),
                  ('rms','pct_masked',1e2),
                  ('pb','max',1.),
                  ('pb','min',1.),
                  ('pb','median',1.)]
      %>

      % for t,i,s in type_item_scale:
        % if name_pol!='I' and (t,i) in [('rms','pct_masked'),('pb','max'),('pb','min'),('pb','median')]:
          <% continue %> 
        % endif

        <%
        if 'pct' in i:
          suffix='&#37'
        else:
          suffix=''
        %>

        % if spw==stats_summary[t][i]['spw_outlier'][idx_pol]:
            <td bgcolor="#D3D3D3">${fmt_val(stats_spw[t][i][idx_pol],scale=s)}${suffix}</td>
        % else:
            <td>${fmt_val(stats_spw[t][i][idx_pol],scale=s)}${suffix}</td>
        % endif
        <%
        # bgcolor=val2color(stats_spw[t][i][idx_pol],cmap_name='Reds',vmin=stats_range[t][i][0],vmax=stats_range[t][i][1])
        # <td bgcolor="${bgcolor}">${fmt_val(stats_spw[t][i][idx_pol],scale=s)}</td>
        # <td>${fmt_val(stats_spw[t][i][idx_pol],scale=s)}</td>
        %>
      % endfor

    </tr>

  % endfor

% endfor

</tbody>

</table>

<div style="clear:both;"></div>


<%self:plot_group plot_dict="${madrmsplots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band"
                  sort_row_by="isalpha">

        <%def name="title()">
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
        </%def>

        <%def name="caption_title(plot)">
        </%def>
</%self:plot_group>

<%self:plot_group plot_dict="${subplots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band"
                  sort_row_by="isalpha">

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
