<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Self-Calibration</%block>

<script type="text/javascript">
  $("td:contains('Selfcal Success')").addClass('desc-summary');
</script>


<style type="text/css">

.desc-summary {
  font-weight:bold;
  background-color:#F9F9F9;
}

.table {
  border: 2px solid #CCC;
  font-family: Arial, Helvetica, sans-serif;
  border-collapse: collapse;
  vertical-align: middle;
  text-align: center;
  float: left;
  margin: 0px 10px;
  width: auto;
}

.table td {
  border: 1px solid #CCC;
  vertical-align: middle;
  text-align: center;
  background-clip: padding-box;
  min-width: 60px;     
}

.table th {
  font-weight: bold;
  vertical-align: middle;
  text-align: center;  
}

.table td:first-child {
  vertical-align: middle;
  text-align: center;       
}

.anchor { padding-top: 90px; }

img {
    height: auto;
    display: block;
    margin-left: auto;
    margin-right: auto;
}

.rotate {
    /* FF3.5+ */
    -moz-transform: rotate(-90.0deg);
    /* Opera 10.5 */
    -o-transform: rotate(-90.0deg);
    /* Saf3.1+, Chrome */
    -webkit-transform: rotate(-90.0deg);
    /* IE6,IE7 */
    filter: progid: DXImageTransform.Microsoft.BasicImage(rotation=0.083);
    /* IE8 */
    -ms-filter: "progid:DXImageTransform.Microsoft.BasicImage(rotation=0.083)";
    /* Standard */
    transform: rotate(-90.0deg);
}


</style>

<%
import numpy as np

def fm_band(band):
    return band.replace('_',' ')

def fm_target(target):
    return target.replace('_',' ') 

def fm_sc_success(success):
    if success:
        return '<a style="color:blue">Yes</a>'
    else:
        return '<a style="color:red">No</a>'

def fm_reason(slib):
    rkey='Stop_Reason'
    if rkey not in slib:
        return 'Estimated Selfcal S/N too low for solint'
    else:
        return slib[rkey]
%>

<p> Self-calibration using the science target visibilities.</p>

<!-- Brief Summary -->

<a class="anchor" id="targetlist"></a>
<h3>List of Self-cal Targets</h3>

<div class="table-responsive">
<table class="table table-bordered">
  <thead>
        <tr>
            <th>Field</th>
            <th>Band</th>
            <th>spw</th>
            <th>phasecenter</th>
            <th>cell</th>
            <th>imsize</th>
            <th>Solints to Attempt</th>
        <tr>
  </thead>
  <caption>Self-calibration Target(s) Summary</caption>
  <tbody>
  % for tr in targets_summary_table:
    <tr>
    % for td in tr:
      ${td}
    % endfor
    </tr>
  %endfor
  </tbody>
</table> 
</div>

<h3>Per-Target Details</h3>

% for target in cleantargets:

    <a class="anchor" id="${target['field_name']}${target['sc_band']}"></a>
    <h4>
      ${fm_target(target['field'])}&nbsp;${fm_band(target['sc_band'])}
      <a href="#targetlist" class="btn btn-link btn-sm">
        <span class="glyphicon glyphicon-th-list"></span>
      </a>
    </h4>
    
    <%
    slib=target['sc_lib']
    key=(target['field_name'],target['sc_band'])
    %>

            
    <div class="table-responsive">
    <table class="table table-bordered">
      <!-- <caption style="caption-side:top">Initial/Final Image Comparisons</caption> -->
      <caption>Initial/Final Image Comparisons</caption>
      <tbody>
      % for tr in summary_tabs[key]:
        <tr>
        % for td in tr:
          ${td}
        % endfor
        </tr>
      %endfor
      </tbody>
    </table>      
    </div>      
    

    % if slib['SC_success']:
    <div class="table-responsive">
    <table class="table table-bordered">
      <!-- <caption style="caption-side:top">Per solint stats</caption> -->
      <caption>Per solint stats</caption>
      <thead>
          <tr>
              <th>Solint</th>
              % for solint in target['sc_solints']:
                <th>${solint}</th>
              % endfor
          <tr>
      </thead>      
      <tbody>
      % for tr in solint_tabs[key]:
          <tr>
          % for td in tr:
            ${td}
          % endfor
          </tr>
      %endfor
      </tbody>
    </table>      
    </div>
    % endif

    % if slib['SC_success'] and spw_tabs[key] is not None :
    <div class="table-responsive row">
    <table class="table table-bordered">
      <caption>Per Spectral-window Summary</caption>
      <tbody>
      % for tr in spw_tabs[key]:
        <tr>
        % for td in tr:
          ${td}
        % endfor
        </tr>
      %endfor
      </tbody>
    </table>      
    </div>        
    % endif
        
% endfor



