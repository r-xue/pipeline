<%!
rsc_path = ''
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Analysis of spectral index map</%block>

<table class="table table-bordered table-striped table-condensed">
  <tr>
    <td><b>Restored max location</b></td>
    <td>${result[0].max_location}</td>
  </tr>
  <tr>
    <td><b>Alpha at restored max</b></td>
    <td>${result[0].alpha_and_error}</td>
  </tr>
</table>
