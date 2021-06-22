<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Renormalization</%block>

<p>MS/Source/SPW that trigger the need for renormalization above a threshold of ${result[0].threshold}.</p>

<table class="table table-bordered table-striped" summary="Renormalization results">
	<caption>Renormalization results</caption>
    <thead>
        <tr>
            <th>MS Name</th>
            <th>Source Name</th>
            <th>SPW</th>
            <th>Max Renorm Scale Factor (field id)</th>
            <th>PDF Link to Diagnostic Plots</th>
	    </tr>
	</thead>
	<tbody>
    % if not table_rows:
      <tr>
          <td colspan="5">No Corrections</td>
      </tr>
    % else:
        % for tr in table_rows:
        <tr>
            % for td in tr:
                ${td}
            % endfor
        </tr>
        %endfor
    % endif
	</tbody>
</table>
