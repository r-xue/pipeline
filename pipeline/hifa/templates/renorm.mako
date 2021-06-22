<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Renorm</%block>

<p>Renorm</p>

<table class="table table-bordered table-striped" summary="Renormalization results">
	<caption>Renormalization results</caption>
    <thead>
        <tr>
            <th>MS Name</th>
            <th>Source Name</th>
            <th>SPW</th>
            <th>Max Renorm Scale Factor</th>
            <th>PDF Link to Diagnostic Plots</th>
	    </tr>
	</thead>
	<tbody>
    % for tr in table_rows:
    <tr>
        % for td in tr:
            ${td}
        % endfor
    </tr>
	%endfor
	</tbody>
</table>