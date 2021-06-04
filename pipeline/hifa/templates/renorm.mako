<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Renorm</%block>

<p>Renorm</p>

<table class="table table-bordered table-striped" summary="Targetflag flagging results">
	<caption>Renormalization parameters</caption>
        <thead>
	    <tr>
	        <th>apply</th>
	        <th>threshold</th>
	    </tr>
	</thead>
	<tbody>
    	%for r in result:
		<tr>
			<td>${r.apply}</td>
			<td>${r.threshold}</td>
		</tr>
		%endfor
	</tbody>
</table>