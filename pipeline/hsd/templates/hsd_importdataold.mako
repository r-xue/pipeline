<%!
import os
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Register measurement sets with the pipeline</%block>

<!--
<ul class="unstyled">
	<li class="alert alert-info">
		<strong>To-do</strong> Missing Source.xml + no solar system object or cal known to CASA.
	</li>
	<li class="alert alert-info">
		<strong>To-do</strong> Missing BDFs.
	</li>
</ul>
-->

<p>Data from ${num_mses} measurement set${'s were' if num_mses != 1 else ' was'}
 registered with the pipeline. The imported data 
${'is' if num_mses == 1 else 'are'} summarised below.</p>

<table class="table table-bordered table-striped table-condensed"
	   summary="Summary of Imported Measurement Sets">
	<caption>Summary of Imported Measurement Sets</caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="2">Measurement Set</th>
	        <th scope="col" rowspan="2">SchedBlock ID</th>
			<th scope="col" rowspan="2">Src Type</th>
			<th scope="col" rowspan="2">Dst Type</th>
			<th scope="col" colspan="2">Number Imported</th>
			<th scope="col" rowspan="2">Size</th>
		</tr>
		<tr>
			<th>Scans</th>
			<th>Fields</th>
		</tr>
	</thead>
	<tbody>
% for importdata_result in result:
	% for ms in importdata_result.mses:
		<tr>
			<td>${ms.basename}</td>
			<td>${ms.schedblock_id}</td>
			<td>${importdata_result.origin[ms.basename]}</td>
			<!-- ScanTables are handled in the template for hsd_importdata, so
				 we can hard-code MS here -->
			<td>MS</td>
			<td>${len(ms.scans)}</td>
			<td>${len(ms.fields)}</td>
			<td>${str(ms.filesize)}</td>
		</tr>
	% endfor
% endfor
	</tbody>
</table>
