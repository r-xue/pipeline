<%!
rsc_path = "../"
import os
%>
<%inherit file="t2-4m_details-base.html"/>

<%block name="header" />

<%block name="title">Flag data by Tsys, weather, and statistics of spectra</%block>

<%
def get_fraction(flagged, total):
   if total == 0 or flagged < 0.0:
       return 'N/A'
   else:
       return '%0.1f%%' % (100.0 * float(flagged) / float(total))

try:
   stage_number = result.stage_number
   stage_dir = os.path.join(pcontext.report_dir,'stage%d'%(stage_number))
   if not os.path.exists(stage_dir):
       os.mkdir(stage_dir)

   rel_path = os.path.basename(stage_dir)   ### stage#

   html_names = []
   asdm_names = []
   ant_names = []
   spw = []
   pol = []
   nrows = []
   flags = []
   field_map = {}
   default_field = "default"
   for r in result:
       summaries = r.outcome['summary']
       for summary in summaries:
           html_names.append(summary['html'])
           asdm_names.append(summary['name'])
           ant_names.append(summary['antenna'])
           spw.append(summary['spw'])
           pol.append(summary['pol'])
           nrows.append(summary['nrow'])
           flags.append(summary['nflags'])
           field = summary['field'] if r.outcome['byfield'] else default_field
           if not field_map.has_key(field):
               field_map[field] = []
           field_map[field].append(len(html_names) -1)

   unique_fields = field_map.keys()
   do_field = False if (len(unique_fields)==1 and unique_fields[0] == default_field) else True
   flag_types = ['Total', 'Tsys', 'Weather', 'User', 'Online']
   fit_flags = ['Baseline RMS', 'Running mean', 'Expected RMS']
except Exception, e:
   print 'hsd_imaging html template exception:', e
   raise e
%>

<!-- short description of what the task does -->
<p>This task flags spectra by several criteria:
<ol>
	<li> eliminate spectra with outlier RMS (Baseline RMS)</li>
	<li> eliminate rapid variation of spectra using deviation from the running mean (Running mean)</li>
	<li> eliminate spectra with remarkably large RMS than expected (Expected RMS)</li>
	<li> eliminate spectra with outlier Tsys value</li>
	<li> by weather (currently disabled)</li>
	<li> by user defined threshold (currently disabled)</li>
</ol>
For 1.-3., the RMSes of spectra before and after baseline fit are obtained using line free channels.
</p>

<H2>Flag Summaries</H2>

% if html_names:
	%for field in unique_fields:
		%if do_field:
			<H3>${field}</H3>
		%endif
		<table class="table table-bordered table-striped " summary=field>
		<thead>
			<tr>
			<th rowspan="2">
			<th colspan="5">Data Selection</th>
			<th colspan="2">Flagged Total</th>
			%for ftype in flag_types[1:]:
			<th rowspan="2">${ftype}</th>
			%endfor
			%for fflag in fit_flags:
			<th colspan="2">${fflag}</th>
			%endfor
			</tr>
			<tr>
			<th>Name</th><th>Ant.</th><th>spw</th><th>Pol</th><th># of rows</th>
			<th>row #</th><th>fraction</th>
			%for fflag in fit_flags:
			<th>post-fit</th><th>pre-fit</th>
			%endfor
			</tr>
		</thead>
		<tbody>
		%for idx in field_map[field]:
		<tr>
			<th><a class="replace-pre" href="${os.path.join(rel_path, html_names[idx])}">details</a></th>
			<td>${asdm_names[idx]}</td><td>${ant_names[idx]}</td><td>${spw[idx]}</td><td>${pol[idx]}</td><td>${nrows[idx]}</td>
			<td>${flags[idx][0]}</td>
			%for nflg in flags[idx]:
			<td>${get_fraction(nflg, nrows[idx])}</td>
			%endfor
		</tr>
		%endfor <!-- end of table row loop -->
		</tbody>
		</table>
	%endfor <!-- end of per field loop -->
% else:
No Flag Summaries
% endif
