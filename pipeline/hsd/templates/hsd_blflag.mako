<%!
rsc_path = "../"
import collections
import os
import pipeline.infrastructure.utils as utils

def get_fraction(flagged, total):
   if total == 0 or flagged < 0.0:
       return 'N/A' 
   else:
       return '%0.1f%%' % (100.0 * float(flagged) / float(total))

FlagDetailTR = collections.namedtuple("FlagDetailTR", "name spw ant pol nrow totnrow totfrac tsys before postbl prebl postrmean prermean postrms prerms link")
FlagDetailTRV = collections.namedtuple("FlagDetailTR", "name vspw spw ant pol nrow totnrow totfrac tsys before postbl prebl postrmean prermean postrms prerms link")
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Flag data by Tsys and statistics of spectra</%block>

<%
def make_detailed_table(result, stage_dir, fieldname):
   rel_path = os.path.basename(stage_dir)   ### stage#
   rows = []
   for r in result:
       summaries = r.outcome['summary']
       vis = r.inputs['vis']
       ms = pcontext.observing_run.get_ms(vis)
       for summary in summaries:
           if summary['field'] != fieldname:
               continue
           html_name = summary['html']
           asdm_name = summary['name']
           ant_name = summary['antenna']
           spw = summary['spw']
           vspw = pcontext.observing_run.real2virtual_spw_id(spw, ms)
           pol = summary['pol']
           nrows = summary['nrow']
           flags = summary['nflags_list']
           cell_elems = [asdm_name, spw, ant_name, pol, nrows, flags[0]]
           for nflg in flags:
               cell_elems.append(get_fraction(nflg, nrows))
#            htext = '<a class="replace-pre" href="%s">details</a>' % (os.path.join(rel_path, html_name),)
           htext = '<a href="%s">details</a>' % (os.path.join(rel_path, html_name),)
           cell_elems.append(htext)
           if dovirtual:
               cell_elems.insert(1, vspw)
               trow = FlagDetailTRV(*cell_elems)
           else:
               trow = FlagDetailTR(*cell_elems)
           rows.append(trow)
   if len(rows) == 0:
       return []
   return utils.merge_td_columns(rows, num_to_merge=4)

try:
   stage_number = result.stage_number
   stage_dir = os.path.join(pcontext.report_dir,'stage%d'%(stage_number))
   if not os.path.exists(stage_dir):
       os.mkdir(stage_dir)

   trim_name = lambda s : s if not s.startswith('"') or not s.endswith('"') else s[1:-1]
   unique_fields = []
   for r in result:
       summaries = r.outcome['summary']
       for summary in summaries:
            if summary['field'] not in unique_fields:
                unique_fields.append(summary['field'])
 
   flag_types = ['Total', 'Tsys', 'After calibration']
   fit_flags = ['Baseline RMS', 'Running mean', 'Expected RMS']
except Exception as e:
   print('hsd_imaging html template exception:{}'.format(e))
   raise
%>

<!-- short description of what the task does -->
<p>This task flags spectra by several criteria:
<ol>
	<li> eliminate spectra with outlier RMS (Baseline RMS)</li>
	<li> eliminate rapid variation of spectra using deviation from the running mean (Running mean)</li>
	<li> eliminate spectra with remarkably large RMS than expected (Expected RMS)</li>
	<li> eliminate spectra with outlier Tsys value</li>
</ol>
For 1.-3., the RMSes of spectra before and after baseline fit are obtained using line free channels.
</p>

<h2>Contents</h2>
<ul>
<li><a href="#summarytablepereb">Flag Summary per EB</a></li>
<li><a href="#summarytableperfield">Flag Summary per Field and SpW</a></li>
<li><a href="#detailtable">Flag by Reason</a></li>
  <ul>
%for field in unique_fields:
	<li><a href="#${trim_name(field)}">${field}</a></li>
%endfor
  </ul>
</ul>


<H2 id="summarytablepereb" class="jumptarget">Flag Summary per EB</H2>
<table class="table table-bordered table-striped" summary="Flag Summary per EB">
	<caption>Summary of flagged solutions.<br>
            "Flags by Reason" states the amount of solutions flagged (in number of data rows) as a fraction of the specified data. 
            Pre-fit metrics are performed on calibrated spectra before the baseline-fit, 
            while the post-fit ones are performed on data after the baseline-fit.<br>
            "Flagged Fraction" indicates the amount of data flagged (in number of spectral channels). 
    </caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="3">Measurement Set</th>
	        <th scope="col" colspan="7">Flags by Reason</th>
	        <th scope="col" rowspan="2" colspan=3>Flagged Fraction</th>
            <th scope="col" rowspan="3">Flagging Details</th>
		</tr>
		<tr>
	        <th scope="col" colspan="2">Baseline RMS</th>
	        <th scope="col" colspan="2">Running mean</th>
	        <th scope="col" colspan="2">Expected RMS</th>
	        <th scope="col" rowspan="2">Outlier Tsys</th>
	    </tr>
        <tr>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Before</th>
            <th scope="col">Additional</th>
            <th scope="col">Total</th>
	</thead>
	<tbody>
	% for tr, subpage in zip(per_eb_summary_table_rows, statistics_subpages):
        <%
            subpage_html = os.path.join( dirname, subpage['html'] )
        %>
		<tr>
		% for td in tr:
			${td} 
		% endfor
        <TD>
        <a href="${subpage_html}" class="replace" data-vis="${subpage['vis']}">
        Plots
        </a>
        </TD>
		</tr>
	%endfor
	</tbody>
</table>



<H2 id="summarytableperfield" class="jumptarget">Flag Summary per Field and SpW</H2>
<table class="table table-bordered table-striped" summary="Flag Summary per Field and SpW">
	<caption>Flag summary of ON-source target scans per source and SpW <(in number of spectral channels).</caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="2">Field</th>
	        %if dovirtual:
	        <th scope="col" rowspan="2">Virtual SpW</th>
	        %else:
	        <th scope="col" rowspan="2">SpW</th>
	        %endif
	        <th scope="col" colspan="3">Flagged Fraction</th>
		</tr>
		<tr>
	        <th scope="col">Before</th>
	        <th scope="col">Additional</th>
	        <th scope="col">Total</th>
	    </tr>
	</thead>
	<tbody>
	% for tr in per_field_summary_table_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
</table>


<H2 id="detailtable" class="jumptarget">Flag by Reason</H2>
%for field in unique_fields:
	<H3 id="${trim_name(field)}" class="jumptarget">${field}</H3>
	<table class="table table-bordered table-striped " summary="${field}">
	<thead>
		<tr>
            %if dovirtual:
    			<th colspan="6">Data Selection</th>
	        %else:
        		<th colspan="5">Data Selection</th>
            %endif
   			<th colspan="2">Flagged Total</th>
			%for ftype in flag_types[1:]:
				<th rowspan="2">${ftype}</th>
			%endfor
			%for fflag in fit_flags:
				<th colspan="2">${fflag}</th>
			%endfor
			<th rowspan="2">Plots</th>
		</tr>
		<tr>
		    %if dovirtual:
			<th>Name</th><th>vspw</th><th>spw</th><th>Ant.</th><th>Pol</th><th># of rows</th>
			<th>row #</th><th>fraction</th>
			%else:
			<th>Name</th><th>spw</th><th>Ant.</th><th>Pol</th><th># of rows</th>
			<th>row #</th><th>fraction</th>
			%endif
			%for fflag in fit_flags:
				<th>post-fit</th><th>pre-fit</th>
			%endfor
		</tr>
		</thead>
		<tbody>
		% for tr in make_detailed_table(result, stage_dir, field):
			<tr>
			% for td in tr:
				${td}
			% endfor
			</tr>
		%endfor <!-- end of table row loop -->
		</tbody>
		</table>
	%endfor <!-- end of per field loop -->

