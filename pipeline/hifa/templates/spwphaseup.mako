<%!
rsc_path = ""
import os

def format_spwmap(spwmap, scispws):
    if not spwmap:
        return ''
    else:
        spwmap_strings=[]
        for ind, spwid in enumerate(spwmap):
        	if ind in scispws:
        		spwmap_strings.append("<strong>{0}</strong>".format(spwid))
        	else:
        		spwmap_strings.append(str(spwid))
        
        return ', '.join(spwmap_strings)
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Compute Spw Phaseup Map and Offsets</%block>

<p>This task computes the spectral window map that will be used to apply the time gaincal phase solutions
and the caltable containing per spw phase offsets.</p>

<h2>Results</h2>
<table class="table table-bordered table-striped" summary="Narrow to wide spw mapping results">
	<caption>Phase solution spw map per measurement set.</caption>
        <thead>
	    <tr>
	        <th>Measurement Set</th>
	        <th>Spectral Window Map</th>
	    </tr>
	</thead>
	<tbody>
    % for spwmap in spwmaps:
		<tr>
			<td>${os.path.basename(spwmap.ms)}</td>
			<td>${format_spwmap(spwmap.spwmap, spwmap.scispws)}</td>
		</tr>
    % endfor
	</tbody>
</table>

% if snr_table_rows:
<table class="table table-bordered table-striped" summary="Estimated phase signal to noise ratios">
	<caption>Estimated phase calibrator signal to noise ratios per measurement set. For spectral
        windows where the estimated SNR is below the specified threshold ('phasesnr' parameter),
        the SNR value is indicated in <strong>bold</strong>.</caption>
    <thead>
	    <tr>
	        <th>Measurement Set</th>
	        <th>Phase SNR threshold</th>
            <th>Spectral Window</th>
            <th>Estimated SNR</th>
        </tr>
	</thead>
	<tbody>
    % for tr in snr_table_rows:
        <tr>
        % for td in tr:
            ${td}
        % endfor
        </tr>
    %endfor
	</tbody>
</table>
% else:
    <p>No information available on estimated phase signal to noise ratios.
% endif

<table class="table table-bordered" summary="Application Results">
        <caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
            <th scope="col" colspan="2">Solution Parameters</th>
            <th scope="col" colspan="2">Applied To</th>
            <th scope="col" rowspan="2">Calibration Table</th>
        </tr>
        <tr>
            <th>Type</th>
            <th>Interval</th>
            <th>Scan Intent</th>
            <th>Spectral Windows</th>
        </tr>
    </thead>
    <tbody>
    % for application in applications:
        <tr>
            <td>${application.ms}</td>
            <td>${application.solint}</td>
            <td>${application.calmode}</td>
            <td>${application.intent}</td>
            <td>${application.spw}</td>
            <td>${application.gaintable}</td>
        </tr>
    % endfor
    </tbody>
</table>
