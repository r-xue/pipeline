<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Renormalization</%block>

<p>ALMA cross-correlations are divided by the auto-correlation as a function of frequency, in the correlator. This has a variety of advantages for operations and calibration, but if there is strong line emission detected in the autocorrelation (i.e. as would be detected in a single dish spectrum), that emission can anomalously decrease the cross-correlation amplitude at those frequencies.</p>
<p>This effect can be mitigated by comparing the autocorrelation spectrum (AC) of the target with the AC of the bandpass, which is generally located away from any such bright contaminating line emission. The ratio of the bandpass AC to the target AC provides a scaling factor as a function of frequency that can be used as a first order correction spectrum. However, atmospheric and instrumental variation (e.g. baseline ripple) need to be fitted and removed, so the spectrum is divided into several segments (marked on the plots as thin dotted vertical lines) for that fitting. The fitted AC ratio is presented here as the 'renorm scale factor' or 'renorm amplitude'.</p>
<p>All targets, spws, and measurement sets with maximum scaling above the observatory determined threshold will have the scaling applied.
<p>Informative plots are collected in a pdf for each spw and source, linked from the table below.</p>
<p>The first plot in the pdf is a ReNormSpectra summary plot showing the average scaling spectrum over all scans, and for mosaics, all fields in the mosaic with peak scaling above the threshold. All antennas are plotted as dashed red and blue (for XX and YY), and the mean is plotted solid.</p>
<p>The pdf next contains RenormDiagnosicCheck plots corresponding to each field and scan. The scaling spectrum is plotted as solid lines for each antenna (again red and blue for XX and YY), and the median as a dashed line (green and black for XX and YY).</p>
<p>Heuristics in the renormalization script have been applied to detect and correct spikes, dips, and jumps near the segment boundaries (marked with thin vertical dotted lines). Less significant (below the threshold for applying the correction) features may remain.</p>
<p>Features in the scaling spectrum associated with atmospheric features require additional care - ALMA data reduction staff will have evaluated these and minimized them insofar as possible with current heuristics, but PIs should take note of the shape and magnitude of any applied correction when performing line science at frequencies overlapping atmospheric lines.</p>

<hr/>

<p><b>MS/Source/SPW that trigger the need for renormalization above a threshold of ${result[0].threshold} highlighted in red.</b></p>
<p><em>Please refer to the Pipeline User's Guide (linked to this weblog's Home page) for more details on renormalization and interpretation of the plots.</em></p>
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
