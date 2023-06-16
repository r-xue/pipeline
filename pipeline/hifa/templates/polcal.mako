<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Polarisation Calibration</%block>

<p>This task creates polarisation solutions for each polarisation session of measurement sets.</p>

<h2>Sessions</h2>
<table class="table table-bordered table-striped">
    <caption>Summary of polarisation calibrator per session.</caption>
    <thead>
        <tr>
            <th>Session</th>
            <th>Measurement Sets</th>
            <th>Polarisation Calibrator</th>
            <th>Reference Antenna</th>
        </tr>
    </thead>
    <tbody>
    % for session_name in session_names:
        <%
        nvis = len(vislists[session_name])
        %>
        <tr>
            <td rowspan="${nvis}">${session_name}</td>
            <td>${vislists[session_name][0]}</td>
            <td rowspan="${len(vislists[session_name])}">${polfields[session_name]}</td>
            <td rowspan="${len(vislists[session_name])}">${refants[session_name]}</td>
        </tr>
        % if len(vislists[session_name]) > 1:
            % for vis in vislists[session_name][1:]:
            <tr>
                <td>${vis}</td>
            </tr>
            % endfor
        % endif
    % endfor
    </tbody>
</table>
