<%!
rsc_path = "../"
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Edit image list</%block>

<h2>Image list settings</h2>

%if not len(result[0].targets):
    <p>There are no clean targets.</p>
%else:
    <%
      target = result[0].targets[0]
    %>
    <table class="table">
        <tr>
            <td><strong>Imaging heuristics mode</strong></td>
            <td>${result[0].img_mode}</td>
        </tr>
        <tr>
            <td><strong>Image name</strong></td>
            %if result[0].img_mode == 'VLASS-SE-CUBE':
                <td>${os.path.basename(result[0].targets_imagename)}</td>
            %else:
                <td>${os.path.basename(target['imagename'])}</td>
            %endif
        </tr>
        <tr>
            <td><strong>Phase center</strong></td>
            <td>${target['phasecenter']}</td>
        </tr>
        <tr>
            <td><strong>Cell size</strong></td>
            <td>${target['cell']}</td>
        </tr>
        <tr>
            <td><strong>Image size</strong></td>
            <td>${target['imsize']}</td>
        </tr>
        <tr>
            <td><strong>Search buffer radius (arcsec)</strong></td>
            <td>${result[0].buffer_size_arcsec}</td>
        </tr>
        <tr>
            <td><strong>Number of fields</strong></td>
            <td>${len(target['field'].split(','))}</td>
        </tr>
        <tr>
            <td><strong>spw</strong></td>
            %if result[0].img_mode == 'VLASS-SE-CUBE':
                <td>${result[0].targets_spw}</td>
            %else:
                <td>${target['spw']}</td>
            %endif
        </tr>        
        %for key in target.keys():
            %if key in target.keys() and key not in ('imagename', 'spw', 'phasecenter', 'cell', 'imsize', 'field', 'heuristics', 'vis', 'is_per_eb', 'antenna'):
                <tr>
                    <td><strong>${key}</strong></td>
                    <td>${target[key]}</td>
                </tr>
            %endif
        %endfor
    </table>
%endif
