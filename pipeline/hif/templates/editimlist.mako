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
      # targets only contain 1 element execept vlass-se-cube
      target = result[0].targets[0]
    %>
    <table class="table table-bordered table-striped table-condensed">
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
            %if result[0].img_mode == 'VLASS-SE-CUBE':
                <td><strong>spw (per plane)</strong></td>
                <td>${'<br>'.join(result[0].targets_spw)}</td>
            %else:
                <td><strong>spw</strong></td>
                <td>${target['spw']}</td>
            %endif
        </tr>
        <tr>
            %if result[0].img_mode == 'VLASS-SE-CUBE':
                <td><strong>reffreq (per plane)</strong></td>
                <td>${'<br>'.join(result[0].targets_reffreq)}</td>
            %else:
                <td><strong>reffreq</strong></td>
                <td>${target['reffreq']}</td>
            %endif
        </tr>
        <%
        if isinstance(target['mask'], list):
            mask='<br>'.join(target['mask'])
        else:
            mask=result[0].mask
        %>
        <tr>
            <td><strong>mask (per iter)</strong></td>
            <td>${mask}</td>
        </tr>            
        <tr>
            <td><strong>Search buffer radius (arcsec)</strong></td>
            <td>${result[0].buffer_size_arcsec}</td>
        </tr>
        <tr>
            <td><strong>Number of fields</strong></td>
            <td>${len(target['field'].split(','))}</td>
        </tr>           
        %for key in target.keys():
            %if key in target.keys() and key not in ('imagename', 'spw', 'phasecenter', 'cell', 'imsize', 'field', 'heuristics', 'vis', 'is_per_eb', 'antenna', 'reffreq', 'mask'):
                <tr>
                    <td><strong>${key}</strong></td>
                    <td>${target[key]}</td>
                </tr>
            %endif
        %endfor
    </table>
%endif
