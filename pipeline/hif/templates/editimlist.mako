
<%!
rsc_path = "../"
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr

from typing import Sequence, Any

def get_multiple_line_string(
    targets: Sequence[dict[str, Any]], 
    key: str, 
    str_format: str = '{}',
    separator: str = '<br>'
) -> str:
    """Convert a list of dictionaries to a formatted multi-line string.
    
    Extracts values from each dictionary using the specified key and formats them
    into a single string with customizable separators and formatting.
    
    Args:
        targets: Sequence of dictionaries to extract values from
        key: Dictionary key to extract values from each target
        str_format: Format string for each value (e.g., '{:.2%}' for percentages)
        separator: String to join formatted values (default: '<br>' for HTML)
        
    Returns:
        Formatted string with each value on a separate line
        
    Examples:
        >>> data = [{'name': 'Alice', 'score': 0.95}, {'name': 'Bob', 'score': 0.87}]
        >>> get_multiple_line_string(data, 'name')
        'Alice<br>Bob'
        >>> get_multiple_line_string(data, 'score', '{:.1%}')
        '95.0%<br>87.0%'
        >>> get_multiple_line_string(data, 'name', separator='\\n')
        'Alice\\nBob'
    """
    if not targets:
        return ''

    formatted_values = [str_format.format(target[key]) for target in targets]
    return separator.join(formatted_values)
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Edit image list</%block>

%if vlass_flagsummary_plots_html is not None:
    <h2>VLASS Flagging Summary</h2>
    ${vlass_flagsummary_plots_html}
%endif

<h2>Image list settings</h2>

<%
r=result[0]
targets=result[0].targets
%>

%if not len(targets):
    <p>There are no clean targets.</p>
%else:
    <%
    # targets only contain 1 element execept vlass-se-cube
    target = targets[0]
    %>
    <div class="table-responsive">
    <table class="table table-bordered table-striped table-hover table-condensed">
        <tr>
            <td><strong>Imaging heuristics mode</strong></td>
            <td>${r.img_mode}</td>
        </tr>
        <tr>
            %if r.img_mode == 'VLASS-SE-CUBE':
                <td><strong>Image name (per plane)</strong></td>
                <td>${get_multiple_line_string(targets, 'imagename')}</td>                
            %else:
                <td><strong>Image name</strong></td>
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
            %if r.img_mode == 'VLASS-SE-CUBE':
                <td><strong>spw (per plane)</strong></td>
                <td>${get_multiple_line_string(targets, 'spw')}</td>  
            %else:
                <td><strong>spw</strong></td>
                <td>${target['spw']}</td>
            %endif
        </tr>
        <tr>
            %if r.img_mode == 'VLASS-SE-CUBE':
                <td><strong>reffreq (per plane)</strong></td>
                <td>${get_multiple_line_string(targets, 'reffreq')}</td> 
            %else:
                <td><strong>reffreq</strong></td>
                <td>${target['reffreq']}</td>
            %endif
        </tr>
        <tr>
            %if r.img_mode == 'VLASS-SE-CUBE':
                <td><strong>flagpct (per plane)</strong></td>
                <td>${get_multiple_line_string(targets, 'flagpct', str_format='{:.2%}')}</td>
            %endif
        </tr>        
        <%
        if isinstance(target['mask'], list):
            mask='<br>'.join(target['mask'])
        else:
            mask=target['mask']
        %>
        <tr>
            <td><strong>mask (per iter)</strong></td>
            <td>${mask}</td>
        </tr>            
        <tr>
            <td><strong>Search buffer radius (arcsec)</strong></td>
            <td>${r.buffer_size_arcsec}</td>
        </tr>
        <tr>
            <td><strong>Number of fields</strong></td>
            <td>${len(target['field'].split(','))}</td>
        </tr>           
        %for key in target.keys():
            %if key in target.keys() and key not in ('imagename', 'spw', 'phasecenter', 'cell', 'imsize', 'field', 'heuristics', 'vis', 'is_per_eb', 'antenna', 'reffreq', 'mask', 'flagpct'):
                <tr>
                    <td><strong>${key}</strong></td>
                    <td>${target[key]}</td>
                </tr>
            %endif
        %endfor
    </table>
    </div>
%endif
