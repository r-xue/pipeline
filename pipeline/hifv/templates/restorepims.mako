<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%
def boolean_cell(is_true):
    if is_true:
        return '<td><span class="glyphicon glyphicon-ok"></span></td>'
    else:
        return '<td><span class="glyphicon glyphicon-remove"></span></td>'
%>

<%block name="title">Restore PIMS</%block>

<p>Restore rfi-flagged and self-calibrated visibility for a per-image measurement set (PIMS), using reimaging resources from the single-epoch continuum imaging products</p>



% for r in result:

    <%
    rr =r.restore_resources
    item_list=[('Reimaging Resource Tarball','reimaging_resources'),
               ('Selfcal Table','selfcal_table'),
               ('Flag Restore Version','flag_table'),
               ('Tier 1 Mask','tier1_mask'),
               ('Combined Final Mask','tier2_mask'),
               ('Selfcal Model Image','model_image')]
    %>
    <table class="table table-bordered table-striped table-condensed">
        <tbody>
            % for name, key in item_list:
            <tr>
                <th>${name}</th>
                <td>${rr[key][0]}</td>
                ${boolean_cell(rr[key][1])}
            </tr>
            % endfor                    
        </tbody>
    </table>

% endfor