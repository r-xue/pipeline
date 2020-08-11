<%inherit file="t2-4m_details-base.mako"/>
<%block name="header" />

<%block name="title">Find Continuum</%block>

% if not table_rows:
    <p>There are no continuum finding results.
% else:

    <%
    field_block_indices = []
    field = None
    for i, row in enumerate(raw_rows):
        if row.field != field:
            field_block_indices.append(i)
            field = row.field
    field_block_indices.append(len(raw_rows))
    %>

    %if len(field_block_indices) > 2:
        <h3>
        Fields
        </h3>
        <ul>
            %for i in field_block_indices[:-1]:
                <li>
                <a href="#field_block_${i}">${raw_rows[i].field}</a>
                </li>
            %endfor
        </ul>
    %endif

    <table class="table">
        <thead>
            <tr>
                <th rowspan="2">Field</th>
                <th rowspan="2">Spw</th>
                <th colspan="3">Continuum Frequency Range</th>
                <th rowspan="2">Status</th>
                <th rowspan="2">Average spectrum</th>
                <th rowspan="2">Joint mask</th>
            </tr>
            <tr>
                <th>Start</th>
                <th>End</th>
                <th>Frame</th>
            </tr>
        </thead>
        <tbody>
            <%
            field_block = 0
            %>
            %for i, tr in enumerate(table_rows):
                %if len(field_block_indices) > 2 and field_block_indices[field_block] == i:
                    <tr id="field_block_${field_block_indices[field_block]}" class="jumptarget" style="border-bottom:2px solid black">
                        <td colspan="8"></td>
                    </tr>
                    <%
                    field_block += 1
                    %>
                %endif
                <tr>
                    %for td in tr:
                        ${td}
                    %endfor
                </tr>
            %endfor
        </tbody>
    </table>
    <p>${contdat_path_link}
%endif
