<%!
rsc_path = ""
import os
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils

# method to output flagging percentages neatly
def percent_flagged(flagsummary):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total is 0:
        return 'N/A'
    else:
        return '%0.1f%%' % (100.0 * flagged / total)

_types = {
    'before': 'Calibrated data before flagging',
    'after': 'Calibrated data after flagging'
}

def plot_type(plot):
    return _types[plot.parameters['type']]

%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Polcal Flag</%block>

<p>Polcalflag</p>

<h2>Contents</h2>
<ul>
% if htmlreports:
    <li><a href="#flagging_commands">Flagging commands</a></li>
%endif
    <li><a href="#flagged_data_summary">Flagged data summary table</a></li>
</ul>

% if htmlreports:
    <h2 id="flagging_commands" class="jumptarget">Flagging</h2>
    <table class="table table-bordered table-striped">
        <caption>Report Files</caption>
        <thead>
            <tr>
                <th>Measurement Set</th>
                <th>Flagging Commands</th>
                <th>Number of Statements</th>
            </tr>
        </thead>
        <tbody>
        % for msname, relpath in htmlreports.items():
            <tr>
                <td>${msname}</td>
                <td><a class="replace-pre" href="${relpath}">${os.path.basename(relpath)}</a></td>
                <td>${rendererutils.num_lines(os.path.join(pcontext.report_dir, relpath))}</td>
            </tr>
        % endfor
        </tbody>
    </table>
% endif

<h2 id="flagged_data_summary" class="jumptarget">Flagged data summary</h2>

% for ms in flags.keys():
<h4>Measurement Set: ${os.path.basename(ms)}</h4>
<table class="table table-bordered table-striped ">
        <caption>Summary of flagged data. Each cell states the amount of data
                flagged as a fraction of the specified data selection.
        </caption>
        <thead>
                <tr>
                        <th rowspan="2">Data Selection</th>
                        <!-- flags before task is always first agent -->
                        <th rowspan="2">flagged before</th>
                        <th rowspan="2">flagged after</th>
                </tr>
        </thead>
        <tbody>
                % for k in ['TOTAL', 'BANDPASS', 'AMPLITUDE', 'PHASE', 'POLARIZATION', 'TARGET']:
                <tr>
                        <th>${k}</th>
                        % for step in ['before','after']:
                        % if flags[ms].get(step) is not None:
                                <td>${percent_flagged(flags[ms][step]['Summary'][k])}</td>
                        % else:
                                <td>0.0%</td>
                        % endif
                        % endfor
                </tr>
                % endfor
        </tbody>
</table>

% endfor
