<%!
rsc_path = ""
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

<h2>Plots</h2>

<%self:plot_group plot_dict="${amp_vs_parang}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="amp_vs_parang_plots">

    <%def name="title()">
        Amplitude vs. Parallactic Angle
    </%def>

    <%def name="preamble()">
        <p>Plots show the amplitude vs. parallactic angle.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Amplitude vs. Parallactic Angle</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        Amplitude vs. Parallactic Angle
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${amp_vs_scan_before}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="amp_vs_scan_before_plots">

    <%def name="title()">
        Amplitude vs. Scan before polarisation calibration
    </%def>

    <%def name="preamble()">
        <p>Plots show the polarisation ratio amplitude vs. scan.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Amplitude vs. Scan</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_text(plot, _)">
        Amplitude vs. Scan.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${amp_vs_scan_after}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="amp_vs_scan_after_plots">

    <%def name="title()">
        Amplitude vs. Scan after polarisation calibration
    </%def>

    <%def name="preamble()">
        <p>Plots show the polarisation ratio amplitude vs. scan.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Amplitude vs. Scan</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_text(plot, _)">
        Amplitude vs. Scan.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${phase_vs_channel}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="phase_vs_channel_plots">

    <%def name="title()">
        Phase vs. Channel
    </%def>

    <%def name="preamble()">
        <p>Plots show the phase vs. channel.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Phase vs. Channel</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_text(plot, _)">
        Phase vs. Channel.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${amp_vs_ant}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="amp_vs_ant_plots">

    <%def name="title()">
        X-Y amplitude vs. antenna
    </%def>

    <%def name="preamble()">
        <p>Plots show the X-Y amplitude vs. antenna.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show X-Y Amplitude vs. Antenna</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        X-Y Amplitude vs. Antenna.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${ampratio_vs_ant}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="ampratio_vs_ant_plots">

    <%def name="title()">
        X-Y amplitude ratio vs. antenna
    </%def>

    <%def name="preamble()">
        <p>Plots show the X-Y amplitude ratio vs. antenna.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show X-Y Amplitude Ratio vs. Antenna</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        X-Y Amplitude Ratio vs. Antenna.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${real_vs_imag}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="real_vs_imag_plots">

    <%def name="title()">
        Real vs. Imaginary
    </%def>

    <%def name="preamble()">
        <p>Plots show the real vs. imaginary for XX/YY and XY/YX.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Real vs. Imag ${plot.parameters['correlation']}</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        Corr: ${plot.parameters['correlation']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        Corr: ${plot.parameters['correlation']}
    </%def>

    <%def name="caption_text(plot, _)">
        Real vs. Imaginary ${plot.parameters['correlation']}
    </%def>

</%self:plot_group>
