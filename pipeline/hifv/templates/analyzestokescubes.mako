<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Analyzestokescubes</%block>

<p>Analyzestokescubes</p>


<%self:plot_group plot_dict="${stokesplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.x_axis} vs. ${plot.y_axis}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.x_axis} vs. ${plot.y_axis}
        </%def>
</%self:plot_group>


<%self:plot_group plot_dict="${rmsplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
          ${plot.x_axis} vs. ${plot.y_axis}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.x_axis} vs. ${plot.y_axis}
        </%def>

</%self:plot_group>

