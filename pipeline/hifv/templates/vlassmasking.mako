<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Vlassmasking</%block>

% for single_result in result:
    <p>Catalog: <b>${single_result.catalog_fits_file}</b></p>
    <p>Catalog search size: <b>${single_result.catalog_search_size} degrees</b></p>
    <p>Final Output Mask: <b>${single_result.combinedmask}</b></p>
% endfor

<hr>

<p> Number of islands found: <b>${single_result.number_islands_found}</b> </p>
<p> Number of islands found (inner square degree): <b>${single_result.number_islands_found_onedeg}</b> </p>

<hr>

<p> Fraction of pixels enclosed in the mask: <b>${"{:.{}f}".format(single_result.pixelfraction, 8)}</b> </p>
<p> Fraction of pixels enclosed in the mask (inner square degree): <b>${"{:.{}f}".format(single_result.pixelfraction, 8)}</b> </p>

<%self:plot_group plot_dict="${summary_plots}" url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Summary plot for ${single_result.combinedmask}
        </%def>

        <%def name="preamble()">

        </%def>

        <%def name="mouseover(plot)">
            Summary window
        </%def>

        <%def name="fancybox_caption(plot)">
          ${single_result.combinedmask}
        </%def>

        <%def name="caption_title(plot)">
           ${single_result.combinedmask}
        </%def>
</%self:plot_group>