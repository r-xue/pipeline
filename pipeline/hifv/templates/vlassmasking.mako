<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Vlassmasking</%block>

% for single_result in result:
    <p>Inext:   <b>${single_result.inext}</b></p>
    <p>Outext:  <b>${single_result.outext}</b></p>
    <p>Catalog: <b>${single_result.catalog_fits_file}</b></p>
    <p>Catalog search size: <b>${single_result.catalog_search_size}</b></p>
    <p>Outfile: <b>${single_result.outfile}</b></p>
    <p>Combined Mask: <b>${single_result.combinedmask}</b></p>
% endfor

<hr>

<p> Number of islands found: <b>${numfound}</b> </p>
<p> Number of islands found (inner square degree): <b>${numaccepted}</b> </p>

<hr>

<%self:plot_group plot_dict="${summary_plots}" url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Mask summary plot
        </%def>

        <%def name="preamble()">

        </%def>

        <%def name="mouseover(plot)">
            Summary window
        </%def>

        <%def name="fancybox_caption(plot)">
          Mask
        </%def>

        <%def name="caption_title(plot)">
           Mask
        </%def>
</%self:plot_group>