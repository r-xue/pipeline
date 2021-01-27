<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Vlassmasking</%block>

% for single_result in result:
    % if single_result.inputs['maskingmode'] == 'vlass-se-tier-1':
        <p>Catalog: <b>${single_result.catalog_fits_file}</b></p>
        <p>Catalog search size: <b>${single_result.catalog_search_size} degrees</b></p>
        <p>Tier 1 Mask: <b>${single_result.tier1mask}</b></p>

        <hr>

        <p> Number of new islands found: <b>${single_result.number_islands_found}</b> </p>
        <p> Number of new islands found (inner square degree): <b>${single_result.number_islands_found_onedeg}</b> </p>

        <hr>

        <p> Fraction of pixels enclosed in the tier-1 mask: <b>${"{:.{}f}".format(single_result.pixelfractiontier1, 8)}</b> </p>
        <p> Fraction of pixels enclosed in the tier-1 mask (inner square degree): <b> TBD </b> </p>

        <hr>

    % elif single_result.inputs['maskingmode'] == 'vlass-se-tier-2':

        <p>Catalog: <b>${single_result.catalog_fits_file}</b></p>
        <p>Catalog search size: <b>${single_result.catalog_search_size} degrees</b></p>
        <p>Tier 1 Mask: <b>${single_result.tier1mask}</b></p>
        <p>Tier 2 Mask: <b>${single_result.tier2mask}</b></p>
        <p>Combined Final Mask: <b>${single_result.combinedmask}</b></p>

        <hr>

        <p> Number of new islands found: <b>${single_result.number_islands_found}</b> </p>
        <p> Number of new islands found (inner square degree): <b>${single_result.number_islands_found_onedeg}</b> </p>
        <p> Number of new islands rejected (tier-2): <b>${single_result.num_rejected_islands}</b> </p>
        <p> Number of new islands rejected (tier-2, inner square degree)  TBD: <b>${single_result.num_rejected_islands_onedeg}</b> </p>

        <hr>

        <p> Fraction of pixels enclosed in the tier-1 mask: <b>${"{:.{}f}".format(single_result.pixelfractiontier1, 8)}</b> </p>
        <p> Fraction of pixels enclosed in the tier-1 mask (inner square degree): <b> TBD </b> </p>

        <p> Fraction of pixels enclosed in the tier-2 mask: <b>${"{:.{}f}".format(single_result.pixelfractiontier2, 8)}</b> </p>
        <p> Fraction of pixels enclosed in the tier-2 mask (inner square degree): <b> TBD </b> </p>

        <p> Fraction of pixels enclosed in the final combined mask: <b>${"{:.{}f}".format(single_result.pixelfractionfinal, 8)}</b> </p>
        <p> Fraction of pixels enclosed in the final combined mask (inner square degree): <b> TBD </b> </p>

        <hr>

        <p> Fractional increase of masked pixels in final combined mask relative to Quicklook Mask: <b> TBD </b> </p>
        <p> Fractional increase of masked pixels in final combined mask relative to Quicklook Mask (inner square degree): <b> TBD </b> </p>

    % else:
        <p>Incompatible mode used in maskingmode input</P>
    % endif



    <%self:plot_group plot_dict="${summary_plots}" url_fn="${lambda ms:  'noop'}">

            <%def name="title()">
                Summary plot for ${single_result.plotmask}
            </%def>

            <%def name="preamble()">

            </%def>

            <%def name="mouseover(plot)">
                Summary window
            </%def>

            <%def name="fancybox_caption(plot)">
              ${single_result.plotmask}
            </%def>

            <%def name="caption_title(plot)">
               ${single_result.plotmask}
            </%def>
    </%self:plot_group>

% endfor