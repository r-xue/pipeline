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