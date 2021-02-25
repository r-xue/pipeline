<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Selfcal</%block>

<p>Pipeline task running gaincal and applycal.</p>

% for ms in summary_plots:
    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, selfcalphasegaincal_subpages[ms])}">Phase vs. time plots </a>
    </h4>
%endfor
