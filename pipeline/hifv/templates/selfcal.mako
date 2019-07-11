<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Selfcal</%block>

<p>Pipeline task running gaincal and applycal.</p>

% for ms in summary_plots:

    <h4>Plots:  <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, selfcalphasegaincal_subpages[ms]), pcontext.report_dir)}">Phase vs. time plots </a>
    </h4>



%endfor

