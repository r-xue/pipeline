<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
bandsort = {'4':0, 'P':1, 'L':2, 'S':3, 'C':4, 'X':5, 'U':6, 'K':7, 'A':8, 'Q':9}
%>

<a id="topofpage"></a>
<div class="page-header">
	<h1>${plots[0].parameters['type'].title()} Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

<br>

% for ms in testdelay_subpages.keys():
    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, testdelay_subpages[ms])}">Test delay plots </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, ampgain_subpages[ms])}">Gain Amplitude </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, phasegain_subpages[ms])}">Gain Phase </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolamp_subpages[ms])}">BP Amp solution </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphase_subpages[ms])}">BP Phase solution </a>
    </h4>
% endfor


% for band in bandsort.keys():
    % if band in bandlist:
    <a id="${band}"></a><br>
    <hr>
    <div class="row">
    <h4>
    % for bb in bandsort.keys():
        % if bb in bandlist:
            <a href="#${bb}">${bb}-band</a>&nbsp;|&nbsp;
        % endif
    % endfor
     <a href="#topofpage">Top of page </a> | (Click to Jump)<br><br>
            ${band}-band

    </h4> <br>
    % for plot in sorted(plots, key=lambda p: bandsort[p.parameters['bandname']]):
        % if band == plot.parameters['bandname']:
            <div class="col-md-2 col-sm-3">
                    <div class="thumbnail">
                        <a data-fancybox="allplots"
                           href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                           title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']}">
                                <img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                     title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']}"
                                       alt="">
                        </a>
                        <div class="caption">
                            <span class="text-center">Antenna ${plot.parameters['ant']} &nbsp;&nbsp; Band: ${plot.parameters['bandname']}</span>
                        </div>
                    </div>
            </div>
        % endif
    % endfor

    </div>

    % endif
% endfor
