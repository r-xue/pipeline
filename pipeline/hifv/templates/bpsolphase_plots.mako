<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
bandsort = {'4':0, 'P':1, 'L':2, 'S':3, 'C':4, 'X':5, 'U':6, 'K':7, 'A':8, 'Q':9}
%>

<a id="topofpage"></a>
<div class="page-header">
	<h1>Bandpass Phase Solution Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

<br>

% for ms in testdelay_subpages.keys():
    <h4>Plots:  <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, testdelay_subpages[ms]), pcontext.report_dir)}">Test delay plots</a>|
        <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, ampgain_subpages[ms]), pcontext.report_dir)}">Gain Amplitude </a>|
        <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, phasegain_subpages[ms]), pcontext.report_dir)}">Gain Phase </a>|
        <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, bpsolamp_subpages[ms]), pcontext.report_dir)}">BP Amp solution </a>|
        <a class="replace"
           href="${os.path.relpath(os.path.join(dirname, bpsolphase_subpages[ms]), pcontext.report_dir)}">BP Phase solution </a>
    </h4>
%endfor

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
