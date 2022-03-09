<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>

<div class="page-header">
	<h1>SysPower ${plots[0].parameters['type'].title()} Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>


<h4>Per antenna plots:<br>
<ul>
        %for band in syspowerspgain_subpages.keys():
            %for ms in syspowerspgain_subpages[band].keys():
                   <li> <b>${band}-band</b>:
                    <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, syspowerspgain_subpages[band][ms])}">Syspower RQ SPgain plots</a> |
                    <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, pdiffspgain_subpages[band][ms])}">Syspower Pdiff Template SPgain plots</a></li>
            %endfor
        %endfor
</ul>
</h4>

<br>

% for plot in sorted(plots, key=lambda p: p.parameters['ant']):
<div class="col-md-2 col-sm-3">
	<div class="thumbnail">
		<a data-fancybox="allplots"
		   href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
		   title="Antenna ${plot.parameters['ant']}">
			<img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
				 title="Antenna ${plot.parameters['ant']}"
				   alt="">
		</a>
		<div class="caption">
			<span class="text-center">Antenna ${plot.parameters['ant']}</span>
		</div>
	</div>
</div>
% endfor
