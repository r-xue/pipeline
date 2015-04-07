<%!
rsc_path = ""
import os
%>

<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>
<script src="${self.attr.rsc_path}resources/plotgroup.js"></script>

<div class="page-header">
	<h1>${plot_group.title} plots <button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

<div class="row">
	<div class="span11">
		% for selectors in plot_group.selectors:
		<div class="row-fluid">
			<div class="span2">
				<span class="pull-right">${selectors[0].description}</span>
			</div>
			<div class="span10">
				<div class="btn-group plotfilter">
					% for selector in selectors:
						<button data-toggle="buttons-checkbox" data-value="${selector.css_class}" class="btn btn-mini">${selector.value}</button>
					% endfor
				</div>
			</div>
		</div>
		% endfor
	</div>
	<div class="span1">
		<button class="btn btn-mini btn-warning btn-block" id="clearbutton" style="display: inline-block !important; vertical-align: middle !important;">Clear All Filters</button>
	</div>
</div>

% for plot in sorted(plot_group.plots, key=lambda plot: int(plot.parameters['spw'])):
<div class="row-fluid">
	<ul class="thumbnails">
		<li class="span2" data-value="${plot.css_class}">
			<div class="thumbnail">
				<a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
				   title="${plot.y_axis} vs ${plot.x_axis} for antenna #${plot.parameters['ant']} spw #${plot.parameters['spw']}">
					<img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
						 title="${plot.y_axis} vs ${plot.x_axis} for antenna #${plot.parameters['ant']} spw #${plot.parameters['spw']}"
						 alt="${plot.y_axis} vs ${plot.x_axis} for antenna #${plot.parameters['ant']} spw #${plot.parameters['spw']}" >
					</img>
				</a>
			</div>
		</li>
% endfor
	</ul>
</div>
