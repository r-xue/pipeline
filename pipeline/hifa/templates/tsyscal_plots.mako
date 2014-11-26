<%!
rsc_path = ""
import os
%>

<link href="${self.attr.rsc_path}resources/css/select2.css" rel="stylesheet"/>
<link href="${self.attr.rsc_path}resources/css/select2-bootstrap.css" rel="stylesheet"/>
<script src="${self.attr.rsc_path}resources/js/select2.min.js"></script>

<!-- include required files and styles for histograms -->
<script src="${self.attr.rsc_path}resources/js/d3.v3.min.js"></script>
<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>

<script>
$(document).ready(function () {
    // push JSON directly into page, avoiding XHR cross-site domain problems
	var json='${json}';
	var scores_dict = JSON && JSON.parse(json) || $.parseJSON(json);

    // activate the input fields for spw, antenna, etc.
    $('.select2').select2();

    // create a new filter pipeline, and tell it to filter based on the scores
    // in the scores_dict JSON dictionary that is set in the template.
    var filterPipeline = new FILTERS.FilterPipeline();
    filterPipeline.setScores(scores_dict);

    // create filters that listen for events on the .select2 input fields we just
    // created, and add them to the filter pipeline, filtering on the appropriate
    // dictionary key
    filterPipeline.addFilter(FILTERS.createMatchFilter('tsys_spw', '#select-tsys_spw'));
    filterPipeline.addFilter(FILTERS.createMatchFilter('spw', '#select-spw'));
    filterPipeline.addFilter(FILTERS.createMatchFilter('antenna', '#select-ant'));

    // get the X-axis label for Tsys
    var xAxis = PLOTS.xAxisLabels["K"];

    // create histograms and histogram filters for the three distributions we want
    // to highlight: RMS, average median, and maximum median reached.
    var charts = [ALL_IN_ONE.easyHistogram(filterPipeline, scores_dict, "median", "#histogram-tsysmedian", xAxis),
    			  ALL_IN_ONE.easyHistogram(filterPipeline, scores_dict, "median_max", "#histogram-tsysmedianmax", xAxis),
				  ALL_IN_ONE.easyHistogram(filterPipeline, scores_dict, "rms", "#histogram-tsysrms", xAxis)]

    // link histogram ranges to the range checkbox
    var rangeCheckbox = $("input#rangeCheckbox");
	rangeCheckbox.click(function() {
		var state = rangeCheckbox.prop("checked");
		charts.forEach(function(chart) {
			chart.histogram.duration(1000).plotExtent(state);
		});
	});
    
    // add on-click handler to our thumbnails to launch FancyBox with the
    // relevant thumbnails
    $("ul.thumbnails li div a").click(function (evt) {
        evt.preventDefault();
        var target = this.href;
        UTILS.launchFancybox(target);
        return false;
    });

    // fix the thumbnail margins for plots on the n>1 row
    UTILS.fixThumbnailMargins();
});
</script>

<div class="page-header">
	<h1>${plot_title}<button class="btn btn-large pull-right" onClick="javascript:location.reload();">Back</button></h1>
</div>

<div class="column-fluid">
	<label class="checkbox"> 
		<input type="checkbox" id="rangeCheckbox" checked></input>Clip histogram range to match data
	</label>
	<div class="row-fluid">
		<div class="column-fluid span4">
			<fieldset>
				<legend>Average of Median T<sub>sys</sub> over time</legend>
				<div id="histogram-tsysmedian" class="span12">
			</fieldset>
		</div>
		<div class="column-fluid span4">
			<fieldset>
				<legend>Maximum of Median T<sub>sys</sub> over time</legend>
				<div id="histogram-tsysmedianmax"  class="span12">
			</fieldset>
		</div>
		<div class="column-fluid span4">
			<fieldset>
				<legend>RMS deviation from Average Median T<sub>sys</sub></legend>
				<div id="histogram-tsysrms"  class="span12">
			</fieldset>
		</div>
	</div>
	
	<div class="row-fluid">
		<div class="column-fluid span4">
			<div>
			<fieldset>
				<legend>Tsys Spectral Window Filter</legend>
				<select id="select-tsys_spw" class="select2" multiple style="width:100%" placeholder="Show all spectral windows">
					% for spw in sorted(list(set([p.parameters['tsys_spw'] for p in plots]))):
					<option>${spw}</option>
					% endfor
		       	</select>
			</fieldset>
			</div>
		</div>		

		<div class="column-fluid span4">
			<div>
			<fieldset>
				<legend>Science Spectral Window Filter</legend>
				<select id="select-spw" class="select2" multiple style="width:100%" placeholder="Show all spectral windows">
					% for spw in sorted(list(set([p.parameters['spw'] for p in plots]))):
					<option>${spw}</option>
					% endfor
		       	</select>
			</fieldset>
			</div>
		</div>		
	
		<div class="column-fluid span4">
			<div>
			<fieldset>
				<legend>Antenna Filter</legend>
				<select id="select-ant" class="select2" multiple style="width:100%" placeholder="Show all antennas">
					% for ant in sorted(list(set([p.parameters['ant'] for p in plots]))):
					<option>${ant}</option>
					% endfor
		       	</select>
			</fieldset>
			</div>
		</div>		
	</div>
</div>

<br>

<div class="column-fluid">
	<ul class="thumbnails">
	% for plot in sorted(plots, key=lambda p: p.parameters['ant']):
		<li class="span2">
			<div class="thumbnail">
				<a class="fancybox"
				   href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
				   title="${plot.parameters['ant']} Tsys spw ${plot.parameters['tsys_spw']}"
				   data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}">
					<img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
						 title="${plot.parameters['ant']} Tsys spw ${plot.parameters['tsys_spw']}"
						   alt="">
					</img>
				</a>
					<p class="text-center">${plot.parameters['ant']} Tsys spw 
					${plot.parameters['tsys_spw']} Science spw 
					${plot.parameters['spw']}</p>
			</div>
		</li>
	% endfor
	</ul>
</div>
