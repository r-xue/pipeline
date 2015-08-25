<%!
rsc_path = ""
import os
import pipeline.infrastructure.utils as utils

# method to output flagging percentages neatly
def percent_flagged(flagsummary):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total is 0:
        return 'N/A'
    else:
        return '%0.1f%%' % (100.0 * flagged / total)

steps = ['nmedian', 'derivative', 'edgechans', 'fieldshape', 'birdies']

comp_descriptions = {'nmedian'    : 'Flag T<sub>sys</sub> spectra with high median values.',
                 	 'derivative' : 'Flag T<sub>sys</sub> spectra with high median derivative (ringing).',
                 	 'fieldshape' : 'Flag T<sub>sys</sub> spectra whose shape differs from those associated with BANDPASS data.',
                 	 'edgechans'  : 'Flag edge channels of T<sub>sys</sub> spectra.',
                 	 'birdies'    : 'Flag spikes or birdies in T<sub>sys</sub> spectra.'}

std_plot_desc = {'nmedian'    : 'shows the images used to flag',
                 'derivative' : 'shows the images used to flag',
                 'fieldshape' : 'shows the images used to flag',
                 'edgechans'  : 'shows the views used to flag',
                 'birdies'    : 'shows the views used to flag'}

extra_plot_desc = {'nmedian'    : ' shows the spectra flagged in',
     	   		   'derivative' : ' shows the spectra flagged in',
            	   'fieldshape' : ' shows the spectra flagged in'}

%>

<%inherit file="t2-4m_details-base.html"/>
<%block name="title">Flag T<sub>sys</sub> calibration</%block>

<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
            $("#select-tsys_spw").select2("val", [spw]).trigger("change");
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        o.data("callback", createSpwSetter(spw));
    });

    $(".fancybox").fancybox({
        type: 'image',
        prevEffect: 'none',
        nextEffect: 'none',
        loop: false,
        helpers: {
            title: {
                type: 'outside'
            },
            thumbs: {
                width: 50,
                height: 50,
            }
        }     
    });
});
</script>

<%self:plot_group plot_dict="${summary_plots}"
				  url_fn="${lambda x: summary_subpage[x]}"
				  data_tsysspw="${True}">

	<%def name="title()">
		T<sub>sys</sub> vs frequency after flagging
	</%def>

	<%def name="preamble()">
		<p>Plots of time-averaged T<sub>sys</sub> vs frequency, colored by antenna.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show Tsys vs frequency for Tsys spw ${plot.parameters['tsys_spw']}</%def>

	<%def name="fancybox_caption(plot)">
		T<sub>sys</sub> spw: ${plot.parameters['tsys_spw']}<br/>
		Science spws: ${', '.join([str(i) for i in plot.parameters['spw']])}
	</%def>

	<%def name="caption_title(plot)">
		T<sub>sys</sub> spw ${plot.parameters['tsys_spw']}
	</%def>

	<%def name="caption_text(plot, _)">
		Science spw${utils.commafy(plot.parameters['spw'], quotes=False, multi_prefix='s')}.
	</%def>

</%self:plot_group>

<h2>Flagging steps</h2>
<table class="table table-bordered table-striped">
	<thead>
		<tr>
			<th>Measurement Set</th>
			% for step in steps:
			<th>${step}</th>
			% endfor
		</tr>                           
	</thead>
	<tbody>
	% for ms in flags.keys():
		<tr>
			<td>${ms}</td>
			% for step in steps:
			% if flags[ms][step] is None:
			<td><span class="glyphicon glyphicon-remove"></span></td>
      		% else:
      		<td><span class="glyphicon glyphicon-ok"></span></td>
      		% endif
  			% endfor
		</tr>
	% endfor
	</tbody>
</table>

<h2>Flagged data summary</h2>

% for ms in flags.keys():
<h4>Table: ${ms}</h4>
<table class="table table-bordered table-striped ">
	<caption>Summary of flagged data. Each cell states the amount of data 
		flagged as a fraction of the specified data selection, with the 
		<em>Flagging Step</em> columns giving this information per flagging
		 step.
	</caption>
	<thead>
		<tr>
			<th rowspan="2">Data Selection</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th colspan="${len(steps)}">Flagging Step</th>
			<th rowspan="2">flagged after</th>
		</tr>
		<tr>
			% for step in steps:
			<th>${step}</th>
			% endfor
		</tr>
	</thead>
	<tbody>
		% for k in ['TOTAL', 'BANDPASS', 'AMPLITUDE', 'PHASE', 'TARGET','ATMOSPHERE']: 
		<tr>
			<th>${k}</th>               
			% for step in ['before'] + steps + ['after']:
			% if flags[ms][step] is not None:
				##<td>${step} ${k} ${flags[ms][step]['Summary'][k]}</td>
				<td>${percent_flagged(flags[ms][step]['Summary'][k])}</td>
			% else:
				<td>0.0%</td>
			% endif
			% endfor
		</tr>
		% endfor
	</tbody>
</table>

% endfor

<h2>Flag Step Details</h2>
The following section provides plots showing the flagging metrics that the pipeline
uses to determine deviant Tsys measurements, and the flagging commands that resulted 
from each flagging metric. For certain flagging metrics, the pipeline evaluates the 
metric separately for each polarisation. However, if the Tsys measurement for an 
antenna is found to be deviant in one polarisation, the pipeline will flag the 
antenna for both polarisations. 

<ul>
% for component in components: 
	<li>
	<h3>${component}</h3>
	${comp_descriptions[component]}

    % if htmlreports[component]:
  <h4>Flags</h4>
  <table class="table table-bordered table-striped">
	<thead>
	    <tr>
	        <th>Flagging Commands</th>
	        <th>Flagging Report</th>
	    </tr>
	</thead>
	<tbody>
	    % for file,reports in htmlreports[component].items():
	    <tr>
	        <td><a class="replace-pre" href="${reports[0]}" 
                   data-title="Flagging Commands">${file}</a></td>
	        <td><a class="replace-pre"
                   href="${reports[1]}" data-title="Flagging Report">
                   printTsysFlags</a></td>
	    </tr>
	    % endfor
	</tbody>
  </table>
    % endif
 
    % if component in stdplots:
	<h4>Plots</h4>
	<ul>
		% for vis, renderer in stdplots[component].items():
		<li><a class="replace" href="${os.path.relpath(renderer.path, pcontext.report_dir)}">${renderer.shorttitle}</a> ${std_plot_desc[component]} ${vis}.</li>
		% endfor
	    % if component in extraplots:
			% for vis, renderer in extraplots[component].items():
			<li><a class="replace" href="${os.path.relpath(renderer.path, pcontext.report_dir)}">${renderer.shorttitle}</a> ${extra_plot_desc[component]} ${vis}.</li>
			% endfor
		% endif

	</ul>
    % endif
 </li>
% endfor
</ul>
