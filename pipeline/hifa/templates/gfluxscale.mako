<%!
rsc_path = ""
import cgi
import decimal
import os
import string
import types

import pipeline.domain.measures as measures
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils

agent_description = {
	'before'   : 'Before',
	'applycal' : 'After',
}

total_keys = {
	'TOTAL'        : 'All Data',
	'SCIENCE SPWS' : 'Science Spectral Windows',
	'BANDPASS'     : 'Bandpass',
	'AMPLITUDE'    : 'Flux',
	'PHASE'        : 'Phase',
	'TARGET'       : 'Target'
}

def template_agent_header1(agent):
	span = 'col' if agent in ('online','template') else 'row'
	return '<th %sspan=2>%s</th>' % (span, agent_description[agent])

def template_agent_header2(agent):
	if agent in ('online', 'template'):
		return '<th>File</th><th>Number of Statements</th>'
	else:
		return ''		

def get_template_agents(agents):
	return [a for a in agents if a in ('online', 'template')]

def sanitise(url):
	return filenamer.sanitize(url)




%>
<%inherit file="t2-4m_details-base.html"/>

<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
            $("#select-spw").select2("val", [spw]).trigger("change");
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

<%block name="title">Phased-up fluxscale</%block>

<h2>Results</h2>




<h4>Antennas Used for Flux Scaling</h4>

<p>The following antennas were used for flux scaling, entries for unresolved flux calibrators are blank</p>

<table class="table table-bordered table-striped" summary="Flux Scaling Antennas">
	<caption>Antennas for Flux Calibration</caption>
	<thead>
	    <tr>
	        <th scope="col">Measurement Set</th>
	        <th scope="col">Antennas</th>
	    </tr>
	</thead>
	<tbody>
% for single_result in result:
		<tr>
			<td>${os.path.basename(single_result.vis)}</td>
                	<td>${single_result.resantenna.replace(',', ', ').replace('&', '')}</td>
		</tr>
% endfor
	</tbody>
</table>



<h4>Computed Flux Densities</h4>

<p>The following flux densities were set in the measurement set model column and recorded in the pipeline context:</p>

<table class="table table-bordered table-striped" summary="Flux density results">
	<caption>Phased-up Fluxscale Results</caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="2">Measurement Set</th>
	        <th scope="col" rowspan="2">Field</th>
	        <th scope="col" rowspan="2">SpW</th>
	        <th scope="col" colspan="4">Flux Density</th>
		</tr>
		<tr>
	        <th scope="col">I</th>
	        <th scope="col">Q</th>
	        <th scope="col">U</th>
	        <th scope="col">V</th>
	    </tr>
	</thead>
	<tbody>
	% for tr in table_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
</table>


%if ampuv_allant_plots:
    <h3>Flux Calibrator Model Comparison</h3>
    Antenna selection used for flux transfer to the secondary calibrators.

	% for ms in ampuv_allant_plots:
	    <h4>${ms}</h4>
		% for intent in ampuv_allant_plots[ms]:
			<div class="row">
		        % for i, plot in enumerate(ampuv_allant_plots[ms][intent]):
		        	<!--  Select on antenna -->
		            <%
		              antplot = ampuv_ant_plots[ms][intent][i]
		            %>
		            <div class="col-md-3">
			            % if os.path.exists(plot.thumbnail):
			                <div class="thumbnail">
			                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
			                       class="fancybox"
			                       title="Baseband ${plot.parameters['baseband']}.(spw ${plot.parameters['spw']}). 
			                              Receiver bands: ${utils.commafy(plot.parameters['receiver'], False)}.  ${'All antennas.' if plot.parameters.get('ant','') == '' else 'Antennas: '+str(plot.parameters['ant'])+'.' }
	                              Flux calibrator fields: ${plot.parameters['field']}."
			                       rel="amp_vs_uv-${ms}">
			                        <img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
			                             title="Click to show amplitude vs UV plot for Baseband ${plot.parameters['baseband']}"
			                             data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}">
			                    </a>
			                    <div class="caption">
									<h4>Baseband ${plot.parameters['baseband']}(spw ${plot.parameters['spw']})<br />
									    Receiver bands: ${utils.commafy(plot.parameters['receiver'], False)}<br />
									</h4>
								    <p>Amp vs. uvdist for 
								    <%
									antlist = plot.parameters.get('ant','').split(',')
									antdisp = ' '.join([','.join(antlist[i:i+4])+'<br>' for i in range(0,len(antlist),4)])
								    %>
								    ${'all antennas.' if plot.parameters.get('ant','') == '' else 'antennas: '+antdisp}
								    Color coded by spw.<br> Flux calibrator fields: ${plot.parameters['field']}.
								    </p>
								</div>
							</div>
			            % endif

			            % if os.path.exists(antplot.thumbnail):
			                <div class="thumbnail">
			                    <a href="${os.path.relpath(antplot.abspath, pcontext.report_dir)}"
			                       class="fancybox"
			                       title="Baseband ${antplot.parameters['baseband']} (spw ${antplot.parameters['spw']}). 
			                              Receiver bands: ${utils.commafy(antplot.parameters['receiver'], False)}.  ${'All antennas.' if antplot.parameters.get('ant','') == '' else 'Antennas: '+str(antplot.parameters['ant'])+'.' }
	                              Flux calibrator fields: ${antplot.parameters['field']}."
			                       rel="amp_vs_uv-${ms}">
			                        <img src="${os.path.relpath(antplot.thumbnail, pcontext.report_dir)}"
			                             title="Click to show amplitude vs UV plot for Baseband ${antplot.parameters['baseband']}"
			                             data-thumbnail="${os.path.relpath(antplot.thumbnail, pcontext.report_dir)}">
			                    </a>
			                    <div class="caption">
									<h4>Baseband ${antplot.parameters['baseband']}(spw ${antplot.parameters['spw']})<br />
									    Receiver bands: ${utils.commafy(antplot.parameters['receiver'], False)}<br />
									    </h4>
								    <p>Selection for 
								    <%
									antlist = antplot.parameters.get('ant','').split(',')
									antdisp = ' '.join([','.join(antlist[i:i+4])+'<br>' for i in range(0,len(antlist),4)])
								    %>
								    ${' all antennas.' if antplot.parameters.get('ant','') == '' else ' antennas: '+antdisp}
								    </p>
								</div>
			                </div>
		            	% endif		            
		            </div>
		        % endfor
			</div>
		% endfor
	%endfor
%endif
