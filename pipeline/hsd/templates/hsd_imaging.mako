<%!
rsc_path = "../"
import os
%>
<%inherit file="t2-4m_details-base.html"/>

<%block name="header" />

<%block name="title">Image single dish data</%block>

<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
        	if (typeof spw !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-spw").select2("val", [spw]).trigger("change");
        	}
        };
    };

    // return a function that sets the Antenna text field to the given spw
    var createAntennaSetter = function(ant) {
        return function() {
        	if (typeof ant !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-ant").select2("val", [ant]).trigger("change");
        	}
        };
    };

    // return a function that sets the Polarization text field to the given spw
    var createPolarizationSetter = function(pol) {
        return function() {
        	if (typeof pol !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-pol").select2("val", [pol]).trigger("change");
        	}
        };
    };

    // 
    var createMixedSetter = function(spw, ant, pol) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
        	if (typeof spw !== "undefined") {
	            $("#select-spw").select2("val", [spw]).trigger("change");
        	}
        	if (typeof ant !== "undefined") {
            	$("#select-ant").select2("val", [ant]).trigger("change");
        	}
        	if (typeof pol !== "undefined") {
	            $("#select-pol").select2("val", [pol]).trigger("change");
        	}
        };
    };    

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        var ant = o.data("ant");
        var pol = o.data("pol");
        o.data("callback", createMixedSetter(spw, ant, pol));
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

<%
plots_list = [{'title': 'Channel Map',
               'subpage': channelmap_subpage,
               'plot': channelmap_plots},
              {'title': 'Baseline Rms Map',
               'subpage': rmsmap_subpage,
               'plot': rmsmap_plots},
              {'title': 'Integrated Intensity Map',
               'subpage': integratedmap_subpage,
               'plot': integratedmap_plots}]
%>

<p>This task generates single dish images per source per spectral window. 
It generates an image combined spectral data from whole antenna as well as images per antenna.</p>

<div class="row">
<h3>Profile Map</h3>
% for field in sparsemap_subpage.keys():
    <h4><a class="replace"
           href="${os.path.join(dirname, sparsemap_subpage[field])}">${field}</a>
    </h4>
    % for plot in sparsemap_plots[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   class="fancybox"
	                   rel="thumbs">
	                    <img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="Profile map summary for Spectral Window ${plot.parameters['spw']}"
	                         data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}">
	                </a>
	
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, sparsemap_subpage[field])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-ant="${plot.parameters['ant']}">
	                           Spectral Window ${plot.parameters['spw']}
	                        </a>
	                    </h4>
	
	                    <p>Profile map for spectral
	                        window ${plot.parameters['spw']}.
	                    </p>
	                    
	                    <h4>Detailed profile map</h4>
	                    <table border width="100%">
		                    <tr><th>ANTENNA</th><th colspan="${len(profilemap_entries[field].values()[0])}">POL</th></tr>
		                    % for (ant, pols) in profilemap_entries[field].items():
		                        <tr><td>${ant}</td>
		                        % for pol in pols:
		                            <td align="center">
		                            <a href="${os.path.join(dirname, profilemap_subpage[field])}"
		                               class="replace"
		                               data-spw="${plot.parameters['spw']}"
		                               data-ant="${ant}"
		                               data-pol="${pol}">
		                            ${pol}
		                            </a>
		                            </td>
		                        % endfor
		                        </tr>
		                    % endfor
	                    </table>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
%endfor
</div>

% for plots in plots_list:
<div class="row">
    % if plots.has_key('note'):
        <h3> ${plots['title']} (${plots['note']})</h3>
    % else:
        <h3>${plots['title']}</h3>
    % endif
    % for field in plots['subpage'].keys():
        <h4><a class="replace"
               href="${os.path.join(dirname, plots['subpage'][field])}">${field}</a>
        </h4>
        % for plot in plots['plot'][field]:
            % if os.path.exists(plot.thumbnail):
            <div class="col-md-3">
                <div class="thumbnail">
                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                       class="fancybox"
                       rel="thumbs">
                        <img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                             title="${plots['title']} for Spectral Window ${plot.parameters['spw']}"
                             data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}">
                    </a>

                    <div class="caption">
                        <h4>
                            <a href="${os.path.join(dirname, plots['subpage'][field])}"
                               class="replace"
                               data-spw="${plot.parameters['spw']}"
                               data-ant="${plot.parameters['ant']}">
                               Spectral Window ${plot.parameters['spw']}
                            </a>
                        </h4>

                        <p>${plots['title']} for spectral
                            window ${plot.parameters['spw']}.
                        </p>
                    </div>
                </div>
            </div>
            % endif
        % endfor
    % endfor
</div>
% endfor

