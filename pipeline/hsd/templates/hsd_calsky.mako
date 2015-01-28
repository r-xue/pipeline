<%!
rsc_path = ""
import os
%>
<%inherit file="t2-4m_details-base.html"/>

<%block name="header" />

<%block name="title">Generate Sky calibration table</%block>

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

<p>This task generates a sky calibration table, a collection of OFF spectra for single dish data calibration.</p>

<h3>Sky Level vs Frequency</h3>
% for ms in summary_plots.keys():
    <h4><a class="replace"
           href="${os.path.join(dirname, summary_subpage[ms])}">${ms}</a>
    </h4>
    % for plot in summary_plots[ms]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   class="fancybox"
	                   rel="thumbs">
	                    <img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="Sky level summary for Spectral Window ${plot.parameters['spw']}"
	                         data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}">
	                </a>
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, summary_subpage[ms])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}">
	                           Spectral Window ${plot.parameters['spw']}
	                        </a>
	                    </h4>
	
	                    <p>Plot of sky level for spectral
						window ${plot.parameters['spw']}.</p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
%endfor
