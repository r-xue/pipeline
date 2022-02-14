<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Syspower</%block>

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

<p>Syspower task for compression fix.</p>

%if syspowerspgain_subpages:

        <h2>Sys power plots</h2>
        Switched Power table written to:
        %for single_result in result:
	        <p><b>${os.path.basename(single_result.gaintable)}</b></p>
	        <ul>
	        %for band in single_result.band_baseband_spw:
	            <li>${band}-band</li>
	            <ul>
	            %for baseband in single_result.band_baseband_spw[band]:
	                <li>Baseband ${baseband}:  spws: ${','.join([str(spw) for spw in single_result.band_baseband_spw[band][baseband]])}</li>
	            %endfor
	            </ul>
	        %endfor
	        </ul>
        %endfor
        This table has been modified.

        %for ms in bar_plots.keys():
            <h4>Per antenna plots:
                <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, syspowerspgain_subpages[ms])}">Syspower RQ SPgain plots</a> |
                <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, pdiffspgain_subpages[ms])}">Syspower Pdiff Template SPgain plots</a>
            </h4>
        %endfor


<%self:plot_group plot_dict="${all_plots}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            Summary plots
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.parameters['caption']} </%def>

        <%def name="fancybox_caption(plot)">
          ${plot.parameters['caption']}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.parameters['caption']}
        </%def>
</%self:plot_group>



%else:

No bands/basebands in these data will be processed for this task.

%endif