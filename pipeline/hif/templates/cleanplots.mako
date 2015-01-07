<%!
rsc_path = ""
import os

columns = {'cleanmask' : 'Clean Mask',
		   'flux' : 'Flux',
		   'image' : 'Image',
		   'residual' : 'Residual',
		   'model' : 'Final Model',
		   'psf' : 'PSF'}

colorder = ['image', 'residual', 'cleanmask']

def get_plot(plots, field, spw, i, colname):
	try:
		return plots[field][spw][i][colname]
	except KeyError:
		return None
%>

<!-- some black magic from Stewart that gilds the switch between thumbnail and main plot -->
<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>
<script src="${self.attr.rsc_path}resources/plotgroup.js"></script>

<div class="page-header">
        <h2>Clean results for ${field} SpW ${spw} <button class="btn btn-large pull-right" onClick="javascript:location.reload();">Back</button></h2>
</div>

<div>
    <!-- column headings -->
    <div class="row-fluid">
        <!-- iteration column heading -->
        <div class="span2">
            <h4 class="text-center">Iteration</h4>
        </div>
        <!-- headings for columns containing plots -->
        <div class="span10">
        % for colname in colorder:
            <div class="span4">
                <h4 class="text-center">${columns[colname]}</h4>
            </div>
        % endfor
        </div>
    </div><!-- /div row-fluid -->
	
    <!-- reverse iterations so final images are shown without scrolling -->
    % for i in sorted(plots_dict[field][spw].keys())[::-1]:
    <div class="row-fluid">
        <!-- iteration row heading -->
        <div class="span2">
            <h4 class="text-center">${i}</h4>
        </div>
        <!-- plots for this iteration, in column order -->
        <div class="span10">
            % for colname in colorder:
            <div class="span4">
                <% plot = get_plot(plots_dict, field, spw, i, colname) %>
                <!-- use bootstrap markup for thumbnails -->
                <ul class="thumbnails">
                    <!-- span 12 because the parent div has shrunk this
                         container already -->					  
                    <li class="span12">
                        % if plot is not None:
                        <div class="thumbnail">
                            <a class="fancybox"
                               href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                               title="Iteration ${i}: ${columns[colname]}"
                               data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                               data-field="${field}"
                               data-spw="${spw}"
                               data-colname="${colname}">
                               <img src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                               title="Iteration ${i}: ${columns[colname]}"
                               alt="Iteration ${i}: ${columns[colname]}">
                               </img>
                            </a>
                        </div>
                        % endif
                    </li>
                </ul>
            </div>					
            % endfor <!-- /colname loop-->
        </div>
    </div><!-- /div row-fluid -->			
    % endfor <!-- /iteration loop -->

    <div class="row-fluid">
        <div class="span10 offset2">
        % for colname in ['flux', 'psf', 'model']:
            <div class="span4">
                <!-- flux and PSF plots are associated with iteration 0 -->
                % if colname == 'model':
                    <% 
                    lastiter = sorted(plots_dict[field][spw].keys())[-1]
                    plot = get_plot(plots_dict, field, spw, lastiter, colname)
                    %>
                % else:
                    <% plot = get_plot(plots_dict, field, spw, 0, colname) %>
                % endif
                <ul class="thumbnails">
                    <li class="span12">
                    % if plot is not None:
                        <div class="thumbnail">
                            <a class="fancybox"
                              href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                                title="${columns[colname]}"
                                data-thumbnail="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                data-spw="${spw}"
                                data-colname="${colname}">									   
                                <img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                title="${columns[colname]}"
                                alt="${columns[colname]}">
                                </img>
                            </a>
                            <p class="text-center">${columns[colname]}</p>
                        </div>
                    % endif
                    </li>
                </ul>
            </div><!-- /div span4 -->
        % endfor <!-- /colname loop -->
    </div>
</div>
