<%!
import os
%>
<html>
<head>
    <script>
        lazyload();
    </script>
</head>
<body>

<div class="page-header">
    <h1>Telescope Pointing Details for ${ms.basename}<button class="btn btn-default pull-right" onclick="javascript:window.history.back();">Back</button></h1>
</div>

<% 
def antenna_name(plot):
    return plot.parameters['antenna']

def intent(plot):
    return plot.parameters['intent'].capitalize()

def caption_string(plot):
    if plot.parameters['intent'].capitalize() == 'Target':
        return 'raster scan on source'
    else:
        return 'raster scan including reference'

# TODO: multi-source support
field_name = ms.get_fields(intent='TARGET')[0].name

def get_field_name(plot):
    field_attr = plot.parameters['field']
    if len(field_attr) == '':
        return field_name
    else:
        return field_attr.replace('"','')
%>

<div class="row">
% if target_pointing is not None and len(target_pointing) > 0:
    % for plots in zip(target_pointing):
        % for plot in plots:
            <div class="col-md-6">
                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                    data-fancybox="group_target_pointing"
                    data-caption='Antenna: ${plot.parameters["antenna"]}<br>
                        Field: ${plot.parameters["field"]}<br>
                        Intent: ${plot.parameters["intent"]}'>
                    <h3>Antenna ${antenna_name(plot)} Field ${get_field_name(plot)}</h3>
                    <div class="thumbnail">
                        <img class="lazyload"
                            data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                            title="Telescope pointing for antenna ${antenna_name(plot)}"
                            alt="Telescope pointing for antenna ${antenna_name(plot)}" />
                    </div>
                </a>
                <div class="caption">
                    <h4>raster scan on source</h4>
                </div>
            </div>
        % endfor
    % endfor
    % for plots in zip(whole_pointing):
        % for plot in plots:
            <div class="col-md-6">
                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                    data-fancybox="group_target_pointing"
                    data-caption='Antenna: ${plot.parameters["antenna"]}<br>
                        Field: ${plot.parameters["field"]}<br>
                        Intent: ${plot.parameters["intent"]}'>
                    <h3>Antenna ${antenna_name(plot)} Field ${get_field_name(plot)}</h3>
                    <div class="thumbnail">
                        <img class="lazyload"
                            data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                            title="Telescope pointing for antenna ${antenna_name(plot)}"
                            alt="Telescope pointing for antenna ${antenna_name(plot)}" />
                    </div>
                </a>
                <div class="caption">
                    <h4>raster scan including reference</h4>
                </div>
            </div>
        % endfor
    % endfor
% endif
% if shift_pointing is not None and len(shift_pointing) > 0:
    % for plot in shift_pointing:
        <div class="col-md-6">
            <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                data-fancybox="group_shift_pointing"
                data-caption='Antenna: ${plot.parameters["antenna"]}<br>
                    Field: ${plot.parameters["field"]}<br>
                    Intent: ${plot.parameters["intent"]}'>
                <h3>Antenna ${antenna_name(plot)} Field ${get_field_name(plot)}</h3>
                <div class="thumbnail">
                    <img class="lazyload"
                        data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                        title="Shifted telescope pointing for antenna ${antenna_name(plot)}"
                        alt="Shifted telescope pointing for antenna ${antenna_name(plot)}" />
                </div>
            </a>
            <div class="caption">
                <h4>raster scan including reference after ephemeris correction</h4>
            </div>
        </div>
    % endfor
% endif
</div>

</body>
</html>
