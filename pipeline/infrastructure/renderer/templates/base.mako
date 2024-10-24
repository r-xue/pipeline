<%!
rsc_path = ""
css_file = "css/pipeline.css"
navbar_active=''

import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
    <%block name="head">
        % if use_minified_js:
            <script src="${self.attr.rsc_path}resources/js/pipeline_common.min.js"></script>
            <link rel="stylesheet" href="${self.attr.rsc_path}resources/css/all.min.css" type="text/css"/>

        % else:
            <!-- Add jQuery library -->
            <script src="${self.attr.rsc_path}resources/js/jquery-3.3.1.js"></script>
            <script src="${self.attr.rsc_path}resources/js/bootstrap.js"></script>

            <!-- Add lazy image loading library -->
            <script src="${self.attr.rsc_path}resources/js/lazyload.js"></script>

            <!--  Add purl-JS URL parsing extension -->
            <script src="${self.attr.rsc_path}resources/js/purl.js"></script>

            <!-- Add fancybox and pipeline fancybox extension -->
            <link rel="stylesheet" href="${self.attr.rsc_path}resources/css/jquery.fancybox.css" type="text/css"
                  media="screen"/>
            <script src="${self.attr.rsc_path}resources/js/jquery.fancybox.js"></script>
            <script src="${self.attr.rsc_path}resources/js/plotcmd.js"></script>
            <script src="${self.attr.rsc_path}resources/js/tcleancmd.js"></script>

            <!--  Add image holder library for missing plots -->
            <script src="${self.attr.rsc_path}resources/js/holder.js"></script>

            <!--  add FontAwesome -->
            <link rel="stylesheet" href="${self.attr.rsc_path}resources/css/font-awesome.css">

            <script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>
            <link href="${self.attr.rsc_path}resources/css/pipeline.css" rel="stylesheet" type="text/css">

        % endif
    </%block>

<title>
% if pcontext.project_summary.proposal_code != '':
${pcontext.project_summary.proposal_code} -
% endif
<%block name="title">Untitled Page</%block></title>
</head>
<body>

<%block name="header">

<nav class="navbar navbar-default navbar-fixed-top hidden-print">
    <div class="container-fluid">
	    <div class="navbar-header">
			<button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar-collapse-1">
				<span class="sr-only">Toggle navigation</span>
				<span class="icon-bar"></span>
				<span class="icon-bar"></span>
				<span class="icon-bar"></span>
			</button>
	    	<a class="navbar-brand" href="#">
	    		<img alt="${pcontext.project_summary.telescope}" 
	    		     src="${self.attr.rsc_path}resources/img/${pcontext.project_summary.telescope.lower()}logo.png"/>
	    	</a>
	    </div>
        <div class="collapse navbar-collapse" id="navbar-collapse-1">
            <ul class="nav navbar-nav navbar-left">
                <li class="${'active' if self.attr.navbar_active == 'Home' else ''}">
                	<a href="t1-1.html"><span class="glyphicon glyphicon-home"></span> Home</a>
                </li>
                % if pcontext.logtype != 'GOUS':
                <li class="${'active' if self.attr.navbar_active == 'By Topic' else ''}">
                	<a href="t1-3.html">By Topic</a>
                </li>
                % endif
                <li class="${'active' if self.attr.navbar_active == 'By Task' else ''}">
                	<a href="t1-4.html">By Task</a>
                </li>
            </ul>
            % if pcontext.project_summary.proposal_code != '':
			<p class="navbar-text navbar-right navbar-projectcode">${pcontext.project_summary.proposal_code}</p>
	        % else:
			<p class="navbar-text navbar-right navbar-projectcode">Project Code N/A</p>
			% endif
        </div>
    </div>
</nav>
</%block>

<div id="app-container" class="container-fluid">
	${next.body()}
	
	<div class="footer">
		<%block name="footer"></%block>
	</div>
</div>

</body>
</html>
