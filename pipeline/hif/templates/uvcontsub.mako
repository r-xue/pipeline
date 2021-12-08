<%!
rsc_path = ""
import os

%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Subtract UV Continuum Model from Target Data </%block>

<p>This task subtracts the UV continuum model from the science target data and leaves the result in the DATA column of a new set of MSes called "&lt;UID&gt;_line.ms"</p>



