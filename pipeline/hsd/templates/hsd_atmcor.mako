<%!
rsc_path = ""
import os
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Apply correction for atmospheric effects</%block>

<p>This task applies the correction for atmospheric effects to
science targets in the calibrated measurement sets.</p>

<h2>Contents</h2>
<ul>
<li><a href="#applied_corrections">Applied Corrections</a></li>
<li><a href="#plots">Plots</a>
  <ul>
    <li><a href="#plots_amp_vs_freq">Science targets: ATM corrected amplitude vs frequency</a></li>
  </ul>
</li>
</ul>

<h2 id="applied_corrections" class="jumptarget">Applied Corrections</h2>

<p>The correction for atmospheric effects applies a model in each
measurement set with the parameters listed in the following table.</p>

<table class="table table-bordered table-striped table-condensed"
       summary="Applied Corrections">
    <caption>Applied Corrections</caption>
    <thead>
        <tr>
            <th>Measurement set</th>
            <th scope="col" colspan="3">Model parameters</th>
        </tr>
        <tr>
            <th>Name</th>
            <th>atmType</th>
            <th>h0</th>
            <th>dTem_dh</th>
    </thead>
    <tbody>
        %for r in result:
            <tr>
                <td>${r.inputs['vis']}</td>
                <td>${r.inputs['atmtype']}</td>
                <td>${r.inputs['h0']}</td>
                <td>${r.inputs['dtem_dh']}</td>
            </tr>
        %endfor
    </tbody>
</table>


<h2 id="plots" class="jumptarget">Plots</h2>
<h2 id="plots_amp_vs_freq" class="jumptarget">Science targets: ATM corrected amplitude vs frequency</h2>

<p>Corrected by atmospheric effects amplitude vs frequency plots of each source in each
measurement set. The atmospheric transmission for each spectral window is overlayed
on each plot in pink.</p>

<p>Data are plotted for all antennas and correlations, with different antennas shown
in different colors.</p>
