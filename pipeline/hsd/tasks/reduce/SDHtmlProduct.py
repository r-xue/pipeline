#*******************************************************************************
# ALMA - Atacama Large Millimeter Array
# Copyright (c) NAOJ - National Astronomical Observatory of Japan, 2011
# (in the framework of the ALMA collaboration).
# All rights reserved.
# 
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
# 
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
#*******************************************************************************
#
# $Revision: 1.24.2.2 $
# $Date: 2012/04/02 15:32:42 $
# $Author: tnakazat $
#
import os
import sys
import string
import shutil

JavaScript1 = '\
<script type="text/javascript">\n \
<!--\n \
var n = 0;\n \
var m = 0;\n \
var k = 0;\n \
var nIm = 0;\n \
var img = new Array();\n \
var btn = new Array();'

JavaScript2a = '\
function prev(){\n \
    n = window.parent.submenu1.curBtn;\n \
    if (--n < 0) { n = m - 1; }\n \
    setBtnColor(btn[n]);\n \
    parent.submenu1.document.getElementById(btn[n]).focus();\n \
    window.parent.main.location.href = img[n];\n \
    if (n == m) {\n \
       resetBtnColor(btn[0]);\n \
    }\n \
     else {\n \
       resetBtnColor(btn[n+1]);\n \
    }\n \
}\n \
function next(){\n \
    n = window.parent.submenu1.curBtn;\n \
    if (++n == m) { n = 0; }\n \
    setBtnColor(btn[n]);\n \
    parent.submenu1.document.getElementById(btn[n]).focus();\n \
    resetBtnColor(btn[n-1]);\n \
    window.parent.main.location.href = img[n];\n \
}\n '

JavaScript2b = '\
var curBtn = 0;\n \
function hit(win){\n \
    n = win.value;\n \
    window.parent.main.location.href = img[n];\n \
}\n \
function active(win){\n \
    curBtn = win.value;\n \
    window.parent.main.location.href = img[n];\n \
}\n '

JavaScript2c = '\
function prev(){\n \
    imgpath = window.parent.main.location.pathname;\n \
    a = imgpath.split("/");\n \
    idx = a.length - 1;\n \
    imgname = a[idx-1]+"/"+a[idx];\n \
    n = 0;\n \
    for ( i = 0 ; i < m ; i++ ) {\n \
       if ( imgname == img[i] ) {\n \
          n = i;\n \
          break;\n \
       }\n \
    }\n \
    if (--n < 0) { n = m - 1; }\n \
    setBtnColor(btn[n]);\n \
    parent.submenu1.document.getElementById(btn[n]).focus();\n \
    window.parent.main.location.href = img[n];\n \
    if (n == m) {\n \
       resetBtnColor(btn[0]);\n \
    }\n \
     else {\n \
       resetBtnColor(btn[n+1]);\n \
    }\n \
}\n \
function next(){\n \
    imgpath = window.parent.main.location.pathname;\n \
    a = imgpath.split("/");\n \
    idx = a.length - 1;\n \
    imgname = a[idx-1]+"/"+a[idx];\n \
    n = 0;\n \
    for ( i = 0 ; i < m ; i++ ) {\n \
       if ( imgname == img[i] ) {\n \
          n = i;\n \
          break;\n \
       }\n \
    }\n \
    if (++n == m) { n = 0; }\n \
    setBtnColor(btn[n]);\n \
    parent.submenu1.document.getElementById(btn[n]).focus();\n \
    resetBtnColor(btn[n-1]);\n \
    window.parent.main.location.href = img[n];\n \
}\n '

JavaScript3 = '\
function setBtnColor(btname){\n \
    window.parent.submenu1.document.getElementById(btname).style.backgroundColor=\'lightblue\';\n \
}\n \
function resetBtnColor(btname){\n \
    window.parent.submenu1.document.getElementById(btname).style.backgroundColor=\'lightgrey\';\n \
}\n \
// -->\n \
</script>'

JavaScript4 = '\
<script type="text/javascript">\n \
<!--\n \
setBtnColor(btn[%s]);\n \
document.getElementById(btn[%s]).focus();\n \
// -->\n \
</script>'




def HtmlInit(Directory):
    baselinefitINITpage = "helpbaselinefit.html"
    clusteringINITpage = "helpclustering.html"
    fitstatisticsINITpage = "helpfitstatistics.html"
    griddingINITpage = "helpgridding.html"
    multispectraINITpage = "helpmultispectra.html"
    sparsespectramapINITpage = "helpsparsespectramap.html"

    TargetSrc = Directory.split('/')[-1].replace('_html', '')
    Out = open(Directory+'/index.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAME src="Summary/index.html" name="submenu1">'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/menu.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Pipeline Menu</title>\n<style>'
    print >> Out, '.ttl{font-size:20px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '</style>\n</head>\n<body>'
    print >> Out, '<p class="ttl">SD Pipeline</p>'
    print >> Out, '<ul><small>'
    print >> Out, '<li><a href="summary.html" target="_parent">Summary</a></li>'
    print >> Out, '<li><a href="clustering.html" target="_parent">Clustering</a></li>'
    print >> Out, '<li><a href="baselinefit.html" target="_parent">BaselineFit</a></li>'
    print >> Out, '<li><a href="fitstatistics.html" target="_parent">FitStatistics</a></li>'
    print >> Out, '<li><a href="multispectra.html" target="_parent">MultiSpectra</a></li>'
    print >> Out, '<li><a href="gridding.html" target="_parent">Gridding</a></li>'
    print >> Out, '<li><a href="sparsespectramap.html" target="_parent">SparseSpectraMap</a></li>'
    print >> Out, '<li><a href="log.html" target="_parent">Log</a></small></li>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/summary.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAME src="Summary/index.html" name="submenu1">'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/log.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAME src="log1.html" name="submenu1">'
    print >> Out, '</FRAMESET>'
    #print >> Out, '<FRAME src="Logs/PIPELINE.txt" name="main">'
    print >> Out, '<FRAME src="../Logs/PIPELINE.txt" name="main">'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/log1.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Pipeline Log Menu</title>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '</style>\n</head>\n<body>'
    print >> Out, '<p class="ttl">Log Window</p>'
    print >> Out, '<ul><small>'
    #print >> Out, '<li><a href="Logs/PIPELINE.txt" target="main">Pipeline</a></li>'
    print >> Out, '<li><a href="../Logs/PIPELINE.txt" target="main">Pipeline</a></li>'
    if ( os.path.exists( Directory+'/Logs/Calibration.txt' ) ):
        print >> Out, '<li><a href="Logs/Calibration.txt" target="main">Calibration</a></li>'
    #print >> Out, '<li><a href="Logs/Recipe.txt" target="main">Recipe</a></li>'
    print >> Out, '<li><a href="Logs/Data.txt" target="main">Data</a></li>'
    print >> Out, '<li><a href="Logs/BF_Grouping.txt" target="main">Grouping</a></li>'
    print >> Out, '<li><a href="Logs/BF_DetectLine.txt" target="main">LineDetection</a></li>'
    print >> Out, '<li><a href="Logs/BF_Cluster.txt" target="main">Clustering</a></li>'
    print >> Out, '<li><a href="Logs/BF_FitOrder.txt" target="main">FittingOrder</a></li>'
    print >> Out, '<li><a href="Logs/BF_Fit.txt" target="main">Fitting</a></li>'
    print >> Out, '<li><a href="Logs/Flagger.txt" target="main">Flagging</a></li>'
    print >> Out, '<li><a href="Logs/Gridding.txt" target="main">Gridding</a></li>'
    print >> Out, '<li><a href="Logs/exp_rms_factors.txt" target="main">exp_rms_factors</a></li>'
    print >> Out, '</small></ul>\n</body>\n</html>'
    Out.close()

    Out = open(Directory+'/baselinefit.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="195, *">'
    #print >> Out, '<FRAME src="baselinefit1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="baselinefit1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="baselinefit1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % baselinefitINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/baselinefit1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/baselinefit1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/clustering.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="285, *">'
    #print >> Out, '<FRAME src="clustering1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="clustering1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="clustering1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % clusteringINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/clustering1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/clustering1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/fitstatistics.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="175, *">'
    #print >> Out, '<FRAME src="fitstatistics1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="fitstatistics1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="fitstatistics1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % fitstatisticsINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/fitstatistics1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/fitstatistics1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/gridding.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="195, *">'
    #print >> Out, '<FRAME src="gridding1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="gridding1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="gridding1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % griddingINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/gridding1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/gridding1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/multispectra.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="200, *">'
    #print >> Out, '<FRAME src="multispectra1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="multispectra1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="multispectra1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % multispectraINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/multispectra1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/multispectra1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/sparsespectramap.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="190, *">'
    #print >> Out, '<FRAME src="sparsespectramap1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="sparsespectramap1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="sparsespectramap1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % sparsespectramapINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/sparsespectramap1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/sparsespectramap1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/helpbaselinefit.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Baseline Fitting process:</h1>'
    print >> Out, '<p>Baseline fit Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpclustering.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Clustering Analysis:</h1>'
    print >> Out, '<p>Clustering Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpfitstatistics.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Fitting Statistics:</h1>'
    print >> Out, '<p>FitStatistics Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpgridding.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Gridding process:</h1>'
    print >> Out, '<p>Gridding Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpmultispectra.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the MultiSpectra:</h1>'
    print >> Out, '<p>MultiSpectra Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpsparsespectramap.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Sparse Spectra Map:</h1>'
    print >> Out, '<p>SparseSpectraMap Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()


def HtmlInitCombine(Directory):
#    baselinefitINITpage = "helpbaselinefit.html"
    clusteringINITpage = "helpclustering.html"
#    fitstatisticsINITpage = "helpfitstatistics.html"
    griddingINITpage = "helpgridding.html"
    multispectraINITpage = "helpmultispectra.html"
    sparsespectramapINITpage = "helpsparsespectramap.html"

    TargetSrc = Directory.split('/')[-1].replace('_html', '')
    Out = open(Directory+'/index.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    #print >> Out, '<FRAME src="Summary/index.html" name="submenu1">'
    print >> Out, '<FRAME src="%s" name="submenu1">'%(clusteringINITpage)
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/menu.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Pipeline Menu</title>\n<style>'
    print >> Out, '.ttl{font-size:20px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '</style>\n</head>\n<body>'
    print >> Out, '<p class="ttl">SD Pipeline</p>'
    print >> Out, '<ul><small>'
#    print >> Out, '<li><a href="summary.html" target="_parent">Summary</a></li>'
    print >> Out, '<li><a href="clustering.html" target="_parent">Clustering</a></li>'
#    print >> Out, '<li><a href="baselinefit.html" target="_parent">BaselineFit</a></li>'
#    print >> Out, '<li><a href="fitstatistics.html" target="_parent">FitStatistics</a></li>'
    print >> Out, '<li><a href="multispectra.html" target="_parent">MultiSpectra</a></li>'
    print >> Out, '<li><a href="gridding.html" target="_parent">Gridding</a></li>'
    print >> Out, '<li><a href="sparsespectramap.html" target="_parent">SparseSpectraMap</a></li>'
    #print >> Out, '<li><a href="log.html" target="_parent">Log</a></small></li>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/summary.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAME src="Summary/index.html" name="submenu1">'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/log.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAME src="log1.html" name="submenu1">'
    print >> Out, '</FRAMESET>'
    #print >> Out, '<FRAME src="Logs/PIPELINE.txt" name="main">'
    print >> Out, '<FRAME src="../Logs/PIPELINE.txt" name="main">'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()

    Out = open(Directory+'/log1.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Pipeline Log Menu</title>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '</style>\n</head>\n<body>'
    print >> Out, '<p class="ttl">Log Window</p>'
    print >> Out, '<ul><small>'
    #print >> Out, '<li><a href="Logs/PIPELINE.txt" target="main">Pipeline</a></li>'
    print >> Out, '<li><a href="../Logs/PIPELINE.txt" target="main">Pipeline</a></li>'
    if ( os.path.exists( Directory+'/Logs/Calibration.txt' ) ):
        print >> Out, '<li><a href="Logs/Calibration.txt" target="main">Calibration</a></li>'
    #print >> Out, '<li><a href="Logs/Recipe.txt" target="main">Recipe</a></li>'
    print >> Out, '<li><a href="Logs/Data.txt" target="main">Data</a></li>'
    print >> Out, '<li><a href="Logs/BF_Grouping.txt" target="main">Grouping</a></li>'
#    print >> Out, '<li><a href="Logs/BF_DetectLine.txt" target="main">LineDetection</a></li>'
    print >> Out, '<li><a href="Logs/BF_Cluster.txt" target="main">Clustering</a></li>'
#    print >> Out, '<li><a href="Logs/BF_FitOrder.txt" target="main">FittingOrder</a></li>'
#    print >> Out, '<li><a href="Logs/BF_Fit.txt" target="main">Fitting</a></li>'
#    print >> Out, '<li><a href="Logs/Flagger.txt" target="main">Flagging</a></li>'
    print >> Out, '<li><a href="Logs/Gridding.txt" target="main">Gridding</a></li>'
#    print >> Out, '<li><a href="Logs/exp_rms_factors.txt" target="main">exp_rms_factors</a></li>'
    print >> Out, '</small></ul>\n</body>\n</html>'
    Out.close()

##     Out = open(Directory+'/baselinefit.html', 'w')
##     print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
##     print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
##     print >> Out, '<FRAMESET cols="175, *">'
##     print >> Out, '<FRAMESET rows="260, *">'
##     print >> Out, '<FRAME src="menu.html" name="menu">'
##     print >> Out, '<FRAMESET rows="195, *">'
##     #print >> Out, '<FRAME src="baselinefit1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
##     print >> Out, '<FRAME src="baselinefit1h.html" name="submenu1h" frameborder="NO">'
##     print >> Out, '<FRAME src="baselinefit1.html" name="submenu1" frameborder="NO">'
##     print >> Out, '</FRAMESET>'
##     print >> Out, '</FRAMESET>'
##     print >> Out, '<FRAME src="%s" name="main">' % baselinefitINITpage
##     print >> Out, '</FRAMESET>'
##     print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
##     print >> Out, '</FRAMESET>\n</html>'
##     Out.close()
##     Out = open(Directory+'/baselinefit1h.html', 'w')
##     print >> Out, 'Not Processed Yet'
##     Out.close()
##     Out = open(Directory+'/baselinefit1.html', 'w')
##     print >> Out, 'Not Processed Yet'
##     Out.close()

    Out = open(Directory+'/clustering.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="285, *">'
    #print >> Out, '<FRAME src="clustering1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="clustering1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="clustering1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % clusteringINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/clustering1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/clustering1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

##     Out = open(Directory+'/fitstatistics.html', 'w')
##     print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
##     print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
##     print >> Out, '<FRAMESET cols="175, *">'
##     print >> Out, '<FRAMESET rows="260, *">'
##     print >> Out, '<FRAME src="menu.html" name="menu">'
##     print >> Out, '<FRAMESET rows="175, *">'
##     #print >> Out, '<FRAME src="fitstatistics1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
##     print >> Out, '<FRAME src="fitstatistics1h.html" name="submenu1h" frameborder="NO">'
##     print >> Out, '<FRAME src="fitstatistics1.html" name="submenu1" frameborder="NO">'
##     print >> Out, '</FRAMESET>'
##     print >> Out, '</FRAMESET>'
##     print >> Out, '<FRAME src="%s" name="main">' % fitstatisticsINITpage
##     print >> Out, '</FRAMESET>'
##     print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
##     print >> Out, '</FRAMESET>\n</html>'
##     Out.close()
##     Out = open(Directory+'/fitstatistics1.html', 'w')
##     print >> Out, 'Not Processed Yet'
##     Out.close()
##     Out = open(Directory+'/fitstatistics1h.html', 'w')
##     print >> Out, 'Not Processed Yet'
##     Out.close()

    Out = open(Directory+'/gridding.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="195, *">'
    #print >> Out, '<FRAME src="gridding1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="gridding1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="gridding1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % griddingINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/gridding1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/gridding1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/multispectra.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="200, *">'
    #print >> Out, '<FRAME src="multispectra1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="multispectra1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="multispectra1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % multispectraINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/multispectra1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/multispectra1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

    Out = open(Directory+'/sparsespectramap.html', 'w')
    print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
    print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>' % TargetSrc
    print >> Out, '<FRAMESET cols="175, *">'
    print >> Out, '<FRAMESET rows="260, *">'
    print >> Out, '<FRAME src="menu.html" name="menu">'
    print >> Out, '<FRAMESET rows="190, *">'
    #print >> Out, '<FRAME src="sparsespectramap1h.html" name="submenu1h" frameborder="NO" scrolling="NO">'
    print >> Out, '<FRAME src="sparsespectramap1h.html" name="submenu1h" frameborder="NO">'
    print >> Out, '<FRAME src="sparsespectramap1.html" name="submenu1" frameborder="NO">'
    print >> Out, '</FRAMESET>'
    print >> Out, '</FRAMESET>'
    print >> Out, '<FRAME src="%s" name="main">' % sparsespectramapINITpage
    print >> Out, '</FRAMESET>'
    print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
    print >> Out, '</FRAMESET>\n</html>'
    Out.close()
    Out = open(Directory+'/sparsespectramap1.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()
    Out = open(Directory+'/sparsespectramap1h.html', 'w')
    print >> Out, 'Not Processed Yet'
    Out.close()

##     Out = open(Directory+'/helpbaselinefit.html', 'w')
##     print >> Out, '<html>\n<head>\n</head>\n<body>'
##     print >> Out, '<h1>Descriptions for the Baseline Fitting process:</h1>'
##     print >> Out, '<p>Baseline fit Help......</p>'
##     print >> Out, '</body>\n</html>'
##     Out.close()

    Out = open(Directory+'/helpclustering.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Clustering Analysis:</h1>'
    print >> Out, '<p>Clustering Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

##     Out = open(Directory+'/helpfitstatistics.html', 'w')
##     print >> Out, '<html>\n<head>\n</head>\n<body>'
##     print >> Out, '<h1>Descriptions for the Fitting Statistics:</h1>'
##     print >> Out, '<p>FitStatistics Help......</p>'
##     print >> Out, '</body>\n</html>'
##     Out.close()

    Out = open(Directory+'/helpgridding.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Gridding process:</h1>'
    print >> Out, '<p>Gridding Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpmultispectra.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the MultiSpectra:</h1>'
    print >> Out, '<p>MultiSpectra Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

    Out = open(Directory+'/helpsparsespectramap.html', 'w')
    print >> Out, '<html>\n<head>\n</head>\n<body>'
    print >> Out, '<h1>Descriptions for the Sparse Spectra Map:</h1>'
    print >> Out, '<p>SparseSpectraMap Help......</p>'
    print >> Out, '</body>\n</html>'
    Out.close()

def HtmlBaselineFit(Directory):

    Outh = open(Directory+'/baselinefit1h.html', 'w')
    Out = open(Directory+'/baselinefit1.html', 'w')
    FileName = Directory+'/BF_Fit/listofplots.txt'
    baselinefitINITpage = "helpbaselinefit.html"
    Images = []
    Values = []
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        i = 0
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            (IF, POL, ITER, PAGE) = Plot.split('.')[0].split('_')[1:]
            Images.append("BF_Fit/"+Plot)
            #Values.append((ITER, IF, POL, PAGE, i))
            bname='btn'+str(i)
            Values.append((ITER, IF, POL, PAGE, bname, i))
            i += 1
            if INIT:
                baselinefitINITpage = "BF_Fit/"+Plot
                INIT = False
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Outh, JavaScript1
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][4] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][4] 
    print >> Outh, JavaScript2c
    print >> Outh, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Baseline Fit</p>'
    print >> Outh, '<a href="helpbaselinefit.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>\n</p>'
    #print >> Outh, 'Page</br>\n</p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>Page</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()


def HtmlClustering(Directory,DestList=[]):

    Out = open(Directory+'/clustering1.html', 'w')
    Outh = open(Directory+'/clustering1h.html', 'w')
    FileName = Directory+'/BF_Clstr/listofplots.txt'
    clusteringINITpage = "helpclustering.html"
    Images = []
    Values = []
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        i = 0
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            (IF, POL, ITER, PAGE) = Plot.split('.')[0].split('_')[1:]
            if PAGE.upper() == 'DETECTION': STAGE = 'D'
            elif PAGE.upper() == 'VALIDATION': STAGE = 'V'
            elif PAGE.upper() == 'SMOOTHING': STAGE = 'S'
            else: STAGE = 'R'
            if STAGE == 'D':
                PlotMap = Plot.replace('detection', 'ChannelSpace')
                Images.append("BF_Clstr/"+PlotMap)
                #Values.append((ITER, IF, POL, 'Map', i))
                bname='btn'+str(i)
                Values.append((ITER, IF, POL, 'Map', bname, i))
                i += 1
                if INIT:
                    clusteringINITpage = "BF_Clstr/"+PlotMap
                    INIT = False
            Images.append("BF_Clstr/"+Plot)
            #Values.append((ITER, IF, POL, PAGE, i))
            bname='btn'+str(i)
            PAGE = PAGE.capitalize()
            Values.append((ITER, IF, POL, PAGE, bname, i))
            i += 1
            #if INIT:
                #clusteringINITpage = "BF_Clstr/"+Plot
                #INIT = False
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Outh, JavaScript1
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][4] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][4] 
    #print >> Out, 'n = 1;'
    print >> Outh, JavaScript2a
    print >> Outh, JavaScript3
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Clustering Analysis</p>'
    print >> Outh, '<a href="helpclustering.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>'
    print >> Outh, 'Stage</br>\n'
    print >> Outh, '&nbsp&nbsp Map: Cluster Space</br>'
    print >> Outh, '&nbsp&nbsp D: Detection</br>'
    print >> Outh, '&nbsp&nbsp V: Validation</br>'
    print >> Outh, '&nbsp&nbsp S: Smoothing</br>'
    print >> Outh, '&nbsp&nbsp R: Regions</br></p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>Stage</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    # 2008/11/11 initial plot is a map
    #print >> Out, JavaScript3 % (1,1)
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Outh.close()
    Out.close()

    for dest in DestList:
        shutil.copy(Directory+'/clustering1.html',dest)
        shutil.copy(Directory+'/clustering1h.html',dest)
        if os.path.exists(dest+'/BF_Clstr'):
            shutil.rmtree(dest+'/BF_Clstr')
        shutil.copytree(Directory+'/BF_Clstr',dest+'/BF_Clstr')


def HtmlFitStatistics(Directory):

    Outh = open(Directory+'/fitstatistics1h.html', 'w')
    Out = open(Directory+'/fitstatistics1.html', 'w')
    FileName = Directory+'/BF_Stat/listofplots.txt'
    fitstatisticsINITpage = "helpfitstatistics.html"
    Images = []
    Values = []
    i = 0
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        (IF0, POL0, ITER0) = (-1, -1, -1)
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            PlotRoot = Plot.split('.')[0]
            (IF, POL, ITER, PAGE) = PlotRoot.split('_')[1:]
            if (IF0, POL0, ITER0) != (IF, POL, ITER):
                if IF0 != -1:
                    print >> Out2, '</body>\n</html>'
                    Out2.close()
                (IF0, POL0, ITER0) = (IF, POL, ITER)
                Fname = 'Stat_%s_%s_%s.html' % (IF, POL, ITER)
                NewFname = 'FitStat_%s_%s_%s.html' % (IF, POL, ITER)
                if INIT:
                    fitstatisticsINITpage = "BF_Stat/"+NewFname
                    INIT = False
                Out2 = open(Directory+'/BF_Stat/'+NewFname, 'w')
                if os.access(Directory+'/BF_Stat/'+Fname, os.F_OK):
                    ReadFile = open(Directory+'/BF_Stat/'+Fname, 'r')
                    while 1:
                        line0 = ReadFile.readline().split('\n')[0]
                        if line0.find('</body>') >= 0:
                            print >> Out2, '<HR>'
                            ReadFile.close()
                            break
                        print >> Out2, line0
                else:
                    print >> Out2, '<html>\n<head>\n</head>\n<body>'
                print >> Out2, 'Note to all the plots below: short green vertical lines indicate position gaps; short cyan vertical lines indicate time gaps<HR>'
                print >> Out2, '<img src="%s">\n<HR>' % (PlotRoot+'_trim.png')
                Images.append("BF_Stat/"+NewFname)
                #Values.append((ITER, IF, POL, i))
                bname='btn'+str(i)
                Values.append((ITER, IF, POL, bname, i))
                i += 1
            else:
                print >> Out2, '<img src="%s">\n<HR>' % (PlotRoot+'_trim.png')
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Outh, JavaScript1
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][3] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][3] 
    print >> Outh, JavaScript2a
    print >> Outh, JavaScript3
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Fitting Statistics</p>'
    print >> Outh, '<a href="helpfitstatistics.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>\n</p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n</form>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()


def HtmlGridding(Directory):

    Outh = open(Directory+'/gridding1h.html', 'w')
    Out = open(Directory+'/gridding1.html', 'w')
    FileName = Directory+'/ChannelMap/listofplots.txt'
    griddingINITpage = "helpgridding.html"
    Images = []
    Values = []
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        i = 0
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            (IF, POL, ITER, PAGE) = Plot.split('.')[0].split('_')[1:]
            Images.append("ChannelMap/"+Plot)
            #Values.append((ITER, IF, POL, PAGE, i))
            bname='btn'+str(i)
            Values.append((ITER, IF, POL, PAGE, bname, i))
            i += 1
            if INIT:
                griddingINITpage = "ChannelMap/"+Plot
                INIT = False
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Outh, JavaScript1
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][4] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][4] 
    print >> Outh, JavaScript2a
    print >> Outh, JavaScript3
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Gridding</p>'
    print >> Outh, '<a href="helpgridding.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>\n</p>'
    #print >> Outh, 'Page</br>\n</p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>Page</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()


def HtmlMultiSpectra(Directory):

    Outh = open(Directory+'/multispectra1h.html', 'w')
    Out = open(Directory+'/multispectra1.html', 'w')
    FileName = Directory+'/Gridding/listofplots.txt'
    multispectraINITpage = "helpmultispectra.html"
    Images = []
    Values = []
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        i = 0
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            (IF, POL, ITER, PAGE) = Plot.split('.')[0].split('_')[1:]
            Images.append("Gridding/"+Plot)
            #Values.append((ITER, IF, POL, PAGE, i))
            bname='btn'+str(i)
            Values.append((ITER, IF, POL, PAGE, bname, i))
            i += 1
            if INIT:
                multispectraINITpage = "Gridding/"+Plot
                INIT = False
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Outh, JavaScript1
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][4] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][4] 
    print >> Outh, JavaScript2c
    print >> Outh, JavaScript3
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">MultiSpectra</p>'
    print >> Outh, '<a href="helpmultispectra.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>\n</p>'
    #print >> Outh, 'Page</br>\n</p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>Page</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()


def HtmlSparseSpectraMap(Directory):

    Outh = open(Directory+'/sparsespectramap1h.html', 'w')
    Out = open(Directory+'/sparsespectramap1.html', 'w')
    FileName = Directory+'/SparseSpMap/listofplots.txt'
    sparsespectramapINITpage = "helpsparsespectramap.html"
    Images = []
    Values = []
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        i = 0
        while 1:
            line = File.readline()
            if not line: break
            if line.find(':') != -1: continue
            Plot = line.split()[0]
            (IF, POL, ITER, PAGE) = Plot.split('.')[0].split('_')[1:]
            Images.append("SparseSpMap/"+Plot)
            #Values.append((ITER, IF, POL, i))
            bname='btn'+str(i)
            Values.append((ITER, IF, POL, bname, i))
            i += 1
            if INIT:
                sparsespectramapINITpage = "SparseSpMap/"+Plot
                INIT = False
        File.close()
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Outh, JavaScript1
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][3] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][3] 
    print >> Outh, JavaScript2a
    print >> Outh, JavaScript3
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Sparse Spectra Map</p>'
    print >> Outh, '<a href="helpsparsespectramap.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br></p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()


def HtmlBaselineFit2(Directory):

    #from heuristics.resources.pathfinder import getPath
    from pipeline.infrastructure.renderer.logger import getPath
    import zipfile
    import subprocess
    
    # thumbnail template
    thumbTemplate = getPath('thumbnailsTemplate')
    
    fin = open(thumbTemplate,'r')
    template = string.Template(fin.read())
    insertTemplate = string.Template('<li class="$css_class">'
        + '<a rel="selected" href="$filename">'
        + '<img src="$thumb_filename" title="$title" alt="$title"/></a></li>')
    fin.close()
    
    # tools for thumbnail plot
    #jQuery = getPath('jquery.js')
    jQuery = getPath('templates/resources/jquery-2.1.3.min.js')
    fin = open(jQuery,'r')
    fout = open(Directory+'/BF_Fit/jquery.js','w')
    fout.write(fin.read())
    fin.close()
    fout.close()
    #fancyBoxZip = getPath('fancybox.zip')
    fancyBoxZip = getPath('templates/resources/fancybox.zip')
    z = zipfile.ZipFile(fancyBoxZip)
    z.extractall(path=Directory+'/BF_Fit')
    del z

    # set up
    Outh = open(Directory+'/baselinefit1h.html', 'w')
    Out = open(Directory+'/baselinefit1.html', 'w')
    FileName = Directory+'/BF_Fit/listofplots.txt'
    baselinefitINITpage = "helpbaselinefit.html"
    Images = []
    Values = []

    # get image list
    ImageList = {}
    if os.access(FileName, os.F_OK):
        File = open(FileName, 'r')
        INIT = True
        l = 'none'
        while len(l) != 0:
            l = File.readline().rstrip('\n')
            if l.find('.png') != -1:
                (IF,POL,ITER,PAGE) = l.split('.')[0].split('_')[1:]
                IF = string.atoi(IF)
                POL = string.atoi(POL)
                ITER = string.atoi(ITER)
                PAGE = string.atoi(PAGE)
                if not IF in ImageList.keys():
                    ImageList[IF] = {}
                if not POL in ImageList[IF].keys():
                    ImageList[IF][POL] = {}
                if not ITER in ImageList[IF][POL].keys():
                    ImageList[IF][POL][ITER] = []
                ImageList[IF][POL][ITER].append(l)

    # create thumbnail pages
    thumbDir = 'thumbs/'
    figDir = 'BF_Fit/'
    thumbAbsPath = string.join([Directory,figDir,thumbDir],'/')
    figAbsPath = string.join([Directory,figDir],'/')
    cssClass = ''
    button = ''
    selector = ''
    if not os.path.exists( thumbAbsPath ):
        os.mkdir( thumbAbsPath )
    pages = {}
    for ikey in ImageList.keys():
        pages[ikey] = {}
        for pkey in ImageList[ikey].keys():
            pages[ikey][pkey] = {}
            for tkey in ImageList[ikey][pkey].keys():
                filelist = ImageList[ikey][pkey][tkey]
                outfile = 'thumbnails_%s_%s_%s.html'%(ikey,pkey,tkey)
                outfilePath = string.join([Directory,figDir,outfile],'/')
                if not os.path.exists( outfilePath ):
                    title = 'Thumbnail Navigator for the Baseline Fit<BR>Iteration:%d   IF:%d  POL:%d'%(tkey,ikey,pkey)
                    thumbnail = ''
                    for idx in xrange(len(filelist)):
                        figfile = filelist[idx]
                        thumbfile = 'thumb.'+figfile
                        if not os.path.exists( thumbAbsPath+'/'+thumbfile ):
                            retcode = subprocess.call(['convert',figAbsPath+figfile,'-thumbnail','250x188',thumbAbsPath+thumbfile])
                        else:
                            retcode = 0
                        if retcode == 0:
                            thumbnail += insertTemplate.safe_substitute(css_class=cssClass,title=figfile,thumb_filename=thumbDir+thumbfile,filename=figfile)
                        else:
                            thumbnail += insertTemplate.safe_substitute(css_class=cssClass,title=figfile,thumb_filename=figfile,filename=figfile)
                        thumbnail += '\n'
                    resultStr = template.safe_substitute(title=title,buttons=button,selectors=selector,thumbnails=thumbnail)
                    File = open(outfilePath,'w')
                    File.write(resultStr)
                    File.close()
                pages[ikey][pkey][tkey] = outfile

    # init page
    key0 = min(pages.keys())
    key1 = min(pages[key0].keys())
    key2 = 1
    baselinefitINITpage = pages[key0][key1][key2]

    # menu page
    ifkeys = pages.keys()
    ifkeys.sort()
    ibtn = 0
    for ikey in ifkeys:
        polkeys = pages[ikey].keys()
        polkeys.sort()
        for pkey in polkeys:
            iterkeys = pages[ikey][pkey].keys()
            iterkeys.sort()
            for tkey in iterkeys:
                Images.append(figDir+pages[ikey][pkey][tkey])
                bname = 'btn%s'%ibtn
                Values.append((tkey,ikey,pkey,bname,ibtn))
                ibtn += 1
    
    print >> Outh, '<html>\n<head>\n<style>'
    print >> Outh, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Outh, '.stt{font-size:12px;font-weight:bold;}'
    print >> Outh, '.stc{font-size:12px;font-weight:normal;}'
    print >> Outh, '.cap{font-size:12px;font-weight:normal;}'
    print >> Outh, '.btn{font-size:10px;font-weight:normal;}'
    print >> Outh, '</style>'
    print >> Outh, JavaScript1
    print >> Out, '<html>\n<head>\n<style>'
    print >> Out, '.ttl{font-size:16px;font-weight:bold;color:white;background-color:navy;}'
    print >> Out, '.stt{font-size:12px;font-weight:bold;}'
    print >> Out, '.stc{font-size:12px;font-weight:normal;}'
    print >> Out, '.cap{font-size:12px;font-weight:normal;}'
    print >> Out, '.btn{font-size:10px;font-weight:normal;}'
    print >> Out, '</style>'
    print >> Out, JavaScript1
    for i in range(len(Images)):
        print >> Outh, 'img[m++] = "%s";' % Images[i]
        print >> Outh, 'btn[k++] = "%s";' % Values[i][3] 
        print >> Out, 'img[m++] = "%s";' % Images[i]
        print >> Out, 'btn[k++] = "%s";' % Values[i][3] 
    print >> Outh, JavaScript2c
    print >> Outh, JavaScript3
    print >> Outh, '</head>\n<body>'
    print >> Out, JavaScript2b
    print >> Out, JavaScript3
    print >> Out, '</head>\n<body>'
    print >> Outh, '<p class="ttl">Baseline Fit</p>'
    print >> Outh, '<a href="helpbaselinefit.html" target="main">Description</a>'
    print >> Outh, '<p class="cap">Itr: Iteration Cycle</br>'
    print >> Outh, 'IF: Frequency ID</br>'
    print >> Outh, 'Pol: Polarization</br>\n</p>'
    #print >> Outh, 'Page</br>\n</p>'
    print >> Outh, '<form action="#">'
    print >> Outh, '<input type="button" value="&lt; Prev" onclick="prev()" class="stt">'
    print >> Outh, '<input type="button" value="Next &gt;" onclick="next()" class="stt">'
    print >> Outh, '</form>\n</body>\n</html>'
    print >> Out, '<table border="1">'
    print >> Out, '<tr align="center" class="stt"><th>Itr</th><th>IF</th><th>Pol</th><th>&nbsp;</th></tr>'
    for i in range(len(Images)):
        print >> Out, '<tr align="right" class="stc"><th>%s</th><th>%s</th><th>%s</th><th><input id=%s type="button" value=%s onclick="hit(this); setBtnColor(this.id)" onblur="resetBtnColor(this.id)" onfocus="active(this)" class="btn"/></th></tr>' % Values[i] 
    print >> Out, '</table>\n'
    print >> Out, JavaScript4 % (0,0)
    print >> Out, '</body>\n</html>'
    Out.close()
    Outh.close()

###
#
# HtmlFrontPage
#
# The class creates top page of HTML summary output.
# It also handles dynamic change of top page during process.
#
# Inputs for constructor:
#
#    Directory --- root directory for HTML summary output
#    numAnt    --- number of antenna to be processed
#
###
class HtmlFrontPage:
    def __init__(self,Directory,numAnt=None,rawFile=None):
        self.numAnt=1
        if numAnt is not None:
            self.numAnt=numAnt
        self.index=Directory+'/index.html'
        self.main=Directory+'/main.html'
        self.menu=Directory+'/menu.html'
        self.logDir='Logs'
        if os.path.exists(Directory+'/'+self.logDir):
            os.system('rm -rf %s'%(self.logDir))
        os.mkdir(Directory+'/'+self.logDir)
        self.log=self.logDir+'/index.html'
        if rawFile is None:
            self.target=Directory.split('/')[-1].replace('_html', '')
        else:
            if type(rawFile) == str:
                self.target=rawFile.rsptrip('/')
            else:
                # list is assumed
                if len(rawFile) == 1:
                    self.target=rawFile[0].rstrip('/')
                else:
                    self.target = '[ '
                    for f in rawFile:
                        self.target += f+' '
                    self.target += ']'
        self.templateMain=None
        self.templateMenu=None

        # markers
        self.mark='<!--mark-->'
        self.markobs='<!--markobs-->'
        self.markspw='<!--markspw-->'
        self.markpol='<!--markpol-->'
        self.markstat='<!--markstat-->'
        self.markend='<!--markend-->'
        self.markelap='<!--markelap-->'

        # template for Menu
        self.atemplateMenu='<TD ALIGN="center"><A HREF="./${antenna}/index.html" TARGET="fp_main">${antenna}</A></TD>'
        self.btemplateMenu='<TD ALIGN="center">${antenna}</TD>'
        self.a2templateMenu='<TD ALIGN="center"><A HREF="./${subdir}/index.html" TARGET="fp_main">${subdir}</A></TD>'
        self.b2templateMenu='<TD ALIGN="center">${subdir}</TD>'

        # template for Result section
        self.atemplateAnt='<TD ALIGN="left"><A HREF="./${antenna}/index.html">ANTENNA ${id}<BR>NAME="${antenna}"</A></TD>'
        self.btemplateAnt='<TD ALIGN="left">ANTENNA ${id}<BR>NAME="${antenna}"</TD>'
        self.a2templateAnt='<TD ALIGN="left"><A HREF="./${subdir}/index.html">ANTENNA ${id}<BR>DATA="${data}"<BR>NAME="${antenna}"</A></TD>'
        self.b2templateAnt='<TD ALIGN="left">ANTENNA ${id}<BR>DATA="${data}"<BR>NAME="${antenna}"</TD>'
        self.combined='COMBINED'
        self.atemplateCombine='<TD ALIGN="left"><A HREF="./${antenna}/index.html">ANTENNA ${id}<BR>NAME="${antenna}"</A></TD></TR></TABLE><BR>\n<TABLE NOBORDER WIDTH="100%">\n<TR>'

        # template for Observation section
        self.templateObs='<DT>File: ${file}</DT>\n'
        self.templateObs+='<DD><P ALIGN="center">\n<TABLE NOBORDER WIDTH="80%">\n'
        self.templateObs+='<TR>\n<TD ALIGN="left">Observer:</TD>\n<TD ALIGN="left">${who}</TD>\n</TR>\n<TR>\n<TD ALIGN="left">Date:</TD>\n<TD ALIGN="left">${when}</TD>\n</TR>\n<TR><TD ALIGN="left">Antenna:</TD><TD ALIGN="left">${antenna}</TD></TR>\n<TR>\n<TD ALIGN="left">Source:</TD>\n<TD ALIGN="left">${what} (${where})</TD>\n</TR>\n<TR VALIGN="top">\n<TD ALIGN="left">Observed frequency:</TD>\n<TD ALIGN="left">\n<TABLE BORDER>\n<TR><TH>SPW_ID</TH><TH>FRAME</TH><TH>FREQ [GHz]</TH><TH>NCHAN</TH><TH>BW [MHz]</TH><TH>INTENT</TH><TH>TYPE</TH></TR>\n'+self.markspw+'\n</TABLE>\n</TD>\n</TR>\n'
        self.templateObs+='<TR VALIGN="top">\n<TD ALIGN="left">Polarizattion:</TD>\n<TD ALIGN="left">\n<TABLE BORDER>\n<TR><TH>POL_ID</TH><TH>TYPE</TH><TH>CORR</TH><TH>SPW_ID</TH><TR>\n'+self.markpol+'\n</TABLE>\n</TD>\n</TR>\n'
        self.templateObs+='\n</TABLE>\n</P></DD>\n<BR>'
        self.spwtemplate='<TR><TD>${spw}</TD><TD>${frame}</TD><TD>${freqrange}</TD><TD>${nchan}</TD><TD>${bw}</TD><TD>${intent}</TD><TD>${spwtype}</TD></TR>'
        self.poltemplate='<TR><TD>${pol}</TD><TD>${poltype}</TD><TD>${corr}</TD><TD>${assocspw}</TD></TR>'

        # initialization
        self.__initIndex()
        self.__initMain()
        self.__initMenu()

    def setNumAnt(self, num):
        if self.numAnt == 1 and num > 1:
            self.addCombine('ALL',self.combined,back=False,nolink=False)
        self.numAnt=num

    def __initIndex(self):
        # create top page 
        Out = open(self.index, 'w')
        print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
        print >> Out, '<head>\n<title>SD Heuristic Pipeline for %s</title>\n</head>'%(self.target)
        print >> Out, '<FRAMESET FRAMEBORDER="1" rows="92%, *">'
        print >> Out, '<FRAME src="./main.html" name="fp_main">'
        print >> Out, '<FRAME src="./menu.html" name="fp_menu">'
        print >> Out, '<NOFRAMES><p>No frame is supported in your browser</p></NOFRAMES>'
        print >> Out, '</FRAMESET>\n</html>'
        Out.close()

    def __initMain(self):
        # create main
        Out = open(self.main, 'w')
        print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
        print >> Out, '<head>\n<title>Single-Dish Heuristic Pipeline for %s</title>\n</head>\n<body>'%(self.target)
        print >> Out, '<H1 ALIGN="center">Heuristics Result for %s</H1>'%(self.target)
        print >> Out, '<HR>'
        print >> Out, '<A HREF="#observation">Observation Summary</A><BR>'
        print >> Out, '<A HREF="#result">Result Summary</A><BR>'
        print >> Out, '<A HREF="#process">Process Summary</A>'
        print >> Out, '<HR>'
        print >> Out, '<B ID="observation">Observation Summary</B><BR>'
        print >> Out, '<DL>\n'+self.markobs+'\n</DL>'
        print >> Out, '<B ID="result">Result Summary</B><BR>'
        print >> Out, '<P ALIGN="center">\n<TABLE NOBORDER WIDTH="70%">'
        print >> Out, '<TR>\n<TD ALIGN="left">Status:</TD>'
        print >> Out, '<TD ALIGN="left">'+self.markstat+'<B>Not started yet</B>'+self.markstat+'</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left" VALIGN="baseline">Result:</TD>'
        print >> Out, '<TD>\n<TABLE NOBORDER WIDTH="100%">\n<TR>'
        print >> Out, self.mark
        print >> Out, '</TR>\n</TABLE>\n</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left">Global log:</TD>'
        print >> Out, '<TD ALIGN="left"><A HREF="%s/PIPELINE.txt">PIPELINE.txt</A></TD>\n</TR>'%('./'+self.logDir)
        print >> Out, '</TABLE>\n</P>\n<BR>'
        print >> Out, '<B ID="process">Process Summary</B><BR>'
        print >> Out, '<P ALIGN="center">\n<TABLE NOBODER WIDTH="70%">'
        print >> Out, '<TR VALIGN="baseline">\n<TD ALIGN="left">Software version:</TD>'
        print >> Out, '<TD ALIGN="left">CASA ${casaver} (revision ${casarev})<BR>'
        print >> Out, 'Heuristics ${hver}</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left">Recipe file:</TD>'
        print >> Out, '<TD ALIGN="left"><A HREF="./%s/Recipe.txt">${recipename}</A></TD>\n</TR>'%(self.logDir)
        print >> Out, '<TR>\n<TD ALIGN="left">Input parameter summary:</TD>'
        print >> Out, '<TD ALIGN="left"><A HREF="./%s/params.txt">Parameter List</A></TD>\n</TR>'%(self.logDir)
        print >> Out, '<TR>\n<TD ALIGN="left">Process start time:</TD>'
        print >> Out, '<TD ALIGN="left">${starttime}</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left">Process end time:</TD>'
        print >> Out, '<TD ALIGN="left">'+self.markend+'Process not finished yet'+self.markend+'</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left">Elapsed time:</TD>'
        print >> Out, '<TD ALIGN="left">'+self.markelap+'Process not finished yet'+self.markelap+'</TD>\n</TR>'
        print >> Out, '<TR>\n<TD ALIGN="left">Profiling:</TD>'
        print >> Out, '<TD ALIGN="left"><A HREF="./profile.html">Profiling Summary</A></TD>'
        print >> Out, '</TABLE>\n</P>'
        print >> Out, '</body>\n</html>'
        Out.close()

        self.__updateTemplateMain()

    def __initMenu(self):
        # create menu
        Out = open(self.menu, 'w')
        print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
        print >> Out, '<head>\n<title>Menu</title>\n</head>\n<body>'
        print >> Out, '<TABLE WIDTH="100%" CELLSPACING="0" NOBORDER>'
        print >> Out, '<TR>'
        print >> Out, self.mark
        if self.numAnt > 1:
            print >> Out, '<TD ALIGN="center">%s</TD>'%(self.combined)
        print >> Out, '<TD ALIGN="center"><A HREF="%s/PIPELINE.txt" TARGET="fp_main">Log</A></TD>'%('./'+self.logDir)
        print >> Out, '<TD ALIGN="center"><A HREF="./profile.html" TARGET="fp_main">Profile</A></TD>'
        print >> Out, '<TD ALIGN="center"><A HREF="./main.html" TARGET="fp_main">Top</A></TD>'
        print >> Out, '</TR>\n</TABLE>'
        print >> Out, '</body>\n</html>'
        Out.close()

        self.__updateTemplateMenu()

    def __updateTemplateMenu(self):
        f = open(self.menu)
        self.templateMenu = f.read()
        f.close()

    def __updateTemplateMain(self):
        f = open(self.main)
        self.templateMain = f.read()
        f.close()

    def __postMenu(self):
        Out = open(self.menu,'w')
        print >> Out, self.templateMenu
        Out.close()

    def __postMain(self):
        Out = open(self.main,'w')
        print >> Out, self.templateMain
        Out.close()        
            
    def postInfo(self,**kwargs):
        tmp=string.Template(self.templateMain)
        self.templateMain=tmp.safe_substitute(kwargs)
        self.__postMain()

        if kwargs.has_key('subdir') and kwargs['subdir'] is not None:
            tmp=string.Template(self.templateMenu)
            self.templateMenu=tmp.safe_substitute(subdir=kwargs['subdir'])
            self.__postMenu()
        elif kwargs.has_key('antenna'):
            tmp=string.Template(self.templateMenu)
            self.templateMenu=tmp.safe_substitute(antenna=kwargs['antenna'])
            self.__postMenu()
    
    def addAntenna(self,id,name,back=False,nolink=False,data=None):
        if back:
            offset=1
        else:
            offset=0
        if data is not None:
            subdir = data.rstrip('/')+'.'+name
        else:
            subdir = None
        if nolink:
            if data is None:
                templateAnt = self.btemplateAnt
                templateMenu = self.btemplateMenu
            else:
                templateMenu = self.b2templateMenu
                templateAnt = self.b2templateAnt
        else:
            if data is None:
                templateMenu = self.atemplateMenu
                templateAnt = self.atemplateAnt
            else:
                templateMenu = self.a2templateMenu
                templateAnt = self.a2templateAnt
        self.templateMain=self.__insert(self.templateMain,
                                        templateAnt,
                                        self.mark,
                                        offset)
        self.templateMenu=self.__insert(self.templateMenu,
                                        templateMenu,
                                        self.mark,
                                        offset)
        self.postInfo(antenna=name,id=id,data=data,subdir=subdir)

    def addCombine(self,id,name,back=False,nolink=False,data=None):
        if back:
            offset=1
        else:
            offset=0
        if data is not None:
            subdir = data.rstrip('/')+'.'+name
        else:
            subdir = None
        templateAnt = self.atemplateCombine
        templateMenu = self.atemplateMenu
        self.templateMain=self.__insert(self.templateMain,
                                        templateAnt,
                                        self.mark,
                                        offset)
        self.templateMenu=self.__insert(self.templateMenu,
                                        templateMenu,
                                        self.mark,
                                        offset)
        self.postInfo(antenna=name,id=id,data=data,subdir=subdir)

    def addFile( self, file ):
        tmp=string.Template(self.templateObs)
        template=tmp.safe_substitute(file=file)
        self.templateMain=self.__insert(self.templateMain,
                                        template,
                                        self.markobs)
        self.__postMain()

    def finishFile( self ):
        self.__removeMark(self.markspw)
        self.__removeMark(self.markpol)

    def finishObs( self ):
        self.__removeMark(self.markobs)

    def finishPage( self ):
        self.__updateTemplateMain()
        self.__removeMark(self.markobs)
        self.__removeMark(self.markspw)
        self.__removeMark(self.markpol)
        self.__removeMark(self.markstat)
        self.__removeMark(self.mark,True)
        self.__postMain()
        self.__postMenu()

    def __removeMark( self, mark, menuAlso=False ):
        if self.templateMain.find(mark) != -1:
            s=self.templateMain.split(mark)
            self.templateMain=string.join(s,'')
        if menuAlso and self.templateMenu.find(mark) != -1:
            s=self.templateMenu.split(mark)
            self.templateMenu=string.join(s,'')

    def __insert(self,t,s,mark,offset=0):
        l=t.split('\n')
        idx=l.index(mark)
        idx+=offset
        l.insert(idx,s)
        return string.join(l,'\n')

    def __refreshStatus( self ):
        s=self.templateMain.split(self.markstat)
        s[1] = '${status}'
        self.templateMain = string.join(s,self.markstat)

    def postStatus(self,status,color):
        self.__refreshStatus()
        message='<FONT COLOR="'+color+'"><B>'+status+'</B></FONT>'
        self.postInfo(status=message)

    def __refreshEnd( self ):
        s=self.templateMain.split(self.markend)
        s[1] = '${endtime}'
        self.templateMain = string.join(s)
        s=self.templateMain.split(self.markelap)
        s[1] = '${elapsedtime} sec'
        self.templateMain = string.join(s)
        
    def postEndTime(self,endt,elapsedt):
        self.__refreshEnd()
        self.postInfo(endtime=endt,elapsedtime=elapsedt)

    def addPolarization(self,id,ptype,corr,spw):
        self.templateMain=self.__insert(self.templateMain,
                                        self.poltemplate,
                                        self.markpol)
        self.postInfo(pol=id,poltype=ptype,corr=corr,assocspw=spw)

    def addSpectralWindow(self,id,freq,nchan,bw,frame='LSRK',intent='NONE',spwtype='NONE'):
        self.templateMain=self.__insert(self.templateMain,
                                        self.spwtemplate,
                                        self.markspw)
        self.postInfo(spw=id,freqrange=freq,nchan=nchan,bw=bw,frame=frame,
                      intent=intent,spwtype=spwtype)
        
    def setSourceDirection(self,dir,ref='J2000'):
        """
        dir: [d0,d1] in deg
        """
        from SDPlotter import Deg2HMS, Deg2DMS
        (h,m,s)=Deg2HMS(dir[0],0)
        (D,M,S)=Deg2DMS(dir[1],0)
        self.postInfo(where='%s:%s:%s %s.%s.%s %s'%(h,m,s,D,M,S,ref))

###
#
# HtmlProfile
#
# Creates profiling result page.
#
# Inputs for constructor:
#
#     dir --- root directory for profile result
#
###
class HtmlProfile:
    def __init__(self,dir):
        self.dir=dir
        self.page=self.dir+'/profile.html'
        self.tplot0='TimeStatistics.png'
        self.tplot1='TimeProfile.png'
        self.timedic={}
        self.template=None
        self.markt='<!--markt-->'
        self.markm='<!--markm-->'
        self.templateT='<TR><TD>${id}</TD><TD>${stage}</TD><TD>${call}</TD><TD>${max}</TD><TD>${min}</TD><TD>${average}</TD><TD>${total}</TD></TR>'
        
        self.__initPage()
        
    def __initPage(self):
        # create main
        Out = open(self.page, 'w')
        print >> Out, '<html xmlns="http://www.w3.org/1999/xhtml">'
        print >> Out, '<head>\n<title>Profiling Summary</title>\n</head>\n<body>'
        print >> Out, '<H1 ALIGN="center">Profiling Summary</H1>'
        print >> Out, '<HR>'
        print >> Out, '<B>Elapsed Time Profile</B><BR>'
        print >> Out, '<P ALIGN="center">'
        print >> Out, '<TABLE BORDER WIDTH="70%">'
        print >> Out, '<TR><TH>ID</TH><TH>STAGE NAME</TH><TH>CALL</TH><TH>MAX [sec]</TH><TH>MIN [sec]</TH><TH>AVERAGE [sec]</TH><TH>TOTAL [sec]</TH>'
        print >> Out, self.markt
        print >> Out, '</TABLE></P>'
        #print >> Out, '<B>Memory Usage Profile</B><BR>'
        #print >> Out, '<P ALIGN="center">'
        #print >> Out, '<TABLE BORDER WIDTH="70%">'
        #print >> Out, '<TR><TH>STAGE</TH><TH>CALL</TH><TH>MAX [MB]</TH><TH>MIN [MB]</TH><TH>AVERAGE [MB]</TH><TH>TOTAL [MB]</TH>'
        #print >> Out, self.markm
        #print >> Out, '</TABLE></P>'
        print >> Out, '</body>\n</html>'
        Out.close()

        f = open(self.page, 'r')
        self.template = f.read()
        f.close()

    def addTime(self,stage,elapsed):
        if self.timedic.has_key(stage):
            self.timedic[stage].append(elapsed)
        else:
            self.timedic[stage]=[elapsed]

    def makeTimeProfileTable(self):
        for i in xrange(len(self.timedic)):
            k=self.timedic.keys()[i]
            v=self.timedic[k]
            self.template=self.__insert(self.template,
                                        self.templateT,
                                        self.markt)
            [ncall,vmax,vmin,vave,vsum]=self.__getStats(v)
            maxval='%7.4g'%(vmax)
            minval='%7.4g'%(vmin)
            total='%7.4g'%(vsum)
            ave='%7.4g'%(vave)
            self.postInfo(id=i,stage=k,max=maxval,min=minval,call=ncall,total=total,average=ave)

    def makeTimeProfilePlot(self):
        txt='<P ALIGN="center"><IMG SRC="%s" ALT="Time Statistics Plot"></P>'%(self.tplot0)
        self.template=self.__insert(self.template,
                                    txt,
                                    self.markt,
                                    offset=2)
        self.postInfo()
        self.__createTimeStatisticsPlot()
        txt='<P ALIGN="center"><IMG SRC="%s" ALT="Time Profile Plot"></P>'%(self.tplot1)
        self.template=self.__insert(self.template,
                                    txt,
                                    self.markt,
                                    offset=3)
        self.postInfo()
        self.__createTimeProfilePlot()
        
    def __createTimeStatisticsPlot(self):
        import pylab as pl
        pl.ioff()
        pl.clf()
        ctab=['#0000ff','#00ff00','#ff44ff','#ff0000']
        cedge='#000000'
        leg=['max','min','average','total']
        margin=2
        nstage=len(self.timedic)
        ntime=len(leg)
        nbin=ntime*nstage+margin*nstage
        if nbin == 0.0:
            # process not yet done, do nothing
            return
        wbin=1.0/nbin
        loc=wbin
        pos=[]
        ytick=[]
        for i in xrange(len(self.timedic)-1,-1,-1):
            stage=self.timedic.keys()[i]
            val=self.__getStats(self.timedic[stage])[1:]
            idx=len(val)-1
            if i == 0:
                for j in xrange(len(val)):
                    ymin=loc+(idx-j)*wbin
                    xmax=val[j]
                    pl.barh(bottom=ymin,width=xmax,height=wbin,color=ctab[j],edgecolor=cedge,label=leg[j])
            else:
                for j in xrange(len(val)):
                    ymin=loc+(idx-j)*wbin
                    xmax=val[j]
                    pl.barh(bottom=ymin,width=xmax,height=wbin,color=ctab[j],edgecolor=cedge)
            loc+=(ntime+margin)*wbin
            #pos.append(ymin+wbin-0.5*ntime*wbin)
            pos.append(loc-(margin+0.5*ntime)*wbin)
            ytick.append(str(i))
        pl.ylabel('Stage ID')
        pl.xlabel('Elapsed Time [sec]')
        pl.title('Stage Statistics')
        pl.yticks(pos,ytick)
        pl.legend(loc=0)
        ax=pl.gca()
        for tick in ax.yaxis.get_major_ticks():
            tick.tick1On=False
            tick.tick2On=False
        pl.savefig(self.dir+'/'+self.tplot0,format='png')


    def __createTimeProfilePlot(self):
        import pylab as pl
        pl.ioff()
        pl.clf()
        ctab='#0000ff'
        cedge='#000000'
        leg=['max','min','average','total']
        margin=2
        nstage=len(self.timedic)
        totaltime=0
        for key in self.timedic.keys():
            totaltime+=len(self.timedic[key])
        nbin=totaltime+margin*nstage
        if nbin == 0.0:
            # process not yet done, do nothing
            return
        wbin=1.0/nbin
        loc=wbin
        pos=[]
        ytick=[]
        for i in xrange(len(self.timedic)-1,-1,-1):
            stage=self.timedic.keys()[i]
            val=self.timedic[stage]
            ntime=len(val)
            idx=ntime-1
            for j in xrange(len(val)):
                ymin=loc+(idx-j)*wbin
                xmax=val[j]
                #pl.barh(bottom=ymin,width=xmax,height=wbin,color=ctab,edgecolor=cedge)
                pl.barh(bottom=ymin,width=xmax,height=wbin,color=ctab)
            loc+=(ntime+margin)*wbin
            if i != 0:
                pl.axhline(y=loc-wbin,color=cedge,ls=':')
            pos.append(loc-(margin+0.5*ntime)*wbin)
            ytick.append(str(i))
        pl.ylabel('Stage ID')
        pl.xlabel('Elapsed Time [sec]')
        pl.title('Stage Profile')
        pl.yticks(pos,ytick)
        ax=pl.gca()
        for tick in ax.yaxis.get_major_ticks():
            tick.tick1On=False
            tick.tick2On=False
        pl.savefig(self.dir+'/'+self.tplot1,format='png')
        
        
    def __insert(self,t,s,mark,offset=0):
        l=t.split('\n')
        idx=l.index(mark)
        idx+=offset
        l.insert(idx,s)
        return string.join(l,'\n')

    def postInfo(self,**kwargs):
        tmp=string.Template(self.template)
        self.template=tmp.safe_substitute(kwargs)
        Out = open(self.page,'w')
        print >> Out, self.template
        Out.close()

    def __getStats(self,v):
        vmax=max(v)
        vmin=min(v)
        ncall=len(v)
        vsum=sum(v)
        vave=vsum/ncall
        return [ncall,vmax,vmin,vave,vsum]
