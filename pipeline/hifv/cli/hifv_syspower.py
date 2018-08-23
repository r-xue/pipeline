#
# This file was generated using xslt from its XML file
#
# Copyright 2009, Associated Universities Inc., Washington DC
#
import sys
import os
from  casac import *
import string
from taskinit import casalog
from taskinit import xmlpath
#from taskmanager import tm
import task_hifv_syspower
def hifv_syspower(vis=[''], clip_sp_template=[0.7, 1.2], pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Base syspower task
The hifv_syspower task

Keyword arguments:

---- pipeline parameter arguments which can be set in any pipeline mode

vis -- List of visisbility  data files. These may be ASDMs, tar files of ASDMs,
   MSs, or tar files of MSs, If ASDM files are specified, they will be
   converted  to MS format.
   default: []
   example: vis=['X227.ms', 'asdms.tar.gz']

clip_sp_template -- clipping range
    default: [0.7, 1.2]

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs
   automatically.  In 'interactive' mode the user can set the pipeline
   context defined parameters manually.  In 'getinputs' mode the user
   can check the settings of all pipeline parameters without running
   the task.
   default: 'automatic'.

---- pipeline context defined parameter argument which can be set only in
'interactive mode'


--- pipeline task execution modes

dryrun -- Run the commands (True) or generate the commands to be run but
   do not execute (False).
   default: True

acceptresults -- Add the results of the task to the pipeline context (True) or
   reject them (False).
   default: True

Output:

results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
   the results object for the pipeline task is returned.


Examples

1. Basic syspower task

   hifv_syspower()



        """
        if type(vis)==str: vis=[vis]
        if type(clip_sp_template)==float: clip_sp_template=[clip_sp_template]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['clip_sp_template'] = clip_sp_template
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifv/cli/"
	trec = casac.utils().torecord(pathname+'hifv_syspower.xml')

        casalog.origin('hifv_syspower')
        if trec.has_key('hifv_syspower') and casac.utils().verify(mytmp, trec['hifv_syspower']) :
	    result = task_hifv_syspower.hifv_syspower(vis, clip_sp_template, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
