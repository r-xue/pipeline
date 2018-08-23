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
import task_hifv_checkflag
def hifv_checkflag(vis=[''], checkflagmode='', pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Run flagdata in rflag mode
The hifv_checkflag task runs flagdata in rflag mode

Keyword arguments:

---- pipeline parameter arguments which can be set in any pipeline mode

vis -- List of visibility data files. These may be ASDMs, tar files of ASDMs,
   MSs, or tar files of MSs, If ASDM files are specified, they will be
   converted  to MS format.
   default: []
   example: vis=['X227.ms', 'asdms.tar.gz']


checkflagmode -- blank string default use of rflag on bandpass and delay calibrators
              -- use string 'semi' after hifv_semiFinalBPdcals() for executing rflag on calibrators
              -- use string 'bpd', for the bandpass and delay calibrators:
                   execute rflag on all calibrated cross-hand corrected data;
                       extend flags to all correlations
                   execute rflag on all calibrated parallel-hand residual data;
                       extend flags to all correlations
                   execute tfcrop on all calibrated cross-hand corrected data,
                       per visibility; extend flags to all correlations
                   execute tfcrop on all calibrated parallel-hand corrected data,
                       per visibility; extend flags to all correlations
              -- use string 'allcals', for all the other calibrators, with delays and BPcal applied:
                       similar procedure as 'bpd' mode, but uses corrected data throughout
              -- use string 'target', for the target data:
                       similar procedure as 'allcals' mode, but with a higher SNR cutoff
                       for rflag to avoid flagging data due to source structure, and
                       with an additional series of tfcrop executions to make up for
                       the higher SNR cutoff in rflag
              -- VLASS specific modes include 'bpd-vlass', 'allcals-vlass', and 'target-vlass'
                       which calculate thresholds to user per spw/field/scan (action='calculate', then,
                       per baseband/field/scan, replace all spw thresholds above the median with the median,
                       before re-running rflag with the new thresholds.  This has the effect of
                       lowering the thresholds for spws with RFI to be closer to the RFI-free
                       thresholds, and catches more of the RFI.


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

1. Run RFLAG with associated heuristics in the VLA CASA pipeline.

   hifv_checkflag()



        """
        if type(vis)==str: vis=[vis]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['checkflagmode'] = checkflagmode
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifv/cli/"
	trec = casac.utils().torecord(pathname+'hifv_checkflag.xml')

        casalog.origin('hifv_checkflag')
        if trec.has_key('hifv_checkflag') and casac.utils().verify(mytmp, trec['hifv_checkflag']) :
	    result = task_hifv_checkflag.hifv_checkflag(vis, checkflagmode, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
