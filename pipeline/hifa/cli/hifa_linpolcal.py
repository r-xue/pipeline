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
import task_hifa_linpolcal
def hifa_linpolcal(vis=[''], field='', intent='', g0table='', delaytable='', xyf0table='', g1table='', df0table='', refant='', spw='', pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Compute polarization calibration
Compute a polarization calibration.

Keyword arguments:

--- pipeline parameter arguments which can be set in any pipeline mode

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
       determines the values of all context defined pipeline inputs
       automatically.  In interactive mode the user can set the pipeline
       context defined parameters manually.  In 'getinputs' mode the user
       can check the settings of all pipeline parameters without running
       the task.
       default: 'automatic'.


---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- The list of input MeasurementSets. Defaults to the list of MeasurementSets 
    in the context. CURRENTLY THE LIST MUST CONTAIN 1 MEASUREMENT SET.
    default: ''
    example: vis=['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']

--- pipeline task execution modes
dryrun -- Run the commands (True) or generate the commands to be run but
   do not execute (False).
   default: False

acceptresults -- Add the results of the task to the pipeline context (True) or
   reject them (False).
   default: True

Output:

results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.


Examples:
TBD

        """
        if type(vis)==str: vis=[vis]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['field'] = field
        mytmp['intent'] = intent
        mytmp['g0table'] = g0table
        mytmp['delaytable'] = delaytable
        mytmp['xyf0table'] = xyf0table
        mytmp['g1table'] = g1table
        mytmp['df0table'] = df0table
        mytmp['refant'] = refant
        mytmp['spw'] = spw
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifa/cli/"
	trec = casac.utils().torecord(pathname+'hifa_linpolcal.xml')

        casalog.origin('hifa_linpolcal')
        if trec.has_key('hifa_linpolcal') and casac.utils().verify(mytmp, trec['hifa_linpolcal']) :
	    result = task_hifa_linpolcal.hifa_linpolcal(vis, field, intent, g0table, delaytable, xyf0table, g1table, df0table, refant, spw, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
