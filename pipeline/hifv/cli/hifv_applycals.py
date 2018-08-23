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
import task_hifv_applycals
def hifv_applycals(vis=[''], field='', intent='', spw='', antenna='', applymode='', flagbackup=True, flagsum=True, flagdetailedsum=True, gainmap=False, pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Applycals
The hifv_applycals task

Apply precomputed calibrations to the data.

---- pipeline parameter arguments which can be set in any pipeline mode

applymode -- Calibration apply mode
    ''='calflagstrict': calibrate data and apply flags from solutions using
        the strict flagging convention
    'trial': report on flags from solutions, dataset entirely unchanged
    'flagonly': apply flags from solutions only, data not calibrated
    'calonly': calibrate data only, flags from solutions NOT applied
    'calflagstrict':
    'flagonlystrict':same as above except flag spws for which calibration is
        unavailable in one or more tables (instead of allowing them to pass
        uncalibrated and unflagged)
   default: ''

flagsum -- Compute before and after flagging statistics summaries.
   default: True

flagdetailedsum -- Compute detailed before and after flagging statistics summaries
   if flagsum is True.
   default: True

gainmap -- Mode to map gainfields to a particular list of scans
    default: False

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs automatically.
   In interactive mode the user can set the pipeline context defined parameters
   manually.  In 'getinputs' mode the user can check the settings of all
   pipeline parameters without running the task.
   default: 'automatic'.


---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- The list of input MeasurementSets. Defaults to the list of MeasurementSets
    in the pipeline context.
    default: []
    example: ['X227.ms']

field -- A string containing the list of field names or field ids to which
    the calibration will be applied. Defaults to all fields in the pipeline
    context.
    default: ''
    example: '3C279', '3C279, M82'

intent -- A string containing a the list of intents against which the
    selected fields will be matched. Defaults to all supported intents
    in the pipeline context.
    default: ''
    example: '*TARGET*'

spw -- The list of spectral windows and channels to which the calibration
    will be applied. Defaults to all science windows in the pipeline
    context.
    default: ''
    example: '17', '11, 15'

antenna -- The list of antennas to which the calibration will be applied.
    Defaults to all antennas. Not currently supported.


--- pipeline task execution modes
dryrun -- Run the commands (True) or generate the commands to be run but
   do not execute (False).
   default: False

acceptresults -- Add the results of the task to the pipeline context (True) or
   reject them (False).
   default: True

Output:

results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned

Description

hivf_applycals applies the precomputed calibration tables stored in the pipeline
context to the set of visibility files using predetermined field and
spectral window maps and default values for the interpolation schemes.

Users can interact with the pipeline calibration state using the tasks
hif_export_calstate and hif_import_calstate.

Issues

There is some discussion about the appropriate values of calwt. Given
properly scaled data, the correct value should be the CASA default of True.
However at the current time ALMA is suggesting that calwt be set to True for
applying observatory calibrations, e.g. antenna positions, WVR, and system
temperature corrections, and to False for applying instrument calibrations,
e.g. bandpass, gain, and flux.


Examples

1. Run the final applycals stage of the VLA CASA pipeline.

hifv_applycals()



        """
        if type(vis)==str: vis=[vis]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['field'] = field
        mytmp['intent'] = intent
        mytmp['spw'] = spw
        mytmp['antenna'] = antenna
        mytmp['applymode'] = applymode
        mytmp['flagbackup'] = flagbackup
        mytmp['flagsum'] = flagsum
        mytmp['flagdetailedsum'] = flagdetailedsum
        mytmp['gainmap'] = gainmap
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifv/cli/"
	trec = casac.utils().torecord(pathname+'hifv_applycals.xml')

        casalog.origin('hifv_applycals')
        if trec.has_key('hifv_applycals') and casac.utils().verify(mytmp, trec['hifv_applycals']) :
	    result = task_hifv_applycals.hifv_applycals(vis, field, intent, spw, antenna, applymode, flagbackup, flagsum, flagdetailedsum, gainmap, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
