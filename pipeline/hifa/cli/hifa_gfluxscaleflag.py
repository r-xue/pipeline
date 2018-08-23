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
import task_hifa_gfluxscaleflag
def hifa_gfluxscaleflag(vis=[''], phaseupsolint='int', solint='inf', minsnr=2.0, refant='', antnegsig=4.0, antpossig=4.6, tmantint=0.063, tmint=0.085, tmbl=0.175, antblnegsig=3.4, antblpossig=3.2, relaxed_factor=2.0, niter=2, pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Derive the flux density scale with flagging

Keyword arguments:

--- pipeline parameter arguments which can be set in any pipeline mode

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs automatically.
   In interactive mode the user can set the pipeline context defined parameters
   manually.  In 'getinputs' mode the user can check the settings of all
   pipeline parameters without running the task.
   default: 'automatic'.

---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- The list of input MeasurementSets. Defaults to the list of MeasurementSets
    specified in the pipeline context.
    default: ''
    example: ['M51.ms']

phaseupsolint -- The phase correction solution interval in CASA syntax.
    default: 'int'
    example: phaseupsolint='300s'

solint --  Time and channel solution intervals in CASA syntax.
    default: 'inf'
    example: solint='inf,10ch', solint='inf'

minsnr -- Solutions below this SNR are rejected.
    default: 2.0

refant -- Reference antenna names. Defaults to the value(s) stored in the
    pipeline context. If undefined in the pipeline context defaults to
    the CASA reference antenna naming scheme.
    default: ''
    example: refant='DV01', refant='DV06,DV07'

antnegsig -- Lower sigma threshold for identifying outliers as a result of bad
    antennas within individual timestamps.
    default: 4.0

antpossig -- Upper sigma threshold for identifying outliers as a result of bad
    antennas within individual timestamps.
    default: 4.6

tmantint -- Threshold for maximum fraction of timestamps that are allowed to
    contain outliers.
    default: 0.063

tmint -- Initial threshold for maximum fraction of "outlier timestamps" over
    "total timestamps" that a baseline may be a part of.
    default: 0.085

tmbl -- Initial threshold for maximum fraction of "bad baselines" over "all
    baselines" that an antenna may be a part of.
    default: 0.175

antblnegsig -- Lower sigma threshold for identifying outliers as a result of
    "bad baselines" and/or "bad antennas" within baselines, across all
    timestamps.
    default: 3.4

antblpossig -- Upper sigma threshold for identifying outliers as a result of
    "bad baselines" and/or "bad antennas" within baselines, across all
    timestamps.
    default: 3.2

relaxed_factor -- Relaxed value to set the threshold scaling factor to under
    certain conditions (see task description).
    default: 2.0

niter -- Maximum number of times to iterate on evaluation of flagging
    heuristics. If an iteration results in no new flags, then subsequent
    iterations are skipped.
    default: 2

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

1. run with recommended settings to create flux scale calibration with flagging
    using recommended thresholds:

    hifa_gfluxscaleflag()


        """
        if type(vis)==str: vis=[vis]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['phaseupsolint'] = phaseupsolint
        mytmp['solint'] = solint
        mytmp['minsnr'] = minsnr
        mytmp['refant'] = refant
        mytmp['antnegsig'] = antnegsig
        mytmp['antpossig'] = antpossig
        mytmp['tmantint'] = tmantint
        mytmp['tmint'] = tmint
        mytmp['tmbl'] = tmbl
        mytmp['antblnegsig'] = antblnegsig
        mytmp['antblpossig'] = antblpossig
        mytmp['relaxed_factor'] = relaxed_factor
        mytmp['niter'] = niter
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifa/cli/"
	trec = casac.utils().torecord(pathname+'hifa_gfluxscaleflag.xml')

        casalog.origin('hifa_gfluxscaleflag')
        if trec.has_key('hifa_gfluxscaleflag') and casac.utils().verify(mytmp, trec['hifa_gfluxscaleflag']) :
	    result = task_hifa_gfluxscaleflag.hifa_gfluxscaleflag(vis, phaseupsolint, solint, minsnr, refant, antnegsig, antpossig, tmantint, tmint, tmbl, antblnegsig, antblpossig, relaxed_factor, niter, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
