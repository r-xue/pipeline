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
import task_hifa_spwphaseup
def hifa_spwphaseup(vis=[''], caltable=[''], field='', intent='', spw='', hm_spwmapmode='auto', maxnarrowbw='300MHz', minfracmaxbw=0.8, samebb=True, phasesnr=32.0, bwedgefrac=0.03125, hm_nantennas='all', maxfracflagged=0.90, combine='', refant='', minblperant=4, minsnr=3.0, pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Compute phase calibration spw map and per spw phase offsets

Compute the gain solutions.

---- pipeline parameter arguments which can be set in any pipeline mode

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
       determines the values of all context defined pipeline inputs
       automatically.  In interactive mode the user can set the pipeline
       context defined parameters manually.  In 'getinputs' mode the user
       can check the settings of all pipeline parameters without running
       the task.
       default: 'automatic'.

hm_spwmapmode -- The spectral window mapping mode. The options are: 'auto',
    'combine', 'simple', and 'default'. In 'auto' mode hifa_spwphaseup estimates
    the SNR of the phase calibrator observations and uses these estimates to
    choose between 'combine' mode (low SNR) and  'default' mode (high SNR). In
    combine mode all spectral windows are combined and mapped to one spectral
    window. In 'simple' mode narrow spectral windows are mapped to wider ones 
    sing an algorithm defined by 'maxnarrowbw', 'minfracmaxbw', and 'samebb'.
    In 'default' mode the spectral window map defaults to the standard one
    to one mapping.
    default: 'auto'
    example: hm_spwmapmode='combine' 

maxnarrowbw -- The maximum bandwidth defining narrow spectral windows. Values
    must be in CASA compatible frequency units.
    default: '300MHz'
    example: maxnarrowbw=''

minfracmaxbw -- The minimum fraction of the maximum bandwidth in the set of
    spws to use for matching.
    default: 0.8
    example: minfracmaxbw=0.75

samebb -- Match within the same baseband if possible ?
    default: True
    example: samebb=False

phasesnr -- The required gaincal solution signal to noise
    default: 32.0
    example: phaseupsnr = 20.0

bwedgefrac -- The fraction of the bandwidth edges that is flagged
    default: 0.03125
    example: bwedgefrac = 0.0

hm_nantennas -- The heuristics for determines the number of antennas to use
    in the signal to noise estimate. The options are 'all' and 'unflagged'.
    The 'unflagged' options is not currently supported.
    default: 'all'
    example: hm_nantennas='unflagged'

maxfracflagged -- The maximum fraction of an antenna that can be flagged
    before its is excluded from the signal to noise estimate.
    default: 0.90
    example: maxfracflagged=0.80


combine -- Data axes to combine for solving. Options are  '','scan','spw',field'
    or any comma-separated combination.
    default: ''
    example: combine=''

minblperant -- Minimum number of baselines required per antenna for each solve
    Antennas with fewer baselines are excluded from solutions. 
    default: 4
    example: minblperant=2

minsnr -- Solutions below this SNR are rejected. 
    default: 3.0

---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- The list of input MeasurementSets. Defaults to the list of MeasurementSets
    specified in the pipeline context
    default: ''
    example: ['M82A.ms', 'M82B.ms'] 

caltable -- The list of output calibration tables. Defaults to the standard
    pipeline naming convention.
    default: ''
    example: ['M82.gcal', 'M82B.gcal']

field -- The list of field names or field ids for which phase offset solutions are
    to be computed. Defaults to all fields with the default intent.
    default: '' 
    example: '3C279', '3C279, M82'

intent -- A string containing a comma delimited list of intents against
    which the the selected fields are matched. Defaults to the BANDPASS
    observations/
    default: '' 
    example:  intent='PHASE'

spw -- The list of spectral windows and channels for which gain solutions are
    computed. Defaults to all the science spectral windows.
    default: '' 
    example: '13,15'

refant -- Reference antenna name(s) in priority order. Defaults to most recent
    values set in the pipeline context.  If no reference antenna is defined in
    the pipeline context the CASA defaults are used.
    default: '' 
    example: refant='DV01', refant='DV05,DV07'

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

hif_spwphaseup performs tow functions
    o determines the spectral window mapping mode for the phase vs time
      calibrations and computes spectral window map that will be used to
      apply those calibrations
    o computes the per spectral window phase offset table that will be
      applied to the data to remove mean phase differences between
      the spectral windows

If hm_spwmapmode = 'auto' the spectral window map is computed using the
following algorithm

o estimate the per spectral window per scan signal to noise ratio of the phase
  calibrator observations
o if the signal to noise of any single phase calibration spectral window is less
  than the value of 'phasesnr' hm_spwmapmode defaults to 'combine'
o if all phase calibrator spectral windows meet the low  signal to noise criterion
  then hm_spwmapmode defaults to default'
o if the phase calibrator signal to noise values cannot be computed for any reason,
  for example there is no flux information, then hm_spwmapmode defaults to 'combine'

If hm_spwmapmode = 'combine' hifa_spwphaseup maps all the science windows to a single
science spectral window. For example if the list of science spectral windows is
[9, 11, 13, 15] then all the science spectral windows in the data will be combined and
mapped to the science window 9 in the combined phase vs time calibration table.

If hm_spwmapmode = 'simple', a mapping from narrow science to wider science
spectral windows is computed using the following algorithms:

o construct a list of the bandwidths of all the science spectral windows
o determine the maximum bandwidth in this list maxbandwidth
o for each science spectral window  with bandwidth less than maxbandwidth
    o construct a list of spectral windows with bandwidths greater than
      minfracmaxbw * maxbandwidth
    o select the spectral window in this list whose band center most closely
      matches the band center of the narrow spectral window
    o preferentially match within the same baseband if samebb is True


If hm_spwmapmode = 'default' the spw mapping is assumed to be one to one.

Phase offsets per spectral window are determined by computing a phase only gain calibration
on the selected data, normally the high signal to noise bandpass calibrator observations,
using the solution interval 'inf'.

At the end of the task the spectral window map and the phase offset calibration table
in the pipeline are stored in the  context for use by later tasks.


Examples

1. Compute the default spectral window map and the per spectral window phase offsets.

hif_spwphaseup()

2. Compute the default spectral window map and the per spectral window phase offsets
   set the spectral window mapping mode to 'simple'.

hif_spwphaseup(hm_spwmapmode='simple')


        """
        if type(vis)==str: vis=[vis]
        if type(caltable)==str: caltable=[caltable]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['caltable'] = caltable
        mytmp['field'] = field
        mytmp['intent'] = intent
        mytmp['spw'] = spw
        mytmp['hm_spwmapmode'] = hm_spwmapmode
        mytmp['maxnarrowbw'] = maxnarrowbw
        mytmp['minfracmaxbw'] = minfracmaxbw
        mytmp['samebb'] = samebb
        mytmp['phasesnr'] = phasesnr
        mytmp['bwedgefrac'] = bwedgefrac
        mytmp['hm_nantennas'] = hm_nantennas
        mytmp['maxfracflagged'] = maxfracflagged
        mytmp['combine'] = combine
        mytmp['refant'] = refant
        mytmp['minblperant'] = minblperant
        mytmp['minsnr'] = minsnr
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifa/cli/"
	trec = casac.utils().torecord(pathname+'hifa_spwphaseup.xml')

        casalog.origin('hifa_spwphaseup')
        if trec.has_key('hifa_spwphaseup') and casac.utils().verify(mytmp, trec['hifa_spwphaseup']) :
	    result = task_hifa_spwphaseup.hifa_spwphaseup(vis, caltable, field, intent, spw, hm_spwmapmode, maxnarrowbw, minfracmaxbw, samebb, phasesnr, bwedgefrac, hm_nantennas, maxfracflagged, combine, refant, minblperant, minsnr, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
