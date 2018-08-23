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
import task_hifa_wvrgcal
def hifa_wvrgcal(vis=[''], caltable=[''], offsetstable=[''], hm_toffset='automatic', toffset=0, segsource=True, sourceflag=[''], hm_tie='automatic', tie=[''], nsol=1, disperse=False, wvrflag=[''], hm_smooth='automatic', smooth='', scale=1., maxdistm=-1, minnumants=2, mingoodfrac=0.8, refant='', qa_intent='', qa_bandpass_intent='', qa_spw='', accept_threshold=1.0, pipelinemode='automatic', dryrun=False, acceptresults=True):

        """

Generate a gain table based on the Water Vapour Radiometer data in each vis
file. By applying the wvr calibration to the data specified by 'qa_intent' and
'qa_spw', calculate a QA score to indicate its effect on interferometric data;
a score > 1 implies that the phase noise is improved, a score < 1 implies
that it is made worse. If the score is less than 'accept_threshold' then the 
wvr gain table is not accepted into the context for subsequent use.
  
vis -- List of input visibility files
    default: none, in which case the vis files to be used will be read
             from the context. 
    example: vis=['ngc5921.ms']

caltable -- List of output gain calibration tables
    default: none, in which case the names of the caltables will be 
             generated automatically.
    example: caltable='ngc5921.wvr'

offsetstable -- List of input temperature offsets table files to subtract from
                WVR measurements before calculating phase corrections.
    default: none, in which case no offsets are applied.
    example: offsetstable=['ngc5921.cloud_offsets']

hm_toffset -- If 'manual', set the 'toffset' parameter to the user-specified value.
          If 'automatic', set the 'toffset' parameter according to the 
          date of the MeasurementSet; toffset=-1 if before 2013-01-21T00:00:00
          toffset=0 otherwise.
    default: 'automatic'

toffset -- Time offset (sec) between interferometric and WVR data
    default: 0

segsource -- If True calculate new atmospheric phase correction 
             coefficients for each source, subject to the constraints of 
             the 'tie' parameter. 'segsource' is forced to be True if 
             the 'tie' parameter is set to a non-empty value by the 
             user or by the automatic heuristic.
    default: True

hm_tie -- If 'manual', set the 'tie' parameter to the user-specified value.
          If 'automatic', set the 'tie' parameter to include with the
          target all calibrators that are within 15 degrees of it: 
          if no calibrators are that close then 'tie' is left empty.
    default: 'automatic'

tie -- Use the same atmospheric phase correction coefficients when 
       calculating the wvr correction for all sources in the 'tie'. If 'tie' 
       is not empty then 'segsource' is forced to be True. Ignored unless
       hm_tie='manual'.
    default: []
    example: ['3C273,NGC253', 'IC433,3C279']

sourceflag -- Flag the WVR data for these source(s) as bad and do not produce
    corrections for it. Requires segsource=True.
    default: []
    example: ['3C273']

nsol -- Number of solutions for phase correction coefficients during this
    observation, evenly distributed in time throughout the observation. It
    is used only if segsource=False because if segsource=True then the
    coefficients are recomputed whenever the telescope moves to a new source
    (within the limits imposed by 'tie').
    default: 1

disperse -- Apply correction for dispersion
    default: False

wvrflag -- Flag the WVR data for the listed antennas as bad and replace 
    their data with values interpolated from the 3 nearest antennas with
    unflagged data.
    default: []
    example: ['DV03','DA05','PM02']           

hm_smooth -- If 'manual' set the 'smooth' parameter to the user-specified value.
    If 'automatic', run the wvrgcal task with the range of 'smooth' parameters
    required to match the integration time of the wvr data to that of the
    interferometric data in each spectral window.

smooth -- Smooth WVR data on this timescale before calculating the correction.
    Ignored unless hm_smooth='manual'.
    default: ''

scale -- Scale the entire phase correction by this factor.
    default: 1

maxdistm -- Maximum distance in meters of an antenna used for interpolation
    from a flagged antenna.

    default: -1  (automatically set to 100m if >50% of antennas are 7m
        antennas without WVR and otherwise set to 500m)

    example: 550

minnumants -- Minimum number of nearby antennas (up to 3) used for
    interpolation from a flagged antenna.
    default: 2
    example: 3

mingoodfrac -- Minimum fraction of good data per  antenna
    default: 0.8

refant -- Ranked comma delimited list of reference antennas.
    default: ''
    example 'DV01,DV02'

qa_intent -- The list of data intents on which the wvr correction is to be 
    tried as a means of estimating its effectiveness.

    A QA 'view' will be calculated for each specified intent, in each spectral
    window in each vis file.

    Each QA 'view' will consist of a pair of 2-d images with dimensions
    ['ANTENNA', 'TIME'], one showing the data phase-noise before the 
    wvr application, the second showing the phase noise after (both 'before'
    and 'after' images have a bandpass calibration applied as well).

    An overall QA score is calculated for each vis file, by dividing the
    'before' images by the 'after' and taking the median of the result. An
    overall score of 1 would correspond to no change in the phase noise,
    a score > 1 implies an improvement.

    If the overall score for a vis file is less than the value in          
    'accept_threshold' then the wvr calibration file is not made available 
    for merging into the context for use in the subsequent reduction.

    If you do not want any QA calculations then set qa_intent=''.

    default: ''
    example: 'PHASE'

qa_bandpass_intent -- The data intent to use for the bandpass calibration
    in the qa calculation. The default is blank to allow the 
    underlying bandpass task to select a sensible intent if the dataset
    lacks BANDPASS data.

    default: ''

qa_spw -- The SpW(s) to use for the qa calculation, in the order
    that they should be tried. Input as a comma-separated list.
    The default is blank, in which case the task will try SpWs
    in order of decreasing median sky opacity.

    default: ''

accept_threshold -- The phase-rms improvement ratio
    (rms without wvr / rms with wvr) above which the wrvg file will be
    accepted into the context for subsequent application.

    default: 1.0


Example

1. Compute the WVR calibration for all the MeasurementSets.

    hifa_wvrgcal (hm_tie='automatic')


        """
        if type(vis)==str: vis=[vis]
        if type(caltable)==str: caltable=[caltable]
        if type(offsetstable)==str: offsetstable=[offsetstable]
        if type(sourceflag)==str: sourceflag=[sourceflag]
        if type(tie)==str: tie=[tie]
        if type(wvrflag)==str: wvrflag=[wvrflag]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['caltable'] = caltable
        mytmp['offsetstable'] = offsetstable
        mytmp['hm_toffset'] = hm_toffset
        mytmp['toffset'] = toffset
        mytmp['segsource'] = segsource
        mytmp['sourceflag'] = sourceflag
        mytmp['hm_tie'] = hm_tie
        mytmp['tie'] = tie
        mytmp['nsol'] = nsol
        mytmp['disperse'] = disperse
        mytmp['wvrflag'] = wvrflag
        mytmp['hm_smooth'] = hm_smooth
        mytmp['smooth'] = smooth
        mytmp['scale'] = scale
        mytmp['maxdistm'] = maxdistm
        mytmp['minnumants'] = minnumants
        mytmp['mingoodfrac'] = mingoodfrac
        mytmp['refant'] = refant
        mytmp['qa_intent'] = qa_intent
        mytmp['qa_bandpass_intent'] = qa_bandpass_intent
        mytmp['qa_spw'] = qa_spw
        mytmp['accept_threshold'] = accept_threshold
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifa/cli/"
	trec = casac.utils().torecord(pathname+'hifa_wvrgcal.xml')

        casalog.origin('hifa_wvrgcal')
        if trec.has_key('hifa_wvrgcal') and casac.utils().verify(mytmp, trec['hifa_wvrgcal']) :
	    result = task_hifa_wvrgcal.hifa_wvrgcal(vis, caltable, offsetstable, hm_toffset, toffset, segsource, sourceflag, hm_tie, tie, nsol, disperse, wvrflag, hm_smooth, smooth, scale, maxdistm, minnumants, mingoodfrac, refant, qa_intent, qa_bandpass_intent, qa_spw, accept_threshold, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
