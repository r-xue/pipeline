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
import task_hifa_antpos
def hifa_antpos(vis=[''], caltable=[''], hm_antpos='file', antenna='', offsets=[], antposfile='', pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Derive an antenna position calibration table

Keyword arguments:

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context dependent pipeline inputs automatically.
   In interactive mode the user can set the pipeline context defined parameters
   manually.  In 'getinputs' mode the user can check the settings of all
   pipeline parameters without running the task.
   default: 'automatic'.

---- pipeline parameter arguments which can be set in any pipeline mode

hm_antpos -- Heuristics method for retrieving the antenna position
    corrections. The options are 'online' (not yet implemented), 'manual',
    and 'file'.
    default: 'file'
    example: hm_antpos='manual'

antenna -- The list of antennas for which the positions are to be corrected
    if hm_antpos is 'manual'
    default: none
    example 'DV05,DV07'

offsets -- The list of antenna offsets for each antenna in 'antennas'. Each
    offset is a set of 3 floating point numbers separated by commas, specified
    in the ITRF frame.
    default: none
    example: [0.01, 0.02, 0.03, 0.03, 0.02, 0.01] 

antposfile -- The file(s) containing the antenna offsets. Used if hm_antpos
    is 'file'. The default file name is 'antennapos.csv'

---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- List of input visibility files
    default: []
    example: ['ngc5921.ms']

caltable -- Name of output gain calibration tables
    default: []
    example: caltable=['ngc5921.gcal']

-- Pipeline task execution modes

dryrun -- Run the commands (True) or generate the commands to be run but
   do not execute (False).
   default: True

acceptresults -- Add the results of the task to the pipeline context (True) or
   reject them (False).
   default: True

Output:

results -- If pipeline mode is 'getinputs' then None is returned. Otherwise
    the results object for the pipeline task is returned.


Description

The hifa_antpos task corrects the antenna positions recorded in the ASDMs using
updated antenna position calibration information determined after the
observation was taken.

Corrections can be input by hand, read from a file on disk, or in future
by querying an ALMA database service.

The antenna positions file is in 'csv' format containing 6 comma delimited
columns as shown below. The default name of this file is 'antennapos.csv'

List if sample antennapos.csv file

ms,antenna,xoffset,yoffset,zoffset,comment
uid___A002_X30a93d_X43e.ms,DV11,0.000,0.010,0.000,"No comment"
uid___A002_X30a93d_X43e.dup.ms,DV11,0.000,-0.010,0.000,"No comment"

The corrections are used to generate a calibration table which is recorded
in the pipeline context and applied to the raw visibility data, on the fly to
generate other calibration tables, or permanently to generate calibrated
visibilities for imaging.


Issues

The hm_antpos 'online' option will be  implemented when the observing system
provides an antenna position determination service.


Example

1. Correct the position of antenna 5 for all the visibility files in a single
pipeline run.  

    hifa_antpos (antenna='DV05', offsets=[0.01, 0.02, 0.03])

2. Correct the position of antennas for all the visibility files in a single
pipeline run using antenna positions files on disk. These files are assumed
to conform to a default naming scheme if 'antposfile' is unspecified by the
user.

    hifa_antpos (hm_antpos='myantposfile.csv')


        """
        if type(vis)==str: vis=[vis]
        if type(caltable)==str: caltable=[caltable]
        if type(offsets)==float: offsets=[offsets]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['caltable'] = caltable
        mytmp['hm_antpos'] = hm_antpos
        mytmp['antenna'] = antenna
        mytmp['offsets'] = offsets
        mytmp['antposfile'] = antposfile
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifa/cli/"
	trec = casac.utils().torecord(pathname+'hifa_antpos.xml')

        casalog.origin('hifa_antpos')
        if trec.has_key('hifa_antpos') and casac.utils().verify(mytmp, trec['hifa_antpos']) :
	    result = task_hifa_antpos.hifa_antpos(vis, caltable, hm_antpos, antenna, offsets, antposfile, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
