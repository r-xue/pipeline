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
import task_hsd_k2jycal
def hsd_k2jycal(reffile='jyperk.csv', pipelinemode='automatic', infiles=[''], caltable=[''], dryrun=False, acceptresults=True):

        """Derive Kelvin to Jy calibration tables

Derive the Kelvin to Jy calibration for list of MeasurementSets

Keyword arguments:

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs
   automatically.  In interactive mode the user can set the pipeline
   context defined parameters manually.  In 'getinputs' mode the user
   can check the settings of all pipeline parameters without running
   the task.
   default: 'automatic'.

reffile -- Path to a file containing Jy/K factors for science data, which 
    must be provided by associating calibrator reduction or the observatory 
    measurements. Jy/K factor must take into account all efficiencies, i.e.,
    it must be a direct conversion factor from Ta* to Jy. The file must be 
    in either MS-based or session-based format. The MS-based format must 
    be in an CSV format with five fields: MS name, antenna name, spectral 
    window id, polarization string, and Jy/K conversion factor. Example for 
    the file is as follows:
    
        MS,Antenna,Spwid,Polarization,Factor
        uid___A002_X316307_X6f.ms,CM03,5,XX,10.0
        uid___A002_X316307_X6f.ms,CM03,5,YY,12.0
        uid___A002_X316307_X6f.ms,PM04,5,XX,2.0
        uid___A002_X316307_X6f.ms,PM04,5,YY,5.0
        
    The first line in the above example is a header which may or may not 
    exist. Example for the session-based format is as follows:
    
        #OUSID=XXXXXX
        #OBJECT=Uranus
        #FLUXJY=yy,zz,aa
        #FLUXFREQ=YY,ZZ,AA
        #sessionID,ObservationStartDate(UTC),ObservationEndDate(UTC),Antenna,BandCenter(MHz),BandWidth(MHz),POL,Factor
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM02,86243.0,500.0,I,10.0
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM02,86243.0,1000.0,I,30.0
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM03,86243.0,500.0,I,50.0
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM03,86243.0,1000.0,I,70.0
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,ANONYMOUS,86243.0,500.0,I,30.0
        1,2011-11-11 01:00:00,2011-11-11 01:30:00,ANONYMOUS,86243.0,1000.0,I,50.0
        2,2011-11-13 01:45:00,2011-11-13 02:15:00,PM04,86243.0,500.0,I,90.0
        2,2011-11-13 01:45:00,2011-11-13 02:15:00,PM04,86243.0,1000.0,I,110.0
        2,2011-11-13 01:45:00,2011-11-13 02:15:00,ANONYMOUS,86243.0,500.0,I,90.0
        2,2011-11-13 01:45:00,2011-11-13 02:15:00,ANONYMOUS,86243.0,1000.0,I,110.0
        
    The line starting with '#' indicates a meta data section and header. 
    The header must exist. The factor to apply is identified by matching the
    session ID, antenna name, frequency and polarization of data in each line of
    the file. Note the observation date is supplementary information and not used 
    for the matching so far. The lines whose antenna name is 'ANONYMOUS' are used 
    when there is no measurement for specific antenna in the session. In the above 
    example, if science observation of session 1 contains the antenna PM04, Jy/K 
    factor for ANONYMOUS antenna will be applied since there is no measurement for 
    PM04 in session 1.
    If no file name is specified or specified file doesn't exist, all Jy/K factors 
    are set to 1.0. 
    default: 'jyperk.csv'
    example: '', 'working/jyperk.csv'

---- pipeline parameter arguments which can be set in any pipeline mode

---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

infiles -- List of input visibility files
    default: none; example: vis='ngc5921.ms'

caltable -- Name of output gain calibration tables
    default: none; example: caltable='ngc5921.gcal'

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

Derive the Kelvin to Jy calibration for list of MeasurementSets

Issues

Example

1. Compute the Kevin to Jy calibration tables for a list of MeasurementSets

    hsd_k2jycal()



        """
        if type(infiles)==str: infiles=[infiles]
        if type(caltable)==str: caltable=[caltable]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['reffile'] = reffile
        mytmp['pipelinemode'] = pipelinemode
        mytmp['infiles'] = infiles
        mytmp['caltable'] = caltable
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hsd/cli/"
	trec = casac.utils().torecord(pathname+'hsd_k2jycal.xml')

        casalog.origin('hsd_k2jycal')
        if trec.has_key('hsd_k2jycal') and casac.utils().verify(mytmp, trec['hsd_k2jycal']) :
	    result = task_hsd_k2jycal.hsd_k2jycal(reffile, pipelinemode, infiles, caltable, dryrun, acceptresults)

	else :
	  result = False
        return result
