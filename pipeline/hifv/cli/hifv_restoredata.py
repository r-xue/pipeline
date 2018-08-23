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
import task_hifv_restoredata
def hifv_restoredata(vis=[''], session=[''], products_dir='../products', copytoraw=True, rawdata_dir='../rawdata', lazy=False, bdfflags=False, ocorr_mode='co', gainmap=False, asis='', pipelinemode='automatic', dryrun=False, acceptresults=True):

        """Restore flagged and calibration interferometry data from a pipeline run

The hifv_restoredata task restores flagged and calibrated MeasurementSets
from archived ASDMs and pipeline flagging and calibration date products.

Keyword arguments:

---- pipeline parameter arguments which can be set in any pipeline mode

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs automatically.
   In 'interactive' mode the user can set the pipeline context defined
   parameters manually.  In 'getinputs' mode the user can check the settings
   of all pipeline parameters without running the task.
   default: 'automatic'.

---- pipeline context defined parameter argument which can be set only in
'interactive mode'

vis -- List of raw visibility data files to be restored. Assumed to be
   in the directory specified by rawdata_dir.
   default: None
   example: vis=['uid___A002_X30a93d_X43e']

session -- List of sessions one per visibility file.
   default: []
   example: session=['session_3']

products_dir -- Name of the data products directory. Currently not
   used.
   default: '../products'
   example: products_dir='myproductspath'

rawdata_dir -- Name of the rawdata subdirectory.
   default: '../rawdata'
   example: rawdata_dir='myrawdatapath'

lazy -- Use the lazy filler option
   default: False
   example: lazy=True

bdfflags -- Set the BDF flags
   default: True
   example: bdfflags=False

ocorr_mode -- Set ocorr_mode
   default: 'ca'
   example: ocorr_mode='ca'

asis -- Set list of tables to import as is
   default: ''
   example: ocorr_mode='Source Receiver'


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

Description

The hifv_restoredata restores flagged and calibrated data from archived
ASDMs and pipeline flagging and calibration data products. Pending archive
retrieval support hifv_restoredata assumes that the required products
are available in the rawdata_dir in the format produced by the
h_exportdata task.

hifv_restoredata assumes that the following entities are available in the raw
data directory

o the ASDMs to be restored
o for each ASDM in the input list
   o a compressed tar file of the final flagversions file, e.g.
     uid___A002_X30a93d_X43e.ms.flagversions.tar.gz
   o a text file containing the applycal instructions, e.g.
     uid___A002_X30a93d_X43e.ms.calapply.txt
   o a compressed tar file containing the caltables for the parent session,
     e.g. uid___A001_X74_X29.session_3.caltables.tar.gz

h_restore data performs the following operations

o imports the ASDM(s))
o removes the default MS.flagversions directory created by the filler
o restores the final MS.flagversions directory stored by the pipeline
o restores the final set of pipeline flags to the MS
o restores the final calibration state of the MS
o restores the final calibration tables for each MS
o applies the calibration tables to each MS


Issues

Examples

1. Restore the pipeline results for a single ASDM in a single session

    hifv_restoredata (vis=['myVLAsdm'], session=['session_1'], ocorr_mode='ca')


        """
        if type(vis)==str: vis=[vis]
        if type(session)==str: session=[session]

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['session'] = session
        mytmp['products_dir'] = products_dir
        mytmp['copytoraw'] = copytoraw
        mytmp['rawdata_dir'] = rawdata_dir
        mytmp['lazy'] = lazy
        mytmp['bdfflags'] = bdfflags
        mytmp['ocorr_mode'] = ocorr_mode
        mytmp['gainmap'] = gainmap
        mytmp['asis'] = asis
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifv/cli/"
	trec = casac.utils().torecord(pathname+'hifv_restoredata.xml')

        casalog.origin('hifv_restoredata')
        if trec.has_key('hifv_restoredata') and casac.utils().verify(mytmp, trec['hifv_restoredata']) :
	    result = task_hifv_restoredata.hifv_restoredata(vis, session, products_dir, copytoraw, rawdata_dir, lazy, bdfflags, ocorr_mode, gainmap, asis, pipelinemode, dryrun, acceptresults)

	else :
	  result = False
        return result
