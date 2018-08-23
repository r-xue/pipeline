#
# This file was generated using xslt from its XML file
#
# Copyright 2014, Associated Universities Inc., Washington DC
#
import sys
import os
#from casac import *
import casac
import string
import time
import inspect
import gc
import numpy
from casa_stack_manip import stack_frame_find
from odict import odict
from types import *
from task_hsd_restoredata import hsd_restoredata
class hsd_restoredata_cli_:
    __name__ = "hsd_restoredata"
    rkey = None
    i_am_a_casapy_task = None
    # The existence of the i_am_a_casapy_task attribute allows help()
    # (and other) to treat casapy tasks as a special case.

    def __init__(self) :
       self.__bases__ = (hsd_restoredata_cli_,)
       self.__doc__ = self.__call__.__doc__

       self.parameters={'vis':None, 'session':None, 'products_dir':None, 'copytoraw':None, 'rawdata_dir':None, 'lazy':None, 'bdfflags':None, 'ocorr_mode':None, 'asis':None, 'pipelinemode':None, 'dryrun':None, 'acceptresults':None, }


    def result(self, key=None):
	    #### and add any that have completed...
	    return None


    def __call__(self, vis=None, session=None, products_dir=None, copytoraw=None, rawdata_dir=None, lazy=None, bdfflags=None, ocorr_mode=None, asis=None, pipelinemode=None, dryrun=None, acceptresults=None, ):

        """Restore flagged and calibration single-dish data from a pipeline run

	Detailed Description:


	Arguments :
		vis:	List of input visibility data
		   Default Value: 

		session:	List of sessions one per visibility file
		   Default Value: 

		products_dir:	The archived pipeline data products directory
		   Default Value: ../products

		copytoraw:	Copy calibration and flagging tables to raw data directory
		   Default Value: True

		rawdata_dir:	The rawdata directory
		   Default Value: ../rawdata

		lazy:	Use the lazy filler option
		   Default Value: False

		bdfflags:	Set the BDF flags
		   Default Value: True

		ocorr_mode:	Correlation import mode
		   Default Value: ao

		asis:	List of tables to import asis
		   Default Value: SBSummary ExecBlock Antenna Station Receiver Source CalAtmosphere CalWVR

		pipelinemode:	The pipeline operating mode
		   Default Value: automatic
		   Allowed Values:
				automatic
				interactive
				getinputs

		dryrun:	Run the task (False) or display task command (True)
		   Default Value: False

		acceptresults:	Add the results into the pipeline context
		   Default Value: True

	Returns: void

	Example :


The hsd_restoredata task restores flagged and calibrated MeasurementSets
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
   default: 'Antenna Station Receiver Source CalAtmosphere CalWVR'
   example: asis='Source Receiver'


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

The hsd_restoredata restores flagged and calibrated data from archived
ASDMs and pipeline flagging and calibration data products. Pending archive
retrieval support hsd_restoredata assumes that the required products
are available in the rawdata_dir in the format produced by the
hifa_exportdata task.

hsd_restoredata assumes that the following entities are available in the raw
data directory

o the ASDMs to be restored
o for each ASDM in the input list
   o a compressed tar file of the final flagversions file, e.g.  
     uid___A002_X30a93d_X43e.ms.flagversions.tar.gz
   o a text file containing the applycal instructions, e.g.
     uid___A002_X30a93d_X43e.ms.calapply.txt
   o a compressed tar file containing the caltables for the parent session,
     e.g. uid___A001_X74_X29.session_3.caltables.tar.gz

hsd_restore data performs the following operations

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

    hsd_restoredata (vis=['uid___A002_X30a93d_X43e'], session=['session_1'], ocorr_mode='ao')


        """
	if not hasattr(self, "__globals__") or self.__globals__ == None :
           self.__globals__=stack_frame_find( )
	#casac = self.__globals__['casac']
	casalog = self.__globals__['casalog']
	casa = self.__globals__['casa']
	#casalog = casac.casac.logsink()
        self.__globals__['__last_task'] = 'hsd_restoredata'
        self.__globals__['taskname'] = 'hsd_restoredata'
        ###
        self.__globals__['update_params'](func=self.__globals__['taskname'],printtext=False,ipython_globals=self.__globals__)
        ###
        ###
        #Handle globals or user over-ride of arguments
        #
        if type(self.__call__.func_defaults) is NoneType:
            function_signature_defaults={}
	else:
	    function_signature_defaults=dict(zip(self.__call__.func_code.co_varnames[1:],self.__call__.func_defaults))
	useLocalDefaults = False

        for item in function_signature_defaults.iteritems():
                key,val = item
                keyVal = eval(key)
                if (keyVal == None):
                        #user hasn't set it - use global/default
                        pass
                else:
                        #user has set it - use over-ride
			if (key != 'self') :
			   useLocalDefaults = True

	myparams = {}
	if useLocalDefaults :
	   for item in function_signature_defaults.iteritems():
	       key,val = item
	       keyVal = eval(key)
	       exec('myparams[key] = keyVal')
	       self.parameters[key] = keyVal
	       if (keyVal == None):
	           exec('myparams[key] = '+ key + ' = self.itsdefault(key)')
		   keyVal = eval(key)
		   if(type(keyVal) == dict) :
                      if len(keyVal) > 0 :
		         exec('myparams[key] = ' + key + ' = keyVal[len(keyVal)-1][\'value\']')
		      else :
		         exec('myparams[key] = ' + key + ' = {}')

        else :
            print ''

            myparams['vis'] = vis = self.parameters['vis']
            myparams['session'] = session = self.parameters['session']
            myparams['products_dir'] = products_dir = self.parameters['products_dir']
            myparams['copytoraw'] = copytoraw = self.parameters['copytoraw']
            myparams['rawdata_dir'] = rawdata_dir = self.parameters['rawdata_dir']
            myparams['lazy'] = lazy = self.parameters['lazy']
            myparams['bdfflags'] = bdfflags = self.parameters['bdfflags']
            myparams['ocorr_mode'] = ocorr_mode = self.parameters['ocorr_mode']
            myparams['asis'] = asis = self.parameters['asis']
            myparams['pipelinemode'] = pipelinemode = self.parameters['pipelinemode']
            myparams['dryrun'] = dryrun = self.parameters['dryrun']
            myparams['acceptresults'] = acceptresults = self.parameters['acceptresults']

        if type(vis)==str: vis=[vis]
        if type(session)==str: session=[session]

	result = None

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
        mytmp['asis'] = asis
        mytmp['pipelinemode'] = pipelinemode
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hsd/cli/"
	trec = casac.casac.utils().torecord(pathname+'hsd_restoredata.xml')

        casalog.origin('hsd_restoredata')
	try :
          #if not trec.has_key('hsd_restoredata') or not casac.casac.utils().verify(mytmp, trec['hsd_restoredata']) :
	    #return False

          casac.casac.utils().verify(mytmp, trec['hsd_restoredata'], True)
          scriptstr=['']
          saveinputs = self.__globals__['saveinputs']
          if type(self.__call__.func_defaults) is NoneType:
              saveinputs=''
          else:
              saveinputs('hsd_restoredata', 'hsd_restoredata.last', myparams, self.__globals__,scriptstr=scriptstr)
          tname = 'hsd_restoredata'
          spaces = ' '*(18-len(tname))
          casalog.post('\n##########################################'+
                       '\n##### Begin Task: ' + tname + spaces + ' #####')
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('Begin Task: ' + tname)
          if type(self.__call__.func_defaults) is NoneType:
              casalog.post(scriptstr[0]+'\n', 'INFO')
          else :
              casalog.post(scriptstr[1][1:]+'\n', 'INFO')
          result = hsd_restoredata(vis, session, products_dir, copytoraw, rawdata_dir, lazy, bdfflags, ocorr_mode, asis, pipelinemode, dryrun, acceptresults)
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('End Task: ' + tname)
          casalog.post('##### End Task: ' + tname + '  ' + spaces + ' #####'+
                       '\n##########################################')

	except Exception, instance:
          if(self.__globals__.has_key('__rethrow_casa_exceptions') and self.__globals__['__rethrow_casa_exceptions']) :
             raise
          else :
             #print '**** Error **** ',instance
	     tname = 'hsd_restoredata'
             casalog.post('An error occurred running task '+tname+'.', 'ERROR')
             pass
	casalog.origin('')

        gc.collect()
        return result
#
#
#
#    def paramgui(self, useGlobals=True, ipython_globals=None):
#        """
#        Opens a parameter GUI for this task.  If useGlobals is true, then any relevant global parameter settings are used.
#        """
#        import paramgui
#	if not hasattr(self, "__globals__") or self.__globals__ == None :
#           self.__globals__=stack_frame_find( )
#
#        if useGlobals:
#	    if ipython_globals == None:
#                myf=self.__globals__
#            else:
#                myf=ipython_globals
#
#            paramgui.setGlobals(myf)
#        else:
#            paramgui.setGlobals({})
#
#        paramgui.runTask('hsd_restoredata', myf['_ip'])
#        paramgui.setGlobals({})
#
#
#
#
    def defaults(self, param=None, ipython_globals=None, paramvalue=None, subparam=None):
	if not hasattr(self, "__globals__") or self.__globals__ == None :
           self.__globals__=stack_frame_find( )
        if ipython_globals == None:
            myf=self.__globals__
        else:
            myf=ipython_globals

        a = odict()
        a['vis']  = ['']
        a['session']  = ['']
        a['products_dir']  = '../products'
        a['copytoraw']  = True
        a['rawdata_dir']  = '../rawdata'
        a['lazy']  = False
        a['bdfflags']  = True
        a['ocorr_mode']  = 'ao'
        a['asis']  = 'SBSummary ExecBlock Antenna Station Receiver Source CalAtmosphere CalWVR'
        a['pipelinemode']  = 'automatic'

        a['pipelinemode'] = {
                    0:{'value':'automatic'}, 
                    1:odict([{'value':'interactive'}, {'dryrun':False}, {'acceptresults':True}]), 
                    2:{'value':'getinputs'}}

### This function sets the default values but also will return the list of
### parameters or the default value of a given parameter
        if(param == None):
                myf['__set_default_parameters'](a)
        elif(param == 'paramkeys'):
                return a.keys()
        else:
            if(paramvalue==None and subparam==None):
               if(a.has_key(param)):
                  return a[param]
               else:
                  return self.itsdefault(param)
            else:
               retval=a[param]
               if(type(a[param])==dict):
                  for k in range(len(a[param])):
                     valornotval='value'
                     if(a[param][k].has_key('notvalue')):
                        valornotval='notvalue'
                     if((a[param][k][valornotval])==paramvalue):
                        retval=a[param][k].copy()
                        retval.pop(valornotval)
                        if(subparam != None):
                           if(retval.has_key(subparam)):
                              retval=retval[subparam]
                           else:
                              retval=self.itsdefault(subparam)
		     else:
                        retval=self.itsdefault(subparam)
               return retval


#
#
    def check_params(self, param=None, value=None, ipython_globals=None):
      if ipython_globals == None:
          myf=self.__globals__
      else:
          myf=ipython_globals
#      print 'param:', param, 'value:', value
      try :
         if str(type(value)) != "<type 'instance'>" :
            value0 = value
            value = myf['cu'].expandparam(param, value)
            matchtype = False
            if(type(value) == numpy.ndarray):
               if(type(value) == type(value0)):
                  myf[param] = value.tolist()
               else:
                  #print 'value:', value, 'value0:', value0
                  #print 'type(value):', type(value), 'type(value0):', type(value0)
                  myf[param] = value0
                  if type(value0) != list :
                     matchtype = True
            else :
               myf[param] = value
            value = myf['cu'].verifyparam({param:value})
            if matchtype:
               value = False
      except Exception, instance:
         #ignore the exception and just return it unchecked
         myf[param] = value
      return value
#
#
    def description(self, key='hsd_restoredata', subkey=None):
        desc={'hsd_restoredata': 'Restore flagged and calibration single-dish data from a pipeline run',
               'vis': 'List of input visibility data',
               'session': 'List of sessions one per visibility file',
               'products_dir': 'The archived pipeline data products directory',
               'copytoraw': 'Copy calibration and flagging tables to raw data directory',
               'rawdata_dir': 'The rawdata directory',
               'lazy': 'Use the lazy filler option',
               'bdfflags': 'Set the BDF flags',
               'ocorr_mode': 'Correlation import mode',
               'asis': 'List of tables to import asis',
               'pipelinemode': 'The pipeline operating mode',
               'dryrun': 'Run the task (False) or display task command (True)',
               'acceptresults': 'Add the results into the pipeline context',

              }

#
# Set subfields defaults if needed
#

        if(desc.has_key(key)) :
           return desc[key]

    def itsdefault(self, paramname) :
        a = {}
        a['vis']  = ['']
        a['session']  = ['']
        a['products_dir']  = '../products'
        a['copytoraw']  = True
        a['rawdata_dir']  = '../rawdata'
        a['lazy']  = False
        a['bdfflags']  = True
        a['ocorr_mode']  = 'ao'
        a['asis']  = 'SBSummary ExecBlock Antenna Station Receiver Source CalAtmosphere CalWVR'
        a['pipelinemode']  = 'automatic'
        a['dryrun']  = False
        a['acceptresults']  = True

        #a = sys._getframe(len(inspect.stack())-1).f_globals

        if self.parameters['pipelinemode']  == 'interactive':
            a['dryrun'] = False
            a['acceptresults'] = True

        if a.has_key(paramname) :
	      return a[paramname]
hsd_restoredata_cli = hsd_restoredata_cli_()
