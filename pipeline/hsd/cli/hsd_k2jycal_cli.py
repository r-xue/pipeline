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
from task_hsd_k2jycal import hsd_k2jycal
class hsd_k2jycal_cli_:
    __name__ = "hsd_k2jycal"
    rkey = None
    i_am_a_casapy_task = None
    # The existence of the i_am_a_casapy_task attribute allows help()
    # (and other) to treat casapy tasks as a special case.

    def __init__(self) :
       self.__bases__ = (hsd_k2jycal_cli_,)
       self.__doc__ = self.__call__.__doc__

       self.parameters={'reffile':None, 'pipelinemode':None, 'infiles':None, 'caltable':None, 'dryrun':None, 'acceptresults':None, }


    def result(self, key=None):
	    #### and add any that have completed...
	    return None


    def __call__(self, reffile=None, pipelinemode=None, infiles=None, caltable=None, dryrun=None, acceptresults=None, ):

        """Derive Kelvin to Jy calibration tables

	Detailed Description:

Derive the Kelvin to Jy calibration for list of MeasurementSets

	Arguments :
		reffile:	File of Jy/K conversion factor
		   Default Value: jyperk.csv

		pipelinemode:	The pipeline operations mode
		   Default Value: automatic
		   Allowed Values:
				automatic
				interactive
				getinputs

		infiles:	List of input MeasurementSets
		   Default Value: 

		caltable:	List of output caltable(s)
		   Default Value: 

		dryrun:	Run the task (False) or list commands(True)
		   Default Value: False

		acceptresults:	Automatically apply results to context
		   Default Value: True

	Returns: void

	Example :


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
	if not hasattr(self, "__globals__") or self.__globals__ == None :
           self.__globals__=stack_frame_find( )
	#casac = self.__globals__['casac']
	casalog = self.__globals__['casalog']
	casa = self.__globals__['casa']
	#casalog = casac.casac.logsink()
        self.__globals__['__last_task'] = 'hsd_k2jycal'
        self.__globals__['taskname'] = 'hsd_k2jycal'
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

            myparams['reffile'] = reffile = self.parameters['reffile']
            myparams['pipelinemode'] = pipelinemode = self.parameters['pipelinemode']
            myparams['infiles'] = infiles = self.parameters['infiles']
            myparams['caltable'] = caltable = self.parameters['caltable']
            myparams['dryrun'] = dryrun = self.parameters['dryrun']
            myparams['acceptresults'] = acceptresults = self.parameters['acceptresults']

        if type(infiles)==str: infiles=[infiles]
        if type(caltable)==str: caltable=[caltable]

	result = None

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
	trec = casac.casac.utils().torecord(pathname+'hsd_k2jycal.xml')

        casalog.origin('hsd_k2jycal')
	try :
          #if not trec.has_key('hsd_k2jycal') or not casac.casac.utils().verify(mytmp, trec['hsd_k2jycal']) :
	    #return False

          casac.casac.utils().verify(mytmp, trec['hsd_k2jycal'], True)
          scriptstr=['']
          saveinputs = self.__globals__['saveinputs']
          if type(self.__call__.func_defaults) is NoneType:
              saveinputs=''
          else:
              saveinputs('hsd_k2jycal', 'hsd_k2jycal.last', myparams, self.__globals__,scriptstr=scriptstr)
          tname = 'hsd_k2jycal'
          spaces = ' '*(18-len(tname))
          casalog.post('\n##########################################'+
                       '\n##### Begin Task: ' + tname + spaces + ' #####')
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('Begin Task: ' + tname)
          if type(self.__call__.func_defaults) is NoneType:
              casalog.post(scriptstr[0]+'\n', 'INFO')
          else :
              casalog.post(scriptstr[1][1:]+'\n', 'INFO')
          result = hsd_k2jycal(reffile, pipelinemode, infiles, caltable, dryrun, acceptresults)
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('End Task: ' + tname)
          casalog.post('##### End Task: ' + tname + '  ' + spaces + ' #####'+
                       '\n##########################################')

	except Exception, instance:
          if(self.__globals__.has_key('__rethrow_casa_exceptions') and self.__globals__['__rethrow_casa_exceptions']) :
             raise
          else :
             #print '**** Error **** ',instance
	     tname = 'hsd_k2jycal'
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
#        paramgui.runTask('hsd_k2jycal', myf['_ip'])
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
        a['reffile']  = 'jyperk.csv'
        a['pipelinemode']  = 'automatic'

        a['pipelinemode'] = {
                    0:{'value':'automatic'}, 
                    1:odict([{'value':'interactive'}, {'infiles':[]}, {'caltable':[]}, {'dryrun':False}, {'acceptresults':True}]), 
                    2:odict([{'value':'getinputs'}, {'vis':[]}, {'caltable':[]}])}

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
    def description(self, key='hsd_k2jycal', subkey=None):
        desc={'hsd_k2jycal': 'Derive Kelvin to Jy calibration tables',
               'reffile': 'File of Jy/K conversion factor',
               'pipelinemode': 'The pipeline operations mode',
               'infiles': 'List of input MeasurementSets',
               'caltable': 'List of output caltable(s)',
               'dryrun': 'Run the task (False) or list commands(True)',
               'acceptresults': 'Automatically apply results to context',

              }

#
# Set subfields defaults if needed
#

        if(desc.has_key(key)) :
           return desc[key]

    def itsdefault(self, paramname) :
        a = {}
        a['reffile']  = 'jyperk.csv'
        a['pipelinemode']  = 'automatic'
        a['infiles']  = ['']
        a['caltable']  = ['']
        a['dryrun']  = False
        a['acceptresults']  = True

        #a = sys._getframe(len(inspect.stack())-1).f_globals

        if self.parameters['pipelinemode']  == 'interactive':
            a['infiles'] = []
            a['caltable'] = []
            a['dryrun'] = False
            a['acceptresults'] = True

        if self.parameters['pipelinemode']  == 'getinputs':
            a['vis'] = []
            a['caltable'] = []

        if a.has_key(paramname) :
	      return a[paramname]
hsd_k2jycal_cli = hsd_k2jycal_cli_()
