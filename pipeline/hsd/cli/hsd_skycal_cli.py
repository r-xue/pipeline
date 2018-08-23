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
from task_hsd_skycal import hsd_skycal
class hsd_skycal_cli_:
    __name__ = "hsd_skycal"
    rkey = None
    i_am_a_casapy_task = None
    # The existence of the i_am_a_casapy_task attribute allows help()
    # (and other) to treat casapy tasks as a special case.

    def __init__(self) :
       self.__bases__ = (hsd_skycal_cli_,)
       self.__doc__ = self.__call__.__doc__

       self.parameters={'calmode':None, 'fraction':None, 'noff':None, 'width':None, 'elongated':None, 'pipelinemode':None, 'infiles':None, 'field':None, 'spw':None, 'scan':None, 'dryrun':None, 'acceptresults':None, }


    def result(self, key=None):
	    #### and add any that have completed...
	    return None


    def __call__(self, calmode=None, fraction=None, noff=None, width=None, elongated=None, pipelinemode=None, infiles=None, field=None, spw=None, scan=None, dryrun=None, acceptresults=None, ):

        """Calibrate data

	Detailed Description:


	Arguments :
		calmode:	Calibration mode (default auto)
		   Default Value: auto
		   Allowed Values:
				auto
				ps
				otf
				otfraster

		fraction:	fraction of the OFF data to mark
		   Default Value: 10%

		noff:	number of the OFF data to mark
		   Default Value: -1

		width:	width of the pixel for edge detection
		   Default Value: 0.5

		elongated:	whether observed area is elongated in one direction or not
		   Default Value: False

		pipelinemode:	The pipeline operating mode
		   Default Value: automatic
		   Allowed Values:
				automatic
				interactive
				getinputs

		infiles:	List of input files to be calibrated (default all)
		   Default Value: 

		field:	Field to be calibrated (default all)
		   Default Value: 

		spw:	select data by IF IDs (spectral windows), e.g. \'3,5,7\' (\'\'=all)
		   Default Value: 

		scan:	select data by scan numbers, e.g. \'21~23\' (\'\'=all)
		   Default Value: 

		dryrun:	Run the task (False) or display task command (True)
		   Default Value: False

		acceptresults:	Add the results into the pipeline context
		   Default Value: True

	Returns: void

	Example :

The hsd_calsky task generates a caltable for sky calibration that  
stores reference spectra, which is to be subtracted from on-source 
spectra to filter out non-source contribution.

Keyword arguments:

---- pipeline parameter arguments which can be set in any pipeline mode

calmode -- Calibration mode. Available options are 'auto' (default), 
   'ps', 'otf', and 'otfraster'. When 'auto' is set, the task will 
   use preset calibration mode that is determined by inspecting data.
   'ps' mode is simple position switching using explicit reference 
   scans. Other two modes, 'otf' and 'otfraster', will generate 
   reference data from scans at the edge of the map. Those modes 
   are intended for OTF observation and the former is defined for 
   generic scanning pattern such as Lissajous, while the later is 
   specific use for raster scan.
   default: 'auto'
   options: 'auto', 'ps', 'otf', 'otfraster'

fraction -- Subparameter for calmode. Edge marking parameter for
   'otf' and 'otfraster' mode. It specifies a number of OFF scans
   as a fraction of total number of data points.
   default: '10%'
   options: String style like '20%', or float value less than 1.0.
            For 'otfraster' mode, you can also specify 'auto'.
            
noff -- Subparameter for calmode. Edge marking parameter for 'otfraster'
   mode. It is used to specify a number of OFF scans near edge directly
   instead to specify it by fractional number by 'fraction'. If it is
   set, the value will come before setting by 'fraction'.
   default: -1 (use setting by 'fraction')
   options: any positive integer value

width -- Subparameter for calmode. Edge marking parameter for 'otf'
   mode. It specifies pixel width with respect to a median spatial
   separation between neighboring two data in time. Default will
   be fine in most cases.
   default: 0.5
   options: any float value

elongated -- Subparameter for calmode. Edge marking parameter for
   'otf' mode. Please set True only if observed area is elongated
   in one direction.
   default: False

pipelinemode -- The pipeline operating mode. In 'automatic' mode the 
   pipeline determines the values of all context defined pipeline inputs
   automatically.  In 'interactive' mode the user can set the pipeline
   context defined parameters manually.  In 'getinputs' mode the user
   can check the settings of all pipeline parameters without running
   the task.
   default: 'automatic'.

---- pipeline context defined parameter argument which can be set only in
'interactive mode'

infiles -- List of data files. These must be a name of Scantables that 
   are registered to context via hsd_importdata task.
   default: []
   example: vis=['X227.ms', 'X228.ms']

field -- Data selection by field name.
   default: '' (all fields)

spw -- Data selection by spw.
   default: '' (all spws)
   example: '3,4' (generate caltable for spw 3 and 4)
            ['0','2'] (spw 0 for first data, 2 for second)

scan -- Data selection by scan number.
   default: '' (all scans)
   example: '22,23' (use scan 22 and 23 for calibration)
            ['22','24'] (scan 22 for first data, 24 for second)

pol -- Data selection by pol.
   default: '' (all polarizations)
   example: '0' (generate caltable for pol 0)
            ['0~1','0'] (pol 0 and 1 for first data, only 0 for second)

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


Examples

1. Generate caltables for all data managed by context.
   default(hsd_skycal)
   hsd_skycal()



        """
	if not hasattr(self, "__globals__") or self.__globals__ == None :
           self.__globals__=stack_frame_find( )
	#casac = self.__globals__['casac']
	casalog = self.__globals__['casalog']
	casa = self.__globals__['casa']
	#casalog = casac.casac.logsink()
        self.__globals__['__last_task'] = 'hsd_skycal'
        self.__globals__['taskname'] = 'hsd_skycal'
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

            myparams['calmode'] = calmode = self.parameters['calmode']
            myparams['fraction'] = fraction = self.parameters['fraction']
            myparams['noff'] = noff = self.parameters['noff']
            myparams['width'] = width = self.parameters['width']
            myparams['elongated'] = elongated = self.parameters['elongated']
            myparams['pipelinemode'] = pipelinemode = self.parameters['pipelinemode']
            myparams['infiles'] = infiles = self.parameters['infiles']
            myparams['field'] = field = self.parameters['field']
            myparams['spw'] = spw = self.parameters['spw']
            myparams['scan'] = scan = self.parameters['scan']
            myparams['dryrun'] = dryrun = self.parameters['dryrun']
            myparams['acceptresults'] = acceptresults = self.parameters['acceptresults']

        if type(infiles)==str: infiles=[infiles]

	result = None

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['calmode'] = calmode
        mytmp['fraction'] = fraction
        mytmp['noff'] = noff
        mytmp['width'] = width
        mytmp['elongated'] = elongated
        mytmp['pipelinemode'] = pipelinemode
        mytmp['infiles'] = infiles
        mytmp['field'] = field
        mytmp['spw'] = spw
        mytmp['scan'] = scan
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hsd/cli/"
	trec = casac.casac.utils().torecord(pathname+'hsd_skycal.xml')

        casalog.origin('hsd_skycal')
	try :
          #if not trec.has_key('hsd_skycal') or not casac.casac.utils().verify(mytmp, trec['hsd_skycal']) :
	    #return False

          casac.casac.utils().verify(mytmp, trec['hsd_skycal'], True)
          scriptstr=['']
          saveinputs = self.__globals__['saveinputs']
          if type(self.__call__.func_defaults) is NoneType:
              saveinputs=''
          else:
              saveinputs('hsd_skycal', 'hsd_skycal.last', myparams, self.__globals__,scriptstr=scriptstr)
          tname = 'hsd_skycal'
          spaces = ' '*(18-len(tname))
          casalog.post('\n##########################################'+
                       '\n##### Begin Task: ' + tname + spaces + ' #####')
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('Begin Task: ' + tname)
          if type(self.__call__.func_defaults) is NoneType:
              casalog.post(scriptstr[0]+'\n', 'INFO')
          else :
              casalog.post(scriptstr[1][1:]+'\n', 'INFO')
          result = hsd_skycal(calmode, fraction, noff, width, elongated, pipelinemode, infiles, field, spw, scan, dryrun, acceptresults)
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('End Task: ' + tname)
          casalog.post('##### End Task: ' + tname + '  ' + spaces + ' #####'+
                       '\n##########################################')

	except Exception, instance:
          if(self.__globals__.has_key('__rethrow_casa_exceptions') and self.__globals__['__rethrow_casa_exceptions']) :
             raise
          else :
             #print '**** Error **** ',instance
	     tname = 'hsd_skycal'
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
#        paramgui.runTask('hsd_skycal', myf['_ip'])
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
        a['calmode']  = 'auto'
        a['pipelinemode']  = 'automatic'

        a['calmode'] = {
                    0:{'value':'auto'}, 
                    1:{'value':'ps'}, 
                    2:odict([{'value':'otf'}, {'fraction':'10%'}, {'width':0.5}, {'elongated':False}]), 
                    3:odict([{'value':'otfraster'}, {'fraction':'10%'}, {'noff':-1}])}
        a['pipelinemode'] = {
                    0:{'value':'automatic'}, 
                    1:odict([{'value':'interactive'}, {'infiles':[]}, {'field':''}, {'spw':''}, {'scan':''}, {'pol':''}, {'dryrun':False}, {'acceptresults':True}]), 
                    2:odict([{'value':'getinputs'}, {'infiles':[]}, {'field':''}, {'spw':''}, {'scan':''}, {'pol':''}])}

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
    def description(self, key='hsd_skycal', subkey=None):
        desc={'hsd_skycal': 'Calibrate data',
               'calmode': 'Calibration mode (default auto)',
               'fraction': 'fraction of the OFF data to mark',
               'noff': 'number of the OFF data to mark',
               'width': 'width of the pixel for edge detection',
               'elongated': 'whether observed area is elongated in one direction or not',
               'pipelinemode': 'The pipeline operating mode',
               'infiles': 'List of input files to be calibrated (default all)',
               'field': 'Field to be calibrated (default all)',
               'spw': 'select data by IF IDs (spectral windows), e.g. \'3,5,7\' (\'\'=all)',
               'scan': 'select data by scan numbers, e.g. \'21~23\' (\'\'=all)',
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
        a['calmode']  = 'auto'
        a['fraction']  = '10%'
        a['noff']  = -1
        a['width']  = 0.5
        a['elongated']  = False
        a['pipelinemode']  = 'automatic'
        a['infiles']  = ['']
        a['field']  = ''
        a['spw']  = ''
        a['scan']  = ''
        a['dryrun']  = False
        a['acceptresults']  = True

        #a = sys._getframe(len(inspect.stack())-1).f_globals

        if self.parameters['calmode']  == 'otf':
            a['fraction'] = '10%'
            a['width'] = 0.5
            a['elongated'] = False

        if self.parameters['calmode']  == 'otfraster':
            a['fraction'] = '10%'
            a['noff'] = -1

        if self.parameters['pipelinemode']  == 'interactive':
            a['infiles'] = []
            a['field'] = ''
            a['spw'] = ''
            a['scan'] = ''
            a['pol'] = ''
            a['dryrun'] = False
            a['acceptresults'] = True

        if self.parameters['pipelinemode']  == 'getinputs':
            a['infiles'] = []
            a['field'] = ''
            a['spw'] = ''
            a['scan'] = ''
            a['pol'] = ''

        if a.has_key(paramname) :
	      return a[paramname]
hsd_skycal_cli = hsd_skycal_cli_()
