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
from task_hifv_flagdata import hifv_flagdata
class hifv_flagdata_cli_:
    __name__ = "hifv_flagdata"
    rkey = None
    i_am_a_casapy_task = None
    # The existence of the i_am_a_casapy_task attribute allows help()
    # (and other) to treat casapy tasks as a special case.

    def __init__(self) :
       self.__bases__ = (hifv_flagdata_cli_,)
       self.__doc__ = self.__call__.__doc__

       self.parameters={'vis':None, 'autocorr':None, 'shadow':None, 'scan':None, 'scannumber':None, 'quack':None, 'clip':None, 'baseband':None, 'intents':None, 'edgespw':None, 'fracspw':None, 'online':None, 'fileonline':None, 'template':None, 'filetemplate':None, 'hm_tbuff':None, 'tbuff':None, 'pipelinemode':None, 'flagbackup':None, 'dryrun':None, 'acceptresults':None, }


    def result(self, key=None):
	    #### and add any that have completed...
	    return None


    def __call__(self, vis=None, autocorr=None, shadow=None, scan=None, scannumber=None, quack=None, clip=None, baseband=None, intents=None, edgespw=None, fracspw=None, online=None, fileonline=None, template=None, filetemplate=None, hm_tbuff=None, tbuff=None, pipelinemode=None, flagbackup=None, dryrun=None, acceptresults=None, ):

        """Do basic deterministic flagging of a list of MeasurementSets
	Arguments :
		vis:	List of input MeasurementSets to flag
		   Default Value: 

		autocorr:	Flag autocorrelation data
		   Default Value: True

		shadow:	Flag shadowed antennas
		   Default Value: True

		scan:	Flag specified scans
		   Default Value: True

		scannumber:	List of scans to be flagged
		   Default Value: 

		quack:	Quack scans
		   Default Value: True

		clip:	Clip mode
		   Default Value: True

		baseband:	Flag 20MHz of each edge of basebands
		   Default Value: True

		intents:	List of intents of scans to be flagged
		   Default Value: *POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*, *UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*

		edgespw:	Flag edge channels
		   Default Value: True

		fracspw:	Fraction of baseline correlator edge channels to be flagged
		   Default Value: 0.05

		online:	Apply the online flags
		   Default Value: True

		fileonline:	File of online flags to be applied
		   Default Value: 

		template:	Apply a flagging template
		   Default Value: True

		filetemplate:	File that contains the flagging template
		   Default Value: 

		hm_tbuff:	The time buffer computation heuristic
		   Default Value: 1.5int
		   Allowed Values:
				1.5int
				halfint
				manual

		tbuff:	List of time buffers (sec) to pad timerange in flag commands
		   Default Value: 0.0

		pipelinemode:	The pipeline operating mode
		   Default Value: automatic
		   Allowed Values:
				automatic
				interactive
				getinputs

		flagbackup:	Backup pre-existing flags before applying new ones
		   Default Value: False

		dryrun:	Run the task (False) or display the command (True)
		   Default Value: False

		acceptresults:	Add the results into the pipeline context
		   Default Value: True

	Returns: void

	Example :

The hifv_flagdata data performs basic flagging operations on a list of
MeasurementSets.


Keyword arguments:

pipelinemode -- The pipeline operating mode. In 'automatic' mode the pipeline
   determines the values of all context defined pipeline inputs automatically.
   In interactive mode the user can set the pipeline context defined parameters
   manually.  In 'getinputs' mode the user can check the settings of all
   pipeline parameters without running the task.
   default: 'automatic'.


---- pipeline parameter arguments which can be set in any pipeline mode

autocorr -- Flag autocorrelation data.
    default: True

shadow -- Flag shadowed antennas.
    default: True

scan -- Flag a list of specified scans.
    default: True

scannumber -- A string containing a  comma delimited list of scans to be
    flagged.
    example: '3,5,6'
    default: ''

quack -- Quack scans
    default: True

clip -- Clip mode
    default: True

baseband -- Flag 20MHz of each edge of basebands
    default: True

intents -- A string containing a comma delimited list of intents against
    which the scans to be flagged are matched.
    example: '*BANDPASS*'
    default: '*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*'

edgespw -- Flag the edge spectral window channels.
    default: True

fracspw -- Fraction of the baseline correlator TDM edge channels to be flagged.
    default: 0.05



online -- Apply the online flags.
   default: True

fileonline -- File containing the online flags. These are computed by the
    h_init or hif_importdata data tasks. If the online flags files
    are undefined a name of the form 'msname.flagonline.txt' is assumed.
    default: ''

template -- Apply flagging templates
    default: False

filetemplate -- The name of an text file that contains the flagging template
    for RFI, birdies, telluric lines, etc.  If the template flags files
    is undefined a name of the form 'msname.flagtemplate.txt' is assumed.
    default: ''


---- pipeline context defined parameter arguments which can be set only in
'interactive mode'

vis -- The list of input MeasurementSets. Defaults to the list of MeasurementSets
    defined in the pipeline context.
    example:
    default: ''

flagbackup -- Back up any pre-existing flags.
       default: False

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

The hif_flagdata data performs basic flagging operations on a list of measurements including:

o autocorrelation data flagging
o shadowed antenna data flagging
o scan based flagging
o edge channel flagging
o baseband edge flagging
o applying online flags
o apply a flagging template
o quack, shadow, and basebands
o Antenna not-on-source (ANOS)

Issues


Examples

1. Do basic flagging on a MeasurementSet
    
    hifv_flagdata()

2. Do basic flagging on a MeasurementSet as well as flag pointing and
   atmosphere data

    hifv_flagdata(scan=True intent='*BANDPASS*')


        """
	if not hasattr(self, "__globals__") or self.__globals__ == None :
           self.__globals__=stack_frame_find( )
	#casac = self.__globals__['casac']
	casalog = self.__globals__['casalog']
	casa = self.__globals__['casa']
	#casalog = casac.casac.logsink()
        self.__globals__['__last_task'] = 'hifv_flagdata'
        self.__globals__['taskname'] = 'hifv_flagdata'
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
            myparams['autocorr'] = autocorr = self.parameters['autocorr']
            myparams['shadow'] = shadow = self.parameters['shadow']
            myparams['scan'] = scan = self.parameters['scan']
            myparams['scannumber'] = scannumber = self.parameters['scannumber']
            myparams['quack'] = quack = self.parameters['quack']
            myparams['clip'] = clip = self.parameters['clip']
            myparams['baseband'] = baseband = self.parameters['baseband']
            myparams['intents'] = intents = self.parameters['intents']
            myparams['edgespw'] = edgespw = self.parameters['edgespw']
            myparams['fracspw'] = fracspw = self.parameters['fracspw']
            myparams['online'] = online = self.parameters['online']
            myparams['fileonline'] = fileonline = self.parameters['fileonline']
            myparams['template'] = template = self.parameters['template']
            myparams['filetemplate'] = filetemplate = self.parameters['filetemplate']
            myparams['hm_tbuff'] = hm_tbuff = self.parameters['hm_tbuff']
            myparams['tbuff'] = tbuff = self.parameters['tbuff']
            myparams['pipelinemode'] = pipelinemode = self.parameters['pipelinemode']
            myparams['flagbackup'] = flagbackup = self.parameters['flagbackup']
            myparams['dryrun'] = dryrun = self.parameters['dryrun']
            myparams['acceptresults'] = acceptresults = self.parameters['acceptresults']

        if type(vis)==str: vis=[vis]
        if type(filetemplate)==str: filetemplate=[filetemplate]

	result = None

#
#    The following is work around to avoid a bug with current python translation
#
        mytmp = {}

        mytmp['vis'] = vis
        mytmp['autocorr'] = autocorr
        mytmp['shadow'] = shadow
        mytmp['scan'] = scan
        mytmp['scannumber'] = scannumber
        mytmp['quack'] = quack
        mytmp['clip'] = clip
        mytmp['baseband'] = baseband
        mytmp['intents'] = intents
        mytmp['edgespw'] = edgespw
        mytmp['fracspw'] = fracspw
        mytmp['online'] = online
        mytmp['fileonline'] = fileonline
        mytmp['template'] = template
        mytmp['filetemplate'] = filetemplate
        mytmp['hm_tbuff'] = hm_tbuff
        mytmp['tbuff'] = tbuff
        mytmp['pipelinemode'] = pipelinemode
        mytmp['flagbackup'] = flagbackup
        mytmp['dryrun'] = dryrun
        mytmp['acceptresults'] = acceptresults
	pathname="file:///Users/ksugimot/devel/eclipsedev/pipeline-trunk/pipeline/hifv/cli/"
	trec = casac.casac.utils().torecord(pathname+'hifv_flagdata.xml')

        casalog.origin('hifv_flagdata')
	try :
          #if not trec.has_key('hifv_flagdata') or not casac.casac.utils().verify(mytmp, trec['hifv_flagdata']) :
	    #return False

          casac.casac.utils().verify(mytmp, trec['hifv_flagdata'], True)
          scriptstr=['']
          saveinputs = self.__globals__['saveinputs']
          if type(self.__call__.func_defaults) is NoneType:
              saveinputs=''
          else:
              saveinputs('hifv_flagdata', 'hifv_flagdata.last', myparams, self.__globals__,scriptstr=scriptstr)
          tname = 'hifv_flagdata'
          spaces = ' '*(18-len(tname))
          casalog.post('\n##########################################'+
                       '\n##### Begin Task: ' + tname + spaces + ' #####')
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('Begin Task: ' + tname)
          if type(self.__call__.func_defaults) is NoneType:
              casalog.post(scriptstr[0]+'\n', 'INFO')
          else :
              casalog.post(scriptstr[1][1:]+'\n', 'INFO')
          result = hifv_flagdata(vis, autocorr, shadow, scan, scannumber, quack, clip, baseband, intents, edgespw, fracspw, online, fileonline, template, filetemplate, hm_tbuff, tbuff, pipelinemode, flagbackup, dryrun, acceptresults)
          if (casa['state']['telemetry-enabled']):
              casalog.poststat('End Task: ' + tname)
          casalog.post('##### End Task: ' + tname + '  ' + spaces + ' #####'+
                       '\n##########################################')

	except Exception, instance:
          if(self.__globals__.has_key('__rethrow_casa_exceptions') and self.__globals__['__rethrow_casa_exceptions']) :
             raise
          else :
             #print '**** Error **** ',instance
	     tname = 'hifv_flagdata'
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
#        paramgui.runTask('hifv_flagdata', myf['_ip'])
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
        a['autocorr']  = True
        a['shadow']  = True
        a['scan']  = True
        a['quack']  = True
        a['clip']  = True
        a['baseband']  = True
        a['edgespw']  = True
        a['online']  = True
        a['template']  = True
        a['hm_tbuff']  = '1.5int'
        a['pipelinemode']  = 'automatic'

        a['pipelinemode'] = {
                    0:{'value':'automatic'}, 
                    1:odict([{'value':'interactive'}, {'vis':[]}, {'flagbackup':False}, {'dryrun':False}, {'acceptresults':True}]), 
                    2:odict([{'value':'getinputs'}, {'vis':[]}, {'flagbackup':False}])}
        a['scan'] = {
                    0:odict([{'value':True}, {'scannumber':''}, {'intents':'*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*,*UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*'}]), 
                    1:{'value':False}}
        a['edgespw'] = {
                    0:odict([{'value':True}, {'fracspw':0.05}]), 
                    1:{'value':False}}
        a['online'] = {
                    0:odict([{'value':True}, {'fileonline':''}]), 
                    1:{'value':False}}
        a['template'] = {
                    0:odict([{'value':True}, {'filetemplate':[]}]), 
                    1:{'value':False}}
        a['hm_tbuff'] = {
                    0:{'value':'1.5int'}, 
                    1:{'value':'halfint'}, 
                    2:odict([{'value':'manual'}, {'tbuff':0.0}])}

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
    def description(self, key='hifv_flagdata', subkey=None):
        desc={'hifv_flagdata': 'Do basic deterministic flagging of a list of MeasurementSets',
               'vis': 'List of input MeasurementSets to flag',
               'autocorr': 'Flag autocorrelation data',
               'shadow': 'Flag shadowed antennas',
               'scan': 'Flag specified scans',
               'scannumber': 'List of scans to be flagged',
               'quack': 'Quack scans',
               'clip': 'Clip mode',
               'baseband': 'Flag 20MHz of each edge of basebands',
               'intents': 'List of intents of scans to be flagged',
               'edgespw': 'Flag edge channels',
               'fracspw': 'Fraction of baseline correlator edge channels to be flagged',
               'online': 'Apply the online flags',
               'fileonline': 'File of online flags to be applied',
               'template': 'Apply a flagging template',
               'filetemplate': 'File that contains the flagging template',
               'hm_tbuff': 'The time buffer computation heuristic',
               'tbuff': 'List of time buffers (sec) to pad timerange in flag commands',
               'pipelinemode': 'The pipeline operating mode',
               'flagbackup': 'Backup pre-existing flags before applying new ones',
               'dryrun': 'Run the task (False) or display the command (True)',
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
        a['autocorr']  = True
        a['shadow']  = True
        a['scan']  = True
        a['scannumber']  = ''
        a['quack']  = True
        a['clip']  = True
        a['baseband']  = True
        a['intents']  = '*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*, *UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*'
        a['edgespw']  = True
        a['fracspw']  = 0.05
        a['online']  = True
        a['fileonline']  = ''
        a['template']  = True
        a['filetemplate']  = ['']
        a['hm_tbuff']  = '1.5int'
        a['tbuff']  = 0.0
        a['pipelinemode']  = 'automatic'
        a['flagbackup']  = False
        a['dryrun']  = False
        a['acceptresults']  = True

        #a = sys._getframe(len(inspect.stack())-1).f_globals

        if self.parameters['pipelinemode']  == 'interactive':
            a['vis'] = []
            a['flagbackup'] = False
            a['dryrun'] = False
            a['acceptresults'] = True

        if self.parameters['pipelinemode']  == 'getinputs':
            a['vis'] = []
            a['flagbackup'] = False

        if self.parameters['scan']  == True:
            a['scannumber'] = ''
            a['intents'] = '*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*,*UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*'

        if self.parameters['edgespw']  == True:
            a['fracspw'] = 0.05

        if self.parameters['online']  == True:
            a['fileonline'] = ''

        if self.parameters['template']  == True:
            a['filetemplate'] = []

        if self.parameters['hm_tbuff']  == 'manual':
            a['tbuff'] = 0.0

        if a.has_key(paramname) :
	      return a[paramname]
hifv_flagdata_cli = hifv_flagdata_cli_()
