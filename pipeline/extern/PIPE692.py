# L T Maud - April 2020 0RIG
#          - May 2021 full CASA 6 version mockup
#          - March 2022 CASA 6.4 'class' version for PL 


# NOTES for developers

## I have recoded everything that was completed for the PIPEREQ-121 (CASR-422)
## and then subsequently presented and tested as a mock-up in PIPE-692
## - this _hopefully_ makes this code this easy to integrate

## As I do not fully understand all the various links within PL tasks/functs/classes
## I hope this single class format is at least easy to understand 


# *******************************************
# I have tested this inside an active PL run in the call inside spwphaseup:
#
# 1) add import at the top if THIS code is in 'extern' folder
#   
# from pipeline.extern.PIPE692 import SSFanalysis   # not yet written as a class
#
# 2) In the class "SpwPhaseup" at the "prepare" function 
#    add a try/except statement to run the PIPE692 code
#
# a) in the try:
#
#  try:
#      LOG.info(" The Pipe 692 class is running for phase RMS.")
#      LOG.info(" New check testing - March 2022 - L Maud ")
#      pipe692 = SSFanalysis(inputs, outlierlimit=100., ftoll=0.3, maxpoorant=11)
#                                #  pass the inputs from spwphaseup and options (these would be as PL inputs task call)
#
#      pipe692.analysis()        # launches the analysis
#
#      resul = pipe692.score()   # get the results dictionary
#                                # keys are
#                                # basescore, basecolor,shortmsg, longmsg
#
#      LOG.info('This is the phase RMS score '+str(resul['basescore']))
#
#      pipe692.plotSSF() # makes the SSF plot that would be included in the Weblog
#
#      ### optional for full PL weblog page plot - this is not needed for actual PL implementation ###
#      pipe692.plotSSF_mockup(resul) # TESTING pass the results now - triggers mock-up figure options - i.e. a weblog page 
#
#      pipe692.close()
#      
# b) for the exception
#
#  except Exception as e:
#      LOG.error(' Unable to compute Phase RMS Pipe692')
#
#
# *******************************************

import sys
import os
import re
import numpy as np
import glob
try:
    from taskinit import tbtool,msmdtool,qatool,attool, mstool, casalog, metool
except:
    from casatools import table as tbtool
    from casatools import msmetadata as msmdtool
    from casatools import quanta as qatool
    from casatools import atmosphere as attool
    from casatools import ms as mstool
    from casatasks import casalog 
    from casatools import measures as metool

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

mytb=tbtool()
myms=mstool()
mymsmd=msmdtool()

class SSFanalysis(object):
    def __init__(self,inputsin, outlierlimit, ftoll, maxpoorant):   

        # the only gets all inputs needed throughout the class
        print('INITILIZING')

        self.visUse=inputsin.ms.basename
        self.outlierlimit = outlierlimit  ## NOTE TO DEV this (could be) an input variable to the task
        self.ftoll = ftoll                ## NOTE TO DEV this (could be) an input variable to the task
        self.maxpoorant = maxpoorant      ## NOTE TO DEV this (could be) an input - pass defualt as 11 after Pipe692 testing

        # NOTE TO DEV - this can come from context but I do not know how - its a caltable from the bandpass stage
        # here I have to have the long if/else check in order to find any caltables that are averaged
        self.caltable=[]
        self.caltable = glob.glob(self.visUse+'*bandpass.s*intint.gpcal.tbl')
        if len(self.caltable)>0:
            self.caltable=self.caltable[0]
        else:
            self.caltable = glob.glob(self.visUse+'*bandpass.s*int*s.gpcal.tbl')
            if len(self.caltable)>0:
                self.caltable=self.caltable[0]
            else:
                ## should not get here, and in real PL will have correct caltable input
                print('ERROR NO CALTABLE TO ASSESS')
                raise

        
        self.refant = inputsin.refant.split(',')[0] # this is the NAME not the index
        self.antlist = []
        # loop ant list for id of refant
        for elma in inputsin.ms.antennas:
            if elma.name == self.refant:
                self.refantid=elma.id
            self.antlist.append(elma.name)
        # store all ant info for baselines - call later to get lengths
        self.baselines=inputsin.ms.antenna_array

        scispws = inputsin.ms.get_spectral_windows(task_arg=inputsin.spw, science_windows_only=True)
        # Find the maximum science spw bandwidth for each science receiver band.
        bwmaxdict = {}
        for scispw in scispws:
            bandwidth = scispw.bandwidth
            if scispw.band in bwmaxdict:
                if bandwidth > bwmaxdict[scispw.band]:
                    bwmaxdict[scispw.band] = bandwidth
            else:
                bwmaxdict[scispw.band] = bandwidth

        # NOTE to DEV - this gets widest SPW, but would preferably use SPW with highest SNR - I don't know how/where to get that in context
        for scispw in scispws:
            if scispw.band in bwmaxdict and scispw.bandwidth == bwmaxdict[scispw.band]:
                self.spwUse=scispw.id
                break


        # now I need the scan for the bandpass 
        # NOTE TO DEV - this intent is 'BANDPASS' passed from spwphaseup - but could also hard code if needed to be explicit here?
        targeted_scans = inputsin.ms.get_scans(scan_intent=inputsin.intent, spw=str(self.spwUse))
        bp_scan=[]
        bp_field = []
        for elm in targeted_scans:
            bp_scan.append(elm.id) # take the ID
            for elms in elm.fields:
                bp_field.append(elms.name)  # fields is a dict again...
            
        self.scanUse = bp_scan[0] # we only want the first scan, in case there are multiple for the BP (e.g. spectral scan data - NEEDS TESTING ON)
        self.fieldUse = bp_field[0]

        # run my function to check if the data are ACA or not (maybe in Context? but this is an easy check)
        self.PMinACA = self.pmInACA()
 
        # run my converted function from Todd's aU.cycletime
        self.cycletime = self.getcycletime()

        # run my function to get the time/length of the bandpass scan
        alltime = self.getTime() # no inputs needed within the class (uses self.caltab, self.spwUse, self.scanUse)
        # NOTE TO DEV - getTime will return 0 if there is an issue - should never happen as input caltable comes
        # from the bandpass intint solns, hence should be good. But maybe we need a fall-back coded just in case?
        self.totaltime = alltime[-1]
        self.difftime = np.median(np.diff(alltime))        

        # set holder for outlier antennas related to plotting and scores later
        self.antout = []

        # setup the main analysis dictionary
        self.allResult={}

        ## NOTE TO DEV - Print to logger some nice information too ? Like below?
        print('')
        print(' ***************************')
        print(' Working on the MS '+str(self.visUse))
        print(' Selected scan',self.scanUse)
        print(' Selected spw',self.spwUse)
        print(' Using field', self.fieldUse)
        print(' Using caltab', self.caltable)
        print(' The refant is', self.refant, ' id= ', self.refantid)
        print(' Is it ACA with PM data? ', self.PMinACA)
        print(' Total BP scan time ', self.totaltime)
        print(' Phase referencing cycle time ', self.cycletime)
        print(' The median integration time ', self.difftime)
        print(' ***************************')
        print('')


    def close(self):
        # reset all variables from above
        # NOTE TO DEV please check this is sensible as they reset upon closing 
        # I somewhat assume anyway the code would not live as a 'extern' package anyway
        # so this could be redundant 
        self.visUse = None
        self.outlierlimit = None
        self.ftoll = None
        self.maxpoorant = None
        self.scanUse = None
        self.spwUse = None
        self.fieldUse = None
        self.caltable = None
        self.refant = None
        self.refantid = None 
        self.totaltime = None
        self.difftime = None
        self.cycletime = None
        self.PMinACA=None
        self.baselines=None
        self.antlist=[]
        self.antout=[]
        self.allResult={}
        print('Closing PIPE 692 ')


    # Main function to do the phase RMS calculation, and outlier analysis 
    # to report back the phase to use for scoring and plotting

    def analysis(self):
        ''' this is the wrapper to do all the calculation and fill
        the dictionary will everything required for later scoring and plotting

        inputs used are:
                  self.cycletime, self.totaltime, self.PMinACA,
                  self.outlierlimit, self.antout, self.maxpoorant 

        uses functions:
                  self.phRMScaltab, self.MADcalc

        fills:
                  self.allResults

        dict keys are:
                  blphaserms, blphasermscycle, bllen, blname, blout,
                  blphasermsbad, blphasermscyclebad, bllenbad,
                  antphaserms, antphasermscycle, antname,
                  phasermsP80, phasermscycleP80, blP80
        '''
        
        # call to phRMScaltab
        # gets baseline based phase RMS and antenna based phase RMS for
        # the total time (i.e. length of BP scan) and 
        # the cycle time (time it takes to cycle the start of a phase cal scan to the next - ties with a 'decohernce time' over the target)

        print('DOING ANALYSIS')
        if self.cycletime < self.totaltime:
            self.allResult= self.phRMScaltab(timeScale=self.cycletime) # if cycle time is shorter we pass the option so it gets assessed
        else:
            self.allResult= self.phRMScaltab() # otherwise no options added and cycletime value will equal total time value

        # Check the cycle time calculation is not all nans - might happen if there are considerable flags
        # i.e. if a large fraction of data > self.ftoll are flagged
        cycleResultFinite = len(np.array(self.allResult['blphasermscycle'])[np.isfinite(self.allResult['blphasermscycle'])])

        # if it is all flagged, theory to scale down total time with 'lower' scaling constant power (Maud et al. 2022)
        if cycleResultFinite == 0:
            self.allResult['blphasermscycle'] = np.array(self.allResult['blphaserms'])*(self.cycletime/self.totaltime)**0.3

        if self.PMinACA:
            # we need to exclude PM antennas from the calculation as they 'can' be too 'long' baselines
            # and not really useful for the understanding of the target likely phase RMS 
            # where only CM used in the images. Thus cut the PM from the upper 80 cut
            # NOTE TO DEV - maybe a better way than my list comprehension ? 
            blACAid = np.array([blid for blid in range(len(self.allResult['blname'])) if 'PM' not in self.allResult['blname'][blid]])
            antACAid = np.array([antid for antid in range(len(self.allResult['antname'])) if 'PM' not in self.allResult['antname'][antid]])
 
            ## reset allResult to exclude the PM baselines, and PM ants only
            bl_keys(['blphaserms', 'blphasermscycle', 'bllen', 'blname'])
            ant_keys(['antphaserms', 'antphasermscycle', 'antname'])
            for bl_key in bl_keys:
                self.allResult[bl_key]=np.array(self.allResult[bl_key])[blACAid]
            for ant_key in ant_keys:
                self.allResult[ant_key]=np.array(self.allResult[ant_key])[antACAid]

            
        # get 80th percentile baseline length and ID of all baselines above it
        self.allResult['blP80'] = np.percentile(self.allResult['bllen'],80) ## was xy
        ID_all80 = np.where(np.array(self.allResult['bllen'])> self.allResult['blP80'])[0]  ## was xy

        # First calculation of the representative phase RMS values
        self.allResult['phasermsP80'] = np.median(np.array(self.allResult['blphaserms'])[ID_all80[np.isfinite(np.array(self.allResult['blphaserms'])[ID_all80])]])
        self.allResult['phasermscycleP80'] = np.median(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        phaseRMScycleP80mad = self.MADcalc(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        
        # now begin the outlier checks
        # first check if any antennas are just above 100 deg phase RMS (this means ~pure phase noise for phases between -180 to +180 deg)
        # so sensible to identify these antennas
        ID_poorant = np.where(np.array(self.allResult['antphaserms'])[np.isfinite(self.allResult['antphaserms'])]>self.outlierlimit)[0]
        

        if len(ID_poorant)>0:
            for antout in ID_poorant:
                self.antout.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])

        # for what are yellow or red scores where phase RMS >50deg
        # we check for statistical outliers, and then clip these out and re-calculate the phaseRMS
        # in the case PL missed bad antennas were causing the high phase RMS
        # self.antout is used/passed in the score function as it adjusts the score and message too
        if self.allResult['phasermscycleP80'] > 50.:
            statsoutlierlimit = self.allResult['phasermscycleP80'] + 4.*phaseRMScycleP80mad # tested limit works well
        
            ID_poorant = np.where(np.array(self.allResult['antphaserms'])[np.isfinite(self.allResult['antphaserms'])]>statsoutlierlimit)[0]
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.antout.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])
        
            # max limit on number of 'bad' antennas to be allowed to exclude from calculations (set at 11 good as tested in PIPE692)
            # we clip them out and recalculate - score function also tracks and gives a warning
            # if >11, basically the data is rubbish, so don't clip and let scores be low
            print(len(self.antout))
            if len(self.antout) > 0 and len(self.antout) < self.maxpoorant: 

                # crude way to get the index values is a loop over the baselines
                # is there a way to better search allResult['blname'] to check if any self.antout are (or are not) in them
                ID_goodbl=[]
                ID_badbl=[]
                for idg, bln in enumerate(self.allResult['blname']):
                    if (bln.split('-')[0] not in self.antout) and (bln.split('-')[1] not in self.antout):
                        ID_goodbl.append(idg)
                    else: # assume bad
                        ID_badbl.append(idg)

                # now simply make a common list of IDs from good ones and the ID_all80 list - which are want we want to assess
                ID_all80 = np.array(np.sort(np.array(list(set(ID_goodbl).intersection(ID_all80)))))

                # recalculate the phase RMS that is bl >P80 and in the 'good' list
                self.allResult['phasermsP80'] = np.median(np.array(self.allResult['blphaserms'])[ID_all80[np.isfinite(np.array(self.allResult['blphaserms'])[ID_all80])]])
                self.allResult['phasermscycleP80'] = np.median(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])

                # store outliers for passing to plot function - will be plotted in 'shade/alpha'
                self.allResult['blphasermsbad']=np.array(self.allResult['blphaserms'])[np.array(ID_badbl)]
                self.allResult['blphasermscyclebad']=np.array(self.allResult['blphasermscycle'])[np.array(ID_badbl)]
                self.allResult['bllenbad']=np.array(self.allResult['bllen'])[np.array(ID_badbl)]
            else:
                # none the 'bad' entires in dict - they are all just bad 
                self.allResult['blphasermsbad']=None
                self.allResult['blphasermscyclebad']=None
                self.allResult['bllenbad']=None
        else:
            # this else is for <50deg phase RMS where we do not recalcualte the phase RMS as its low already
            # but we still want to identify any outliers to notify in the messages
            statsoutlierlimit = np.max([self.allResult['phasermscycleP80'] + 6.*phaseRMScycleP80mad,2.*self.allResult['phasermscycleP80']])
            ID_poorant = np.where(np.array(self.allResult['antphaserms'])[np.isfinite(self.allResult['antphaserms'])]>statsoutlierlimit)[0]
            # add them to the list so score code picks them up if required and changes the messages
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.antout.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])

            # none the 'bad' entires in dict 
            self.allResult['blphasermsbad']=None
            self.allResult['blphasermscyclebad']=None
            self.allResult['bllenbad']=None


        print("FINISHED ANALYSIS")

    def pmInACA(self):
        ''' Check if the array is ACA and has PM antennas

        input used:
                 self.antlist


        returns: 
                Bool based on PM with CM antennas
        '''

        antUse = self.antlist
 
        if 'CM' in antUse and 'PM' in antUse and 'DA' not in antUse and 'DV' not in antUse:
            PMincACA = True
        else:
            PMincACA = False

        return PMincACA


    def STDC(self,phase,diffTime,over=120.0):
        '''Calculate STD over a set time and return the average of all overlapping
        values of the standard deviation - overlapping estimator. This acts
        upon a phase-time stream of data. Will consider only finite values in 
        the phase-time stream (i.e. phase array). The phase array must be 
        unwrapped, i.e. continous, no breaks or 2PI ambiguities
    
        :param phase: any unwrapped input phase
        :type phase: array
        :param diffTime: the time between each data integration
        :type diffTime: float
        :param over: time in seconds to calculate the SD over
        :type over: float
        :returns: average standard deviation for the dataset calcualted over the input timescale 
        :rtype: float
    
        Note this is STANDARD DEVIATION - takes out the
        mean value. RMS with mean or fit removed provides
        the same value for zero centred phases.
        '''

        # overlap in elements 
        over = np.int(np.round(over / diffTime))
        
        # handle nans as some elements in the array could be flagged, so 
        # use finite ones only
        std_hold =[]
        for i in range(len(phase)-over):
            # this loops over the data in range rounded to size of time step
            std_hold.append(np.std(np.array(phase[i:i+over])[np.isfinite(phase[i:i+over])]))
    
        std_mean = np.mean(std_hold)

        return std_mean




    ## NOTE TO DEV - maybe already code that does this?
    def getCalPhase(self,ant):
        ''' Read a caltable file and select the
        phases from one pol (tested in PIPE692 as sufficient).
        If those phase data have a flag, the phase
        value is set to nan, which is dealt with in the correct
        way during the rest of the calulation. The phases 
        are unwrapped, i.e. solved for the 2PI ambiguitiy 

        input required:
                 ant (int)  - the antenna to get the phases for 

        uses inputs:
                 self.caltable, self.refantid, self.spwUse, self.scanUse
  

        returns: float array of the phases of an antenna
        '''

        mytb.open(self.caltable)
        unwrap = True # hard coded - correct wraps in phase stream
        flag = True # hard coded - excluded flagged data as it is extracted from gaintable 

        tb1 = mytb.query("ANTENNA1 == %s && ANTENNA2 == %s && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s "%(ant,self.refantid,self.spwUse,self.scanUse))
    
        cal_ph = tb1.getcol('CPARAM')
        cal_ph = np.angle(cal_ph[0][0])  ## in radians one pol only - HARD CODED 

        ## code works fine as tested in PIPE 692 with only XX pol extractions
        ## not 'really' required to do both pols, but could do so, and also get an average
        ## single and full-pol to check, this single pull will work, but with more then the order differs (c.f. renorm pol code)
        
        if flag:
            flags=tb1.getcol('FLAG') ## [0][0]
            cal_ph = [val if not flags[0][0][id_u] else np.nan for id_u,val in enumerate(cal_ph)]  ## so only one pol used 'X'
            cal_ph = np.array(cal_ph)

        ## note - everything is in radians         
        if unwrap:
            cal_ph=self.phaseUnwrap(cal_ph)

        mytb.close()

        return cal_ph
        

    def phaseUnwrap(self,phase, factor=1.0): 
        ''' Unwraps the phases to solve for 2PI ambiguities

	:param phase: phase in radians
	:type phase: float array
	:returns: unwrapped phase-array
	:rtype: float array
	
        1) factor can be used to specify what is concidered as a wrap (multiples of pi)
        2) nans are dealt with correctly 
        
	'''
        phase2 = phase.copy()
        if len(phase2[np.isfinite(phase2)]) > 2:
            if np.isnan(phase2[0]):
                phase2[0] =[xp for xp in phase2 if not np.isnan(xp)][0]
		
            phase2 = phase2[np.isfinite(phase2)]
            diff   = np.diff(phase2)   
            pup    = np.cumsum(diff < -factor*np.pi)
            pdown  = np.cumsum(diff > factor*np.pi)
            ph = np.zeros_like(phase)
            ph[0] = phase[0]  
	
            # now need to make pup and pdown related to original phase
            # but they are shorter arrays
            
            lp = 1
            cr = 0
            correct = pup-pdown
            while lp < len(phase):
		# now correct on the fly with the 
		# first element of pup-pdown
		# then remove the element
		# if the main array is not a nan and correction was applied
		# have to do this way as the output is a phase
		# stream to be averaged with another 

                if np.isfinite(phase[lp]): 
                    ph[lp] = correct[cr] * 2*np.pi + phase[lp]
                    cr=cr+1
                else:
                    # i.e. no correction as phase is a nan - so just fill ph with a nan
                    ph[lp] = phase[lp]

                lp=lp+1
        else:
            ph = phase2
        
        return ph
	


    def getTime(self):  
        ''' Read a caltable file and return time
        shifted to start at zero seconds starting time.
        Note these are the recored times in the caltable

        uses inputs:
               self.caltable, self.spwUse, self.scanUse, self.antlist
        
        :returns: the time stamps for the caltable
	:rtype: float array (if a fail just returns 0)
        '''
        
        mytb.open(self.caltable)
        nant=len(self.antlist)
        timeX=[]
        antid=0
        while len(timeX) < 2:
            tb1 = mytb.query("ANTENNA1 == %s  && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s"%(antid,self.spwUse,self.scanUse))
            timeX = tb1.getcol('TIME')
            antid+= 1
            if antid == nant-1:
                break

        # safety check if there is no time found (totally impossible as Bandpass phases will be there always)
        if len(timeX) < 2:
            timeX = 0
        else:
            timeX = np.array(timeX) - timeX[0]
        tb1.close()
        mytb.close()
        return timeX
        
    def avePhase(self, phase, diffTime, over=1.0):
        ''' Routine to do an averaging/smoopthing on the phase data
        The by default for ALMA for phase statistics should be 10s.
        For default 6s integration times this will average 2 values.
        If input phase from gain table have integration(diffTime) > 10s
        then no averaging is made.

        :param phase: phase series of the data to average
        :type phase: array
        :param diffTime: the average difference in time between each data value, i.e. each phase
        :type diffTime: float
        :param over: the time to average over - default is 1s 
        :type over: float
        :returns: array of averaged phases
        :rtype: array
        '''

        ## code
        over = np.int(np.round(over / diffTime))
        # Make an int for using elements
        # will be slightl inaccuracies is there are long timegaps in the data
        # but this is minimal 
        if over > 1.0:
            mean_hold =[]
            for i in range(len(phase)-over):
                # this averages/smoothes the data 
                # make nan resistant by ignoring the nan values
                mean_hold.append(np.mean(np.array(phase[i:i+over])[np.isfinite(phase[i:i+over])]))
        else:
            mean_hold= phase 
            
        return mean_hold


    def MADcalc(self,datain, axis=None):
        ''' This calculates the MAD - median absolute deviation from the median
        The input must be nan free, i.e. finite data 
        :param data: input data stream
        :type data: list or array
        
        :returns: median absolute deviation (from the median)
        :rtyep: float
        '''

        return np.median(np.absolute(datain-np.median(datain,axis)),axis)




    def phRMScaltab(self, antout=[], timeScale=None): 
        ''' This will run the loop over the caltable
        and work out the baseline based phases and calculate the 
        phase RMS. It will also get the Phase RMS per antenna (with
        respect to the refant - i.e. ant based phase RMS) these are all
        passed back to the main class as self.allResult is filled 
        
        inputs used:
              self.antlist, self.ftoll, self.difftime, self.baselines

        calls functions:
              self.getCalPhase, self.avePhase, self.STDC
 
        returns:  rms_results{}
        dict keys: blphaserms, bphasermscycle, bllen, blname,
                   antphaserms, antphasermscycle, antname
              
        '''

        # setup the result list

        rms_results =  {}  
        rms_results['blphaserms'] = []
        rms_results['blphasermscycle'] = []
        rms_results['bllen'] = []
        rms_results['blname'] = []
        rms_results['antphaserms'] = []
        rms_results['antphasermscycle'] = []
        rms_results['antname'] = []

        nant=len(self.antlist)
        iloop = np.arange(nant-1) 

        for i in iloop:
            # ant based parameters
            pHant1 = self.getCalPhase(i)
            rms_results['antname'].append(self.antlist[i]) 

            # make an assessment of flagged data for that antenna
            if len(pHant1[np.isnan(pHant1)]) > self.ftoll*len(pHant1):
                rms_results['antphaserms'].append(np.nan)
                rms_results['antphasermscycle'].append(np.nan)
                
            else:
                ## do averaing -> 10s
                pHant_ave = self.avePhase(pHant1, self.difftime, over=10.0) # for thermal/short term noise
                rmspHant_ave = np.std(np.array(pHant_ave)[np.isfinite(pHant_ave)])
                rms_results['antphaserms'].append(rmspHant_ave)
                if timeScale:
                    rmspHant_ave_cycle = self.STDC(pHant_ave, self.difftime, over=timeScale) 
                    rms_results['antphasermscycle'].append(rmspHant_ave_cycle)
                else:
                    rms_results['antphasermscycle'].append(rmspHant_ave)


            jloop = np.arange(i+1, nant) # so this is baseline loop 
            for j in jloop:
                ## get the phases - needs to be the single read in table 
                
                #pHant1 is read in above already
                pHant2 = self.getCalPhase(j)
                # phases from cal table come in an order, baseline then is simply the subtraction
                pH= pHant1 - pHant2 
                    
                # fill baseline information now
                rms_results['blname'].append(self.antlist[i]+'-'+self.antlist[j])
                rms_results['bllen'].append(float(self.baselines.get_baseline(i,j).length.value)) # from context input
                
                # make assessment if this is a bad antenna
                if self.antlist[i] in antout or self.antlist[j] in antout: 
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)
              
                # make an assessment of flagged data
                elif len(pH[np.isnan(pH)]) > self.ftoll*len(pH):
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)

                else:
                    ## do averaing -> 10s
                    # need to get time and then rebin
                    pH_ave = self.avePhase(pH, self.difftime, over=10.0) # for thermal/short term noise 
                    rmspH_ave = np.std(np.array(pH_ave)[np.isfinite(pH_ave)])  
                    rms_results['blphaserms'].append(rmspH_ave)
                    if timeScale:
                        rmspH_ave_cycle = self.STDC(pH_ave, self.difftime, over=timeScale)   
                        rms_results['blphasermscycle'].append(rmspH_ave_cycle)
                    else:
                        rms_results['blphasermscycle'].append(rmspH_ave)

        # set RMS output in degrees as we want
        for key_res in ['blphaserms','blphasermscycle','antphaserms','antphasermscycle']:
            rms_results[key_res]= np.degrees(rms_results[key_res])
        
        # this is a dict
        return rms_results



    def getcycletime(self):
        """
        Computes the median time (in seconds) between visits to the specified intent.
        Note that other parts of the ALMA project consider the "cycleTime" to be the 
        scan duration on the science target before going back to the phase calibrator,
        i.e. ignoring the duration of the phasecal scan, the ATM cal scans, the 
        checksource, and all the slewing and overhead.
        -Todd Hunter ORIG in analysis Utils (cycleTime)
        - LM edited for this code

        input used:
                self.visUse

        return: the cycletime (float)
        """

        mymsmd.open(self.visUse)
        scans = mymsmd.scansforintent('*PHASE*')
        times = []
        for scan in scans:
            times.append(np.min(mymsmd.timesforscan(scan)))
        mymsmd.close()
        if len(times) == 1:
            print("There was only 1 scan with this intent.")
            return # possible error return ? or default as none
        diffs = np.diff(times)
        return np.median(diffs)



    def score(self): 
        ''' Code to create score (between 0.0 and 1.0)  and short and long messaging.
        This will use assess the cycle time phase RMS value, which is important as everything longer than a cycle time
        is corrected by phase referencing (in terms of atmospheric phase variations). 

        Returns the score and a colour 
        Checks the outlier antennas and such to see what affects the score, i.e. downgrade and message change
        -- logic is explained below in comments

        uses inputs:
                self.allResults, self.visUse, self.antout

        returns a dict
                keys 'basescore','basecolor','shortmsg','longmsg'
        '''
        
        # run in a try/except incase there are errors that nothing was passed
        # NOTE TO DEV - if that occurred should the exception be raised?

        print(' DOING THE SCORE')
        try:
            baseScore = 1.0 - self.allResult['phasermscycleP80']/100. 
        except:
            # if there is some error 
            baseScore = 0.0 

        if baseScore > 0.7:
            # this is for <30 deg phaseRMS - i.e green - stable phases 
            baseScore  = 1.0
            baseCol = 'green'
            shortmsg = 'Excellent stability phase RMS (<30deg).'
            longmsg = 'The baseline-based median phase RMS for baselines longer than P80 is less\n than 30 deg over the cycle time. The residual phase noise for the target(s) is\n expected to be low after phase referencing and decoherence will be minimal.'
            # now check for problem antennas - if yes, change the score
            # these are outliers >100 deg, or those beyond stat outlierlimit in function 'analysis' (6 MAD)
            if len(self.antout)>0:
                baseScore = 0.9 
                baseCol = 'blue'
                shortmsg = shortmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'## NOTE not excluded from median calculation.'
                longmsg = longmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'## NOTE not excluded from median calculation.' 

        elif baseScore > 0.5 and baseScore < 0.7:
            # this is for 30 to 50 deg - should be blue, not really a problem just informative
            # that the phase noise is elevated - no 'need' to look
            # as 50 deg phase RMS can still cause ~30% decoherence
            baseScore = 0.9
            baseCol='blue'
            shortmsg = 'Stable conditions phase RMS (30-50deg).'
            longmsg = 'The baseline-based median phase RMS for baselines longer than P80 is between\n 30 and 50 deg over the cycle time. The residual phase noise of the target(s)\n after phase referncing could be slighty elevated and lead to decoherence\n of up to 30%.  Self-calibration could help achieve the best imaging.' 
            if len(self.antout)>0:
                shortmsg = shortmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'## NOTE not excluded from median calculation.'
                longmsg = longmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'## NOTE not excluded from median calculation.'

        # these are high phase noise - remember the 
        # in funtion 'analysis' we already did outlier clips 
        # past 100deg, and those >4 MAD above the P80 phase RMS value
        # so, if we still get here, the phases were poor/v.bad - or there 
        # were too many antennas classed as bad in the analysis function
        elif baseScore < 0.5 and baseScore > 0.3:
            # this is 50 - 70 deg phase RMS, i.e. 30-50% lost due to decoehrence
            # score is representative
            baseCol ='yellow'
            shortmsg = 'Elevated Phase RMS (50-70deg) exceeds stable parameters.'
            longmsg='The baseline-based median phase RMS on baselines longer than P80 is\n between 50 and 70 deg over the cycle time. The residual phase noise of the\n target(s) after phase referncing will be elevated leading to decoherence\n of up to 50%.  Self-calibration (if possible) is absolutely recommended.'
            if len(self.antout)>0:
                shortmsg = shortmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'
                longmsg = longmsg+' '+str(','.join(self.antout))+' have higher phase RMS.' 

        elif baseScore < 0.3:
            baseCol = 'red'
            if baseScore < 0.0:
                baseScore=0.0
            shortmsg='High Phase RMS (>70deg) exceeds limit for poor stability'
            longmsg= 'The baseline-based median phase RMS on the baselines longer than P80 is\n greater than 70deg over the cycle time. The residual phase noise for the\n target(s) is expected to be significantly elevated after phase referencing. The\n phase transfer could be poor and decoherence could be over 50%, reducing the\n image peak, spreading flux around the image and causing image defects.'
            if len(self.antout)>0:
                shortmsg = shortmsg+' '+str(','.join(self.antout))+' have higher phase RMS.'
                longmsg = longmsg+' '+str(','.join(self.antout))+' have higher phase RMS.' 

        else:  ## default error - set to zero score for stage error
            baseCol = 'red'
            baseScore = 0.0
            shortmsg = 'The phase RMS could not be assessed.'
            longmsg= 'The Spatial structure function could not be assessed for '+self.visUse+'.'
    
        scoreout={}
        scoreout['basescore']=baseScore
        scoreout['basecolor']=baseCol
        scoreout['shortmsg']=shortmsg
        scoreout['longmsg']=longmsg
        print('Scoring', scoreout)
        print(' FINISHED SCORE')
        return scoreout
     
     
    def plotSSF(self): 
        '''plotting task used to make the
        spatial structure functions plots 
        as to be displayed on the weblog page
        
        uses inputs:
              self.allResults

        '''

        # NOTE TO DEV - please check/adjust as required for PL
        # I have NOT included the stage number etc into the fig save name 

        # do the plot
        figsize=[11.0, 8.5] # appears mostly to affect the savefig not the interactive one 
        figsize=np.array(figsize)
        plt.close(1)
        fig=plt.figure(1)
        fig.set_size_inches(figsize[0], figsize[1], forward=True)
        fig.clf()
    

        ax1 = fig.add_axes([0.10,0.17,0.82,0.75])   # positons,  width,  height - main plot

        print('plot the main res')
        ax1.plot(self.allResult['bllen'],self.allResult['blphaserms'],linestyle='',marker='o',c='0.6',zorder=0,label='Total-Time')
        ax1.plot(self.allResult['bllen'],self.allResult['blphasermscycle'],linestyle='',marker='o',c='r',zorder=1, label='Cycle-Time') # after is red - same as WVR plots etc 
        print('completed')

        ax1.set_xscale('log')
        ax1.set_yscale('log')

        ## make wide lines for 'limits' at 30, 50, 70 deg 
        ax1.plot([1.0,20000.0],[29.0,29.0],linestyle='-',c='g',linewidth=10.0,zorder=2,alpha=0.5) # line width  - green limit
        ax1.plot([1.0,20000.0],[48.0,48.0],linestyle='-',c='b',linewidth=10.0,zorder=2,alpha=0.5) # line width  - blue limit
        ax1.plot([1.0,20000.0],[67.0,67.0],linestyle='-',c='yellow',linewidth=10.0,zorder=2,alpha=0.5) # line width  - blue limit

        # median marker 
        print('plot median')
        ax1.plot([self.allResult['blP80'],np.max(self.allResult['bllen'])],[self.allResult['phasermscycleP80'],self.allResult['phasermscycleP80']],c='0.2',linewidth=10,zorder=5, label='Median (bl > P80)')
        print('completed')

        ## need to assess allResults here phasermscycleP80
        if self.allResult['phasermscycleP80'] < 50. :
            ax1.set_yticks([10.0,20.0,30.0,50.0])
        else:
            ax1.set_yticks([10.0,20.0,30.0,50.0,70.0, 100.0, 300.0])

        # max baselines for getting ticks
        if np.max(self.allResult['bllen'])> 5000.:
            ax1.set_xticks((50.0,100.0,500.0,1000.,5000.0,10000.0))
        elif np.max(self.allResult['bllen'])> 1000.:
            ax1.set_xticks((10.0,50.0,100.0,500.0,1000.0,3000.0))
        elif np.max(self.allResult['bllen'])> 500.:
            ax1.set_xticks((10.0,50.0,100.,300.,500.,700.))
        elif np.max(self.allResult['bllen'])> 100.:
            ax1.set_xticks((10.0,30.0,50.0,70.0, 100., 300.))
        else: # ACA should default to this 
            ax1.set_xticks((10.0,20.0,30.0,50.0,70.0,90.0, 100.))

        print('line annotate')
        ax1.annotate('30deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,27),xycoords='data')
        ax1.annotate('50deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,44.5),xycoords='data')
        ax1.annotate('70deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,61.5),xycoords='data')
        print('completed')

        print(self.allResult['bllenbad']) # prob some are nan...?
        ## if there are any outliers passed they are plotted in 'shade' 
        if self.allResult['bllenbad'] is not None:
            print('plotting outliers')
            # should over plot on the plot, i.e cover the full plot already - white them out
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermsbad'],linestyle='',marker='o',c='w',zorder=3)
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermscyclebad'],linestyle='',marker='o',c='w',zorder=4)
            # over plot as shade
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermsbad'],linestyle='',marker='o',c='0.6',zorder=5, alpha=0.1, label='Total-time (outlier)')
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermscyclebad'],linestyle='',marker='o',c='r',zorder=6,alpha=0.1, label='Cycle-time (outlier)') 
            print('completed')

        print('calc max and min')
        phaseRMSmax = np.max([np.max(self.allResult['blphasermscycle'][np.isfinite(self.allResult['blphasermscycle'])]),np.max(self.allResult['blphaserms'][np.isfinite(self.allResult['blphaserms'])])])
        phaseRMSmin = np.min([np.min(self.allResult['blphasermscycle'][np.isfinite(self.allResult['blphasermscycle'])]),np.min(self.allResult['blphaserms'][np.isfinite(self.allResult['blphaserms'])])])
        print('completed')

        # make limit at least 35 on plot for green data
        if phaseRMSmax < 35:
            phaseRMSmax = 35.
            if phaseRMSmin > 3:
                phaseRMSmin = 2.
        ax1.grid(True)

        # NOTE TO DEV
        # crude logic here as log-log plots have some issue when there are <9 tick markers
        # if this happens, matplotlib is adding its own extra (minor) tick markers with wrong formatting
        # e.g. if the plot range is 20 to 100, there are only 9 markers, so the set_ytick is not obeyed
        # se requested ax1.set_yticks([10.0,20.0,30.0,50.0,70.0, 100.0, 300.0])
        # but we get sci format ticks also at 40 and 60
        # hence below we find the range of total ticks and if this is <10 we
        # extend the plot max range
        plotrange = [1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300, 400, 500]
        idplot = [ id for id, val in enumerate(plotrange) if phaseRMSmin < val and phaseRMSmax > val]
        print(len(idplot))
        indexplt=10-len(idplot) + np.max(idplot)
        if len(idplot)<10 and indexplt < len(plotrange)-1:  # i.e. if we cannot adjust further than plotrange, we don't
            phaseRMSmax = plotrange[indexplt]


        # Note TO DEV -  need to make a suitable 'main' figure name, if to be something more than the EB
        fig_name='Execution Block '+str(self.visUse)
        infoLine='SPW '+str(self.spwUse)+' Correlation X        All Unflagged Antennas     Bandpass: '+str(self.fieldUse)+'      Scan '+str(self.scanUse)
        ax1.set_title('Spatial structure function: ' + fig_name+'\n '+infoLine, fontsize=10)
        ax1.set_ylabel('Phase RMS (deg)')
        ax1.set_xlabel('Baseline Length (m)')

        ax1.set_xlim(np.min(self.allResult['bllen'])/2.0,np.max(self.allResult['bllen'])*1.1)
        ax1.set_ylim(phaseRMSmin*0.9,phaseRMSmax*1.1) 

        ax1.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax1.yaxis.set_major_formatter(ticker.ScalarFormatter())
        # Note TO DEV - despite setting tick levels and the ticker here I have seen
        # a handful of cases where extra the axis ticks are added

        ax1.legend(loc='upper left', bbox_to_anchor=(0.01, -0.085), prop={'size':8}, frameon=False, ncol=3)
        # upper center is where the box of the elgend locator is

        # NOTE TO DEV - need to make suitable image save name - other than EB name (e.g. add stage maybe??) 
        plt.savefig('{0}_PIPE-692_SSF.png'.format(self.visUse),format='png', dpi=100.)
        
 
    ### NOTE NEEDED IN FINAL PL IMPLEMENTATION
    ### it is for the mockup weblog plot page 
    def plotSSF_mockup(self, resultuse): # should not need inputs ,QAin,fig_name=None, medline=True, QAinitial=None):
        '''plotting task used to make the
        spatial structure functions plots 
        as are shown on the PIPE-692 ticket
        and ALSO will the fullPL weblog page mock-up. 
        
        # input for mock are the scores and message strings
        # passed from resultsuse via the 'score' function

        '''

        print('NOTE - Running the full MOCKUP weblog plot')

        # do the plot
        figsize=[12.0, 17.5] # appears mostly to affect the savefig not the interactive one 
        figsize=np.array(figsize)
        plt.close(1)
        fig=plt.figure(1)#, figsize=figsize)
        fig.set_size_inches(figsize[0], figsize[1], forward=True)
        fig.clf()
    

        # positions for the PIPE-692 layout - fully flexible with any ideas other changes etc
        # i.e. the text would be on the page, not the plot

        ax1 = fig.add_axes([0.17,0.25,0.7,0.36])   # positons,  width,  height - main plot
   
        # below are layout hacks for PL looking weblog figure

        ax2a = fig.add_axes([0.05,0.66,0.20,0.04])  # mimic the top table left
        ax2b = fig.add_axes([0.25,0.66,0.08,0.04])  # mimic the top table middle
        ax2c = fig.add_axes([0.33,0.66,0.08,0.04])  # mimic the top table right
        ax2d = fig.add_axes([0.41,0.66,0.19,0.04])  # mimic the top table right
        ax2e = fig.add_axes([0.60,0.66,0.38,0.04])  # mimic the top table right

        ax2Ha = fig.add_axes([0.05,0.7,0.20,0.02])  # mimic the top table
        ax2Hb = fig.add_axes([0.25,0.7,0.08,0.02])  # mimic the top table
        ax2Hc = fig.add_axes([0.33,0.7,0.08,0.02])  # mimic the top table
        ax2Hd = fig.add_axes([0.41,0.7,0.19,0.02])  # mimic the top table
        ax2He = fig.add_axes([0.60,0.7,0.38,0.02])  # mimic the top table

        ax3F  = fig.add_axes([0.055,0.04,0.91,0.08])  # Pipeline QA accordian
        ax3T  = fig.add_axes([0.055,0.10,0.91,0.02]) # Pipeline QA accordian
        ax3Ha = fig.add_axes([0.07,0.07,0.15,0.02])
        ax3Hb = fig.add_axes([0.18,0.07,0.76,0.02])
        ax3a = fig.add_axes([0.07,0.05,0.15,0.02])   # mimic the score QA
        ax3b = fig.add_axes([0.18,0.05,0.76,0.02])   # mimic short comment QA

        # warning message at top need set axes 
        ax4H = fig.add_axes([0.055,0.935,0.91,0.02])  # mimic the top table
        ax4 = fig.add_axes([0.055,0.915,0.91,0.02])  # mimic the top table

        ax1.plot(self.allResult['bllen'],self.allResult['blphaserms'],linestyle='',marker='o',c='0.6',zorder=0,label='Total-Time')
        ax1.plot(self.allResult['bllen'],self.allResult['blphasermscycle'],linestyle='',marker='o',c='r',zorder=1, label='Cycle-Time') # after is red - same as WVR plots etc 

        ax1.set_xscale('log')
        ax1.set_yscale('log')

        ## make wide lines for 'limits' at 30, 50, 70 deg 
        ax1.plot([1.0,20000.0],[29.0,29.0],linestyle='-',c='g',linewidth=10.0,zorder=2,alpha=0.5) # line width  - green limit
        ax1.plot([1.0,20000.0],[48.0,48.0],linestyle='-',c='b',linewidth=10.0,zorder=2,alpha=0.5) # line width  - blue limit
        ax1.plot([1.0,20000.0],[67.0,67.0],linestyle='-',c='yellow',linewidth=10.0,zorder=2,alpha=0.5) # line width  - blue limit

        # median marker 
        ax1.plot([self.allResult['blP80'],np.max(self.allResult['bllen'])],[self.allResult['phasermscycleP80'],self.allResult['phasermscycleP80']],c='0.2',linewidth=10,zorder=5, label='Median (bl > P80)')
    
        # green < 30 ,  blue is 30-50, yellow is >50 rms, red is > 70 deg rms - over the cycle time
        # messages pass from score for the mockup
        warStr= resultuse['longmsg']
        shortStr=resultuse['shortmsg']
        xyUse = (0.01,0.1)

        if resultuse['basecolor'] == 'green' or resultuse['basecolor']=='blue':
            ax1.set_yticks([10.0,20.0,30.0,50.0])
        
        elif resultuse['basecolor'] == 'yellow' or resultuse['basecolor']=='red':
            ax1.set_yticks([10.0,20.0,30.0,50.0,70.0, 100.0, 300.0])


        # max baselines for getting ticks
        if np.max(self.allResult['bllen'])> 5000.:
            ax1.set_xticks((50.0,100.0,500.0,1000.,5000.0,10000.0))
        elif np.max(self.allResult['bllen'])> 1000.:
            ax1.set_xticks((10.0,50.0,100.0,500.0,1000.0,3000.0))
        elif np.max(self.allResult['bllen'])> 500.:
            ax1.set_xticks((10.0,50.0,100.,300.,500.,700.))
        elif np.max(self.allResult['bllen'])> 100.:
            ax1.set_xticks((10.0,30.0,50.0,70.0, 100., 300.))
        else: # ACA should default to this 
            ax1.set_xticks((10.0,20.0,30.0,50.0,70.0,90.0, 100.))

        ax1.annotate('30deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,27),xycoords='data')
        ax1.annotate('50deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,44.5),xycoords='data')
        ax1.annotate('70deg RMS limit',xy=(np.min(self.allResult['bllen'])/2.0,61.5),xycoords='data')
                  
        ## if there are any outliers passed they should be added in the message, main code does the logic
        if self.allResult['bllenbad'] is not None:
            # should over plot on the plot, i.e cover the full plot already
            # white out then shade over using python zordering
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermsbad'],linestyle='',marker='o',c='w',zorder=3)
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermscyclebad'],linestyle='',marker='o',c='w',zorder=4) 
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermsbad'],linestyle='',marker='o',c='0.6',zorder=5, alpha=0.1, label='Total-time (outlier)')
            ax1.plot(self.allResult['bllenbad'],self.allResult['blphasermscyclebad'],linestyle='',marker='o',c='r',zorder=6,alpha=0.1, label='Cycle-time (outlier)') 

           
        phaseRMSmax = np.max([np.max(self.allResult['blphasermscycle'][np.isfinite(self.allResult['blphasermscycle'])]),np.max(self.allResult['blphaserms'][np.isfinite(self.allResult['blphaserms'])])])
        phaseRMSmin = np.min([np.min(self.allResult['blphasermscycle'][np.isfinite(self.allResult['blphasermscycle'])]),np.min(self.allResult['blphaserms'][np.isfinite(self.allResult['blphaserms'])])])

        # make limit at least 35 on plot for green data
        if phaseRMSmax < 35:
            phaseRMSmax = 35.
        ax1.grid(True)

        # extend plot range if ticks <9 as matplotlib adding extra values other than the set_yticks
        plotrange = [1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,300, 400, 500]
        idplot = [ id for id, val in enumerate(plotrange) if phaseRMSmin < val and phaseRMSmax > val]
        print(len(idplot))
        indexplt=10-len(idplot) + np.max(idplot)
        if len(idplot)<10 and indexplt < len(plotrange)-1:  # i.e. if we cannot adjust further than plotrange, we don't
            phaseRMSmax = plotrange[indexplt]

        # Added SPW and Scans 
        fig_name='MOCKUP_'+str(self.visUse)
        infoLine='SPW '+str(self.spwUse)+' Correlation X        All Unflagged Antennas     Bandpass: '+str(self.fieldUse)+'     Scan '+str(self.scanUse)
        ax1.set_title('Spatial structure function: ' + fig_name+'\n '+infoLine, fontsize=10)
        ax1.set_ylabel('Phase RMS (deg)')
        ax1.set_xlabel('Baseline Length (m)')

        ax1.set_xlim(np.min(self.allResult['bllen'])/2.0,np.max(self.allResult['bllen'])*1.1)
        ax1.set_ylim(phaseRMSmin*0.9,phaseRMSmax*1.1) 

        ax1.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax1.yaxis.set_major_formatter(ticker.ScalarFormatter())
        #fontL = FontProperties
        #fontL = set_size('small')
        ax1.legend(loc='upper left', bbox_to_anchor=(0.01, -0.085), prop={'size':8}, frameon=False, ncol=3)# upper center is where the box of the elgend locator is

 
        ### Extras for weblog page plot mockup ###

        print(' -- EXTRAS for MOCK UP -- ')
        ax1.annotate(' The pipeline uses the bandpass phase solutions to create structure functions plots, baseline length versus phase RMS.\n The measure of the phase RMS over a time interval equal to the phase referencing cycle-time is useful as a proxy for\n the expected residual phase RMS of a target source(s) after phase referencing. The action of phase referencing itself\n acting to correct phase fluctuations, caused by the atmosphere, on timescales longer than the cycle-time. For excellent\n stability conditions, phase RMS (<30deg), the target images will have minimal decoherence. For stable conditions, phase\n RMS (30-50deg), the target image can have slight decoherence which could be improved by self-calibration. When\n exceeding the phase RMS considered as stable conditions (50-70deg), target images can suffer from significant\n decoherence up to 50%. Self-calibration can help improve the final products. In conditions exceeding the poor stability\n threshold, phase RMS (>70deg), target images are expected to be poor, suffer from extreme levels of decoherence and\n possibly have image structure defects. Only self-calibration of known strong targets could recover these data.', xy=(0.05,0.735), xycoords='figure fraction', fontsize=10)

        ax1.annotate('Phase RMS structure plots', xy=(0.05,0.865), xycoords='figure fraction', fontsize=15, weight='bold')

        # required for CASA 6
        ax3T.set_facecolor('0.9')   
        ax2a.set_facecolor('0.9')   
        ax2b.set_facecolor('0.9')   
        ax2c.set_facecolor('0.9')   
        if resultuse['basecolor']=='green':
            ax3a.set_facecolor('lightgreen') 
            ax3b.set_facecolor('lightgreen') 
        if resultuse['basecolor']=='blue':
            ax3a.set_facecolor('lightskyblue') 
            ax3b.set_facecolor('lightskyblue') 
        if resultuse['basecolor']=='yellow':
            ax3a.set_facecolor('lightyellow') 
            ax3b.set_facecolor('lightyellow') 
        if resultuse['basecolor']=='red':
            print('at color set')
            ax3a.set_facecolor('lightsalmon') 
            ax3b.set_facecolor('lightsalmon') 
            print('red colour set')

        # Headings and Titles
        ax2Ha.annotate('Measurement Set',xy=(0.05,0.1),xycoords='axes fraction',fontsize=8, weight='bold')
        ax2Hb.annotate('Type',xy=(0.05,0.1),xycoords='axes fraction',fontsize=8, weight='bold')
        ax2Hc.annotate('Time (sec)',xy=(0.05,0.1),xycoords='axes fraction',fontsize=8, weight='bold')
        ax2Hd.annotate('Median Phase RMS (deg)',xy=(0.02,0.1),xycoords='axes fraction',fontsize=8, weight='bold')
        ax2He.annotate('Noisier antennas',xy=(0.02,0.1),xycoords='axes fraction',fontsize=8, weight='bold')

        ax2a.annotate(self.visUse,xy=(0.02,0.55),xycoords='axes fraction',fontsize=8)
        ax2b.annotate('Total-Time',xy=(0.02,0.55),xycoords='axes fraction',fontsize=8)
        ax2b.annotate('Cycle-Time',xy=(0.02,0.1),xycoords='axes fraction',fontsize=8)
        ax2c.annotate(np.round(self.totaltime,2),xy=(0.02,0.55),xycoords='axes fraction',fontsize=8)
        ax2c.annotate(np.round(self.cycletime,2),xy=(0.02,0.1),xycoords='axes fraction',fontsize=8)
        plotRMS = np.round(self.allResult['phasermscycleP80'],2)
        plotRMStot = np.round(self.allResult['phasermsP80'],2)
        ax2d.annotate(plotRMS,xy=(0.01,0.1),xycoords='axes fraction',fontsize=8)
        ax2d.annotate(plotRMStot,xy=(0.01,0.55),xycoords='axes fraction',fontsize=8)

        # check length of bad antennas for a return string - or a split
        ax2e.annotate(','.join(self.antout),xy=(0.01,0.55),xycoords='axes fraction',fontsize=8)

        ax3T.annotate('Pipeline QA',xy=xyUse,xycoords='axes fraction',weight='bold')
        ax3Ha.annotate('Score',xy=xyUse,xycoords='axes fraction',weight='bold',fontsize=10)
        ax3Hb.annotate('Reason',xy=xyUse,xycoords='axes fraction',weight='bold',fontsize=10)

        # score dependent
        if resultuse['basecolor'] == 'green':
            ax3a.annotate(str(resultuse['basescore']),xy=xyUse,xycoords='axes fraction',fontsize=8,color='green')
            ax3b.annotate(shortStr,xy=xyUse,xycoords='axes fraction',fontsize=8, color='green')
            ax4.axis('off')
            ax4H.axis('off')

        if resultuse['basecolor']=='blue':
            ax3a.annotate(str(resultuse['basescore']),xy=xyUse,xycoords='axes fraction',fontsize=8,color='blue')
            ax3b.annotate(shortStr,xy=xyUse,xycoords='axes fraction',fontsize=8, color='blue')
            ax4.axis('off')
            ax4H.axis('off')        

        # in case of red or yellow - banner message
        if resultuse['basecolor']=='yellow':
            ax3a.annotate(str(np.round(resultuse['basescore'],1)),xy=xyUse,xycoords='axes fraction',fontsize=8,color='gold')
            ax3b.annotate(shortStr,xy=xyUse,xycoords='axes fraction',fontsize=8, color='gold')
            ax4H.annotate('Task notifications', xy=xyUse, xycoords='axes fraction',weight='bold', fontsize=10) 
            ax4.set_facecolor('lightyellow')
            ax4.annotate('Warning! '+shortStr, xy=xyUse, xycoords='axes fraction',fontsize=8, color='gold')

        if resultuse['basecolor']=='red':
            print('in the color red')
            ax3a.annotate(str(np.round(resultuse['basescore'],1)),xy=xyUse,xycoords='axes fraction',fontsize=10,color='red')
            ax3b.annotate(shortStr,xy=xyUse,xycoords='axes fraction',fontsize=10, color='red')
            ax4H.annotate('Task notifications', xy=xyUse, xycoords='axes fraction', weight='bold', fontsize=10) 
            ax4.set_facecolor('lightsalmon')
            ax4.annotate('QA '+shortStr, xy=xyUse, xycoords='axes fraction', fontsize=8, color='red')
            print('dont color red')
        
        ax2Ha.set_xticks([])
        ax2Hb.set_xticks([])
        ax2Hc.set_xticks([])
        ax2Hd.set_xticks([])
        ax2He.set_xticks([])
        ax2a.set_xticks([])
        ax2b.set_xticks([])
        ax2c.set_xticks([])
        ax2d.set_xticks([])
        ax2e.set_xticks([])

        ax3F.set_xticks([])
        ax3T.set_xticks([])
        ax3Ha.set_xticks([])
        ax3Hb.set_xticks([])
        ax3a.set_xticks([])
        ax3b.set_xticks([])

        ax2Ha.set_yticks([])
        ax2Hb.set_yticks([])
        ax2Hc.set_yticks([])
        ax2Hd.set_yticks([])
        ax2He.set_yticks([])
        ax2a.set_yticks([])
        ax2b.set_yticks([])
        ax2c.set_yticks([])
        ax2d.set_yticks([])
        ax2e.set_yticks([])

        ax3F.set_yticks([])
        ax3T.set_yticks([])
        ax3Ha.set_yticks([])
        ax3Hb.set_yticks([])
        ax3a.set_yticks([])
        ax3b.set_yticks([])

        ax4H.set_xticks([])
        ax4H.set_yticks([])
        ax4.set_xticks([])
        ax4.set_yticks([])

        print('SAVING MOCKUP PLOT')
        plt.savefig('{0}_PIPE-692_SSF.png'.format(fig_name),format='png', dpi=100.)
        print('...DONE...')
 

