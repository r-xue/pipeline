import collections
from typing import Dict, List, Optional, Tuple

import copy
import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

#################
# Adapted from PIPE692.py::SSFanalyis. Original history: 
# L T Maud - ESO 
#          - April 2020 0RIG
#          - May 2021 full CASA 6 version mockup
#          - March 2022 CASA 6.4 'class' version for PL 
#          - July 2022 Edit for PL2022 implementation
#            includes logger output messages
#            included bl-name, bl-len, phase RMS in log
# K Berry - NRAO
#          -  July 2023 moved into PL
##################
class SSFheuristics(object):
    def __init__(self, inputsin, outlier_limit, flag_tolerance, max_poor_ant): 
        self.visUse = inputsin.ms.basename
        self.outlierlimit = outlier_limit
        self.ftoll = flag_tolerance
        self.maxpoorant = max_poor_ant 

        self.spwUse = None
        self.fieldUse = None
        self.scanUse = None
        self.refantid = None
        
        self.caltable = [] 
       
        context = inputsin.context
        ms = context.observing_run.get_ms(self.visUse)
        self.caltable = sorted(ms.bp_gaintable_for_phase_rms)

        # Fetch the bandpass phaseup caltable 
        if len(self.caltable) > 0:
            self.caltable = self.caltable[-1]
        else:
            LOG.error("For {}, missing bandpass phaseup caltable and cannot perform decoherence assessment.".format(self.visUse))
            raise
        
        self.refant = inputsin.refant.split(',')[0] # this is the NAME not the index
        self.antlist = []
        # loop ant list for id of refant
        for elma in inputsin.ms.antennas:
            if elma.name == self.refant:
                self.refantid=elma.id
            self.antlist.append(elma.name)

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

        # Get the widest SPW
        # Future improvement: update to use SPW with highest SNR
        for scispw in scispws:
            if scispw.band in bwmaxdict and scispw.bandwidth == bwmaxdict[scispw.band]:
                self.spwUse = scispw.id
                break

        ##################
        ## PIPE-1661 related
        # phasecal field ID out of the ms context passed - need for later flag assessment
        ph_fieldids = [f.id for f in inputsin.ms.get_fields(intent='PHASE')]#, spw=str(self.spwUse))] 
        # traditionally we should only have one phase calibrator
        # but still pull this as a list and use in the _getblflags function
        self.ph_ids=ph_fieldids

        # future note, if we have multiple phase calibrators and these have
        # a different spectral spec, should we do this as per the bandpass
        # i.e. get the scans, then find the id of the field for that scan?
        # we can loop the first scan of a scan list and find the associated 
        # field id - as below. 
        # bnot expected to be used in standard observing practice 
        ####################

        # Get the bandpass scans
        targeted_scans = inputsin.ms.get_scans(scan_intent='BANDPASS', spw=str(self.spwUse))
        bp_scan = []
        bp_field = []
        bp_id = []
        for elm in targeted_scans:
            bp_scan.append(elm.id) # take the ID
            for elms in elm.fields:
                bp_field.append(elms.name)  # fields is a dict again...
                bp_id.append(elms.id) # PIPE-1661

        self.scanUse = bp_scan[0] # we only want the first scan, in case there are multiple for the BP (e.g. spectral scan data - NEEDS TESTING ON) - might want to do a sorted just to be sure. 
        self.fieldUse = bp_field[0]
        self.fieldId = bp_id[0]  # PIPE-1661 needs ID not a name 

        # Check to see if data are ACA or not
        self.PMinACA = self._pm_in_aca()

        # for BP scan only values from function for given scan/field/spw only
        # this is keyed by blname, e.g.  self.baselines['DA42-DA42'] 
        self.baselines = self._getbaselinesproj()

        # Get time/length of the bandpass scan 
        self.totaltime, self.difftime = self._get_bandpass_scan_time()

        # getcycletime will use a lookup if there is 1 or less phase cal scans (PIPE-1848)
        self.cycletime = self._getcycletime()

        # Holder for baseline flags (PIPE-1661)
        self.blflags = self._getblflags()  # index back is 'all' and 'phasecalonly' intents

        # Set holder for outlier antennas related to plotting and scores later
        self.antout = []

        # Setup the main analysis dictionary
        self.allResult = {}

        # Write the seutp info to the log
        self._log_setup_info()

    def analysis(self) -> Tuple[Dict, str, str, List]:
        """
        Do the phase RMS calculation, outlier analysis,
        and return everything required for later scoring and plotting
        """
        self._do_analysis()
        return copy.deepcopy(self.allResult), self.cycletime, self.totaltime, copy.deepcopy(self.antout)

    def _do_analysis(self):
        ''' this is the wrapper to do the phase RMS calculation, outlier analysis,
          and fill the dictionary will everything required for later scoring and plotting

        inputs used are:
                  self.cycletime, self.totaltime, self.PMinACA,
                  self.outlierlimit, self.antout, self.maxpoorant 

        uses functions:
                  self._phase_rms_caltab, self.madcalc

        fills:
                  self.allResults

        dict keys are:
                  blphaserms, blphasermscycle, bllen, blname, blout,
                  blphasermsbad, blphasermscyclebad, bllenbad,
                  antphaserms, antphasermscycle, antname,
                  phasermsP80, phasermscycleP80, blP80, blP80orig
        '''
        
        # call to phase_rms_caltab
        # gets baseline based phase RMS and antenna based phase RMS for
        # the total time (i.e. length of BP scan) and 
        # the cycle time (time it takes to cycle the start of a phase cal scan to the next - ties with a 'decohernce time' over the target)

        if self.cycletime < self.totaltime:
            self.allResult= self._phase_rms_caltab(timeScale=self.cycletime) # if cycle time is shorter we pass the option so it gets assessed
        else:
            self.allResult= self._phase_rms_caltab() # otherwise no options added and cycletime value will equal total time value

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
            blACAid = np.array([blid for blid in range(len(self.allResult['blname'])) if 'PM' not in self.allResult['blname'][blid]])
            antACAid = np.array([antid for antid in range(len(self.allResult['antname'])) if 'PM' not in self.allResult['antname'][antid]])
 
            ## reset allResult to exclude the PM baselines, and PM ants only
            bl_keys=['blphaserms', 'blphasermscycle', 'bllen', 'blname'] # PIPE-1633
            ant_keys=['antphaserms', 'antphasermscycle', 'antname'] # PIPE-1633
            for bl_key in bl_keys:
                self.allResult[bl_key]=np.array(self.allResult[bl_key])[blACAid]
            for ant_key in ant_keys:
                self.allResult[ant_key]=np.array(self.allResult[ant_key])[antACAid]

            
        # get 80th percentile baseline length and ID of all baselines above it
        self.allResult['blP80orig'] = np.percentile(self.allResult['bllen'], 80) ## was xy

        #PIPE-1662 P80 to acount for flagged antennas, i.e work from P80 of the 'good' data
        self.allResult['blP80'] = np.percentile(np.array(self.allResult['bllen'])[np.isfinite(np.array(self.allResult['blphaserms']))], 80) 
        #####

        ID_all80 = np.where(np.array(self.allResult['bllen'])> self.allResult['blP80'])[0]  ## was xy

        # First calculation of the representative phase RMS values
        self.allResult['phasermsP80'] = np.median(np.array(self.allResult['blphaserms'])[ID_all80[np.isfinite(np.array(self.allResult['blphaserms'])[ID_all80])]])
        self.allResult['phasermscycleP80'] = np.median(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        phaseRMScycleP80mad = self.mad(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        
        # now begin the outlier checks
        # first check if any antennas are just above 100 deg phase RMS (this means ~pure phase noise for phases between -180 to +180 deg)
        # so sensible to identify these antennas - over total time
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
            # ant phase on cycle time !
            ID_poorant = np.where(np.array(self.allResult['antphasermscycle'])[np.isfinite(self.allResult['antphasermscycle'])]>statsoutlierlimit)[0]
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.antout.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])
        
            # max limit on number of 'bad' antennas to be allowed to exclude from calculations (set at 11 good as tested in PIPE692)
            # we clip them out and recalculate - score function also tracks and gives a warning
            # if >11, basically the data is rubbish, so don't clip and let scores be low
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
                # none the 'bad' entires in dict 
                self.allResult['blphasermsbad']=None
                self.allResult['blphasermscyclebad']=None
                self.allResult['bllenbad']=None
        else:
            # this else is for <50deg phase RMS where we do not recalcualte the phase RMS as its low already
            # but we still want to identify any outliers to notify in the messages
            statsoutlierlimit = np.max([self.allResult['phasermscycleP80'] + 6.*phaseRMScycleP80mad,2.*self.allResult['phasermscycleP80']])
            # outlier on cycle time 
            ID_poorant = np.where(np.array(self.allResult['antphasermscycle'])[np.isfinite(self.allResult['antphasermscycle'])]>statsoutlierlimit)[0]
            # add them to the list so score code picks them up if required and changes the messages
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.antout.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])

            # none the 'bad' entires in dict 
            self.allResult['blphasermsbad']= None
            self.allResult['blphasermscyclebad'] = None
            self.allResult['bllenbad'] = None

        # now loop over and log write the phase RMS parameters
        # sort by baseline len
        LOG.info(' Phase RMS calculated over the cycle time as function of baseline length')
        for bnblph in list(zip(np.array(self.allResult['blname'])[np.array(self.allResult['bllen']).argsort()],np.array(self.allResult['bllen'])[np.array(self.allResult['bllen']).argsort()],np.array(self.allResult['blphasermscycle'])[np.array(self.allResult['bllen']).argsort()])):

            # add simple text to flagged (i.e. we made nan for the calculation), or outlier antennas (outlier comes first)
            if str(bnblph[0].split('-')[0]) in self.antout or str(bnblph[0].split('-')[1]) in self.antout:
                LOG.info(str(bnblph)+' - outlier')
            elif not np.isfinite(bnblph[2]):
                LOG.info(str(bnblph)+' - flagged')
            else:
                LOG.info(str(bnblph))


        if len(self.antout)>0:
            antoutstr = ",".join(self.antout)
            LOG.info(" Possible high phase RMS on antenna(s): "+str(antoutstr))

        # Save off spw, scan, field
        self.allResult['spw'] = self.spwUse
        self.allResult['scan'] = self.scanUse
        self.allResult['field'] = self.fieldUse

    # Used by __init__ 
    def _pm_in_aca(self):
        ''' Check if the array is ACA and has PM antennas

        input used:
                 self.antlist

        returns: 
                Bool based on PM with CM antennas
        '''
        antUse = self.antlist
        antUse = ",".join(antUse) # See: PIPE-1633

        if 'CM' in antUse and 'PM' in antUse and 'DA' not in antUse and 'DV' not in antUse:
            PMincACA = True
        else:
            PMincACA = False

        return PMincACA

    def _getbaselinesproj(self, fieldin=None):
        ''' Code to get the projected baseline from the openend 
        visibilitiy file already - these are ordered in 
        terms of antennas. This is a modified stand alone version
        similar to the getProjectedBaselines from Todd Hunter's aUs.

        returns a dict of lengths which are BL name keyed
        e.g. bllens[DA41-DA42] - key is name ant 1 - dash - name ant 2
        '''
        with casa_tools.MSMDReader(self.visUse) as msmd:
            spwchan = msmd.nchan(self.spwUse)
            datadescid = msmd.datadescids(spw=self.spwUse)[0]
        
        with casa_tools.MSReader(self.visUse) as ms:
            ms.selectinit(datadescid=datadescid)
            ms.select({'uvdist': [1e-9,1e12]}) # avoid auto corr
            ms.selectchannel(1, 0, spwchan, 1) # data structure related 
            ms.select({'scan_number': int(self.scanUse)})
            if fieldin:
                ms.select({'field_id': int(fieldin)})
            alldata = ms.getdata(['uvdist', 'antenna1', 'antenna2']) 

        ## the length of e.g. alldata['uvdist'] is >total no. of Bls - it loops over all time stamps of the BP 
        ## we need a mean of the unique values (as Todd's aU) otherwise we just get the first time entry in the below
        bldict = {}
        uniBl = []
        baselineLen = {}
        for allID in range(len(alldata['uvdist'])):
            myBl = '%s-%s' %(self.antlist[alldata['antenna1'][allID]], self.antlist[alldata['antenna2'][allID]])
            thelen = alldata['uvdist'][allID]
            if myBl not in bldict:
                bldict[myBl]=[]
            bldict[myBl].append(thelen)
            uniBl.append(myBl)

        uniBl = np.unique(uniBl)

        for myBl in uniBl:
            baselineLen[myBl]= np.mean(bldict[myBl]) # this has a list for each 
        
        # order irrelavant as keyed here with BL Name
        return baselineLen
    
    def _get_bandpass_scan_time(self):
        ''' Read a caltable file and return time
        shifted to start at zero seconds starting time.
        Note these are the recored times in the caltable

        uses inputs:
               self.caltable, self.spwUse, self.scanUse, self.antlist
        
        :returns: total time of baseline scan, average integration time 
	    :rtype: float, float
        '''
        
        with casa_tools.TableReader(self.caltable) as tb:
            nant = len(self.antlist)
            timeX = []
            antid = 0
            while len(timeX) < 2:
                tb1 = tb.query("ANTENNA1 == %s  && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s"%(antid, self.spwUse, self.scanUse))
                timeX = tb1.getcol('TIME')
                antid += 1
                if antid == nant - 1:
                    break
            tb1.close()

        # If there is no time found (should never happen), throw.
        if len(timeX) < 2:
            raise Exception("No times found in bandpass caltable: {}".format(self.caltable))

        timeX = np.array(timeX) - timeX[0]
        total_bp_scan_time = timeX[-1]
        avg_integration_time = np.median(np.diff(timeX))

        return total_bp_scan_time, avg_integration_time

    def _getcycletime(self):
        """
        Computes the median time (in seconds) between visits to the specified intent.
        Note that other parts of the ALMA project consider the "cycleTime" to be the 
        scan duration on the science target before going back to the phase calibrator,
        i.e. ignoring the duration of the phasecal scan, the ATM cal scans, the 
        checksource, and all the slewing and overhead.
        -Todd Hunter ORIG in analysis Utils (cycleTime)
        - LM edited for this code
        - PIPE-1848 diverts to a lookup table if cycle time is not found

                input used:
                self.visUse

        return: the cycletime (float)
        """
        with casa_tools.MSMDReader(self.visUse) as msmd:
            scans = msmd.scansforintent('*PHASE*')

            # PIPE-1848: if there is no phase calibrator we cannot get cycle time either
            if len(scans) == 0:
                # No phase calibrator in the data
                LOG.warning("Using lookup Cycle Time as there is no PHASE intent in {}".format(self.visUse))
                usecycle = self._lookupcycle()
                return usecycle

            # PIPE-1848
            # changed as before tired to get times even for one scan, but it is pointless
            # already use the lookup 
            if len(scans) == 1:
                LOG.warning("Using lookup Cycle Time as there is only 1 PHASE calibrator scan for {}".format(self.visUse))
                usecycle = self._lookupcycle()
                return usecycle

            # all correectly formed data should go here
            # now get the times for the scans and work out the cycle time 

            times = []

            for scan in scans:
                times.append(np.min(msmd.timesforscan(scan)))

        if len(times) == 1:
            LOG.warning("There was only 1 scan with this intent.")

        diffs = np.diff(times)
        return np.median(diffs)

    def _lookupcycle(self):
        """ Code to lookup the nearest default cycle time 
        as using best practices for Baseline length and 
        the frequency band - this is ONLY needed when the 
        returned cycle time is None - which would only happen
        for malformed data with only one PHASE cal scan,
        that ultimately should not be coming to PIPELINE at all
        PIPE-1848

        """
        if self.PMinACA:
            config=0
        else:
            config = self._getconfig()  # as configs run 1 to 10, 0 is ACA 

        bandu = self._getband()  # this gets freq then band - run 1 to 10

        # just a big list of lists for cycle times these are for Cycle 10
        # BAND 1 is missing, I do not know these
        # [config][band]
        # band index 0 doesnt exist padded with 999
        cycletimes = [[999,999,999,660,660,480,540,480,480,360,360],  
                      [999,999,999,630,630,450,510,390,390,270,270],  
                      [999,999,999,630,630,450,510,390,390,270,270],  
                      [999,999,999,630,630,450,510,270,270,210,210],  
                      [999,999,999,630,630,450,510,270,270,210,210],  
                      [999,999,999,390,390,270,210,130,130,130,130],  
                      [999,999,999,390,390,270,210,130,130,130,130],  
                      [999,999,999,80,80,80,80,80,80,80,80],  
                      [999,999,999,80,80,80,80,80,65,58,45],   
                      [999,999,999,80,80,80,80,80,65,58,45],  
                      [999,999,999,80,80,80,80,80,65,58,45]]  

        cycletime = float(cycletimes[config][bandu])
 
        return cycletime
    
    def _getfreq(self):
        """ wrap in a function here as we open msmd
        return is in Hz used for getting the band

        PIPE-1848
        """
        #TODO: update to fetch from context
        with casa_tools.MSMDReader(self.visUse) as msmd:
            freqval = np.median(msmd.chanfreqs(self.spwUse)) # otherwise all channels 
    
        return freqval
    
    def _getband(self):
        ''' Identify the Band for specific frequency (in GHz)
        PIPE-1848

        '''
        freq = self._getfreq()
        
        lo=np.array([35,67,84,125,157,211,275,385,602,787])*1e9
        hi=np.array([51,85,116,163,212,275,373,500,720,950])*1e9
        # Set Band 2 to stop at current B3

        bandsel = np.arange(1,len(lo)+1)[(freq>lo)&(freq<hi)][0]

        return bandsel

    def _getconfig(self):
        ''' Identify the configuration based on 
        baseline length - returns as an int to 
        allow a table search for cycle time
        PIPE-1848
        '''

        # self.baselines is a dict, rule is max on a dict returns a
        # key for the max so this is max baseline to use 
        maxbl = self.baselines[max(self.baselines)]
        shortbl=np.array([0,44,160,313,499,783,1399,2499,3599,8499,13899])
        longbl=np.array([45,161,314,500,784,1400,2500,3600,8500,13900,20000]) # upper limit greater than baseline len 
             
        config = np.arange(0,len(longbl))[(maxbl>shortbl)&(maxbl<longbl)][0]

        return config
    
    def _getblflags(self, ant1=None, ant2=None):
        ''' Code to open and close the table for the MS 
        and get the baseline based flags in one lump

        we only pass the spwUse as previously established
        here the assumption is that any phase RMS issue with
        baseline length will be flagged accross all the data,
        true atmosphereic things would not be spectral window based

        Added to support PIPE-1661
        
        inputs -
        needs Measurement set, spw, 
        phase cal field id already known to class
        ant1 and ant2 for the antennas

        result - pass out simple a true or false (could be used for a masked array?)
                 to set that baseline to a nan and thus not be used 
        '''

        #set the flagged baseline return to false
        flaggedbl = False
        # MS reads datadescid not spw id 
        #- need to do the conversion to make sure we use
        # the correct value
        with casa_tools.MSMDReader(self.visUse) as msmd:
            datadescid= msmd.datadescids(spw=self.spwUse)[0]

        with casa_tools.TableReader(self.visUse) as tb:
            if ant1 != None and ant2 != None:
                tb1 = tb.query("ANTENNA1 == %s && ANTENNA2 == %s && DATA_DESC_ID == %s "%(ant1,ant2,datadescid))
            else:
                # Speed up - pull everything in one go not per antena, divide out later in main function
                tb1 = tb.query("DATA_DESC_ID == %s "%(datadescid))

            flags=tb1.getcol('FLAG') ## index is [pol][chan][integration]
            a1s=tb1.getcol('ANTENNA1')
            a2s=tb1.getcol('ANTENNA2') 
            field = tb1.getcol('FIELD_ID')

        return flags, a1s,a2s,field  

    def _log_setup_info(self): 
        LOG.info('*** Phase RMS vs Baseline assessment setup ***')
        LOG.info(' Working on the MS {}'.format(self.visUse))
        LOG.info(' Selected scan {}'.format(self.scanUse))
        LOG.info(' Selected spw {}'.format(self.spwUse))
        LOG.info(' Using field {}'.format(self.fieldUse))
        LOG.info(' Using caltab {}'.format(self.caltable))
        LOG.info(' The refant is {} id {}'.format(self.refant, self.refantid))
        LOG.info(' Is ACA with PM data {}'.format(self.PMinACA))
        LOG.info(' Total BP scan time {}'.format(self.totaltime))
        LOG.info(' Phase referencing cycle time {}'.format(self.cycletime))
        LOG.info(' The median integration time {}'.format(self.difftime))

    # Used by _do_analysis()
    def _get_cal_phase(self, ant):
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
        with casa_tools.TableReader(self.caltable) as tb:
            tb1 = tb.query("ANTENNA1 == %s && ANTENNA2 == %s && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s "%(ant, self.refantid, self.spwUse, self.scanUse))
            cal_phases = tb1.getcol('CPARAM')
            cal_phases = np.angle(cal_phases[0][0])  ## in radians one pol only

            ## code works fine as tested in PIPE 692 with only XX pol extractions
            ## not 'really' required to do both pols, but could do so, and also get an average
            ## single and full-pol to check, this single pull will work, but with more then the order differs (c.f. renorm pol code)

            # Exclude flagged data as it is extracted from the gaintable   
            # this is antenna based only       
            flags = tb1.getcol('FLAG') ## [0][0]
            tb1.close()

        cal_phases = [val if not flags[0][0][id_u] else np.nan for id_u, val in enumerate(cal_phases)]  ## so only one pol used 'X'
        cal_phases = np.array(cal_phases)

        # Correct wraps in phase stream     
        # Note: everything is in radians
        cal_phases = self.phase_unwrap(cal_phases)
        return cal_phases #float array of the phases of an antenna

    def _phase_rms_caltab(self, antout : list=[], timeScale=None): 
        ''' This will run the loop over the caltable
        and work out the baseline based phases and calculate the 
        phase RMS. It will also get the Phase RMS per antenna (with
        respect to the refant - i.e. ant based phase RMS) these are all
        passed back to the main class as self.allResult is filled 
        
        inputs used:
              self.antlist, self.ftoll, self.difftime, self.baselines

        calls functions:
              self._get_cal_phase, self.ave_phase, self.stdc
 
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

        # PIPE-1661 baseline based flag awareness - these store flags 
        # that we read in - just in case we 'need' this information
        rms_results['blflags']=[]

        nant=len(self.antlist)
        iloop = np.arange(nant-1) 

        for i in iloop:
            # ant based parameters
            pHant1 = self._get_cal_phase(i)
            rms_results['antname'].append(self.antlist[i]) 

            # make an assessment of flagged data for that antenna
            if len(pHant1[np.isnan(pHant1)]) > self.ftoll*len(pHant1):
                rms_results['antphaserms'].append(np.nan)
                rms_results['antphasermscycle'].append(np.nan)
                
            else:
                ## do averaing -> 10s
                pHant_ave = self.ave_phase(pHant1, self.difftime, over=10.0) # for thermal/short term noise
                rmspHant_ave = np.std(np.array(pHant_ave)[np.isfinite(pHant_ave)])
                rms_results['antphaserms'].append(rmspHant_ave)
                if timeScale:
                    rmspHant_ave_cycle = self.stdc(pHant_ave, self.difftime, over=timeScale) 
                    rms_results['antphasermscycle'].append(rmspHant_ave_cycle)
                else:
                    rms_results['antphasermscycle'].append(rmspHant_ave)


            jloop = np.arange(i+1, nant) # so this is baseline loop 
            for j in jloop:
                ## get the phases - needs to be the single read in table 
                
                #pHant1 is read in above already
                pHant2 = self._get_cal_phase(j)
                # phases from cal table come in an order, baseline then is simply the subtraction
                pH= pHant1 - pHant2 
                    
                # fill baseline information now
                rms_results['blname'].append(self.antlist[i]+'-'+self.antlist[j])
                # OLD (new) WAY from context - average all as overview
                # not direcrtly the BP only
                #rms_results['bllen'].append(float(self.baselines.get_baseline(i,j).length.value)) # from context input
                
                rms_results['bllen'].append(float(self.baselines[self.antlist[i]+'-'+self.antlist[j]])) # from function

                # PIPE-1661 - get baseline based flags 
                blisflagged = self._isblflagged(i,j)

                rms_results['blflags'].append(blisflagged)
                ######

                # make assessment if this is a bad antenna
                if self.antlist[i] in antout or self.antlist[j] in antout: 
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)

                # PIPE-1661 assessment (could tie with if above but separated for clarity
                elif blisflagged:  # can only be T/F bool
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)
                ###############
              
                # make an assessment of flagged data
                elif len(pH[np.isnan(pH)]) > self.ftoll*len(pH):
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)

                else:
                    ## do averaing -> 10s
                    # need to get time and then rebin
                    pH_ave = self.ave_phase(pH, self.difftime, over=10.0) # for thermal/short term noise 
                    rmspH_ave = np.std(np.array(pH_ave)[np.isfinite(pH_ave)])  
                    rms_results['blphaserms'].append(rmspH_ave)
                    if timeScale:
                        rmspH_ave_cycle = self.stdc(pH_ave, self.difftime, over=timeScale)   
                        rms_results['blphasermscycle'].append(rmspH_ave_cycle)
                    else:
                        rms_results['blphasermscycle'].append(rmspH_ave)

        # set RMS output in degrees as we want
        for key_res in ['blphaserms', 'blphasermscycle', 'antphaserms', 'antphasermscycle']:
            rms_results[key_res]= np.degrees(rms_results[key_res])
        
        # this is a dict
        return rms_results
    
    def _isblflagged(self, ant1, ant2):
        ''' function to make the assessment of 
        the flags that are saved and return if the
        full baseline is flagged or not
       
        requires the self.blflags
        '''

        # in checking the order of data read associated with each antenna
        # the ordering system is not 100% preordained it seems,
        # i.e. we have to select each time the index of bls in agreement with 
        # the antenna pair and antenna list - then check the flags
 
        # simply need to select the correct cut for the baseline in question
        # start with the bandpass, if totally flagged assume all data flagged 
        # for that baseline

        # speed assumption is we will hit a false before a true
        # flag for good data, so assume bad, set to good
        flaggedbl = True 


        # we know 0 index has stuff print again to check

        idbl = np.where((self.blflags[1]==ant1) & (self.blflags[2]==ant2) & (self.blflags[3]==self.fieldId))[0]
        # loop over correct ant, integration time index 
        for iduse in idbl:

            if self.blflags[0].shape[0]*self.blflags[0].shape[1]!= np.sum(self.blflags[0][:,:,iduse]):
                flaggedbl = False
                break

        # else here for Phase cal check if not flagged in BP
        if flaggedbl == False:
            flaggedbl = True
            for phid in self.ph_ids: # usually one phase cal anyway but loop incase multiple
                idbl = np.where((self.blflags[1]==ant1) & (self.blflags[2]==ant2) & (self.blflags[3]==phid))[0] 
                for iduse in idbl:
                    if self.blflags[0].shape[0]*self.blflags[0].shape[1]!= np.sum(self.blflags[0][:,:,iduse]):
                        # if there is unflagged data in this antenna pair and time then 
                        # that baseline is, in fact, not fully flagged
                        flaggedbl = False
                        break

        return flaggedbl 

    # Static methods
    @staticmethod
    def phase_unwrap(phase):
        """ Unwraps the phases to solve for 2PI ambiguities

            Input phases may be nan. 

        	:param phase: phase in radians
        	:type phase: float array
	        :returns: unwrapped phase-array
	        :rtype: float array
        """
        working_phase = phase.copy()
        working_phase[~np.isnan(working_phase)] = np.unwrap(working_phase[~np.isnan(working_phase)]) 
        return working_phase
    
    @staticmethod 
    def mad(data, axis=None):
        ''' This calculates the MAD - median absolute deviation from the median
        The input must be nan free, i.e. finite data 
        :param data: input data stream
        :type data: list or array
        
        :returns: median absolute deviation (from the median)
        :rtyep: float
        '''
        return np.median(np.abs(data - np.median(data, axis)), axis)

    @staticmethod
    def stdc(phase, diffTime, over=120.0):
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
        
        # Use nan versions of numpy functions as some elements of 'phase' might be np.nan to indicate that it was flagged
        std_hold = []

        # Loop over the data in range rounded to size of time step
        for i in range(len(phase) - over):
            std_hold.append(
                np.nanstd(np.array(phase[i:i+over]))
            )

        std_mean = np.nanmean(std_hold)

        return std_mean

    @staticmethod
    def ave_phase(phase, diffTime, over=1.0):
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
        over = np.int(np.round(over / diffTime))
        # Make an int for using elements
        # will be slightlt inaccuracies as there are long timegaps in the data
        # but this is minimal 
        if over > 1.0:
            mean_hold = []
            for i in range(len(phase) - over):
                # this averages/smoothes the data 
                # make nan resistant by ignoring the nan values
                mean_hold.append(
                    np.mean(np.array(phase[i:i+over])[~np.isnan(phase[i:i+over])])
                    )
        else:
            mean_hold = phase 
            
        return mean_hold
