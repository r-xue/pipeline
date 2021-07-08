# Version 1.0 - LM - 04 June 2021 - Official delivered script to JAO

# if changing version remember to set self.RNversion
# pre-release version history written below


########################################################################
# almarenorm.py
#
#  Script to calculate and optionally apply spectrally-resolved
#  normalization correction for ALMA in the strong-line case.  This is
#  needed when Tsys measurements inadequately recover these lines
#  either because Tsys is measured off-source (12m array and ACA)
#  and/or at insufficient spectral resolution (12m array only).
#
# Usage (from CASA 5.x or CASA 6.1.1-15):
#
#  (assumes this python file exists in the current working directory) 
#  
#  sys.path.append(os.getcwd())  
#  from almarenorm import *    # import 
#  RN=ACreNorm(<msname>)       # create ACreNorm tool (NB this is
#                                 _not_ an ordinary CASA tool)
#
### To examine Tsys spectra for spurious lines - BY EYE:
#
#  RN.plotRelTsysSpectra(fthresh=0.01) - Luke's default 0.01 (1%) 
#                                        old default 0.002 is a very 
#                                        low trigger theshold 0.2% level
#
#  this will also write a Tsys template file but it should always be checked
#  as TDM (12m) Tsys are hard to fit and sometime diverage at edges suggesting
#  to incorrectly flag a broad range of Tsys channel 
#
#
### To evaluate the need for the correction on all FDM science spws 
#   and scans (excluding spws as specified), but making NO change in
#   MS:
#
#  RN.renormalize(docorr=False)
#
# (The renormalize function will nominally auto-detect all FDM and
#  science scans to evaluate - without extra options runs default paramaters)
#
### To make plots (if hardcopy=True, unique names constructed from <msname>
#   will be generated for each plot):
#
#  RN.plotSpectra(hardcopy=True)     # Scan-averaged Renorm spectra
#  RN.plotSpwStats(hardcopy=True)    # Scan/Ant-averaged peak stats
#  RN.plotScanStats(hardcopy=True)   # Scan-dep peak stats
#
## To apply the correction to fully-calibrated data (this scales the
#  CORRECTED_DATA by default, applies the renormalization correction and writes
#  it back (plot* commands can also be run after this):
#
#  RN.renormalize(docorr=True)
#  
#
### Options for the renormalize code
#  spws=[]  - to manually set only certain SPW to be analysed and/or corrected
#  targscans=[]  - to manually set only certain scans to be analysed and/or corrected
#  nfit=5  - polynomial fit to the AC to flatten the 'baseline' of the scaling 'spectrum'
#  bwthresh=120e6  - bandwidth at which to segment a SPW into chunks to fit separately
#                   Luke's default for ALMA-IMF 120e6 - and works well in disctibuted checker version
#                   George's default 64e6 - to fit out problem ATM lines in earlier versions
#                   by having smaller SPW chunks and when used on FAUST data 
#                   (Luke has since coded in a ATM fix)
#  bwdiv= 'odd'  -  options are: (i) bwdiv=None (George's default), i.e. power of 2, can split lines and
#                   sometimes make a discontinuity if poor fits occur
#                   (ii) bwdiv ='odd' (Luke's default) chooses an odd value of nseg 
#                    to divide the SPW (if SPW_BW > bwthresh) - as lines are usually SPW central
#                    checks if the remainded channels of the SPW are masked within the edges
#                    else defaults to (i)
#                    (iii) bwdiv = int, forces nseg=bwdiv, provided dNchan = nchan/nseg is int, or if 
#                   not the remainder channel number is within the masked edge channels - else defaults to (i) 
#  bwthreshspw ={} - added a dict option to allow the specific input of bwthresh 
#                    for specific SPWs, due to needing potentially different 'nsegments' when 
#                    EBs have very different SPW bandwidths - may not be required since May 2021
#                    version - this was work around as ATM windows needed small bwthresh 
#                    to provide a better correction - but Luke now coded ATMs 
#                    to be handled better 
#                    input as e.g. {'22':200e6}
#  docorr=False   - apply or not the correction (False/True)
#                    True will check the MS file to see if there was HISTORY noting the 
#                    renormalization was already applied - blocks a double application
#   editAC = True   - if docorr=True and editAC=True the AutoCorr for all scans will be set to unity
#                         so that a subsequenct application of the renormalization code cannot occur
#                         THIS COULD HAVE A NEGATIVE IMPACT ON SUBSEQUENT PROCESSING??
#  excludespws=[]  - manually exclude SPWs from the automatic SPW list
#  excludeants=[]  - Antennas to excluded - i.e. sets their scaling to 1.0 (no application)
#                    intended use if these antennas are not in the interferometric data
#                    or are miss-behaved in their AutoCorr and might need to be considered
#                    for flagging in the interferometric data
#  excludechan={}  - Dictionary input set these channels to unity - pre May 2021
#                    this was required for excluding the strong
#                    ATM features that do not fit out well over peak transition 
#                    and these channels were incorrectly ascribed a scaling value
#                    - input as strings, e.g. excludechan={'22':'100~150'}
#  fthresh=0.01  - thereshold to show alarm trigger '***' in the verbose output and log
#                   Luke's default 0.01 conforms to 1% scaling, George's was 0.001 (0.1%) 
#  datacolumn='CORRECTED_DATA'  - data column which will have the application applied
#                    - if the code is run post calibration CORRECTED_DATA should be used
#                       and the renormalize process will also take advantaged ignoring
#                      autocorrelations assosicated with flagged interferometric data
#                   - if the code is run pre-calibration DATA could be used
#                     but the code may show some strange plots for 'bad' antennas
#                     if they have problemetic AutoCorrelation spectra - for which 
#                    the flags are NOT assessed
#  fixOutliers=True    - use heurisitcs to check antennas in case of outlier scaling values and make
#                  a channel by channel assessment and correct the antenna in question - 
#                  will replace probelm channels or for very bad antennas it will
#                  replace the antennas spectrum with a median spectrum that is representative 
#  mededge=0.01  - option to set 1.0% of edge channels to 1.0 scaling - i.e. stop edge effects
#                   typcially FDM edges are usually very well behaved
#  excflagged=True  - exclude flagged antennas, reads Cross-Corr (XC) flag tables 
#                     per SPW, per Scan, per Field and excludes antennas (set them to scaling 
#                     1.0) if it is fully flagged in the interferometric data. 
#                      Useful as usually the PL calibration flagged bad
#                     antennas and these should not be included in the scaling process
#  diagspectra=True  - plot extra diagnostic spectra, made a scaling spectra plot 
#                      for each SPW, scan, field. Both XX and YY and all ants on same plot
#                      and the respective median representative spectra
# usePhaseAC = False   - use phase cal AC instead of bandpass - usually Phase cal can be
#                     contaminated too, so BP default is best
# plotATM = True     - if diagnostic spectra plots are made, plot also the ATM
#                     transmission profile
# correctATM = False - use the ATM model transmission profiles to try correct for
#                      any ATM residual features that get into the scaling spectra
# limATM = 0.85      - only action on correctATM if the ATM transmission at any point
#                      drops below this value, otherwise the are no 'deep' ATM features 
#                      to try to correct
# docorrThresh = None - left a None by default will use the ALMA set 1.02 (2%) threshold
#                     for application. I.e. if docorr=True only fields in SPW where 
#                      peak scaling >1.02 will be corrected. Input a float to overwrite 
#                     the default values B9 and B10 are set to 1.5 
# verbose = False    - write to terminal all output blurb, this is anyway in a log file
#
## Close the ACreNorm tool (detaches from MS, etc.)
#
#  RN.close()
#
######## VERSION REVISIONS ################
#
# Note a number of esits by Luke were interactive testing
# and have been superseeded in later versions. Effort 
# has been made to indicate this in the revision list
# for information purposes
#
# gmoellen (2020Sep24,2020Aug19,2020Sep16)
# gmoellen (2020Nov16: in plotRelTsysSpectra(): return line channel lists in dict,
#                      segment wide FDM Tsys spws, and use nfit=5)
# LM added (2020Dec17: a loop over fields, within the scan loop, all fuctions added with field
#                   read now, if field is None, defaults to data query without specifiying the 
#                   field - e.g. getting the Bandpass does not provide a field
# LM added (2021Jan13: SUPERSEEDED check for outlier antennas (peak scaling >5x the median pk scaing - from 
#                   all ants - as the print out show. Print info about outliers and set
#                   the 'bad' channels to the median of that antenna - if 'dofix=True' 
#                    (CHANGED NAME 31 May 2021) in renormalize
# LM added (2021Jan14:{i}  edge channels in AC can trigger outlier ant code, set 1% edge of bandwidth
#                    to the median scaling value for that antenna - if 'mededge=True' in renormalize
#                    new funtion -- calcSetEdge -- (SUPERSEEDED, mededge is now a value 2021May21)
#                    {ii} check in function -- getACdata -- to see if 'd' (DATA) is filled - otherwise returns 'None'
#                    some ALMA-IMF data are aborted so some fields are empty, these are now ignored 
# LM added (2021Jan19:- exclude antenna manual input option - input a list of strings e.g. ['DA59, DA60']
#                     or IDs [0,1] in the new parameter 'excludeants=[]' to permanantly set that 
#                      scaling to 1.0 for all SPW, scans, fields.
# LM added (2021Jan21:- {i} within the spw, scan, field loop get the cross-corr (XC) flags and find
#                      antennas that are _fully_ flagged and exclude them by setting scaling to 1.0 
#                      new function -- getXCflags -- returns antenna ID list 
#                      {ii} SUPERSEEDED Edited outlier check, finds now per spw, per scan, per field the ants >5x
#                      mean of all ant peaks, and any where -ve are < 10*MAD from 1.0. The MAD is the 
#                      median value of the MADs of all antennas. 
#                      {iii} SUPERSEEDED checking and clipping of the outlier antennas occurs with 'dofix=True'.
#                      If outliers (Ant/Median Scaing spectrum) are >7.5 MAD they are (a) set to the Median
#                      scaling of the antenna in question, (b) if >5% of Bandwidth is flagged, and
#                      only one corellation is effected, we set the problematic corr. to the good corr.
#                      (c) if both corr. are effected and flags >5% we set both to the median correction 
#                      spectrum generated from all antennas - function is calcFixReNorm - and will make plots
# LM added (2021Feb01:- SUPESEEDED 
#                       {i} further refinement to outlier triggering code 2x max peak, 2x min peak (from 1.0)
#                       only occurs if dofix is True now otherwise follows origional code, no antenna checking
#                       {ii} refinement of clipping code. Min threshold is max of(1.0025,10*MAD) 
#                           assessment thereafter .....
#                       {iii} made odd/even check for nseg=2 fits as lines in middle of SPW can cause an issue
#                       {iv} in comparing XX and YY median spectra difference has to be >10% to find 
#                        if there is a birdie in one (i.e. all ants of a given correcation) or not 
#                        - otherwise was triggering when strong lines (CO) differered by more than previous
#                       limit of 10*MAD. (NB this is not the ant clipping just quality check of median spectra)
# LM added (2021Feb02:- For outlier ant plots create a known list, refer to it for low amp (1-2% scalings) and
#                       /or low number changed channels and don't remake a plot if not needed.
#                       - added clause if >10 consequitive channels do replacment of correlation
#
# LM RUNNING VERSION 03 FEB 2021 # - ALMA-IMF delviered 7 B3, and 2 B6 - retracted Band 6 due to discontinuity 
#                                 # issue noted as above odd/even check, changes as below to correct
#                                 # In running ALMA-IMF use bwthresh=120e6 and new option bwdiv= 3 for Band 6 data 
#                                 ### THIS HAS NOW BEEN FURTHER SUPERSEEDED -  18th FEB
#
# LM added (2021Feb09: - SUPERSEEDED input option for calcChanRanges to be any multiple. Previous fixed as power of 2
#                       to break the SPWs into segments, this caused issues for lines in the middle of SPW
#                       with complex structures (inc abs features). Now use option of a multiple, ie. 3
#                      - slight logic changes for replacing channels, for 'almost' continous regions
#                        treat as continous due to piece-meal channel replacements occuring
#                      - added median plots for all scans/fields for checking
# LM added (2021Feb16: -  SUPERSEEDED Changed the outlier ant check to be inside a function, cleaner main code
#                        and changed to do a simple median ant check - this is required as 2x peak
#                        can totally miss any outlier ant that have a large peak scaling of many %
#                        but where 'non-scaled' region could miss-behave and have spikes/jumps/noise
#                      - simply, per ant, divide by median spectrum to see if any are "different"
#                      - set baseline threshold for testing outlier channels to 1.025 (0.25%) 
#                        which is usually notably more than 10*MAD for the antenna spec / median spec
#                        and only for 'line' free segments of the bandwidth
#                      - SUPERSEEDED  added plotMedian - to plot the per spw, per scan, per field scaling
#                         spectra for assessment as defualt - forward look to Pipeline integration/style plots
# LM added (2021Feb18: - adjustments to calcChanRange again, options are: (i) George's default, i.e. power of 2;
#                        (ii) bwdiv ='odd' chooses an odd value of nseg (if SPW_BW > bwthresh) that given dNchan
#                        per nseg as an integer value, or if not edge channel remainder is low and classed as 
#                        edges and set to zero, else default to (i); (iii) bwdiv = int, forces nseg=bwdiv, provided
#                        dNchan = nchan/nseg is int, of if with remainder chans that fall to the edge set 
#                        to ~1.0, else defaults to (i) 
# LM added (2021Feb23: - simple check of the comparions median spectrum to check for birdies, i.e if the 
#                        difference of a channel to med(nch-3:nch+2) is more than max(0.5*pk_val,0.0025) different,
#                        this behaves better than previous comparions of XX and YY 
#                       - ATM lines excluded by means of 'excludechan={}' option
# LM added (2021Feb26:  - nch-2:nch+3, this is correct for middle value analysis
# LM added (2021Mar04:  - Script tidying and running version for most of ALMA-IMF data
#                        plotMedian plots now called plotdiagSpectra, need to set 
#                        diagspectra=True in renormalize call
# LM added (2021Mar26:   - fix of XX-YY replacement logic - no XX was getting the median as YY wrong assesment 
#                         spotted only in case where YY was actually bad too
#                        - fixed that median assessment will not use the flagged antennas
#                         (i.e. those set to 1.0),as if more than half ants are flagged (ACA data)
#                          the median spectrum could have been incorrectly set to 1.0 - use only unflagged ants now
# LM added (2021Mar30    - fix the plotSpectra to use nanmean to account for flagged ants set to scaling 1.0
#                        - added logic before fitting rnstats['N'] to not include flagged ants
#                          which are set to one, into otherwise good scans. If all scans for an 
#                          antenna are set to 1.0 (i.e. a flagged ant) then that antenna
#                          will have rnstats['N'] of 1.0, otherwise only uses values !=1.0 
# LM added (2021Apr8    -  log file introduced which will fill a log per run and per EB
#                       - verbose option to provide a 'minimal' terminal output if False
#                         or more details when True
#                       - added bwthreshspw as a dict to all specification of bwthresh for
#                         a particular SPW, in case width to break for nseg needs to be changed
#                         and separate SPW with and without ATM that need more segs to fit 'better'
#
# LM note - running remaining ALMA-IMF QA3 with this code, and ALMA-IMF pilot projects (new QA3)
#           ATM line still needs manually setting to 1.0 to not incorrectly flag
#           new development will fix this
#
# LM added (2021Apr21)  - added usePhaseAC option to instead use the AutoCorr of the phasecal preceeding the target scan
#                         rather than the Bandpass source - closers in Eleveation/airmass, making ATM less of a problem
#                         PROBLEM - if the Phase cal has any emission (usually CO) then it's AutoCorr
#                         is contaminated and so the phase cal cannot be used for doing renormalization - 
#                         *** deemed not good to use by default (discussion with Bill, Baltasar, Antonio, Myself)
#                             use the bandpass always as default as it is 99.9% a clean AutoCorr
#                       - titlein option in plotSpectra will pre-pend this text as a title name if needed
#                         and so the summary spectra plot can easily be identified for large archive data checks
#
# CHECKVERION of code was spun off here - with most of extra plots 'off' and this was used for checking all 
#             ALMA archive data - no majot fails/crashes were noted/reported on possibly problatic data
#             e.g. spectral scans, full-pol, multi-spectra and multi field
#             - note most of Luke's plots/heuristics deal only with dual pol data
#
#
# LM added (2021May21) - edited mededge option to read the actual value of the edge to flag, no longer
#                        true/false, now 0.01 represents flagging of 1% of edge channels of the SPW
#                        and is the default
#                      - Misc code function and plot name changes. Plots and log all have 'ReNorm' in the name
#                      - added function "unityAC" which is triggered if 'editAC' option in renormalization code
#                        is set to true. If this is run, after application of the ReNorm with docorr=True the
#                        AutoCorr of all fields are overwritten with a value of 1.0. This means any
#                        subsequent run of the ReNorm code, will not find any scaling 
#
#                       *** PLWG wanted a second run to possibly return no scaling plots _IF_ the 
#                         renorm correction was already made previously- however later discussion with PLWG
#                         indicated that it is not good practice however to change the AutoCorr like 
#                         this to achieve the aim of a 'non-scaling' plot -  so by default editAC=False,
#                         and new code (below) will simply add a history comment that can be dealt with to just
#                         not re-run any correction again - but plots can still be made if docorr=False ***  
#
# LM added (2021May23   - ATM function ATMtrans to get ATM transmission profile using the attool - credits
#                        also to T.Hunter as I borrowed code extracted from analysisUtils and plotbandpass3 when
#                        needed - this function make the almarenorm.py dependent on the analysisUtils
# LM added (2021May25)  - ATM transmission line included as plot option for the Tsys spectra (getRelTsysSpectra),
#                        diagnostic spectra (plotdiagSpectra), and summary spectra (plotSpectra) - these are
#                        True by default - code will get ATM profiles where required from ATMtrans
# LM added (2021May26)  - History write (recordApply) and read functionality (checkApply), so that
#                        once the ReNorm was applied with docorr=True
#                        a history notification is written, which is always read upon any run of the code with
#                        docorr=True and which will stop and present a warning as it will NOT re-apply the correction 
# LM added (2021May27) - function ATMcorrection which corrects the divided AutoCorr spectra by the 
#                        ratio of the Target and Bandpass (or phasecal) ATM model profiles.
#                       - added options to renormalize function "correctATM" and "limATM"
#                       - correctATM = False (Luke's default as DRs should set to True) -
#                         Luke has tested True on ALMA-IMF data and it makes a reasonable correction.
#                          this makes the ATMcorrection
#                         by:
#                         (i) first setting the divided AutoCorr spectra (Tar_AC/BP_AC) to its median value
#                         which makes the first overall average baseline fit and sets baseline to ~1.0 
#                         (this is actually similar to the initial process George uses in the fitting
#                         of the scaling spectrum)
#                         (ii) doing a ratio of the ATM profiles for the target and BP, and also setting the
#                         baseline to a median value, i.e. ~1.0.
#                         WHY: the action of this baselining to ~1.0 means that when the divided scaling
#                         spectrum is corrected (i.e. multiplied) by the ATM ratio spectrum, then 
#                         most of the values are actually ~1.0 away from strong ATM lines - i.e no effect, only 
#                         regions where deeper ATM profiles (lower transmission) sometimes differ 
#                         noticably between the target(s) and the Bandpass source - due to airmass differnces 
#                         (i.e. elevation) - which is what we want to correct as otherwise the renorm process
#                         cannot fit-out residual ATM lines differnces, and so incorrectly attributes a scaling
#                         value to them - which must NOT be applied
#                        -limATM = 0.85 (Luke's default based on XYZ), after getting the ATM transmission profiles
#                         and if correctATM=True, then the code will only actually attempt the correction if
#                         ATM transmission in the SPW drops below 0.8 (i.e. 80% transmission) at which point
#                         the correction is made by the multiplication of the ratio of ATM profiles described
#                         above
#                        - added start and end CASA log message to track run-time as well as edit to ReNorm log
#
# LMadded (2021May28)  - functionality where band is obtained for the data RN.Band
#                      - RN.bandThresh are set as the hard coded determined thresholds above which 
#                        data are to be corrected - per Band
#                      - docorrThresh=None in renormalize finds the threshold from the above, or
#                        can be a user defined float, as a fraction, e.g. 1.05 which the peak scaling
#                        has to be for a correction to be applied, even if docorr=True, i.e.
#                        we only correct fields that need correction per SPW
#                      - added functionaility at end of renormalize function that checks the 
#                        dictionary RN.docorrApply (if docorr=True) to see (per spw) if
#                        the field will be correced or not. If the dict is empty the 
#                        peak scaling of that field is checked against the threhsold to 
#                        initiate the dictionary
# LMadded (2021May31)  - edited inside the ATMtrans function to ...
#                      - changed 'dofix' option for outlier antennas to 'fixOutliers'
# LMadded (2021June02) - edited to make the entire code (mainly ATMtrans profile function)
#                        totally independent to the analysis utilities
#                        otherwise this cannot run in PL, and users would also have to 
#                        download and install aU for a 'simple' restore.
#                        This uses a bunch of copied/modified code from Todd Hunter's aU.
# LMadded (2021June03) - Final scaling per spw, scan, field is stored in class variable 
#                        self.scalingValues
#                      - Made ATM plot axis label, number work in CASA6
# LMadded (2021June08)  - nan output warning when all data in interferometric are flagged
#                        and we set to 1.0 and then a nan mask to get a true
#                        median creates warnings if all scaling values are 1.0
#                      - stopped overall mean, and medians returning nan in these cases
#                       from nanmean, nanmedian in renormalize code 
#
# ALadded (2021June18) - Edits for PIPE-1168 for plotting in frequency, turning off antenna
#                       diagnostic plots, plotting into a subdirectory, plotting by spectral
#                       window instead of subplots for plotSpectra(), creation of a dictionary
#                       for PL purposes (self.rnpipestats).
# 
########################################################################
# LM final version provided to JAO - 04 JUNE 2021
########################################################################
#
# LM notes for future development - from some discussion with PLWG 
#
# - log messages now are going to a standalone log - these could be changed
#   to casa log messages (and write optional separate log if requested?)
#   * currently have a start and end casa log message only
#
# - ATM correction could add more 'intelligence' to flag or rather exclude
#   (set to 1.0) the scaling where the line is 'deepest' as it is
#   unavoidable that there will be residual 'incorrect' scaling
#   in some data where ATM are present in the same SPW as the 
#   astronomical line we are trying to correct
#  *** needs discussion & testing *** 
#
# - ATM recognition of the SPW and then will automatically increase
#   nseg, for the SPW segmenting for the fitting, as this is a good
#   way to counteract (fit-out) the ATM line, even after it was accounted
#   for by ATMcorrection (correctATM=True). A caveat is that if astronomical
#   lines are broad tend to also get fitted-out which is not the intention
#   usually nseg is small enough so one segment fully covers the 
#   astronomical line and only fits its baseline
#  *** needs discussion & testing ***
#
# - Make work on full Polarization data - i.e. plots/heuristics
#   are assuming dual corr [xx,yy] now 
#
# - Multi-spectra specs are not tested explicitly - in theory should `work'
#   
# - might names to change in plots for PL harvesting
#
# - useful CASA log messages or returns for PL harvesting
#
# - PLWG might want some information to store in MS keywords
#
#
# - the plotSpwStats and plotScanStats coded by George I did not use 
#   very much - the way these stats are currently built is that
#   they only pass the last field of a scan, i.e. the values are 
#   stored in the RN.rnstat dictionary which never had 'field' 
#   coded in. plotSpectra I have modified to do the correct 
#   cumulative average and takes all fields - even this could be 
#   split out into a per field basis
#
# - Active changing of how to divide up SPWs for the fitting process
#   more dependent on the bandwidth 
#   - LM note - 
#   typcially this is relatively insensitive except:
#          -- George used a small value in bwthresh 64e6 as this better
#            fits-out residual ATM profiles in scaling spectra - 
#             especially for FAUST data
#          -- Luke coded an option to set specific 'bothersome'
#             SPWs to different divides as to possibly better
#            fit those with ATM profiles (divide into more nseg chunks
#            so the profile is traced and fitted), or those with respectively
#            broad astronomical lines (i.e. less nseg chunks where CO
#            was so broad the line was broken-up in fitting and 
#            divergence or breaks could be seen - a wider bwthresh
#            allows baseline values at either side of the wide line)
#  however, since May an ATM correction is applied before the fitting which 
#  uses model ATM profiles to remove any residual differences in 
#  the scaling spectra before fitting cause by ATM transmission difference
#  between the Target and Bandpass. Thus fitting now using a default of 
#  120e6 (Luke's value) basically works most data without issue (CONFIRM)
#
# - Checking of current heuristic method, i.e. how 'outlier' ants are 
#   currently replaced - maybe more efficent/effective method ? 
#
#########################################################################

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pylab as pl
import os
from math import *
from datetime import datetime

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
    # casa log import ? 

mytb=tbtool()
myms=mstool()


class ACreNorm(object):


    def __init__(self,msname):

        # Version

        self.RNversion='v1.1-2021/07/06-alipnick'

        # LM added 
        # file for logger named per EB and runtime - will make a new file every run
        nowrun = datetime.now()
        logReNormFile = msname+nowrun.strftime('_ReNormLog%Y%m%dT%H%M%S.log')
        self.logReNorm=open(logReNormFile,'w')
        print('Logger file initiated: '+str(logReNormFile))

        self.msname=msname
        print('Opening ms: '+str(self.msname))
        self.logReNorm.write('Opening ms: '+str(self.msname)+'\n') # LM Added 

        self.msmeta=msmdtool()
        self.msmeta.open(self.msname)

        mytb.open(self.msname)
        self.correxists=mytb.colnames().count('CORRECTED_DATA')>0

        self.wtspexists=False    
        # George's code was already commented - with forward look to figuing out weights....
        # mytb.colnames().count('WEIGHT_SPECTRUM')>0
        # Need to check it is initialized (contains values!)!
        #try:
        #    # Try to extract value from first row
        #    wtsp=mytb.getcol('WEIGHT_SPECTRUM',0,1,1)
        #except:
        #    self.wtspexists=False
        #    pass

        mytb.close()
        print('CORRECTED_DATA exists = '+str(self.correxists))
        self.logReNorm.write('CORRECTED_DATA exists = '+str(self.correxists)+'\n') # LM Added 

        #mytb.open(self.msname+'/ANTENNA')
        #self.nAnt=mytb.nrows()
        #self.AntName=mytb.getcol('NAME') # LM added 
        #mytb.close()
        self.AntName = self.msmeta.antennanames()
        self.nAnt = self.msmeta.nantennas()
        print('Found '+str(self.nAnt)+' antennas')

        #LM added
        self.logReNorm.write('Found '+str(self.nAnt)+' antennas\n') 
        self.AntOut={} # LM Added initiate for outlier antennas channels
        self.replacedCorr={} # LM added 
        # for tracking of outlier ants per spw, scan, field 
        # that were deemed to need more than 10 channels correcting
        self.birdiechan={} # LM added - actually is set but not assessed - was for testing 
        self.atmtrans= {}  # LM added initiated for atmopsheric transmission regions
                           # options in main code to exclude or "fit out"
        self.corrATM = False # global variable as if we set to True but then cannot find
                           # the PWV value (for whatever reason)
                           # correctATM is input in renormalize function

        # ALMA Bands     0       1     2    3    4     5     6     7     8    9    10
        self.usePWV =  [99.0, 5.186,5.186,2.748,2.748,1.796,1.262,0.913,0.658,0.472,0.472] 

        self.scalingValues={}  # for PL to read the levels at SPW, Scan, Field - max scaling value 

        self.TsysReturn={}  # LM added so Tsys flag channel are already passed and stored
        # useful for ALMA production 

        # ALMA Bands        0    1   2    3    4    5    6    7    8    9    10
        self.bandThresh=[99.0, 1.02,1.02,1.02,1.02,1.02,1.02,1.02,1.02,1.50,1.50] 
        # B9/B10 set high now to now correct
        # set a B0 as its way easier just to then index this array with Band

        self.docorrApply={} # - this is to record if the field in a given SPW is
        # above the threshold for application, in which case that field in that
        # SPW will always be corrected and a boolean can be stored in this dictionary
        # one caveat is what about boarderline data around the threshold
        # and some scans are above, and some are below? Do we add a buffer? - it would be wrong
        # to scale e.g. only half the scans for a field. Code is iterative, so doesn't
        # know about later scans in advanced..so must be based on first scan assessment of the field only
        # usually scalng per scan for same fields is roughtly similar/constant 
        
 
        self.states=[]

        ## not used anywhere
        ##self.nx=ceil(sqrt(self.nAnt))
        ###self.ny=floor(sqrt(self.nAnt)+0.5)+1

        self.nfit=5
        self.fthresh=0.001
        
        self.rnstats={}
        myms.open(self.msname)
        spwInfo = myms.getspectralwindowinfo()
        myms.close()

        self.fdmspws=self.msmeta.fdmspws()
        # Make sure there are FDM windows, if not, work around that.
        if len(self.fdmspws) != 0:
            self.tdm_only = False
            bandFreq = spwInfo[str(self.fdmspws[0])]['Chan1Freq']
            self.num_corrs = self.msmeta.ncorrforpol(self.msmeta.polidfordatadesc(self.fdmspws[0]))
            #mytb.open(self.msname+'/SPECTRAL_WINDOW')
            # try to get the reference frequency directly but REF_FREQUENCY doesn't exist for
            # older data so in those cases we just take the mean of the spw. 
            #try: 
            #    bandFreq = mytb.getcol('REF_FREQUENCY')[self.fdmspws[0]]
            #except RuntimeError:
            #    bandFreq = pl.mean(mytb.getcell('CHAN_FREQ',[self.fdmswps[0]]))
            #mytb.close()
        else:
            print('No FDM windows found! Renormalization unnecessary.')
            self.logReNorm.write('No FDM windows found! Renormalization unnecessary.')
            self.tdm_only = True
            bandFreq = spwInfo['0']['Chan1Freq']
        
        self.Band = int(self.getband(bandFreq))


        # warnings that give nan slice back or empty mean
        warnings.filterwarnings(action='ignore', message='All-NaN slice encountered')
        warnings.filterwarnings(action='ignore', message='Mean of empty slice')

        # LM added initialization for max value of scaling peak
        self.MaxOutScaling=1.0

    def __del__(self):
        print('Closing msmd tool.')
        pl.close(11)
        pl.close(12)
        pl.close(13)
        pl.close(14)
        pl.close(15)
        self.msmeta.close()

    def close(self):
        self.logReNorm.write('Closing msmd tool.\n') # LM added 
        self.logReNorm.close() # LM added close logger 
        self.rnstats=[]
        self.__del__()

    def chanfreqs(self,ispw):
        return self.msmeta.chanfreqs(ispw)
        
    def getBscan(self,spw,verbose):
        Bspw=[]
        Bscans=self.msmeta.scansforintent('*BANDPASS*')
        Spwscans=self.msmeta.scansforspw(spw)
        for iB in Bscans:
            mask=Spwscans==iB
            for iS in Spwscans[mask]:
                Bspw.append(iS)
        if verbose:
            print(" Bandpass scan(s): "+str(Bspw))
        self.logReNorm.write(' Bandpass scan(s): '+str(Bspw)+'\n') # LM added

        return Bspw

    # LM added function - get Phasecal scan
    # - could have just added options to BP scan function but this is explicit
    def getPhscan(self,spw,verbose):
        PHscan=[]
        Phscans=self.msmeta.scansforintent('*PHASE*')
        Spwscans=self.msmeta.scansforspw(spw)
        for iPh in Phscans:
            mask=Spwscans==iPh
            for iS in Spwscans[mask]:
                PHscan.append(iS)
        if verbose:
            print(" Phase calibrator scan(s): "+str(PHscan))
        self.logReNorm.write(' Phase calibrator scan(s): '+str(PHscan)+'\n') 

        return PHscan
        
    def getStateIdsForIntent(self,intent):
        if len(self.states)==0:
            mytb.open(self.msname+'/STATE')
            self.states=mytb.getcol('OBS_MODE')
            mytb.close()
        
        return list(pl.array(range(len(self.states)))[pl.transpose([intent in y for y in self.states])])


    def getBtsysscan(self,spw, verbose=True):   # LM added verbose option
        
        Bflds=list(self.msmeta.fieldsforintent('*BANDPASS*'))  # Bandpass field(s)
        Bscans=[]
        for iBfld in Bflds:   # handle multiple B calibrators (needed?)
            Bscans=Bscans+list(self.msmeta.scansforfield(iBfld))     # Scans on Bandpass field(s)
        TsysScans=list(self.msmeta.scansforintent('*ATM*'))  # all Tsys scans
        SpwScans=list(self.msmeta.scansforspw(spw)) # Scans in specified spw
        
        # [scan in [scans on Bfld among Tsys scans] among spw scans]
        BTsysScans=[iscan for iscan in [iscan1 for iscan1 in TsysScans if iscan1 in Bscans] if iscan in SpwScans]

        if len(BTsysScans)<1:
            print('Could not find Tsys scan on B calibrator in spw='+str(spw))
            return None  ## LM added was  []
        
        if verbose:
            print('Tsys scan(s) for bandpass calibrator(s) {0} in spw={1} are: {2}'.format(Bflds, spw, BTsysScans))
        self.logReNorm.write('Tsys scan(s) for bandpass calibrator(s) '+str(Bflds)+' in spw='+str(spw)+' are:'+str(BTsysScans)+'\n') # LM added

        # already a list
        return BTsysScans

    ## LM ADDED function (actually not even used -> just called msmeta in the renormalise function)
    def getfieldforscan(self,scan):
        TFields=list(self.msmeta.fieldsforscan(scan))

        # make a list
        Tarflds=[itar for itar in TFields]

        if len(Tarflds) < 1:
            print('Could not find a Target field in scan='+str(scan))
            return []

        print('Target field(s) for scan {0} are: {1}'.format(scan,Tarflds))

        # already a list
        return Tarflds

    ## LM added - edited to add field input and query if data is filled
    def getACdata(self,scan,spw,field,rowave=False,stateid=[]):
        if field is None:  # for bandpass case
            self.logReNorm.write('  Extracting AUTO-correlation data from spw='+str(spw)+' and scan='+str(scan)+'\n') # LM added
        else:
            self.logReNorm.write('  Extracting AUTO-correlation data from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field)+'\n') # LM added

        sortlist=''
        if rowave:
            sortlist='ANTENNA1'

        mytb.open(self.msname)
        ddid=str(list(self.msmeta.datadescids(spw)))
        if field is not None:  # i.e. science data
            quer='SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1==ANTENNA2 && FIELD_ID=='+str(field) 
        else:
            # because the getTsysSpectra passes scans but wont pass the field or Bandpass doesn't pass field actually
            quer='SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1==ANTENNA2' 

        if len(stateid)>0:
            quer=quer+'&& STATE_ID IN '+str(stateid)
        st=mytb.query(quer,sortlist=sortlist)

        d=st.getcol('DATA').real

        # need a check here for failed data read - happens for aborted data (e.g. in Mosaics ALMA-IMF)
        # aborted means in a given scan the 'last' fiels is not necessarily recorded
        # simply

        if len(d)>0:
            if d.shape[0] == 4:
                d=d[0::(d.shape[0]-1),:,:]   #  parallel hands only for full pol data
            a1=st.getcol('ANTENNA1')  ## WARNING will CASA 6.2 obey row ordering if getcol ANT is not called with DATA ??
            st.close()
            mytb.close()

            dsh=d.shape
            if rowave and dsh[2]%self.nAnt==0:
                dsh2=(dsh[0],dsh[1],self.nAnt,int(dsh[2]/self.nAnt))  ## for CASA6/Py3  LM added int - as it was otherwise py2 is a float by default
                d=pl.mean(d.reshape(dsh2),3)
                a1=pl.sum(a1.reshape(dsh2[2:]),1)//dsh2[3]
                #print(a1) # for checking in early code
        else:
            d = None # new return which is analysed in the renorm code now 
            mytb.close()
        return d

    # LM added field 
    def getXCdata(self,scan,spw,field,datacolumn='CORRECTED_DATA'):
        print('  Extracting CROSS-correlation data from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))
        self.logReNorm.write('  Extracting CROSS-correlation data from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field)+'\n') # LM added

        mytb.open(self.msname)

        if mytb.colnames().count(datacolumn)==0:
            mytb.close()
            print('ERROR: '+str(datacolumn)+' does NOT exist!')
            self.logReNorm.write('ERROR: '+str(datacolumn)+' does NOT exist!\n') # LM added 
            return

        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        cd=st.getcol(datacolumn)  ## WARNING CASA6.2 might not obey row order 
        a1=st.getcol('ANTENNA1')
        a2=st.getcol('ANTENNA2')
        st.close()
        mytb.close()

        return (a1,a2,cd)

    # LM added this function -  mimics part of getXCdata, but gets the FLAG column
    def getXCflags(self,scan,spw,field,datacolumn='FLAG', verbose=False):
        if verbose:
            print('  Extracting CROSS-correlation flags from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))
        self.logReNorm.write('  Extracting CROSS-correlation flags from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field)+'\n') # LM added
            
        mytb.open(self.msname)

        if mytb.colnames().count(datacolumn)==0:
            mytb.close()
            print('ERROR: '+str(datacolumn)+' does NOT exist!')
            return

        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        cd=st.getcol(datacolumn) ## WARNING CASA6.2 might not obey row order - this might be important as we rely that this order is the same as the XC data extracted
        a1=st.getcol('ANTENNA1')
        a2=st.getcol('ANTENNA2')
        st.close()
        mytb.close()

        # idea is actually to return a list of fully flagged antennas

        # each row in the XC flags is a unique antenna pair
        # loop the rows and make a list of those that are NOT fully flagged, then use exclusion 
        # comparing to the list of all antennas

        gdants=[]
        for irow in range(cd.shape[2]):
            if pl.sum(cd[:,:,irow]) != self.num_corrs*cd.shape[1]: # i.e here we are summing all corrs and the spectral axis
                gdants.append(a1[irow])
                gdants.append(a2[irow])

        # now make an exclusion cause
        aout = [iant for iant in range(self.nAnt) if iant not in gdants]

        # probably this could be coded better - instead of the loop??? - but is relatively fast as robust 

        return (aout)

    #LM added field
    def putXCdata(self,scan,spw,field,cd,datacolumn='CORRECTED_DATA'):
        print('  Writing CROSS-correlation data to spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))
        self.logReNorm.write('  Writing CROSS-correlation data to spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field)+'\n') # LM Added

        mytb.open(self.msname,nomodify=False)
        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        d=st.putcol(datacolumn,cd)
        st.close()
        mytb.close()


    # LM added - untiyAC function to set any of the analysed AC to 1.0
    # so the renormalization cannot be rerun - and if it did, the result = 1.0 scaling
    def unityAC(self,scan,spw):
        print('  Writing AUTO-correlation data to spw='+str(spw)+' and scan='+str(scan))
        self.logReNorm.write('  Writing AUTO-correlation data to spw='+str(spw)+' and scan='+str(scan)+'\n') # LM Added
        self.logReNorm.write('  -- this will be set to 1.0 and the ReNormalize code cannot be re-run on these data -- \n') # LM Added

        mytb.open(self.msname,nomodify=False)
        ddid=str(list(self.msmeta.datadescids(spw)))

        sortlist=''
        quer='SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1==ANTENNA2' 
        st=mytb.query(quer,sortlist=sortlist)
        dOrig=st.getcol('DATA')
        # simply set a unity array 
        dOrig.fill(1.0)
        # put back in
        d=st.putcol('DATA',dOrig)
        st.close()
        mytb.close()


    def getTsysSpectra(self,scan,spw):

        # get the sky
        atmoff=self.getACdata(scan,spw,None,True,self.getStateIdsForIntent('ATMOSPHERE#OFF'))

        # get hot
        atmhot=self.getACdata(scan,spw,None,True,self.getStateIdsForIntent('ATMOSPHERE#HOT'))

        # divide sky by hot (removes bandpass, etc.)
        Tatm=atmoff/atmhot

        # norm on Chan axis:
        Tatm=self.normChanAxis(Tatm)

        return Tatm



    def normChanAxis(self,A):
        # this is for the Tsys checking part/plotting 
        (n0,n1,n2)=A.shape

        # Normalize each spectrum by median
        for i2 in range(n2):
            for i0 in range(n0):
                A[i0,:,i2]/=pl.median(A[i0,:,i2])
                
        return A



    def xyplots(self,N):
        nY=int(floor(sqrt(N)+0.5))
        nX=int(ceil(float(N)/nY))
        return nX,nY

        
    def plotSpws(self,hardcopy=True):  # unchanged George's code (expect figure output name nad hardcopy=True -- I've not really used (L.Maud)

        print('Discerning spw intents...')

        specspws=list(self.msmeta.almaspws(tdm=True,fdm=True))

        scispws=list(self.msmeta.spwsforintent('*TARGET*'))
        scispecspws=[ispw for ispw in scispws if ispw in specspws ]
        print('Found resolved Science spws: '+str(scispecspws))

        tsysspws=list(self.msmeta.spwsforintent('*ATM*'))
        tsysspecspws=[ispw for ispw in tsysspws if ispw in specspws ]
        print('Found resolved Tsys spws = '+str(tsysspecspws))
        
        nSpw=len(tsysspecspws)
        pl.ioff()
        pfig=pl.figure(15,figsize=(14,9))
        pl.ioff()
        pfig.clf()

        for ispw in scispecspws:
            f=self.msmeta.chanfreqs(ispw,'GHz')
            pl.plot(f,len(f)*[ispw+0.01],'r-',lw=3)
            pl.text(max(f),ispw+0.01,str(ispw),fontsize=10)

        for ispw in tsysspecspws:
            f=self.msmeta.chanfreqs(ispw,'GHz')
            pl.plot(f,pl.array(len(f)*[ispw])-0.1,'b-',lw=3)
            pl.text(min(f),ispw-0.1,str(ispw)+'-Tsys',ha='right',va='top',fontsize=10)
            #pl.text(min(f),ispw,'Tsys',ha='right',va='center',fontsize=10)


        flo,fhi,spwlo,spwhi=pl.axis()

        tdmspws=self.msmeta.almaspws(tdm=True)
        for ispw in tsysspecspws:
            if ispw in tdmspws:
                f=self.msmeta.chanfreqs(ispw,'GHz')
                pl.plot([f[0]]*2,[spwlo,spwhi],'b:')
                pl.plot([f[-1]]*2,[spwlo,spwhi],'b:')

        pl.xlabel('Frequency (GHz)')
        pl.ylabel('Spw Id')
        pl.title(self.msname,{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
        # CASA 6 units change unless specificed
        pl.ticklabel_format(style='plain', useOffset=False)
        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormSpwVsFreq.png'
            print('Saving hardcopy plot: '+fname)
            pl.savefig('./RN_plots/'+fname)
            pl.close()
        else:
            pl.show()

                   
    def plotRelTsysSpectra(self,spws=[],scans=[],normByB=True,fthresh=0.01,bwthresh=64e6,bwdiv=None,nfit=5,edge=0.025,hardcopy=True,retflchan=False, plotATM=True, verbose=False):
        # LM edited:
        #         made log message 
        #         CASA 6 fix
        #         default alarm/trigger changed to 0.01, i.e. 1% trigger not 0.002 (0.2%) as before
        #         output channels are saved as class value - for later tsys template 
        #         verbose option
        #         plotATM - will also show ATM transmission profile on the Tsys spectra to help DRs
        #         REMOVED fitTDM  - tired to fit TDM spws in chunks, but this diverges at breaks and just is a mess - don't use
    

        # LM Note - the bwthresh, bwdiv etc all work similarly to those for the main renorm code, but are not shared
        #           so the default here is usually kept are per George's setup
        #           this is probably ok, as anyway this is used only to identify possible channels
        #           that need flagging. IF we are to rely on this code later and automate the channels to 
        #           flag in the Tsys, we need very careful testing as the fitting can really diverge at the edges and
        #           causes upswings in the returned profile at the edges of TDM SPWs which are then triggering flags

        usefthresh=self.fthresh
        if fthresh>0.0:
            usefthresh=fthresh

        print('')
        print('Using fractional alarm threshold for Tsys Spectra='+str(usefthresh))
        self.logReNorm.write('Using fractional alarm threshold for Tsys Spectra='+str(usefthresh)+'\n') # LM Added

        self.nfit=nfit

        if type(spws)!=list:
            return str('Please specify spws as a list.')

        if type(scans)!=list:
            return str('Please specify scans as a list.')
            

        # the spws to process (Tsys spws)
        tsysspecspws=[]
        if len(spws)==0:
            specspws=list(self.msmeta.almaspws(tdm=True,fdm=True))
            tsysspws=list(self.msmeta.spwsforintent('*ATM*'))
            tsysspecspws=[ispw for ispw in tsysspws if ispw in specspws ]
            #spws=list(self.msmeta.almaspws(fdm=True))
            print('Found Resolved Tsys spws = '+str(tsysspecspws))
            self.logReNorm.write('Found Resolved Tsys spws = '+str(tsysspecspws)+'\n') # LM Added
        
        else:
            tsysspecspws=spws
            print('User supplied Tsys spws = '+str(tsysspecspws))
            self.logReNorm.write('User supplied Tsys spws = '+str(tsysspecspws)+'\n') # LM Added

        # global list of target scans
        targtsysscans=[]
        if len(scans)==0:
            tsysscans=list(self.msmeta.scansforintent('*ATM*'))
            bandpassfields=list(self.msmeta.fieldsforintent('*BAND*'))
            fldforscans=self.msmeta.fieldsforscans(tsysscans,False,0,0,asmap=True)
            targtsysscans = [iscan for iscan in tsysscans if fldforscans[str(iscan)][0] not in bandpassfields ]
            #targscans=list(self.msmeta.scansforintent('*TARGET*'))
            print('Found science Tsys scans = '+str(targtsysscans))
            self.logReNorm.write('Found science Tsys scans = '+str(targtsysscans)+'\n') # LM Added

        else:
            targtsysscans=scans
            print('User supplied Tsys scans = '+str(targtsysscans))
            self.logReNorm.write('User supplied Tsys scans = '+str(targtsysscans)+'\n') # LM Added

        nSpw=len(tsysspecspws)

        nXspw,nYspw = self.xyplots(nSpw)

        pl.ioff()
        pfig=pl.figure(14,figsize=(14,9))
        pl.ioff()
        pfig.clf()

        cols=['b','g','r','c','m']
        ncol=len(cols)
        k=1

        flch={}

        for ispw in tsysspecspws:

            if normByB:
                # discern the B TSYS scan
                Bscan=self.getBtsysscan(ispw,verbose=verbose)
                if Bscan is None:
                    return
                # and get corr- and ant-dep B, time-averaged 
                Btsys=self.getTsysSpectra(scan=Bscan,spw=ispw)
        
            # Calculate channel chunks                
            (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,verbose=True) # not an option for Tsys (yet - April 2)

            # George's code for chan range - comes later in plots and Tsys edges are masked too 
            nCha=self.msmeta.nchan(ispw)    # Btsys.shape[1]
            chlo=int(ceil(edge*nCha))
            chhi=nCha-1-chlo
            chans=range(chlo,nCha-chlo)

            # LM added
            if plotATM:
                if type(Bscan) is list:
                    Bscanatm=Bscan[0]
                else:
                    Bscanatm=Bscan
                ATMprof = self.ATMtrans(Bscanatm, ispw, verbose=verbose)
                # mask edges same as the Tsys below,i.e. TDM coes from 128 to 120 channels
                ATMprof=ATMprof[chlo:(nCha-chlo)]

            pl.subplot(nYspw,nXspw,k)
            pl.ioff()
            # CASA 6 units change unless specificed
            pl.ticklabel_format(style='plain', useOffset=False)
            if (k-1)%nXspw==0:
                pl.ylabel('Relative Tsys')

            if k>(nSpw-nXspw):
                pl.xlabel('Channel')

            if k==1:
                pl.title(self.msname+' <Nant='+str(self.nAnt)+'> Nscan='+str(len(targtsysscans)),{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
            k+=1

            c=-1

            for iscan in targtsysscans:
                c+=1  # new color each scan

                # this scan's Tsys
                Tsys=self.getTsysSpectra(iscan,ispw)

                # Divid by Btsys to remove atm, mostly
                if normByB:
                    Tsys/=Btsys
                
                # normalize on channel axis
                Tsys=self.normChanAxis(Tsys)
                
                # average over corrs, antennas
                Tsys=pl.mean(Tsys,(0,self.num_corrs))

                # mask edges
                Tsys=Tsys[chlo:(nCha-chlo)]

                # remove broad non-linearity (usually atm) -- this does not trigger for TDM at present (LM - May 25)
                if self.nfit>0:
                    
                    for iseg in range(nseg):
                        # Tsys was masked so rubbish edges are not fitted 
                        lochan=max(iseg*dNchan-chlo,0)
                        hichan=min((iseg+1)*dNchan-chlo,len(Tsys))
                        if lochan<hichan:
                            self.calcReNorm1(Tsys[lochan:hichan],False)

                #for iant in range(Tsys.shape[1]):
                #    pl.plot(Tsys[:,iant])

                pl.plot(chans,Tsys,cols[c%ncol]+'-')


                TsysMax=Tsys.max()
                TsysMedDev=pl.median(pl.absolute(Tsys-1.0))
                TsysSfrac=pl.mean(Tsys)-1.0
                alarm='   '
                flchans=[]
                flchanstr=''
                if TsysMax>=(1.0+usefthresh):
                    flchans=pl.array(pl.where(Tsys>(1.0+usefthresh))[0])
                    nflchan=len(flchans)
                    if nflchan>0:
                        flchans+=chlo
                        spwkey='spw='+str(ispw)
                        #if not flch.has_key(spwkey):  ## HAS_KEY IS NOT IN CASA6 - PYTHON3
                        #    flch[spwkey]={}

                        #LM added for CASA6 functionality (fine for CASA 5 too)
                        if not spwkey in flch.keys():
                            flch[spwkey]={}

                        scankey='scan='+str(iscan)
                        flch[spwkey][scankey]=flchans
                        if verbose:
                            flchanstr=' Found '+str(nflchan)+' channels in SPW '+str(ispw)+' execeeding fractional threshold ('+str(usefthresh)+')' # LM edited to add SPW
                        pl.plot(flchans,[0.999]*nflchan,'ks',markersize=4)
                    alarm=' ***'
                    pl.plot([3*nCha/8,5*nCha/8],[TsysMax]*2,cols[c%ncol]+'-')
                    note1='Peak='+str(floor(TsysMax*10000.0)/10000.0)
                    pl.text(3*nCha/8,TsysMax,note1,ha='right',va='center',color=cols[c%ncol],size='x-small')
                    note2='Intg~'+str(floor(TsysSfrac*10000.0)/10000.0)
                    pl.text(5*nCha/8,TsysMax,note2,ha='left',va='center',color=cols[c%ncol],size='x-small')
                

    
                pstr=" Science Tsys(spw={0:2d},scan={1:3d}): PEAK Frac Line Contrib={2:.4f}{3}  INTEGRATED Frac Line Contrib={4:.4f}"
                if verbose:
                    print(pstr.format(ispw,iscan,TsysMax,alarm,TsysSfrac))
                self.logReNorm.write(pstr.format(ispw,iscan,TsysMax,alarm,TsysSfrac)+'\n') # LM Added

                if len(flchanstr)>0:
                    print(flchanstr)
                    self.logReNorm.write(flchanstr+'\n') # LM Added


                    
            lims=list(pl.axis())
            lims[0]=chlo-1
            lims[1]=nCha-chlo
            lims[2]=min(0.9985,lims[2])
            lims[3]=max(1.15*lims[3]-0.15*lims[2],1.01)
            pl.axis(lims)

            dy=lims[2]*0.1+lims[3]*0.9
            #pl.text(chlo+(nCha-2*chlo)/20,dy,'Spw='+str(ispw)+'    ',ha='center')
            pl.text(nCha/2,dy,'Spw='+str(ispw),ha='center')
            # (skip the following, because it doesn't fit on plot; info is in messages and return value)
            #spwkey='spw='+str(ispw)
            #if flch.has_key(spwkey):   ## HAS KEY DOES NOT WORK IN CASA6
            #LM added for CASA6 functionality (fine for CASA 5 too) - untested here as code was commented out in orig. George's version
            #if spwkey in flch.keys():
            #    nfl=''
            #    comma=''
            #    for isckey in flch[spwkey].keys():
            #        nfl+=comma
            #        nfl+=isckey
            #        nfl+=': '
            #        nfl+=str(len(flch[spwkey][isckey]))
            #        comma=',  '
            #    dy2=lims[2]*0.98+lims[3]*0.02
            #    pl.text(nCha/2,dy2,'Nflag: '+nfl,ha='center',fontsize=8)

            # LM added - want second axis for ATM line 
            # this is to aid DR in understading there is not a problem here
            # similar to pipeline Tsys plots with transmission shown
            # but keep simple with 0 to 100% shown only
            # and plot only once (while looping Target fields) - BP ATM profile is show
            # as the way Geogre coded the plots we can just called pl.twin and 
            # thus 'shifts' to only registering about the new ATM 'axis'
            if plotATM:
                pl.twinx()
                pl.plot(chans,100.*ATMprof,c='m',linestyle='-',linewidth=2)
                pl.ylim(0,100)
                if (k-1)%nXspw==0: 
                    pl.ylabel('ATM transmission (%)')
        
        if nXspw == 3:
            pl.subplots_adjust(wspace=0.35)
        else:
            pl.subplots_adjust(wspace=0.3)

        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_RelTsysSpectra.png'
            print('Saving hardcopy plot: '+fname)
            pl.savefig('./RN_plots/'+fname)
            pl.close()
        else:
            pl.show()

        # LM added - keep the channels in a dictionary 
        self.TsysReturn = flch

        if retflchan:
            return flch



    # LM added / edited lots
    def renormalize(self,spws=[],targscans=[],nfit=5,bwthresh=120e6,bwthreshspw={},bwdiv='odd',docorr=False,editAC = False, excludespws=[],excludeants=[],excludechan={},fthresh=0.01,datacolumn='CORRECTED_DATA',fixOutliers=True,mededge=0.01,excflagged=True, diagspectra=True, antdiagspectra=True, verbose=False, usePhaseAC=False, plotATM=True, correctATM=False, limATM=0.85, docorrThresh=None):
        """
        spws=[]  - to manually set only certain SPW to be analysed and/or corrected
        targscans=[]  - to manually set only certain scans to be analysed and/or corrected
        nfit=5  - polynomial fit to the AC to flatten the 'baseline' of the scaling 'spectrum'
        bwthresh=120e6  - bandwidth beyond which a SPW is split into chunks to fit separately
                         120e6 Luke's default for ALMA-IMF and used in checking version
                         (64e6 George's default for FAUST as some narrow-bw SPW had ATM lines
                         and smaller chunks were used to try fit these out - since Luke has
                         added ATM handelling so such small values might not be required)
        bwdiv='odd'       - options of how to split the SPW for fitting:
                         (i) bwdiv=None (Georges' default), uses powers of 2 based on bandwidth/bwthresh
                         (ii) bwdiv='odd' (Luke's default), divides into best nseg that is odd following
                          nseg = SPWbandwidth/bwthresh and where dNchan=nchan/nseg
                          will remain an integer, i.e. dividing into equal no of channels per
                          chunk for fitting, if not an int, but the remainder chans are in the excluded edge
                          channels then that nseg can also be accepted, else default to (i)
                         (iii) bwdiv=int, will attempt to divide the SPW by nseg = bwdiv, if 
                         the number of channels per nseg is an integer or if not and the remaining
                         channels fall to the flagged edges this is used (e.g 2048/3 
                         -> 3x682 + 2 left over (which are flagged as edges), else defaults 
                         to option (i)
        bwthreshspw ={} - added a dict to allow the specific input of a different bwthresh 
                         for specific SPWs, due to needing potentially various 'nsegments' when 
                         EBs have very different SPW bandwidths - default none
                         needed if some SPW needed attention of different segmenting chunks where 
                         ATM lines were causing an issue (and be redundant since Luke coded in ATM 
                         handelling)
                         specify as e.g., bwthreshspw={'22':64e6}
        docorr=False   - apply the correction or not (False/True boolean)
                         if correction is applied a history note is written into the MS indicating
                         application. The history is check on any docorr=True run as to stop
                         the code from re-running and doing a double application
        editAC = False   - if docorr=True and editAC=True the AutoCorr for all scans will be set to unity
                         so that a subsequenct application of the renormalization code cannot occur
                         as the AC are set to 1.0 and any divide will return a scaling of 1.0
                         *PLWG said maybe not really a great idea as AC are lost so set to False and
                          possibly never use*
        excludespws=[]  - SPW to exclude from the automatically found SPW list
        excludeants=[]  - Antennas to excluded - i.e. sets their scaling to 1.0 (no application)
                          intended use if these antennas are problematic in the interferometric data
                          or need some kind of manual flag - if cross-corr flags not read (excflagges=False)
                          or if run diretly after importasdm - where no flags were applied
                          and problematic antennas are really messing up the plots/application
        excludechan={}  - Dictionary input set these channels to the unity, required for strong
                          ATM features that do not fit 'out' well over peak transition 
                          input format is as strings, e.g. {'22':'100~150'}
        fthresh=0.01  - thereshold to show alarm trigger in the print statements and logs
                         as "***", 0.01 corresponds to the 1% level re scaling (Luke's default)
                         - Geroge's default was 0.001 

                        LM NOTE - this only sets alarm in print outs, BUT could be have functionality of
                                  docorrThresh that I coded in extra     

        datacolumn='CORRECTED_DATA'  - data column to work on / apply to
                   - if the code is run post-calibration CORRECTED_DATA should be used
                      and the renormalize process will also take advantaged ignoring
                      autocorrelations assosicated with flagged interferometric data
                     (if excflagged=True)
                   - if the code is run pre-calibration DATA could be used
                     but the code may show some strange plots for 'bad' antennas
                     if they have problemetic AutoCorrelation spectra - for which 
                    the flags cannot be assessed, e.g.  directly after importasdm
        fixOutliers=True    - check antennas in case of outlier scaling values and then make
                          a channel by channel assessment of the scaling spectrum
        mededge=0.01  - option to set 1.0% (default) of edge channels to median scaling value ~1.0 
                        - i.e. stops edge effects
        excflagged=True  - exclude flagged antennas, reads Cross-Corr (XC) flag tables 
                     per SPW, per Scan, per Field and excludes antennas (set to scaling 
                     1.0) if that antenna is fully flagged. Useful as usually the PL flagged 
                    antennas and these should not be included in the scaling process
                    and could otherwise confuse the scaling and diagnostic plots
        diagspectra=True  - plot extra diagnostic spectra, made a scaling spectra plot 
                      for each SPW, scan, field. Both XX and YY and all ants on same plot
                      and the respective median representative spectra - these will show
                      the actual correct that will be applied per antenna - if there are 
                      outliers or stange spectra, there is a problem !!!
        usePhaseAC = False - this will use the phase calibrator AutoCorr preceeding the target scan
                          rather than using the Bandpass scan - Phase is better -> similar airmass/elev
                          but the phase cal can contain CO, so cannot be used to correct CO in the target
                         (Antonio, Baltasar, Bill, Luke tested - use BP in general as Phase is not trustworthy,
                          there is otherwise no difference given how the renormalize code fits the 
                          AutoCorrelation divided spectra, e.g. target_AC / BP_AC)
        correctATM = True - this will get the transmission profiles for the SPW bandpass (or phase) and target(s)
                           becuase the BP (or phase) and target(s) autocorr are compared in establishing the
                          scaling in cases spectra, there can be issues 
                           where transmission is low and the source elevations are tens of degrees different
                           -- often the ATM is not divided out well and leads to incorrect scaling in ATM regions
                            - this option acts to fix this discrepancy so ATM are handelled correctly
        limATM = 0.85  - combined with correctATM=True, only if the minimal transmission of a SPW drops
                        below this value, does the code even consider to work out the differences between
                        the bandpass (or phase) and target(s) position. If transmission is always high the
                        difference where ATM features occur are negligable and already fit-out
                        within the renormalization code 
        docorrThresh = None - the threshold above which the scaling for a given field in a given spw
                            must exceed along with docorr=True for the reNorm correction to be applied
                            if this param is set to a string None, then automatically use the 
                            values - per band set by ALMA - hard value at 1.02 (i.e. 2%).
        antdiagspectra=True - plot extra diagnostic plots per antenna for diagnosing and fixing bad AC data. 
                            setting to False will set the "doPlot" option to False within the calcRenorm call.
            

        verbose = False - print all messages to terminal that usually go only in the log file
        """



        # LM added - starting CASA logger message
        casalog.post('*** ALMA almarenorm.py ***', 'INFO', 'ReNormalize')   
        casalog.post('*** '+str(self.RNversion)+' ***', 'INFO', 'ReNormalize')   
        casalog.post('*** Beginning renormalization run ***', 'INFO', 'ReNormalize')   

        print('')
        print('*** ALMA almarenorm.py renormalize '+str(self.RNversion)+' ***')

        # added time stamp of the actual renormalize main function
        startrun = datetime.now()
        logReNormStart = startrun.strftime('Starting_ReNormalize_%Y%m%dT%H%M%S')
        self.logReNorm.write(logReNormStart+'\n')


        usefthresh=self.fthresh
        if fthresh>0.0:
            usefthresh=fthresh

        print('')
        print('Using fractional alarm indication threshold for ReNorm ='+str(usefthresh))
        self.logReNorm.write('Using fractional alarm indication threshold for ReNorm ='+str(usefthresh)+'\n') # LM Added

        # LM added 
        if correctATM:
            self.corrATM = True
            print('Will account for any ATM lines within the SPWs')
            self.logReNorm.write('Will account for any ATM lines within the SPWs\n')

        # Handle correction request
        if docorr:
            if datacolumn=='CORRECTED_DATA' and not self.correxists:
                print('Correction of CORRECTED_DATA requested, but column does not exist! Cannot procede.')
                self.logReNorm.write('Correction of CORRECTED_DATA requested, but column does not exist! Cannot procede.\n') # LM Added
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                return

                # old code from George where DATA was copied to CORRECTED ? LM never used 
                #print 'Creating CORRECTED_DATA column.'
                #mycb=cbtool()
                #mycb.open(self.msname,False,True,False)
                #mycb.close()
                #self.correxists=True

            print('The '+str(datacolumn)+' column will be corrected!')
            self.logReNorm.write('The '+str(datacolumn)+' column will be corrected!\n') # LM Added


            # LM added - check of the history - as the renorm code now writes in that application was made
            alreadyApp = self.checkApply()
            if alreadyApp:
                print('')
                print('Correction requested, but these data have already been ReNormalized! Cannot procede')
                print('                      set docorr=False for plots only')
                print('')
                self.logReNorm.write('Correction requested, but these data have already been ReNormalized! Cannot procede.\n') # LM Added
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                return

        else:
            print('No corrections will be applied (docorr=False)!')
            self.logReNorm.write('No corrections will be applied (docorr=False)!\n') # LM added


        # Check if docorrThresh is set correctly
        # Added extra functionality 28 May for a threshold to apply - is automatic, i.e. as set by 
        # meeting 1.02 hard limit (self.bandThresh) or can read docorrThresh as an input to the renormalize function
        # which would overwrite the automatic value
        # 
        # AL - took this out of the above "if" so that hardLim is always defined.
        if docorrThresh is not None:
            if type(docorrThresh) is not float:
                print('Correction of CORRECTED_DATA requested, but docorrThresh is set incorrectly! Cannot procede.')
                print(' set to "auto" for automatic thresholding during apply, or input a float to use') 
                self.logReNorm.write('Correction of CORRECTED_DATA requested, but docorrThresh is set incorrectly! Cannot procede.\n') # LM Added
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                return
            if docorrThresh > 1.5:
                print('WARNING: Correction of CORRECTED_DATA requested, but docorrThresh is set very high')
                print('         docorrThresh is a factor above which to apply the ReNormalization')
                print('         '+str(docorrThresh)+' is very high and it is likely that no data will pass that limit') 
                self.logReNorm.write('WARNING: Correction of CORRECTED_DATA requested, but docorrThresh is set very high \n') # 
                self.logReNorm.write('         docorrThresh is a factor above which to apply the ReNormalization \n') # 
                self.logReNorm.write('        '+str(docorrThresh)+' is very high and it is likely that no data will pass that limit/n') # 
                hardLim = docorrThresh
            else:
                hardLim = docorrThresh
        else:  ## default:
            hardLim = self.bandThresh[self.Band]
        
        if docorr:
            print('####################')
            print('Using Application threshold for ReNorm ='+str(hardLim))
            print('Only spws where fields exceed this will be corrected')
            print('####################')
            self.logReNorm.write('Using Application threshold for ReNorm ='+str(hardLim)+'\n') # LM Added
            self.logReNorm.write('Only spws where fields exceed this will be corrected\n') # LM Added
        else:
            print('Using threshold limit of '+str(hardLim)+' for renormalization determination')
            self.logReNorm.write('Using threshold limit of '+str(hardLim)+' for renormalization determination\n')


        self.nfit=nfit
        self.fthresh=fthresh

        self.rnstats={}
        self.rnstats['inputs'] = {}
        self.rnstats['inputs']['bwthresh'] = bwthresh
        self.rnstats['inputs']['bwdiv'] = bwdiv
        self.rnstats['inputs']['bwthreshspw'] = bwthreshspw


        # the spws to process (FDM only, for now; may also do TDM?)
        if len(spws)==0:
            spws=list(self.msmeta.almaspws(fdm=True)) 
            print('Found FDM spws = '+str(spws))
            self.logReNorm.write('Found FDM spws = '+str(spws)+'\n') # LM added

        else:
            print('User supplied spws = '+str(spws))
            self.logReNorm.write('User supplied spws = '+str(spws)+'\n') # LM added
            # LM added
            if not any(uspw in spws for uspw in list(self.fdmspws)):
                print('User supplied spw(s) are not in the list of FDM spws => '+str(self.fdmspws))
                self.logReNorm.write('User supplied spw(s) are not in the list of FDM spws => '+str(self.fdmspws)+'\n')


        if len(excludespws)>0:
            print('Will exclude spws='+str(excludespws))
            self.logReNorm.write('Will exclude spws='+str(excludespws)+'\n') # LM added

            for espw in excludespws:
                if spws.count(espw)>0:
                    spws.remove(espw)

        print('Will process spws = '+str(spws))
        self.logReNorm.write('Will process spws = '+str(spws)+'\n') # LM added

        self.rnstats['spws']=spws

        # list of target scans if user didn't input any
        if not targscans:
            targscans=list(self.msmeta.scansforintent('*TARGET*'))

        
        print('Will process science target scans='+str(targscans))
        self.logReNorm.write('Will process science target scans='+str(targscans)+'\n') # LM added

        self.nScan=len(targscans)
        
        # this sets up rnstats for later summary plots
        self.rnstats['scans']=targscans
        self.rnstats['rNmax']=pl.zeros((self.num_corrs,self.nAnt,len(spws),len(targscans)))
        self.rnstats['rNmdev']=pl.zeros((self.num_corrs,self.nAnt,len(spws),len(targscans)))
        self.rnstats['N']={}
        self.rnstats['N_thresh']={} # AL added - same as N except only populated when the hardLim is reached


        # LM added - excludeants function
        if len(excludeants) > 0:
            # check type
            if type(excludeants) is str:
                print(' excludeants requires a list of antenna ID(s) or antenna Name(s)')
                print(' e.g. [0,1] or ["DA44","DA45"]')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                return
            else:  # note this does not check if the Antenna is actually in the Antenna Names list
                if type(excludeants[0]) is str:
                    # convert to antenna ID
                    print('Will exclude antennas = '+str((',').join(excludeants)))
                    self.logReNorm.write('Will exclude antennas = '+str((',').join(excludeants))+'\n') # LM added
                    excludeants=[excn for excn,exca in enumerate(self.AntName) if exca in excludeants]
                else:
                    print('Will exclude antennas = '+str((',').join(list(self.AntName[[excludeants]]))))
                    self.logReNorm.write('Will exclude antennas = '+str((',').join(list(self.AntName[[excludeants]])))+'\n') # LM added


        # LM added - excflagged 
        if excflagged:
            print('For each spw, scan, field will exclude fully flagged antennas')
            self.logReNorm.write('For each spw, scan, field will exclude fully flagged antennas\n') # LM added

        # LM added - diagspectra
        if diagspectra:
            print('Will plot diagnostic spectra per spw, scan, field')
            self.logReNorm.write('Will plot diagnostic spectra per spw, scan, field\n') # LM added

        # LM added - excludechan (dictionary)
        if excludechan:
            # checkformats sucessively for fail modes
            if type(excludechan) is not dict:
                print(' excludechan requires a string dict input')
                print(' e.g. {"22":"100~150"}')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                return
            for excch in excludechan.keys():
                if type(excch) is not str:
                    print(' excludechan requires a string dict input')
                    print(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    return
                if int(excch) not in spws:
                    print(' excludechan SPW specified is not a SPW of this dataset')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    return
                if type(excludechan[excch]) is not str:
                    print(' excludechan requires a string dict input for channels')
                    print(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    return
                if '~' not in excludechan[excch]:
                    print(' excludechan requires a channel range separator of "~"')
                    print(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    return

            # LM added - bwthreshspw (dictionary)
            if bwthreshspw:
                # checkformats sucessively for fail modes
                if type(bwthreshspw) is not dict:
                    print(' bwthreshspw requires a string dict input')
                    print(' e.g. {"22":120e6}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    return
                for spwth in bwthreshspw.keys():
                    if type(spwth) is not str:
                        print(' bwthreshspw requires the spw as a string input')
                        print(' e.g. {"22":120e6}')
                        casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                        return
                    if int(spwth) not in spws:
                        print(' bwthreshspw SPW specified is not a SPW of this dataset')
                        casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                        return
                    if type(bwthreshspw[spwth]) is not float:
                        print(' bwthreshspw requires a float for the bw-threshold')
                        print(' e.g. {"22":120e6}')
                        casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                        return

        # AL added - Want to loop over sources so we can disentangle fields and sources and better plot what is happening
        # for mosaics and multi-target observations. 
        target_list = pl.unique(self.msmeta.namesforfields(self.msmeta.fieldsforintent('*TARGET*')))
        for target in target_list:
            self.rnstats['N'][target] = {}
            self.rnstats['N_thresh'][target] = {}
            self.rnstats['inputs'][target] = {}
            self.docorrApply[target] = {} # adding a target parameter for tracking correction application per target

            print('\n Processing Target='+str(target)+' ******************************')
            self.logReNorm.write('Processing Target='+str(target)+' ******************************\n') # AL added
      
            # process each spw
            for ispw in spws:
                # Not all targets are in all scans, we need to iterate over only those scans containing the target
                target_scans = pl.intersect1d(self.msmeta.scansforintent('*TARGET*'), self.msmeta.scansforfield(target))

                # Make an additional cut to catch only those scans which contain the current spw (usually only relevant
                # for spectral scan datasets)
                target_scans = pl.intersect1d(target_scans, self.msmeta.scansforspw(ispw))

                # If user input list of scans to use, cross check those with the list of all scans on targets to make
                # sure it's necessary to perform this loop. 
                target_scans = pl.intersect1d(target_scans, targscans)
                
                # if there is no intersection of the input scan list and the list of scans with this target, break 
                # out of the spw loop and continue on to the next target.
                if len(target_scans) == 0:
                    print('\n Target '+str(target)+' is not contained in the input scan list '+str(targscans)+'. Moving to next target.\n')
                    self.logReNorm.write('\n Target '+str(target)+' is not contained in the input scan list '+str(targscans)+'. Moving to next target.\n')
                    break

                self.docorrApply[target][str(ispw)] = None # instantiating the spw dictionary for this target

                print('\n Processing spw='+str(ispw)+' (nchan='+str(self.msmeta.nchan(ispw))+') ******************************')
                self.logReNorm.write('Processing spw='+str(ispw)+' (nchan='+str(self.msmeta.nchan(ispw))+') ******************************\n') # LM added

                #LM added option to setup for phase cal AC rather than Bandpass AC
                if usePhaseAC:
                    # just pick up the full list of phase cal scans
                    # later work out which one we get the AC from within the loop over the target scans
                    if verbose:
                        print('Will use PHASE calibrator AutoCorr for comparions')
                    self.logReNorm.write('Will use PHASE calibrator AutoCorr for comparions\n') # LM added
                    Phscan=self.getPhscan(ispw,verbose)
                    # still need Bandpass scan for any ATM plots - these are only for illustration
                    Bscan=self.getBscan(ispw,verbose)
                else:
                    # discern the B scan(s) which will be used - otherwise for using the phase AC we get it later
                    self.logReNorm.write('Will use BANDPASS source AutoCorr for comparions\n') # LM added
                    Bscan=self.getBscan(ispw,verbose)
                    # and get corr- and ant-dep B, time-averaged 
                    B=self.getACdata(Bscan,ispw,None,True)
                    # still get Phase cal scans silently - becuase if a correction is applied, and editAC = True
                    # then all scans of the AC should be set to 1.0, to stop any second application
                    Phscan=self.getPhscan(ispw,False)


                # LM added 
                # if correctATM then we need to get the ATM transmission for the bandpass
                # this can happen outside the scan loop below where we get for the target
                # and optionally for the phase cal
                if correctATM:
                    if type(Bscan) is list:
                        Bscanatm = Bscan[0]
                    else:
                        Bscanatm = Bscan
                    if 'BandPass' not in self.atmtrans.keys():
                        # make initiation check as we loop spw then
                        self.atmtrans['BandPass']={}
                    if str(Bscanatm) not in self.atmtrans['BandPass'].keys():
                        self.atmtrans['BandPass'][str(ispw)]={}
                    self.atmtrans['BandPass'][str(ispw)][str(Bscanatm)]=self.ATMtrans(Bscanatm,ispw,verbose=True)
                    # here will make first call to ATMtrans, which can access resetting the correctATM
                    # global variable in case the PWV cannot be found for the data

                # global list of scans with the current spw
                spwscans=list(self.msmeta.scansforspw(ispw))


                # LM added - bwthreshspw 
                if bwthreshspw:
                    # check if the spw is ispw and use different bwthreshold
                    if str(ispw) in bwthreshspw.keys():
                        if verbose:
                            print(' Using SPW specific bwthresh of '+str(bwthreshspw[str(ispw)])+' for '+str(ispw)) 

                        self.logReNorm.write('Using SPW specific bwthresh of '+str(bwthreshspw[str(ispw)])+' for '+str(ispw)+' \n') # LM added

                        (nseg,dNchan) = self.calcChanRanges(ispw,bwthreshspw[str(ispw)],bwdiv,edge=mededge,verbose=verbose)
                    else:
                        # spw not in the bwthreshspw keys list - calculate channel chunks with defaults
                        (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,edge=mededge,verbose=verbose)
                else:
                    # no bwthreshspw option - calculate channel chunks with defaults as normal
                    (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,edge=mededge,verbose=verbose)
                
                self.rnstats['inputs'][target][str(ispw)] = {}
                self.rnstats['inputs'][target][str(ispw)]['num_segments'] = nseg
                self.rnstats['inputs'][target][str(ispw)]['dNchan'] = dNchan

                # init Norm spectra for this spw
                self.rnstats['N'][target][str(ispw)]= pl.zeros((self.num_corrs,self.msmeta.nchan(ispw),self.nAnt))
                self.rnstats['N_thresh'][target][str(ispw)] = pl.zeros((self.num_corrs, self.msmeta.nchan(ispw), self.nAnt))

                # process each target scan
                ngoodscan=0
                ngoodscan_thresh=0

                # LM added
                if fixOutliers:
                    self.AntOut[str(ispw)]={}
                    self.replacedCorr[str(ispw)]=[]
                    self.birdiechan[str(ispw)]=[]
                    # sets for list of known ants with channel outliers 
                
                
                print('Target is in the following scans: '+str(target_scans))
                self.logReNorm.write('Target is in the following scans: '+str(target_scans)+'\n') # AL added

                            # 
                # We want to apply over all fields even if only one field of a mosaic is over the limit. 
                # This will also catch anything that wobbles around the limit and make sure it is applied.
                if docorr: 
                    num_passes = 2
                else:
                    num_passes = 1
                second_pass = False
                second_pass_required = False
                for npass in range(num_passes):
                    # if num_passes = 2, then npass will be set to 0 on first loop and 1 on second loop (then stops when 2 is reached).
                    # So, if npass == 1, it's the second loop and if second_pass was *not* set to True at the end of the loop, we don't
                    # need to go through the loop again.
                    if npass==1 and not second_pass_required:
                        print('Threshold limit not reached for any field/scan of spw '+str(ispw)+' of target '+target+'.')
                        self.logReNorm.write('Threshold limit not reached for any field/scan of spw '+str(ispw)+' of target '+target+'.\n')
                        continue
                    # Same as previous but if second_pass_required is set to True, we need to apply the correction and run through the scan loop again.
                    elif npass==1 and second_pass_required:
                        second_pass = True
                        print('\nThreshold limit was reached for one or more fields/scans of spw '+str(ispw)+' of target '+target+'. Applying renormalization correction to all scans and fields.')
                        self.logReNorm.write('Threshold limit was reached for one or more fields/scans of spw '+str(ispw)+' of target '+target+'. Applying renormalization correction to all scans and fields.\n')
                    else:
                        pass

                    for iscan in target_scans:
                        print(' Processing scan='+str(iscan)+'------------------------------')
                        self.logReNorm.write(' Processing scan='+str(iscan)+'------------------------------\n') # LM added

                        # LM added
                        # here we will get the Phasecal AC if requested
                        # this will be from the scan preceeding the target scan
                        # get the existing phase cal scan numerically lower than the target scan 'iscan'
                        # do this outside the 'if' because if editAC=True we need scanIdx later to be set phaseAC to unity even if we don't use the phase AC
                        # here in this run, it must be blocked from being used in subsequent runs
                        scanIdx = int(pl.where(pl.array(Phscan)<iscan)[0][-1])
                        if usePhaseAC:
                            print('**************** using the phase cal scan '+str(Phscan[scanIdx])+' *************************')
                            B=self.getACdata(Phscan[scanIdx],ispw,None,True)
                            self.logReNorm.write('Will use Phase Cal AutoCorr scan='+str(Phscan[scanIdx])+'------------------------------\n') # LM added
                            if correctATM: 
                                if 'PhaseCal' not in self.atmtrans.keys():
                                    self.atmtrans['PhaseCal']={}
                                if str(ispw) not in self.atmtrans['PhaseCal'].keys():
                                    self.atmtrans['PhaseCal'][str(ispw)]={}
                                if str(iscan) not in self.atmtrans['PhaseCal'][str(ispw)].keys():
                                    # now we know this field, spw and scan is not filled and we will calc it
                                    # otherwise we just use what's there - i.e for a mosaic it doesn't redo for each ifld
                                    # because the atm trans model reads scan level only
                                    self.atmtrans['PhaseCal'][str(ispw)][str(Phscan[scanIdx])]=self.ATMtrans(Phscan[scanIdx],ispw,verbose=True)

                        # get the fields to process in this scan - i.e. mosaics have many fields per scan
                        Tarfld = list(self.msmeta.fieldsforscan(iscan))
                        print(' Will process science target field(s) '+str(Tarfld)+' within this scan')
                        self.logReNorm.write(' Will process science target field(s) '+str(Tarfld)+' within this scan \n') # LM added
                        # LM added
                        # holder for a max value per scan to print out 
                        scanNmax=[]

                        # LM added - so now we are looping over the target field
                        for ifld in Tarfld:
                            if verbose:
                                print(' Processing field='+str(ifld)+'-----------------------------')
                            self.logReNorm.write(' Processing field='+str(ifld)+'-----------------------------\n') # LM added

                            # step over target scans that don't have the current spw
                            if spwscans.count(iscan)==0:
                                if verbose:
                                    print('Scan='+str(iscan)+' is not a target scan in spw='+str(ispw))
                                self.logReNorm.write('Scan='+str(iscan)+' is not a target scan in spw='+str(ispw)+'\n') # LM added
                                continue

                            # initiate the self.scalingValues dictionary
                            if 'spw'+str(ispw) not in self.scalingValues.keys():
                                        self.scalingValues['spw'+str(ispw)]={}
                            if 'scan'+str(iscan) not in self.scalingValues['spw'+str(ispw)].keys():
                                        self.scalingValues['spw'+str(ispw)]['scan'+str(iscan)]={}
                            if 'field'+str(ifld) not in self.scalingValues['spw'+str(ispw)]['scan'+str(iscan)].keys():
                                        self.scalingValues['spw'+str(ispw)]['scan'+str(iscan)]['field'+str(ifld)]=1.0 # default no scaling

                            # LM added 
                            # AutoCorr is divided by B (can be BANDPASS or PHASE cal AutoCorr) 
                            # make if statement as getACdata can now return None - if data was not filled
                            ToB=self.getACdata(iscan,ispw,ifld,True)
                            if ToB is not None: 
                                ToB/=B

                                # Renorm function will be Nb0 divided by a fit
                                N=ToB.copy()


                                # LM added - ATM functionality
                                # get the ATM transmission here for target - above for BP and Phase already
                                # and fix the data - should we do per scan or bulk - bulk should be enough
                                # to get rid of the main defect so fitting will work close enough (one hopes)
                              

                                if correctATM:
                                    # we are in iscan, ispw  and ifld
                                    # in a mosaic we are safe to use one field as representative
                                    # because these differences are 'negligable' 
                                    # compared to possibly large ones we are trying to fix between the BP and target
                                    fldname=self.msmeta.namesforfields(ifld)[0]  

                                    # flid name or not - code only deals with the pointing of a scan, az and el - all pointing in mosaic are close eough
                                    # need per scan, per spw - if we are just doing a bulk correction we miss any scan variations ??? 
                                
                                    if str(fldname) not in self.atmtrans.keys():
                                        self.atmtrans[str(fldname)]={}
                                    if str(ispw) not in self.atmtrans[str(fldname)].keys():
                                        self.atmtrans[str(fldname)][str(ispw)]={}
                                    if str(iscan) not in self.atmtrans[str(fldname)][str(ispw)].keys():
                                        # now we know this field, spw and scan is not filled and we will calc it
                                        # otherwise we just use what's there - i.e for a mosaic it doesn't redo for each ifld
                                        # because the atm trans model reads scan level only
                                        self.atmtrans[str(fldname)][str(ispw)][str(iscan)]=self.ATMtrans(iscan,ispw,verbose=verbose)

                                    # check if we want to do the fix, it the ATM line is not strong
                                    # its pointless calculation to work out the are differences
                                    # between the BandPass and Target pointings
                                    if min(self.atmtrans[str(fldname)][str(ispw)][str(iscan)])<limATM:
                                        # * check now the global as if the ATM code previously didn't
                                        # * find the correct PWV, only nominal values were input
                                        # * and we probably don't want to use those for ATM correction
                                        # * as it could make ATM residuals worse
                                        if self.corrATM is False:
                                            # statement that is won't do the correction
                                            if verbose:
                                                print('WARNING will not account for any ATM lines as requested as PWV not found')
                                            self.logReNorm.write('WARNING will not account for any ATM lines as requested as PWV not found\n')
                                        else:
                                            # now we pass to a function to do the correction
                                            if usePhaseAC:
                                                self.ATMcorrection(N,iscan,ispw,ifld,str(Phscan[scanIdx]),'PhaseCal', verbose=verbose) # just edits the N in place - i.e. should flattens out the ATM region 
                                                # - could pass fldname also but re-gets this in ATM correction function
                                            else:
                                                # bscanatm already specified above 
                                                self.ATMcorrection(N,iscan,ispw,ifld,str(Bscanatm),'BandPass',verbose=True) # just edits the N in place - i.e. should flattens out the ATM region 


                                # ants and corrs to calculate:
                                (nCor,nCha,nAnt)=N.shape

                                for iant in range(nAnt):

                                    for iseg in range(nseg):
                                        lochan=iseg*dNchan
                                        hichan=(iseg+1)*dNchan

                                        for icor in range(nCor):
                                            # edits N in place! just does the fit to get zero baseline - this is calcuating the ReNorm scaling per ant !!!
                                            self.calcReNorm1(N[icor,lochan:hichan,iant],False)

                                ## LM added 
                                if mededge:
                                    # will set the 0.01 (1% - default) of all edge channels to the median value of the scaling spectrum (circa 1)
                                    # stops high edge outliers
                                    self.calcSetEdge(N, edge=mededge)


                                # LM added - excflagged
                                # regardless of any manually input excludeants we still check the cross-corr
                                # data for those antennas and simply see if it is entirely flagged
                                # i.e. 100% flagged antenna we set to 1.0 - i.e. no scaling
                                # thus plots are not skewed and anyway these antennas are not in the IF data
                                if excflagged:
                                    # get the XC flags - if true returned its 100% flagged - deals with spw SPW, per scan basis as it is selected
                                    antflagged = self.getXCflags(iscan,ispw,ifld,verbose=verbose)
                                    # adds to excludeants list if its not already there
                                    for excant in antflagged:
                                        N[:,:,excant].fill(1.0)
                                        if verbose:
                                            print('**** auto flagged antenna: '+self.AntName[excant]+' for SPW='+str(ispw)+', scan='+str(iscan)+', field='+str(ifld)+' ****')
                                            self.logReNorm.write('**** auto flagged antenna: '+self.AntName[excant]+' for SPW='+str(ispw)+', scan='+str(iscan)+', field='+str(ifld)+' ****\n') # LM added


                                # LM added - excludeants 
                                if excludeants:
                                    # we are excluding antennas all by index - converted above from names if input
                                    # they should be set to 1.0 - this is a workaround to
                                    # avoid bad antennas messing up the plots - if an analyst really needed
                                    # to make a list of badantennas, and they were not flagged by pipeline
                                    # then it is worrying why data are bad ...
                                    for excant in excludeants:
                                        N[:,:,excant].fill(1.0)

                                # LM added
                                if excludechan:
                                    # now check if the spw under analysis needs a range setting to 1.0
                                    # i.e. where we know from testing that some ATM line is not well removed in fitting
                                    # usually over the sharp peak transition - e.g. ALMA-IMF wideband SPW
                                    # -- Note, this might not be required since 2021 May - Luke coded ATM correction function -- 
                                    if str(ispw) in excludechan.keys():
                                        exloch=int(excludechan[str(ispw)].split('~')[0])
                                        exhich=int(excludechan[str(ispw)].split('~')[1]) + 1 
                                        N[:,exloch:exhich,:].fill(1.0)

                                # LM added - the checking and fixing of outlier antennas compared to a representative median spectrumd
                                if fixOutliers: 
                                  
                                    ## LM added Feb 09 - outlier checker in a function (updated End Feb)
                                    AntChk = self.checkOutlierAnt(N)

                                    if len(AntChk) > 0:
                                        # pass badant to the fix code for channel by channel investigation and correction
                                        self.calcFixReNorm(N,AntChk,iscan,ispw,ifld,doplot=antdiagspectra,verbose=verbose) 

                                # No need to do any of this on the second round of data 
                                if not second_pass:            
                                    # now do new stats printout
                                    # need to exclude values of 1.0 if/where ants are flagged - otherwise skews all outputs
                                    Nmax = pl.nanmean(pl.where(N.max(1)!=1,N.max(1),pl.nan),1) # mean of max of all ants/channels
                                    if pl.isnan(pl.sum(Nmax)):# is nan:
                                        Nmax = pl.array([1.0,1.0])
                                    Nmads = pl.nanmedian(pl.where(N!=1.0,pl.absolute(N-1.0),pl.nan),[1,2]) 
                                    if pl.isnan(pl.sum(Nmads)):# is nan:
                                        Nmads = pl.array([0.0,0.0])
                                    # pre-April was pl.median(pl.absolute(N-1.0),[1,2]) in below print out
                                    scanNmax.append(pl.mean(Nmax))
                                    alarm='   '
                                    if pl.any(pl.greater(Nmax,1.0+usefthresh)):
                                        alarm='***'
                                    if verbose:
                                        print('  Mean peak renormalization factor (power) per polarization = '+str(alarm)+str(Nmax))
                                        print('  Median renormalization deviation (power) per polarization = '+'   '+str(Nmads))
                                    self.logReNorm.write('  Mean peak renormalization factor (power) per polarization = '+str(alarm)+str(Nmax)+'\n')
                                    self.logReNorm.write('  Median renormalization deviation (power) per polarization = '+'   '+str(Nmads)+'\n')
                                     

                                    # LM added - diagnoastic lots one level more detail vs. summary plots
                                    # this is really the ant level what will be applied as a scaling
                                    #
                                    # skip these if second pass...
                                    if diagspectra:
                                        if docorr:
                                            self.plotdiagSpectra(N, iscan, ispw, ifld, plotATM=plotATM) # , threshline=hardLim ) # show threshold line, optional - not sure I like it but coded 
                                        else:
                                            self.plotdiagSpectra(N, iscan, ispw, ifld, plotATM=plotATM) # no threshold will be shown

                                    # LM Added/modified rnstats recording
                                    # in the spectra antennas that have some scans as 1.0 due to being 
                                    # interferometrically flagged (or we excluded) we don't want them
                                    # to skew the summary cumulative average plots using the rnstats
                                    # if some scans are flagged and some are unflagged
                                    if excflagged:  
                                        # regardless of flagged antennas or not we need to initiate the rnstats on the first scan
                                        if ngoodscan==0:
                                            self.rnstats['N'][target][str(ispw)]= N
                                            ngoodscan+=1
                                        elif antflagged and ngoodscan!=0:
                                            # enter this loop if there ARE flagged antennas  
                                            for lpAnt in range(nAnt):
                                                # if the antenna is not listed as flagged and the initiated first
                                                # entry to rnstats['N'] is not 1.0 (i.e. flagged) we do
                                                # the cumulative sum for the average spectra
                                                if lpAnt not in antflagged and pl.sum(self.rnstats['N'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                    self.rnstats['N'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N'][target][str(ispw)][:,:,lpAnt]*ngoodscan/(ngoodscan+1)  + N[:,:,lpAnt]/(ngoodscan+1)
                                                # if the stored antenna scan value in rnstats is 1.0 (i.e. initiated with a flagged antenna
                                                # but the antenna scan value we want to add now is good
                                                # then just replace the rnstat antenna scaling values entirely
                                                elif lpAnt not in antflagged and pl.sum(self.rnstats['N'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                    print('replacing scan with good for '+str(self.AntName[lpAnt]))
                                                    self.rnstats['N'][target][str(ispw)][:,:,lpAnt]= N[:,:,lpAnt] 
                                            # remember to add to the scans assessed
                                            ngoodscan+=1
                                        else:
                                            # if no flagged antennas were passed we do the default cumulative average as normal
                                            self.rnstats['N'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N/(ngoodscan+1)
                                            ngoodscan+=1
                                    ## Non flagged antenna cases
                                    else:
                                        # incrementall accumulate scan-mean spectra - keeps adding even as we do per field
                                        self.rnstats['N'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N/(ngoodscan+1)
                                        ngoodscan+=1

                                    # AL added - PIPE 1168 (1)
                                    # Repeat the same process but now we'll only be keeping normalized spectra that is above the threshold.
                                    # This helps us plot mosaic sources and multi-target MSs as the mixture of empty/problem fields can wash the peaks.
                                    if pl.mean(Nmax) > hardLim:
                                        if excflagged:  
                                            # regardless of flagged antennas or not we need to initiate the rnstats on the first scan
                                            if ngoodscan_thresh==0:
                                                self.rnstats['N_thresh'][target][str(ispw)]= N
                                                ngoodscan_thresh+=1

                                            elif antflagged and ngoodscan_thresh!=0:
                                                # enter this loop if there ARE flagged antennas  
                                                for lpAnt in range(nAnt):
                                                    # if the antenna is not listed as flagged and the initiated first
                                                    # entry to rnstats['N_thresh'] is not 1.0 (i.e. flagged) we do
                                                    # the cumulative sum for the average spectra
                                                    if lpAnt not in antflagged and pl.sum(self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                        self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N[:,:,lpAnt]/(ngoodscan_thresh+1)
                                                    # if the stored antenna scan value in rnstats is 1.0 (i.e. initiated with a flagged antenna
                                                    # but the antenna scan value we want to add now is good
                                                    # then just replace the rnstat antenna scaling values entirely
                                                    elif lpAnt not in antflagged and pl.sum(self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                        print('replacing scan with good for '+str(self.AntName[lpAnt]))
                                                        self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]= N[:,:,lpAnt] 
                                                # remember to add to the scans assessed
                                                ngoodscan_thresh+=1
                                            else:
                                                # if no flagged antennas were passed we do the default cumulative average as normal
                                                self.rnstats['N_thresh'][target][str(ispw)]=self.rnstats['N_thresh'][target][str(ispw)]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N/(ngoodscan_thresh+1)
                                                ngoodscan_thresh+=1

                                        ## Non flagged antenna cases
                                        else:
                                            # incrementall accumulate scan-mean spectra - keeps adding even as we do per field
                                            self.rnstats['N_thresh'][target][str(ispw)]=self.rnstats['N_thresh'][target][str(ispw)]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N/(ngoodscan_thresh+1)
                                            ngoodscan_thresh+=1

                                    # below is George's original code and these are used in other RN.plot*  (not the spectral one)    
                                    # this does not add per field, it appears that only the last field will be appended currently
                                    # as it is not cumulative, it is a replacement that calls only ispw and iscan
                                    # the current median will possibly be skewed by antennas set to 1.0 if flagged
                                    # if the last field has no scaling, plots using these scan based stats/and latter plots
                                    # will not show anything useful
                                    self.rnstats['rNmax'][:,:,spws.index(ispw),targscans.index(iscan)]=N.max(1)
                                    self.rnstats['rNmdev'][:,:,spws.index(ispw),targscans.index(iscan)]=pl.median(pl.absolute(N-1.0),1)

                                    # write in the max value for this SPW, scan, field into the self.scalingValues dictionary
                                    self.scalingValues['spw'+str(ispw)]['scan'+str(iscan)]['field'+str(ifld)]=pl.mean(Nmax) # average the correlations

                                    # Need to move this part out of the docorr check so that we can add a check to above thresh, then add to 
                                    # to the dictionary. Then after each scan/field, we check for it to be above the limit and update it each
                                    # time to set to True/False unless it is already set to True. Should do this regardless of the number of
                                    # fields as it would catch single field targets that wobble around the limit. 
                                    #
                                    # Check if above limit but only on the first pass through the data
                                    if pl.mean(Nmax) > hardLim: 
                                        # Nmax hold 2 values for dual corr (xx,yy) coming from mean of all maximal values of all ants  
                                        self.docorrApply[target][str(ispw)]=True # will correct this field
                                    # if this field isn't above the limit but it's already True, pass
                                    elif self.docorrApply[target][str(ispw)]:
                                        pass
                                    # if not above the limit and not already set to True, set to False
                                    else:
                                        self.docorrApply[target][str(ispw)]=False
                                
                                # If we want to apply the correction and it's the second time through the data
                                if docorr and second_pass: 
                                    # apply the correction
                                    # Antenna-based Correction factors are in voltage units (pair products are power)
                                    Nv=pl.sqrt(N)
                                    self.applyReNorm(iscan,ispw,ifld,Nv,datacolumn) # LM pass field too
                            
                                    # do self.recordApply here and pass scan, spw, field too
                                    self.recordApply(iscan,ispw,ifld)

                                    # ****
                                    # In combination with the docorrApply dictionary, and the printed messaged
                                    # PLWG might want to include also Keyword dictionaries into the MS
                                    # here might be a good place for this
                                    # ****

                                    if verbose:
                                        print('Application of the ReNormalization was written to the MS history for spw'+str(ispw)+' scan'+str(iscan)+' field'+str(ifld))
                                    self.logReNorm.write(' Application of the ReNormalization was written to the MS history for spw'+str(ispw)+' scan'+str(iscan)+' field'+str(ifld)+'\n')
                                        

                            # LM added closes the data check whereto see if the AC data is confirmed to be filled - only gets here if None was returned
                            else:
                                if verbose:
                                    print(' **** No data found - skipping field '+str(ifld)+' in scan '+str(iscan)+' ****')
                                self.logReNorm.write(' **** No data found - skipping field '+str(ifld)+' in scan '+str(iscan)+' ****\n')

                            if not second_pass:
                            # LM added print of per scan max val
                                if ifld == max(Tarfld):
                                    print('  Max peak renormalization factor (power) over scan '+str(iscan)+' = '+str(max(scanNmax)))
                                    self.logReNorm.write('  Max peak renormalization factor (power) over scan '+str(iscan)+' = '+str(max(scanNmax))+'\n')

                                # LM added check the scan max vs the overall value and stores to class value 
                                if max(scanNmax) > self.MaxOutScaling:
                                    self.MaxOutScaling = max(scanNmax)

                        # LM added
                        # this is added as protection to overwrite the AutoCorr data if the application was made 
                        # set the AC of the BP, Phase and Target to 1.0 (this should not have negative effects for data redcution as AC is flagged out)
                        # this is a hard protection against any subsequent runs of the ReNormalize code
                        # here we are in the scan loop - don't need to do field level as everything will be overwritten with unity
                        if docorr and editAC and second_pass:
                            print(' Setting AutoCorrelations to 1.0 so no subsequent re-run of renormalization can occur ')
                            self.logReNorm.write(' Setting AutoCorrelations to 1.0 so no subsequent re-run of renormalization can occur \n')

                            self.unityAC(Phscan[scanIdx],ispw,None)
                            self.unityAC(Bscan,ispw,None) # currently crude as it will repeat this for every target scan but the AC is set to 1.0 after first pass
                            self.unityAC(iscan,ispw,None)

                        # After doing the first pass, if docorr is True and docorrApply was set to True, 
                        # we now need to go through again and actually apply the renormalization 
                        if docorr and not second_pass and self.docorrApply[target][str(ispw)]:
                            second_pass_required = True
                
        # AL added - PIPE 1168 (3)
        # Loops through the scalingValue dict and populates the pipeline needed dictionary
        self.rnpipestats = {}
        target_field_ids = self.msmeta.fieldsforintent('*TARGET*')
        target_fields = pl.unique(self.msmeta.namesforfields(target_field_ids))
        for trg in self.rnstats['N'].keys(): #target_fields:
            self.rnpipestats[trg] = {}
            for spw in self.rnstats['N'][trg].keys(): # spws:
                self.rnpipestats[trg][spw] = {}
                scans = pl.intersect1d(self.msmeta.scansforintent('*TARGET*'), self.msmeta.scansforfield(trg)) # find scans related to this target
                scans = pl.intersect1d(scans, self.msmeta.scansforspw(int(spw))) # find scans related to given spw (spectral scan)
                scans = pl.intersect1d(scans, targscans) # if user input scans, limit to those
                pipe_target_sv, pipe_target_fld = [],[]
                for scan in scans:
                    fields = pl.intersect1d(self.msmeta.fieldsforintent('*TARGET*'), self.msmeta.fieldsforname(trg)) # fields for target
                    fields = pl.intersect1d(fields, self.msmeta.fieldsforscan(scan)) # fields for given scan
                    for field in fields:
                        pipe_target_sv.append(self.scalingValues['spw'+str(spw)]['scan'+str(scan)]['field'+str(field)])
                        pipe_target_fld.append(field)
                self.rnpipestats[trg][spw]['max_rn'] = max(pipe_target_sv)
                self.rnpipestats[trg][spw]['max_rn_field'] = pipe_target_fld[pl.where(pl.array(pipe_target_sv) == self.rnpipestats[trg][spw]['max_rn'])[0][0]]
                self.rnpipestats[trg][spw]['threshold'] = hardLim

        
        # LM added - final docorr check to write history as a single value - commented out in 
        #            in favour of adding above where multiple history statements are recorded
        ##if docorr:
        ##    self.recordApply()
        ##    print('Application of the ReNormalization was written to the MS history')
        ##    self.logReNorm.write(' Application of the ReNormalization was written to the MS history \n')

        # final log end of the renormalize function
        # LM added - starting CASA logger message
        casalog.post('*** ALMA almarenorm.py ***', 'INFO', 'ReNormalize')   
        casalog.post('*** '+str(self.RNversion)+' ***', 'INFO', 'ReNormalize')   
        casalog.post('*** End of renormalization run ***', 'INFO', 'ReNormalize')   
        # added time stamp of the actual renormalize main function end
        endrun = datetime.now()
        logReNormEnd = endrun.strftime('End_of_ReNormalize_%Y%m%dT%H%M%S')
        self.logReNorm.write(logReNormEnd+'\n')


    # LM added / edited 
    # added option Feb 18/23 - odd, code will instead find the best odd value for nseg where
    # such that dNchan remains an integer number or remainng 'non-fitted' chans are in the edge
    # also added forced divide - with same rule. Else defaults to power 2 
    # bwthresh Luke's default is 120e6
    def calcChanRanges(self,spw,bwthresh=120e6,bwdiv='odd',onlyfdm=True, edge=0.01,verbose=False):
        
        Tbw=self.msmeta.bandwidths(spw)
        nchan=self.msmeta.nchan(spw)
        
        nseg=1
        dNchan=nchan

        # Only do non-trivial segments if bwthresh exceeded AND spw is FDM (lots of channels)
        #   (This prevents TDM spws, which are wide and low-res, from being segmented)

        # check first if we are forcing a divide
        if type(bwdiv) is int and spw in self.fdmspws:
            # make float so we can check dNchan
            nseg=float(bwdiv)
            dNchan=nchan/nseg
            # check if nseg gives an integer nchans or if the remaining chans when dNchan is not an int
            # will be covered by the edge boundaries 
            if dNchan.is_integer() or nchan%nseg < edge*nchan:
                dNchan = int(dNchan)
                nseg=int(nseg)
            else:
                print(" Input bwdiv (nseg) does not divide the SPW into an integer number of channels ")
                print(" and the remaining channels are not excluded in the edges ")
                print(" Will default to power of 2 division based on bwthresh ")
                nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                dNchan=int(nchan/nseg)

        elif Tbw>bwthresh and spw in self.fdmspws:
            if bwdiv=='odd':
                # work out best odd divisibile when dNchan is an int
                nseg=Tbw/bwthresh # need as a float here
                dNchan=nchan/nseg
                while not dNchan.is_integer():
                    # if not integer right away
                    # recalc making sure nseg is an int
                    nseg=int(ceil(nseg))
                    dNchan=nchan/nseg
                    # check if the new nseg is actually odd, otherwise make it odd
                    if nseg%2 != 1:
                        nseg=int(ceil(nseg))+1.0
                        dNchan=nchan/nseg
                    # check if the remainder for this odd nseg falls into the median set edges
                    # i.e don't need to worry if nchan/nseg is a true integer division
                    # e.g. 2048 into 5, dNchan is 409.6, so 5x409 segs and remaining is 3 chans,
                    #      but edge chans are 20, so they are anyway set to to the median value (~1)
                    #      and we can use nseg = 5 without worry 
                    if nchan%nseg < edge*nchan:
                        dNchan=float(int(dNchan)) 
                        # not ceil, need to round down to nearest int and will exit the while statment
                    else:
                        # if remainder not in edges, add another to nseg and continue 
                        nseg=int(ceil(nseg))+1.0
                            
                    # case where odd numbers just will not go into the channel number inc remainder accounting
                    if nseg > 50:
                        nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                        break
                # breaks when nseg is odd and dNchan is an integer
                # but pass back the correct ints
                nseg=int(ceil(nseg))
                dNchan=int(nchan/nseg) # CASA 6 passing as a float even though two ints are input - explicit for int


            else:
                # George's base function
                nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                dNchan=int(nchan/nseg)


        if nseg>1:
            print(' **Wide spw detected; dividing into '+str(nseg)+' segments of '+str(dNchan)+' channels each.') 
            self.logReNorm.write(' **Wide spw detected; dividing into '+str(nseg)+' segments of '+str(dNchan)+' channels each.\n') # LM added

        return (nseg,dNchan)


    def stats(self):
        return self.rnstats

    # LM note - these are respresentative spectra of the cumilative of averages all scans (and fields)
    #           the is now code to ignore the flagged antennas, i.e. scaling set to exactly 1.0
    #           and the ATM profiles are now plotted - the bandpass is used as its just 
    #           a representative profile for the plots
    #
    # AL added - ability to plot by frequency rather than channel, PIPE 1168 (4)
    #          - ability to plot spws individually and created for each source, PIPE 1168 (1)
    def plotSpectra(self,hardcopy=True, plotATM=True, titlein=None, subplot=False, plotfreq=True, plotDivisions=True, createpdf=True):

        #print('Using fractional alarm threshold='+str(self.fthresh))

        # If data not yet collected, complain (eventually collect it?)
        if len(self.rnstats)==0:
            print('Please run renormalize before plotting!')
            return
            #self.renormalize(spws=spws,doplot=False,docorr=False)
            #doSpws=self.rnstats['spws']


        doSpws=self.rnstats['spws']
        nSpw=len(doSpws)
        
        target_list = pl.unique(self.msmeta.namesforfields(self.msmeta.fieldsforintent('*TARGET*')))

        for target in target_list:
            nXspw,nYspw = self.xyplots(nSpw)
            pl.ioff()
            if subplot:
                pfig=pl.figure(11,figsize=(14,9))
                pl.ioff()
                pfig.clf()

            k=1
            for spw in doSpws:
                ispw=self.rnstats['spws'].index(spw)

                if plotfreq:
                    freqs = self.msmeta.chanfreqs(spw,'GHz')
                
                # Check self.rnstats to see if the threshold was ever reached. If it wasn't, 
                # N_thresh = 0 because it was initialized but never filled.
                if pl.sum(self.rnstats['N_thresh'][target][str(spw)]) == 0.0:
                    N=self.rnstats['N'][target][str(spw)]
                else:
                    N=self.rnstats['N_thresh'][target][str(spw)]
                (nCor,nCha,nAnt)=N.shape
                
                # Setup the figure
                if subplot:
                    pl.subplot(nYspw,nXspw,k)
                else:
                    pl.figure(figsize=(10,8))

                pl.ioff()
                # Set labels appropriately depending on option choices.
                if subplot:
                    if (k-1)%nXspw==0:
                        pl.ylabel('Renorm Amplitude')
                    if k>(nSpw-nXspw):
                        if not plotfreq:
                            pl.xlabel('Channel')
                        else:
                            pl.xlabel('Frequency (GHz)')

                else:
                    pl.ylabel('Renorm Amplitude')
                    if not plotfreq:
                        pl.xlabel('Channel')
                    else:
                        pl.xlabel('Frequency (GHz)')
                
                if subplot:
                    if k==1:
                        if titlein:
                            titleTEXT = str(titlein)+' \n'+self.msname+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'>'
                            pl.title(titleTEXT,{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
                        else:
                            pl.title(self.msname+'\nTarget='+target+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'>',{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
                else:
                    if titlein:
                        titleTEXT = str(titlein)+' \n'+self.msname+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'>'
                        pl.title(titleTEXT,{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
                    else:
                        #pl.title(self.msname+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'> spw='+str(spw),{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
                        pl.title(self.msname+'\nTarget='+target+' Spw='+str(spw)+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'>',{'fontsize': 'medium'})

                k+=1
                style=['r:','b:']
                for iant in range(nAnt):
                    for icor in range(nCor):
                        if not plotfreq:
                            pl.plot(N[icor,:,iant],style[icor])
                        else:
                            pl.plot(freqs, N[icor,:,iant],style[icor])

                # LM added 1.0 check for fully flagged ants
                #Nm = pl.mean(N,2)
                Nm = pl.nanmean(pl.where(N!=1,N,pl.nan),2) 
                # becuase some antennas are flagged and get a scaling of 1.0
                # considerably skewing the mean 
                Nm[:][pl.isnan(Nm[:])]=1.0
                # for those data where part of the spw is excluded due to and ATM line
                
                #if not plotfreq:
                style = ['r-', 'b-']
                if not plotfreq:
                    for icor in range(nCor):
                        pl.plot(Nm[icor,:],style[icor])
                        #pl.plot(Nm[icor,:],'b-')
                else:
                    for icor in range(nCor):
                        pl.plot(freqs, Nm[icor,:], style[icor])
                        #pl.plot(freqs, Nm[icor,:],'b-')
                
                # Find the average of the max fields
                #fields = pl.intersect1d(self.msmeta.fieldsforintent('*TARGET*'), self.msmeta.fieldsforname(target))
                #if len(fields) > 1:
                #    max_values = []
                #    for field in fields:
                #        first_scan = pl.intersect1d(self.msmeta.scansforintent('*TARGET*'),self.msmeta.scansforfield(field))[0]
                #        if self.scalingValues['spw'+str(spw)]['scan'+str(first_scan)]['field'+str(field)] > self.bandThresh[self.Band]:
                #            max_values.append(self.scalingValues['spw'+str(spw)]['scan'+str(first_scan)]['field'+str(field)])
                #    if max_values:
                #        Nmax = pl.mean(max_values)
                #    else:
                #        Nmax = None
                #else:
                if nCor == 1:
                    Nxmax=Nm[0,:].max()
                    Nymax=Nxmax
                elif nCor == 2:
                    Nxmax=Nm[0,:].max()
                    Nymax=Nm[1,:].max()

                #if len(fields) == 1:
                if Nxmax>=(1.+self.fthresh) or Nymax>=(1.+self.fthresh):
                    if not plotfreq:
                        pl.plot([3*nCha/8,5*nCha/8],[Nxmax]*2,'r-')
                        pl.text(3*nCha/8,Nxmax,'<X>='+str(floor(Nxmax*10000.0)/10000.0),ha='right',va='center',color='r',size='x-small')
                        if nCor == 2:
                            pl.plot([3*nCha/8,5*nCha/8],[Nymax]*2,'b-')
                            pl.text(5*nCha/8,Nymax,'<Y>='+str(floor(Nymax*10000.0)/10000.0),va='center',color='b',size='x-small')
                    else:
                        fmin = 3./8.*max(freqs) + 5./8.*min(freqs)
                        fmax = 5./8.*max(freqs) + 3./8.*min(freqs)
                        pl.plot([fmin,fmax],[Nxmax]*2,'r-')
                        pl.text(fmin,Nxmax,'<X>='+str(floor(Nxmax*10000.0)/10000.0),ha='right',va='center',color='r',size='x-small')
                        if nCor == 2:
                            pl.plot([fmin,fmax],[Nymax]*2,'b-')
                            pl.text(fmax,Nymax,'<Y>='+str(floor(Nymax*10000.0)/10000.0),va='center',color='b',size='x-small')
                #else:
                #    if Nmax:
                #        if not plotfreq:
                #            pl.plot([3*nCha/8,5*nCha/8],[Nmax]*2,'r-')
                #            pl.text(3*nCha/8,Nxmax,'<RN>='+str(floor(Nmax*10000.0)/10000.0),ha='right',va='center',color='r',size='x-small')
                #        else:
                #            fmin = 3./8.*max(freqs) + 5./8.*min(freqs)
                #            fmax = 5./8.*max(freqs) + 3./8.*min(freqs)
                #            pl.plot([fmin,fmax],[Nmax]*2,'r-')
                #            pl.text(fmin,Nmax,'<RN>='+str(floor(Nmax*10000.0)/10000.0),ha='right',va='center',color='r',size='x-small')

                #print(ispw, Nxmax, Nymax)

                lims=list(pl.axis())
                if not plotfreq:
                    lims[0]=-1
                    lims[1]=nCha+1
                    lims[2]=min(0.999,lims[2])
                    lims[3]=max(1.15*lims[3]-0.15*lims[2],1.01)
                else:
                    lims[0]=min(freqs)*0.99999
                    lims[1]=max(freqs)*1.00001
                    lims[2]=min(0.999,lims[2])
                    lims[3]=max(1.15*lims[3]-0.15*lims[2],1.01)
                pl.axis(lims)

                dy=lims[2]*0.1+lims[3]*0.9
                if subplot:
                    if not plotfreq:
                        pl.text(nCha/20,dy,'Spw='+str(spw))
                    else:
                        pl.text((lims[1]-lims[0])*.06+lims[0], dy, 'Spw='+str(spw))

                if plotDivisions:
                    dNchan = self.rnstats['inputs'][target][str(spw)]['dNchan']
                    nseg = self.rnstats['inputs'][target][str(spw)]['num_segments']
                    xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
                    if plotfreq:
                        pl.vlines(freqs[xlocs], 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)
                    else:
                        pl.vlines(xlocs, 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)

                if plotATM:
                    # for refernce its easiest to only plot the bandpass ATM profile as representative 
                    # its ONLY for the plot and even if PhaseAC used, the BP ATM is still extracted 
                    # in renormalize if correctATM=True - so the else below works 
                    Bscanatm=self.getBscan(spw,verbose=False)
                    if type(Bscanatm) is list:
                        Bscanatm = Bscanatm[0]
                    if len(self.atmtrans.keys())==0:
                        # get the ATM profiles for the bandpass as they were not retrieved in the renormalize run (i.e. correctATM was false)                        
                        if self.Band in [9, 10]:
                            ATMprof, ATMprof_imageSB = self.ATMtrans(Bscanatm, spw, verbose=False) # this is function to get ATM profiles
                        else:
                            ATMprof = self.ATMtrans(Bscanatm, spw, verbose=False) 
                    else:
                        ATMprof = self.atmtrans['BandPass'][str(spw)][str(Bscanatm)] # this is the profiles stored in the dictionary from the renormalize run
                    pl.twinx()
                    if plotfreq:
                        pl.plot(freqs, 100.*ATMprof, c='m', linestyle='-', linewidth=2)
                        if self.Band in [9, 10]:
                            pl.plot(freqs, 100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
                    else:
                        pl.plot(100.*ATMprof,c='m',linestyle='-',linewidth=2) 
                        if self.Band in [9, 10]:
                            pl.plot(100.*ATMprof_imageSB,c='k',linestyle='-',linewidth=2) 

                    # channels is not called its just the shape of the data e.g. N[0,:,iAnt] = chans - this is the same as the ATMprof as we get it using the SPW data
                    pl.xlim(lims[0], lims[1])
                    #pl.ylim(0,100)
                    if self.Band in [9,10]:
                        peak = max(pl.maximum(ATMprof,ATMprof_imageSB)*100.)+10
                        pl.ylim(peak-100,peak)
                    else:
                        peak = max(ATMprof*100.)+10
                        pl.ylim(peak-100,peak)
                    pl.ticklabel_format(style='plain', useOffset=False)

                    # Setup for properly labelling the y-axis so that labels below 0 are blanked (because they have no meaning).
                    # initialize the axes so we can read the labels
                    pl.draw()
                    # get the axes
                    ax = pl.gca()
                    #grab the label strings
                    ylabels = ax.get_yticklabels()
                    # iterate over the labels and if any are located below 0, blank them out
                    ylabels = ['' if ylabel.get_position()[1] < 0 else int(ylabel.get_position()[1]) for ylabel in ylabels]
                    # set the new label names
                    ax.set_yticklabels(ylabels)
                    
                    if subplot:
                        if (k-1)%nXspw==0:  
                            if self.Band in [9, 10]:
                                pl.ylabel('ATM transmission (%), Image Sideband')
                            else:
                                pl.ylabel('ATM transmission (%)') 
                    else:
                        if self.Band in [9, 10]:
                            pl.ylabel('ATM transmission (%), Image Sideband')
                        else:
                            pl.ylabel('ATM transmission (%)')

                # if we didn't plotATM, we still want to properly display ticklabels
                else:
                    # CASA 6 units change unless specified
                    pl.ticklabel_format(style='plain', useOffset=False)
            
            # Add pl.twiny() here and set the second x-axis to channels. Need to make sure they are going in the right direction!

                if not subplot:
                    if hardcopy:
                        if not os.path.exists('RN_plots'):
                            os.mkdir('RN_plots')
                        fname=self.msname+'_'+target+'_spw'+str(spw)+'_ReNormSpectra.png'
                        print('Saving hardcopy plot: '+fname)
                        pl.savefig('./RN_plots/'+fname)
                        pl.close()
                        # If there is already an entry here, then the threshold was reached. Save the output filename to the pipe dictionary.
                        if self.rnpipestats[target][str(spw)]:
                            self.rnpipestats[target][str(spw)]['spec_plot'] = fname
                        if createpdf:
                            self.convert_plots_pdf(target, spw)
                    else:
                        pl.show()
                        raw_input('Please close plot and press ENTER to continue.')

            if subplot:
                if nXspw == 3:
                    pl.subplots_adjust(wspace=0.35)
                else:
                    pl.subplots_adjust(wspace=0.3)

                if hardcopy:
                    if not os.path.exists('RN_plots'):
                        os.mkdir('RN_plots')
                    fname=self.msname+'_'+target+'_ReNormSpectra.png'
                    print('Saving hardcopy plot: '+fname)
                    pl.savefig('./RN_plots/'+fname)
                    pl.close()
                else:
                    pl.show()

    # George's default code
    def plotScanStats(self,hardcopy=True):

        # If data not yet collected, complain (eventually collect it?)
        if len(self.rnstats)==0:
            print('Please run renormalize before plotting!')
            return
        pl.ioff()
        pfig=pl.figure(12,figsize=(14,9))
        pl.ioff()
        pfig.clf()

        sh=self.rnstats['rNmax'].shape

        nSpw=len(self.rnstats['spws'])
        nXspw,nYspw = self.xyplots(nSpw)
        
        pl.clf()
        k=1
        scans=pl.array(self.rnstats['scans'])
        loscan=scans.min()-1
        hiscan=scans.max()+1
        for spw in self.rnstats['spws']:
            ispw=self.rnstats['spws'].index(spw)
            pl.subplot(nYspw,nXspw,k)
            pl.ioff()
            # CASA 6 units change unless specificed
            pl.ticklabel_format(style='plain', useOffset=False)
            if (k-1)%nXspw==0:
                pl.ylabel('Peak frac renorm scale')

            if k>(nSpw-nXspw):
                pl.xlabel('Scan')

            if k==1:
                pl.title(self.msname+' Nant='+str(self.nAnt)+' Nscan='+str(len(self.rnstats['scans'])),{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})

            k+=1
            F=self.rnstats['rNmax'][:,:,ispw,:]-1.0
            Ferr=self.rnstats['rNmdev'][:,:,ispw,:]
            Fmax=F.max()*1.15
            Fmax=max(0.01,Fmax)
            
            for i in range(sh[1]):
                for j in range(sh[0]):
                    pl.plot(scans,F[j,i,:],'-')
                    pl.plot(scans,F[j,i,:],'k.')
                    pl.plot(scans,Ferr[j,i,:],':')
            pl.axis([loscan,hiscan,0.0,Fmax])
            
            pl.text(loscan+0.25,0.9*Fmax,'Spw='+str(spw))

        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormAmpVsScan.png'
            print('Saving hardcopy plot: '+fname)
            pl.savefig('./RN_plots/'+fname)
            pl.close()
        else:
            pl.show()

    # George's default code 
    #
    # Won't work with 1 correlation
    def plotSpwStats(self,hardcopy=True):

        # If data not yet collected, complain (eventually collect it?)
        if len(self.rnstats)==0:
            print('Please run renormalize before plotting!')
            return
        pl.ioff()
        pfig=pl.figure(13,figsize=(14,9))
        pl.ioff()
        pfig.clf()

        sh=self.rnstats['rNmax'].shape

        spws=pl.array(self.rnstats['spws'])
        lospw=spws.min()-1
        hispw=spws.max()+1

        F=pl.mean(self.rnstats['rNmax'],3)-1.0

        pl.clf()
        for i in range(sh[1]):
            pl.plot(spws-0.05,F[0,i,:],'r.')
            pl.plot(spws+0.05,F[1,i,:],'b.')

        pl.axis([lospw,hispw]+list(pl.axis()[2:]))
        pl.xlabel('Spw Id')
        pl.ylabel('Scan-mean Peak frac renorm scale')
        pl.title(self.msname+' Nant='+str(self.nAnt)+' <Nscan='+str(len(self.rnstats['scans']))+'>',{'horizontalalignment': 'center', 'fontsize': 'medium','verticalalignment': 'bottom'})
       
        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormAmpVsSpw.png'
            print('Saving hardcopy plot: '+fname)
            pl.savefig('./RN_plots/'+fname)
            pl.close()
        else:
            pl.show()

    ## LM added field input
    # AL added XX only case
    def applyReNorm(self,scan,spw,field,rN,datacolumn='CORRECTED_DATA'):

        (a1,a2,X)=self.getXCdata(scan,spw,field,datacolumn)

        (nCor,nCha,nRow)=X.shape
        for irow in range(nRow):
            for icor in range(nCor):
                if nCor==1:
                    X[icor,:,irow]*=(rN[icor,:,a1[irow]]*rN[icor,:,a2[irow]])
                elif nCor==2:
                    X[icor,:,irow]*=(rN[icor,:,a1[irow]]*rN[icor,:,a2[irow]])
                elif nCor==4:
                    X[icor,:,irow]*=(rN[icor/2,:,a1[irow]]*rN[icor%2,:,a2[irow]])
                    
        self.putXCdata(scan,spw,field,X,datacolumn)


    # main renorm scaling fit and find what will be applied - George's origional code
    def calcReNorm1(self,R,doplot=False):   

        # NB:  R will be adjust in place

        nCha=len(R)

        x=pl.array(range(nCha))*1.0/nCha - 0.5

        # initial "fit"
        f=pl.array([pl.median(R)])

        mask=pl.ones(len(R),bool)
        ylim=0.0

        if doplot:
            pl.clf()


        for ifit in range(1,self.nfit):

            R0=R.copy()

            # ~flattened, zeroed spectrum
            R0/=pl.polyval(f,x)
            R0-=1.0
                            
            # thresh is half peak negative
            #thresh=abs(R0.min()/2.0)

            # thresh is 2x median deviation (~1.33 sigma) 
            thresh=pl.median(pl.absolute(R0))*2.0
            
            # mask within thresh
            mask[:]=True
            mask[R0<-thresh]=False
            mask[R0>thresh]=False

            if doplot:
                med=pl.median(R)
                pl.subplot(2,self.nfit,ifit)
                pl.plot(range(nCha),R,'b,')
                pl.plot(pl.array(range(nCha))[pl.logical_not(mask)],R[pl.logical_not(mask)],'r.')
                pl.plot(range(nCha),pl.polyval(f,x),'r-')
                pl.axis([-1,nCha,med-0.003,med+0.003])
                            
                if ylim==0.0:
                    ylim=5*thresh
                pl.subplot(2,self.nfit,ifit+self.nfit)
                pl.plot(range(nCha),R0,'b,')
                pl.plot([-1,nCha],[-thresh,-thresh],'r:')
                pl.plot([-1,nCha],[thresh,thresh],'r:')
                pl.axis([-1,nCha,-ylim,ylim])
                print(ifit-1, thresh, abs(R0.min()/2.0), pl.sum(mask), f)

            # fit to _R_ in masked spectra
            f=pl.polyfit(x[mask],R[mask],ifit)
            
            if doplot:
                pl.subplot(2,self.nfit,ifit)
                pl.plot(range(nCha),pl.polyval(f,x),'g-')
                print(ifit, f)

        R/=pl.polyval(f,x)

        if doplot:
            pl.subplot(2,self.nfit,ifit+1)
            pl.plot(range(nCha),R,'g-')



    # LM added - SUPERSEEDED
    # this is older way i.e. per Feb 15, 2021 (2x peak or -ve as 20*MAD or below min value)
    def checkOutlierAntOLD(self, R):
        Nmax=pl.mean(R.max(1),1)
        AntChk=[]
        Nmin=pl.mean(R.min(1),1) 
        NmaxlimP = R.max(1)
        Nmaxlim = R.max(1).max(0) # max value per ant, both pols
        NminlimP = R.min(1)
        Nminlim = R.min(1).min(0) # max value per ant, both pols
        medMAD =  pl.median(pl.median(pl.absolute(R - 1.0),1))    
        # median in channel axis - left with MAD per cor per ant - then median of all
        maxLim = 1.+(pl.mean(Nmax)-1.)*2.0 # this misses things less than peaks in strong line windows - is that a problem ? if away from lines its less than them - and this in the noise
        minLim = pl.minimum(1.0-20.*medMAD,1.+(Nmin.min(0)-1.0)*2.0)

        for iant in range(R.shape[2]):
            if Nmaxlim[iant] > maxLim: 
                # Put bad ant in the list for this scan, field
                AntChk.append(iant)
            if Nminlim[iant] < minLim: # a negative spike more than 20 MAD (we clip down to 10 though) or 2x Min value from 1.0
                # put in list if not already triggered
                if iant not in AntChk:
                    AntChk.append(iant)

        # return the list 
        return AntChk


    # LM added - outlier check in a function
    # new way post Feb 21 - actually do a quick median ant check 
    #              Feb 23 - with median birdie checker
    def checkOutlierAnt(self, R):
        AntChk=[]
        ##M = pl.median(R,2) # median spectra for each pol - in principle all ants should be the same per field
        # the above is a problem if there are too many 1.0's from flagged data - median ends up being 1.0
        M = pl.nanmedian(pl.where(R!=1,R,pl.nan),2)
        M[:][pl.isnan(M[:])]=1.0 # for excluded chan ranges which are 'nan'
        #need to set back to 1.0, otherwise median has nan values
        # and rest of stats max, min, etc do not deal with it

        medMAD =pl.median(pl.median(pl.absolute(R - 1.0),1))  
        Rmax=pl.mean(R.max(1),1) # mean max value per pol - as above
        ##thresh=1.0 + medMAD * 10.0 # clip level old trial value
        thresh = 1.0025 # thresh if thresh > 1.0025 else 1.0025 # accepted outlier level ? 0.25% ? 
        # TBD some bad ants have >1.025
        for jcor in range(R.shape[0]):
            # set thresh to avoid a line free spectrum defining 
            # 'noise' as differences
            RmaxT = pl.maximum(Rmax[jcor]-1.0,0.0025)
            # first review the median for birdies - same as the calcFixcode pass over a range
            # and check for huge spikes
            for nch in range(10,M.shape[1]-10): 
                if pl.absolute(pl.median(M[jcor,nch-2:nch+3])-M[jcor,nch]) > 0.5*RmaxT:
                    M[jcor,nch]=pl.median(M[jcor,nch-2:nch+3])

            for jant in range(R.shape[2]):
                Rcomp = 1.0+pl.absolute((R[jcor,:,jant]/M[jcor,:])-1.0)

                # now check if any channel triggers a real outlier - do not assess per channel here
                # just store to the outlier ant list for detailed investigation later

                if Rcomp.max(0) > thresh and jant not in AntChk:
                    AntChk.append(jant)

        return AntChk

        

    # LM added - third major iteration of the fixing code for poor channels 
    #
    # AL updated - updated plot titles/formatting
    def calcFixReNorm(self,R,AntUse,scanin, spwin,fldin,doplot=True, plotDivisions=True, hardcopy=True,verbose=False): 
        # this changes actively the renorm value, R,
        # creates a channel based threshold
        # uses the divided spectrum (ant/median-spec) for checks


        ##M = pl.median(R,2) # median spectra for each pol - in principle all ants should be the same per field
        # the above is a problem if there are too many 1.0's from flagged data - median ends up being 1.0
        M = pl.nanmedian(pl.where(R!=1,R,pl.nan),2)  
        M[:][pl.isnan(M[:])]=1.0 # for excluded chan ranges which are 'nan'
        #need to set back to 1.0, otherwise median has nan values
        # and rest of stats max, min, etc do not deal with it

        Mabs = 1.0+pl.absolute(M-1.0)
        Rmax=pl.mean(R.max(1),1) # mean max value per pol - as above
        Rmin=pl.mean(R.min(1),1)
        medMAD =pl.median(pl.median(pl.absolute(R - 1.0),1))     # median in channel axis - left with MAD per cor per ant - then median of all
        #thresh=1.0 + medMAD * 10.0 # clip level
        thresh = 1.0025 # OLD -->> thresh if thresh > 1.0025 else 1.0025 # set a 0.25 of a percent otherwise. Scales of this magnitude are negligable 

        if doplot:
            Rorig = R.copy() # copy 
            # for the plotting

        # assuming 2 corr/pols
        lineOut=[[],[]]
        plttxt=[[],[]]
        corPrt=['XX','YY']
        
        # this repeats the median specrum making and checking actively - fast so just copied as from cehckOutlierAnt code 
        for jcor in range(R.shape[0]):

            RmaxT = pl.maximum(Rmax[jcor]-1.0,0.0025)

            for nch in range(10,M.shape[1]-10): 
                if pl.absolute(pl.median(M[jcor,nch-2:nch+3])-M[jcor,nch]) > 0.5*RmaxT:
                    M[jcor,nch]=pl.median(M[jcor,nch-2:nch+3])
                    self.birdiechan[str(spwin)].append('chan'+str(nch)+'_scan'+str(scanin)+'_field'+str(fldin))


        # setup threshold, for the read in spw, scan, field
        thresharr=M.copy()
        for jcor in range(R.shape[0]): 
            # now we set to a max value of either thresh, or the Median array max taken over 10 channels
            # assesses +/- 5 chans max values in median spec and then compares with thresh and 
            # attribules to the max over +/- 5 assessed over the same 5 channels if over thresh
            # acts as the buffer the not miss -ve in absorbtion features (black-dashed in plot)
            ## set to +/- 10 now due to abs/emm CO lines in ALMA-IMF data 
            thresharr[jcor,0:10][thresharr[jcor,0:10]<thresh]=thresh # set ends
            thresharr[jcor,-10:][thresharr[jcor,-10:]<thresh]=thresh # set ends
            thresharr[jcor,10:-10]=[thresh if thresharr[jcor,nch-10:nch+10].max(0) < thresh else thresharr[jcor,nch-10:nch+10].max(0) for nch in range(10,M.shape[1]-10)]

           
        # loops ants that triggered in outlier list
        for jant in AntUse:

            # append to known dict if not there already
            if self.AntName[jant] not in self.AntOut[str(spwin)].keys(): self.AntOut[str(spwin)][self.AntName[jant]]={}
                
            # for later logic of action if required
            replaceCorr = [False for iCor in range(self.num_corrs)]
            
            for jcor in range(R.shape[0]):  

                Rcomp = 1.0+pl.absolute((R[jcor,:,jant]/M[jcor,:])-1.0)
                # make channel based assessment and reset to specific channel value in median spectrum - default operation
                # but as we replace we store how many channels are replaced for later logic
                R[jcor,:,jant]=[M[jcor][nch] if Rcomp[nch]>thresharr[jcor][nch] else R[jcor,:,jant][nch] for nch in range(M.shape[1])] 
                lineOut[jcor]=[spl for spl in range(M.shape[1]) if Rcomp[spl]>thresharr[jcor][spl]]
                plttxt[jcor]=' **** Replace flagged channels with that from median spectrum **** ' # store a print statement for plot - can be later overwritten
                if len(lineOut[jcor])>0:
                    if verbose:
                        print('   Outlier antenna identified '+str(self.AntName[jant])+' '+str(corPrt[jcor])+' will repair outlier channels')
                    self.logReNorm.write('   Outlier antenna identified '+str(self.AntName[jant])+' '+str(corPrt[jcor])+' will repair outlier channels\n') # LM Added 

                    # open the list for outlier channels if not already existing (fill below)
                    if corPrt[jcor] not in self.AntOut[str(spwin)][self.AntName[jant]].keys(): self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]=[]
                    # also want to know the maximum consecutive channels 
                    maxConseq = self.calcMaxConseq(lineOut[jcor])
                    #print(' ######## consecuitve is '+str(maxConseq)) # for testing

                    # if there are more than 10 consecutive lines follow the replacement with other correlation route, XX -> YY, or YY-> XX
                    if maxConseq > 10: replaceCorr[jcor]=True
                    # the below code will do logic to check if the swap to the oposite correlation is ok or not
                else:
                    # there were no triggered lines, maybe only one pol was bad - done assess any further 
                    # it triggered the outlierAnt but this pol wasn't bad
                    continue 
        
                # if we find a lot of outlier channels, or the consecutive amount of bad channels is triggered  
                # we work out what action to take - 10% of SPW must be bad in total - this is hard coded choice 
                if len(lineOut[jcor])>0.1*M.shape[1] or replaceCorr[jcor]:
                    lineOut1=[]  # set as empty list for checking oposite correlation case
                    if self.num_corrs !=1 and jcor==0 and replaceCorr[1] is False:
                        # this is XX corr, so we test if opposite correlation YY is good - won't do this test if replaceCorr triggered already for YY
                        # and also checks consequtive length, which can trigger replaceCorr for YY too
                        Rcomp1 = 1.0+pl.absolute((R[1,:,jant]/M[1,:])-1.0)
                        lineOut1=[spl for spl in range(M.shape[1]) if Rcomp1[spl]>thresharr[1][spl]]
                        maxConseq1 = self.calcMaxConseq(lineOut1)
                        if maxConseq1>10: replaceCorr[1] = True

                    # after doing YY check above - see what the replacement method should be
                    if self.num_corrs!=1 and jcor==0 and len(lineOut1)<0.1*M.shape[1] and replaceCorr[1] is False:
                        # if YY assessment is ok , correct any few channels on the fly as needed while filling into XX scaling spectrum
                        # this on-the -fly correction of YY is only required as we loop XX then YY, so YY wasn't checked yet
                        R[jcor,:,jant]=[M[jcor][nch] if Rcomp1[nch]>thresharr[1][nch] else R[1,:,jant][nch] for nch in range(M.shape[1])] 
                        plttxt[jcor]=' **** Replaced XX spectrum with good YY spectrum **** '  ## new plot annotation text

                    elif self.num_corrs!=1 and jcor==1 and replaceCorr[0] is False:
                        # replace YY with XX - i.e. jcor = 0 - was either deemed ok, or corrected already as it was looped 
                        # for a few outlier channels, so we simply can fill XX into YY
                        R[jcor,:,jant]=R[0,:,jant]
                        plttxt[jcor]=' **** Replaced YY spectrum with good XX spectrum **** '
                    else:
                        # we get to here as both correclations will have triggered replaceCorr - i.e. lots of channels total or >10 consequtive 
                        R[jcor,:,jant]=M[jcor]
                        plttxt[jcor]=' **** Replaced spectrum with median spectrum **** '



                if len(lineOut[jcor]) < 10 and set(lineOut[jcor]).issubset(self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]):
                    ## usually plot the per spw, per scan, per field correction made, but if that ant was shown already, don't repeat 
                    # i.e. known as problematic don't really need to show the same channel 'fix' for all scans/fields
                    if hardcopy and doplot:
                        if verbose:
                            print('   Not saving hardcopy - channels already identified for this antenna')
                        self.logReNorm.write('   Not saving hardcopy - channels already identified for this antenna\n') # LM Added 
                    continue # i.e. dont need to make any plot

                # add  plotting option to only now do those where some replacement of the spectrum was needed
                                    
                elif maxConseq < 10:
                    if hardcopy and doplot:
                        if verbose:
                            print('   Not saving hardcopy - less than 10 conseq channels adjusted - only birdies outliers')
                        self.logReNorm.write('   Not saving hardcopy - less than 10 conseq channels adjusted - only birdies outliers\n')
                    continue
                else:
                    # extend the list to add known channels now and follow to the plots
                    self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]].extend([lineO for lineO in lineOut[jcor] if lineO not in self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]])
                    
                ## these are the heuristics diagnostic plots
                ## Luke used for checking the heuristics logic to see what was occuring
                # can make 100+ plots, so if heursitics trusted they don't really need investigation
                # mostly for testing and deep dives later if needed
                if doplot:
                    pl.ioff()
                    pl.clf()
                    pl.plot(Rorig[jcor,:,jant],c='r',linestyle='--', label='Orig. Spec.') # orignal spec 
                    pl.plot(Rcomp,c='b',label='Divided (comp) spec.',alpha=0.5)
                    pl.plot(M[jcor],c='g',linewidth='2', label='Median Spec.')# Med spec 
                    pl.plot(R[jcor,:,jant],c='k',label='New Spec.') # new spec 
                    pl.plot(thresharr[jcor],c='0.5',alpha=0.5,linestyle='--',linewidth='3',label='Threshold')
                
                    for lineP in lineOut[jcor]:
                        pl.plot(lineP,0.999,c='y',marker='s')
                    pl.plot(lineOut[jcor][0],0.999,c='y',marker='s', label='Outlier Chns.')# to get the single label
                    pltmin=pl.array([R[jcor,:,jant].min(0),0.9977,Rorig[jcor,:,jant].min(0)]).min(0)
                    Pmax = pl.array([R[jcor,:,jant].max(0),1.015,Rcomp.max(0),thresharr[jcor].max(0)]).max(0)
                    pltmax= 1.+(Pmax-1.)*1.10
                    pl.axis([0.0,M.shape[1],pltmin,pltmax])
                    if plttxt[jcor]:
                        pl.text(0,1.+(Pmax-1.)*1.04,plttxt[jcor])
                    if plotDivisions:
                        target = self.msmeta.namesforfields(fldin)[0]
                        dNchan = self.rnstats['inputs'][target][str(spwin)]['dNchan']
                        nseg = self.rnstats['inputs'][target][str(spwin)]['num_segments']
                        xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
                        pl.vlines(xlocs, 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)                        
                    pl.xlabel('Channels')
                    pl.ylabel('ReNorm Scaling')
                    fname=self.msname+'_ReNormHeuristicOutlierAnt_'+self.AntName[jant]+'_spw'+str(spwin)+'_scan'+str(scanin)+'_field'+str(fldin)+'_'+corPrt[jcor]
                    #pl.title(fname,{'horizontalalignment': 'center', 'fontsize': 'medium','verticalalignment': 'bottom'})
                    pl.title(self.msname+'\nAntenna '+self.AntName[jant]+' Spw: '+str(spwin)+' Scan: '+str(scanin)+' Field: '+str(fldin)+' Corr: '+corPrt[jcor], {'fontsize': 'medium'})

                    # legend lines
                    #pl.legend(loc=(0.70,0.5),prop={'size':8})#fontsize='small')
                    pl.legend(loc='lower center',bbox_to_anchor=(0.5,-0.28),prop={'size':8},ncol=3)#fontsize='small')
                    pl.subplots_adjust(bottom=0.25)
                    # CASA 6 units change unless specificed
                    pl.ticklabel_format(style='plain', useOffset=False)
                    if hardcopy:
                        if not os.path.exists('RN_plots'):
                            os.mkdir('RN_plots')
                        if verbose:
                            print('   Saving hardcopy plot: '+fname)
                        self.logReNorm.write('   Saving hardcopy plot: '+fname+'\n')
                        pl.savefig('./RN_plots/'+fname+'.png')
                        pl.close('all')
                    else:
                        # if not hardcopy, the plots are currently show to screen interactively - i.e. for Luke's huristic checking
                        pl.show()


                            
            

    # LM added function - main diagnostic spectra at lowest level - scaling that each spw, scan, field, ant, correlation will have
    # these plots should look good
    # plotATM will just show the Target ATM transmission profile 
    #
    # AL updated - added target name to plot file names
    #            - changed format of plot titles
    #            - added plotfreq=True parameter to plot xaxis in frequency or plotfreq=False for channels
    def plotdiagSpectra(self, R, scanin, spwin, fldin, threshline=None,plotATM=True, plotfreq=True, plotDivisions=True):
        ##M = pl.median(R,2) # median spectra for each pol - in principle all ants should be the same per field
        # the above is a problem if there are too many 1.0's from flagged data - median ends up being 1.0
        M = pl.nanmedian(pl.where(R!=1,R,pl.nan),2)
        M[:][pl.isnan(M[:])]=1.0 # for excluded chan ranges which are 'nan'
        #need to set back to 1.0, otherwise median has nan values
        # and rest of stats max, min, etc do not deal with it
        
        if plotfreq:
            freqs = self.msmeta.chanfreqs(spwin, 'GHz')
        
        # Simple scaling to obide by for plots
        # to try somewhat keep default axes
        # but as we do a diag for each spw, scan, fld as the scaling spectrum is 
        # found, then we don't store or compare between and get an 'overall' 
        # range for plotting 
        maxVal=[1.0,1.01,1.02,1.05,1.1,1.2,1.3,1.4,1.5,2.0]
        plMax = [plM for plM in maxVal if plM >= R.max()][0]
        if plMax == 1.0:
            plMax == 1.01 
        plMin = min(R.min(), 0.995)
    
        target = self.msmeta.namesforfields(fldin)[0]

        # only works for cross corr 
        #corPrt=['XX','YY']
        #if R.shape[0] == 2:
        pl.close('all')
        pl.ioff()
        pl.clf()
        pl.figure(figsize=(10,8))
        pl.ioff()
        corColor=['r','b']
        medColor=['k','g']
        medLine=[':','--']
        if plotfreq:
            for iCor in range(R.shape[0]):
                for allAnt in range(R.shape[2]):
                    pl.plot(freqs, R[iCor,:,allAnt],c=corColor[iCor],alpha=0.5)
                pl.plot(freqs, M[iCor],c=medColor[iCor],linewidth='4',linestyle=medLine[iCor])
            if threshline and threshline < plMax:
                pl.plot([min(freqs),max(freqs)],[threshline,threshline],linestyle='-',c='c',linewidth='2')
            pl.xlabel('Frequency (GHz)')
        else:
            for iCor in range(R.shape[0]):
                for allAnt in range(R.shape[2]):
                    pl.plot(R[iCor,:,allAnt],c=corColor[iCor],alpha=0.5)
                pl.plot(M[iCor],c=medColor[iCor],linewidth='4',linestyle=medLine[iCor])
            if threshline and threshline < plMax:
                pl.plot([0,R.shape[1]],[threshline,threshline],linestyle='-',c='c',linewidth='2')
            pl.xlabel('Channels')
        pl.ylabel('ReNorm Scaling')  
        fnameM=self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spwin)+'_scan'+str(scanin)+'_field'+str(fldin)
        #pl.title(fnameM,{'horizontalalignment': 'center', 'fontsize': 'large','verticalalignment': 'bottom'})
        pl.title(self.msname+'\nTarget: '+target+' Spw: '+str(spwin)+' Scan: '+str(scanin)+' Field: '+str(fldin), {'fontsize': 'medium'})
        pl.ticklabel_format(useOffset=False)
        if plotfreq:
            pl.xlim(min(freqs)*0.99999, max(freqs)*1.00001)
        else:
            pl.xlim(-1,R.shape[1]+1)
        pl.ylim(plMin,plMax)
        if plotDivisions:
            dNchan = self.rnstats['inputs'][target][str(spwin)]['dNchan']
            nseg = self.rnstats['inputs'][target][str(spwin)]['num_segments']
            xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
            if plotfreq:
                pl.vlines(freqs[xlocs], 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)
            else:
                pl.vlines(xlocs, 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)
        if plotATM:
            # plot the ATM line
            if len(self.atmtrans.keys())==0:
                if self.Band in [9, 10]:
                    ATMprof, ATMprof_imageSB = self.ATMtrans(scanin, spwin, verbose=False)
                else:
                    ATMprof=self.ATMtrans(scanin,spwin,verbose=False)
            else:
                ATMprof=self.atmtrans[target][str(spwin)][str(scanin)]
            pl.twinx()
            if plotfreq:
                pl.plot(freqs, 100.*ATMprof,c='m',linestyle='-',linewidth=2)
                if self.Band in [9, 10]:
                    pl.plot(freqs, 100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
                pl.xlim(min(freqs)*0.99999, max(freqs)*1.00001)
            else: 
                pl.plot(100.*ATMprof,c='m',linestyle='-',linewidth=2) 
                if self.Band in [9, 10]:
                    pl.plot(100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
                pl.xlim(-1,R.shape[1]+1)
            #pl.ylim(0,100)
            if self.Band in [9,10]:
                peak = max(pl.maximum(ATMprof,ATMprof_imageSB)*100.)+10
                pl.ylim(peak-100,peak)
            else:
                peak = max(ATMprof*100.)+10
                pl.ylim(peak-100,peak)
            pl.ticklabel_format(style='plain', useOffset=False)

            # Setup for properly labelling the y-axis so that labels below 0 are blanked (because they have no meaning).
            # update the axes so we can read the labels
            pl.draw()
            # get the axes
            ax = pl.gca()
            #grab the label strings
            ylabels = ax.get_yticklabels()
            # iterate over the labels and if any are located below 0, blank them out
            ylabels = ['' if ylabel.get_position()[1] < 0 else int(ylabel.get_position()[1]) for ylabel in ylabels]
            # set the new label names
            ax.set_yticklabels(ylabels)

            if self.Band in [9, 10]:
                pl.ylabel('ATM Transmission (%), Image Sideband')
            else:
                pl.ylabel('ATM Transmission (%)')
        else:
            # CASA 6 units change unless specificed
            pl.ticklabel_format(style='plain', useOffset=False)
        if not os.path.exists('RN_plots'):
            os.mkdir('RN_plots')
        pl.savefig('./RN_plots/'+fnameM+'.png')
        pl.close('all')


#       else:
#           # note this has not really been tested as we've only dealt with dual corr projects for ALMA-IMF / FAUST so far
#           # maybe this is not the correct way for plots for poliarized data 
#           for allCor in range(R.shape[0]):
#               pl.ioff()
#               pl.clf()
#               if plotfreq:
#                   for allAnt in range(R.shape[2]):
#                       pl.plot(freqs, R[allCor,:,allAnt],c='b',alpha=0.5)
#                   pl.plot(freqs, M[allCor],c='g',linewidth='3')
#                   if threshline and threshline < plMax:
#                       pl.plot([min(freqs), max(freqs)],[threshline,threshline],linestyle='-',c='c',linewidth='2')
#                   pl.xlabel('Frequency (GHz)')
#               else:
#                   for allAnt in range(R.shape[2]):
#                       pl.plot(R[allCor,:,allAnt],c='b',alpha=0.5)
#                   pl.plot(M[allCor],c='g',linewidth='3')
#                   if threshline and threshline < plMax:
#                       pl.plot([0,R.shape[1]],[threshline,threshline],linestyle='-',c='c',linewidth='2')
#                   pl.xlabel('Channels')
#               pl.ylabel('ReNorm Scaling')  
#               fnameM=self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spwin)+'_scan'+str(scanin)+'_field'+str(fldin)+'_'+corPrt[allCor]
#               #pl.title(fnameM,{'horizontalalignment': 'center', 'fontsize': 'medium','verticalalignment': 'bottom'})
#               pl.title(self.msname+'\nTarget: '+target+' Spw: '+str(spwin)+' Scan: '+str(scanin)+' Field: '+str(fldin)+' Corr: '+corPrt[allCor], {'fontsize': 'medium'})
#               if plotfreq:
#                   pl.xlim(min(freqs)*0.99999, max(freqs)*1.00001)
#               else:
#                   pl.xlim(0,R.shape[1])
#               pl.ylim(plMin,plMax)
#               if plotDivisions:
#                   dNchan = self.rnstats['inputs'][target][str(spwin)]['dNchan']
#                   nseg = self.rnstats['inputs'][target][str(spwin)]['num_segments']
#                   xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
#                   if plotfreq:
#                       pl.vlines(freqs[xlocs], 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)
#                   else:
#                       pl.vlines(xlocs, 0.5, 1.5, linestyles='dotted', colors='grey', alpha=0.5, zorder=10)
#               if plotATM:
#                   # plot the ATM line
#                   if len(self.atmtrans.keys())==0:
#                       if self.Band in [9, 10]:
#                           ATMprof, ATMprof_imageSB = self.ATMtrans(scanin, spwin, verbose=False)
#                       else:
#                           ATMprof=self.ATMtrans(scanin,spwin,verbose=False)
#                   else:
#                       ATMprof=self.atmtrans[target][str(spwin)][str(scanin)]
#                   pl.twinx()
#                   if plotfreq:
#                       pl.plot(freqs, 100.*ATMprof,c='m',linestyle='-',linewidth=2)
#                       if self.Band in [9, 10]:
#                           pl.plot(freqs, 100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
#                       pl.xlim(min(freqs)*0.99999, max(freqs)*1.00001)
#                   else:
#                       pl.plot(100.*ATMprof,c='m',linestyle='-',linewidth=2)
#                       if self.Band in [9, 10]:
#                           pl.plot(100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
#                       pl.xlim(-1,R.shape[1]+1)
#                   if self.Band in [9,10]:
#                       peak = max(pl.maximum(ATMprof,ATMprof_imageSB)*100.)+10
#                       pl.ylim(peak-100,peak)
#                   else:
#                       peak = max(ATMprof*100.)+10
#                       pl.ylim(peak-100,peak)
#               # CASA 6 units change unless specificed
#               pl.ticklabel_format(style='plain', useOffset=False)
#               
#               # Setup for properly labelling the y-axis so that labels below 0 are blanked (because they have no meaning).
#               # update the axes so we can read the labels
#               pl.draw()
#               # get the axes
#               ax = pl.gca()
#               #grab the label strings
#               ylabels = ax.get_yticklabels()
#               # iterate over the labels and if any are located below 0, blank them out
#               ylabels = ['' if ylabel.get_position()[1] < 0 else int(ylabel.get_position()[1]) for ylabel in ylabels]
#               # set the new label names
#               ax.set_yticklabels(ylabels)
#                               
#               if self.Band in [9, 10]:
#                   pl.ylabel('ATM Transmission (%), Image Sideband')
#               else:
#                   pl.ylabel('ATM Transmission (%)')
#
#               if not os.path.exists('RN_plots'):
#                   os.mkdir('RN_plots')
#               pl.savefig('./RN_plots/'+fnameM+'.png')
#               pl.close()
        
            

    # LM added
    def calcSetEdge(self,R,edge=0.01):
        # this changes the edge channels to the median value
        (lpcor,lpcha,lpant)=R.shape
        chlo=int(ceil(edge*lpcha))
        chhi=lpcha-1-chlo
        for lcor in range(lpcor):
            for lant in range(lpant):
                Rmed = pl.median(R[lcor,:,lant])
                R[lcor,0:chlo,lant]=Rmed
                R[lcor,chhi:,lant]=Rmed

    # LM added
    def calcMaxConseq(self, linelist):
        cntConsec,maxConsec=0,0
        # loops the differnce of the channels and counts the maximal 
        # consecuitve amount - used later as a trigger to replace channels
        # 2021 Feb 16 added that a gap of 2 can be consecutive
        # as sometimes data are just fluctuating above the threshold 
        # better to do a full replace vs. piece-meal every other channel
        for diffVal in pl.diff(linelist):
            cntConsec = cntConsec + 1 if diffVal == 1 or diffVal == 2 else 0
            maxConsec = max(cntConsec,maxConsec)

        return maxConsec



    # LM added - ATM transmission profile code - aU code dependancy 
    def ATMtrans(self, iscan, ispw, ifld=None, verbose=False):
        # main point of this code to calculate the ATM transmission profile
        # this can be used later in an attempt to remove 
        # large  ATM lines in the scaling specta don't always 'fit-out'

        if verbose:
            print('  Getting ATM transmission profile for spw='+str(ispw)+' and scan='+str(iscan))
        self.logReNorm.write('  Getting ATM transmission profile for spw='+str(ispw)+' and scan='+str(iscan)+'\n') # LM added

        # as sometimes the code was passing scans as single element lists
        # it breaks some msmd calls - make a check - but have tried to pass mainly the bandpass scan as an int now 
        if type(iscan) is list:
            iscan=iscan[0]

        ## find the field if none was input
        if ifld is None:
            # get field from scan
            ifld = int(self.msmeta.fieldsforscan(iscan)[0]) 
            # for a mosaic it will use the first pointing position for that scan

        # fixed parameters adopted from Todd's aU and plotbandpass3 code
        # - could make options later but probably not required for what we need this function to do
        dP=5.0
        dPm=1.1
        maxAltitude=60.0
        h0=1.0
        atmType =  1
        nbands = 1
        telescopeName = 'ALMA'


        numchan=self.msmeta.nchan(ispw)  # numchan=len(freqs)  # is this the same YES
        freqs = self.msmeta.chanfreqs(ispw,'GHz')  # this or above ??
        reffreq=0.5*(freqs[int(numchan/2)-1]+freqs[int(numchan/2)])

        # aU dependnet method
        #azel = aU.computeAzElForScan(self.msname,iscan,mymsmd=self.msmeta) 
        ## OR

        # LM coded for aU independence 
        mydirection=self.renormradec2rad(self.renormdirection2radec(self.msmeta.phasecenter(ifld))) # not truncated then
        myscantime = min(self.msmeta.timesforscan(iscan))
        # message filter as this function prints ALMA's position each call
        casalog.filterMsg('Position:')
        azel=self.renormcomputeAzElFromRADecMJD(mydirection,myscantime/86400)
        casalog.clearFilterMsgList()
        airmass = 1.0/pl.cos((90-azel[1])*pl.pi/180.)

        # get weather params from analysisUtils
        #weatherResult=aU.getWeather(self.msname, scan=str(iscan), mymsmd=self.msmeta,getSolarDirection=False)
        # now separate code
        weatherResult = self.renormWeather(iscan, verbose=False) 
        myqa =qatool()
        if self.Band in [9,10]:
            nbands = 2
            freqs_SB, chansep_SB, center_SB, width_SB = self.getImageSBFreqs(ispw)
            #fCenter = aU.create_casa_quantity(myqa,reffreq,'GHz')# thetool.quantity(value, unit)
            fCenter = myqa.quantity([reffreq, center_SB],'GHz')# thetool.quantity(value, unit)
            chansep=(freqs[-1]-freqs[0])/(numchan-1)
            #fResolution = aU.create_casa_quantity(myqa,chansep,'GHz')
            #fWidth = aU.create_casa_quantity(myqa,numchan*chansep,'GHz')
            fResolution = myqa.quantity([chansep, chansep_SB],'GHz')
            fWidth = myqa.quantity([numchan*chansep, width_SB],'GHz')
        else:
            #fCenter = aU.create_casa_quantity(myqa,reffreq,'GHz')# thetool.quantity(value, unit)
            fCenter = myqa.quantity(reffreq,'GHz')# thetool.quantity(value, unit)
            chansep=(freqs[-1]-freqs[0])/(numchan-1)
            #fResolution = aU.create_casa_quantity(myqa,chansep,'GHz')
            #fWidth = aU.create_casa_quantity(myqa,numchan*chansep,'GHz')
            fResolution = myqa.quantity(chansep,'GHz')
            fWidth = myqa.quantity(numchan*chansep,'GHz')
        P= weatherResult[0]['pressure']
        H= weatherResult[0]['humidity']
        T= weatherResult[0]['temperature']+273.15
        #pwvmedian=aU.getMedianPWV(self.msname)[0]  # this returns 0, if no info is found - even ACA appear to have the main ASDM_CALWVR table saved
        pwvmedian=self.renormMedianPWV(verbose=False) 
        if pwvmedian == 0:
            self.corrATM=False
            # get the band and then set these from quartiles:
            pwvmedian = self.usePWV[self.Band] # so at least it will make resonable plots
            # set global correction of correctATM = False
            # if we don't have the correct PWV the models are off

        # sometimes weather values passed are zeros, even if going through the full weather code
        if P == 0:
            P = 563.0
        if H == 0:
            H = 20.0
        if T == 0:
            T = 273.15    
        
        # note this is more or less from inside Todd's aU of CalcAtmTransmission
        # from Plotbandpass3.py code
        
        myat=attool()

        ## below is aU dependent 
        #ATMresult = myat.initAtmProfile(humidity=H, temperature=aU.create_casa_quantity(myqa,T,"K"),
        #                         altitude=aU.create_casa_quantity(myqa,5059,"m"),
        #                         pressure=aU.create_casa_quantity(myqa,P,'mbar'),
        #                         atmType=atmType,
        #                         h0=aU.create_casa_quantity(myqa, h0,"km"),
        #                         maxAltitude=aU.create_casa_quantity(myqa, maxAltitude,"km"),
        #                         dP=aU.create_casa_quantity(myqa, dP,"mbar"),
        #                         dPm=dPm)
        ATMresult = myat.initAtmProfile(humidity=H, temperature=myqa.quantity(T,"K"),
                                 altitude=myqa.quantity(5059,"m"),
                                 pressure=myqa.quantity(P,'mbar'),
                                 atmType=atmType,
                                 h0=myqa.quantity(h0,"km"),
                                 maxAltitude=myqa.quantity(maxAltitude,"km"),
                                 dP=myqa.quantity(dP,"mbar"),
                                 dPm=dPm)
        # CASA 5 vs 6 check
        if type(ATMresult) == tuple:
            ATMresult = ATMresult[0]
    
        myat.initSpectralWindow(nbands,fCenter,fWidth,fResolution)
        #myat.setUserWH2O(aU.create_casa_quantity(myqa,pwvmedian,'mm'))
        myat.setUserWH2O(myqa.quantity(pwvmedian,'mm'))

        dry = pl.array(myat.getDryOpacitySpec(0)[1])
        wet = pl.array(myat.getWetOpacitySpec(0)[1]['value'])
        transmission = pl.exp(-airmass*(wet+dry))
        if self.Band in [9,10]:
            dry_SB = pl.array(myat.getDryOpacitySpec(1)[1])
            wet_SB = pl.array(myat.getWetOpacitySpec(1)[1]['value'])
            transmission_SB = pl.exp(-airmass*(wet_SB+dry_SB))


        myat.close()
        myqa.done()


        # caution, there is a netsideband affect - LSB and USB
        # if netsideband %2 == 0 then sense = 2, else 1
        # if 1, the trasnsmission flips over the loop in channels 

        mytb.open(self.msname+'/SPECTRAL_WINDOW')
        refFreq = mytb.getcol('REF_FREQUENCY')
        net_sideband = mytb.getcol('NET_SIDEBAND')
        measFreqRef = mytb.getcol('MEAS_FREQ_REF')
        spwname=mytb.getcol('NAME')
        mytb.close()

        orignalspws = range(len(measFreqRef)) ## think this is literally just a list
        # I can't see how this is different if MS use ddid and i've got fdm SPW? 
        # i.e. orignalspw[31] = 31 
        # maybe I don't need as I HAVE the Spw ..


        sense=0
        # LSB are =1, USB are +2 - lower need reversing
        if refFreq[ispw]*1e-9>pl.mean(freqs):
            if net_sideband[ispw] % 2 == 0:
                sense = 1
            else:
                sense = 2

        if sense == 1:
            if verbose:
                print('********* REVERSING THE ATM FOUND FOR LSB ***********')
            # need to super check this !!
            #transhold = pl.zeros(len(transmission))
            #for i in range(len(transmission)):
            #    transhold[i] = transmission[len(transmission)-1-i]
            #transmission = transhold # i.e reveresed order
            transmission = transmission[::-1] # reverse the order
            if self.Band in [9, 10]:
                transmission_SB = transmission_SB[::-1]
        if self.Band in [9, 10]:
            return pl.array(transmission), pl.array(transmission_SB)
        else:
            return transmission

    # LM added 
    def ATMcorrection(self,R,inscan, inspw, infld, calscan, calfld, verbose=False):
        # this is the code that will do the real difference between the 
        # bandpass and the input target and make a correction to the 
        # R, i.e the target AC / Bp AC
        
        # R is the AC input
        if verbose:
            print(' ***** Doing an ATM transmission correction for the ATM line in spw'+str(inspw)+' scan'+str(inscan)+' field'+str(infld)+'*****')
        self.logReNorm.write(' ***** Doing an ATM transmission correction for the ATM line in spw'+str(inspw)+' scan'+str(inscan)+' field'+str(infld)+'*****\n')

        # crude theory here - just correct back in by the BP (or phase) and Target model transmission curves 
        # R is Tar / bandpass   (or phase)
        # so we multiply back in the ATM ? 
        fldnam=self.msmeta.namesforfields(infld)[0]  

        # imporved way - get ratio spectrum to find actually differences and then median outside ant loop
        #
        # AL - Is this the right order??? Souldn't it be Bandpass/Target?? Also, does it make sense to subtract the medians 
        #      first rather than multiplying the ratio and then subtract the median?
        #    - I confirmed that by dividing out the atmo profiles of the target and bandpass from each AC spectrum before
        #      doing the initial divide to create ToB, you get the same answer and using the Bandpass_atm/Target_atm as the ratio
        #      here. So IF that is the right thing to do, it's equivilent. It SEEMS like it, if you want to account for the atmosphere
        #      attenuating the power then you would divide that factor out. 
        #           - except why does it show up as emission then?....
        #    - I guess this more comes down to "how (mathematically) is the atmospheric transmission profile actually effecting the values of the AC 
        #      spectrum?" The answer to that question should shed light on how to "undo" it's effects so that we can make the proper ratio.
        ratioATM = self.atmtrans[fldnam][str(inspw)][str(inscan)] / self.atmtrans[calfld][str(inspw)][calscan]
        #trg_atm = self.atmtrans[fldnam][str(inspw)][str(inscan)]
        #cal_atm = self.atmtrans[calfld][str(inspw)][calscan]
        #ratioATM = abs(trg_atm - cal_atm)/((trg_atm*cal_atm)/2.)
        ratioMed = pl.array(pl.median(ratioATM))
        # shift to baseline of average 1.0
        ratioATM = ratioATM + (1.0 - ratioMed)

        for jcor in range(R.shape[0]):
            for lpant in range(R.shape[2]):
                # TESTING print('ATM correcting corr '+str(jcor)+' antenna '+str(lpant))
                
                # simple correction, the target is attenuated by its own ATM, so we multiply back
                # whereas "R" here is Tar_AC/BP_AC and so to correct the BP attenuated by its own
                # ATM profile we have to divide by BP ATM
                # essntially we are multiplying R by (Tar_ATM/BP_ATM) - NB first test was doing just that and result were actually good
                ## OLD FIRST WAY R[jcor,:,lpant] = R[jcor,:,lpant] * (self.atmtrans[fldnam][str(inspw)][str(inscan)] / self.atmtrans[calfld][str(inspw)][calscan])

                # - improved method - set already R to ~1.0 median and the ATM ratio spectrum (done above), then correct (more notes below)
                medR = pl.array(pl.median(R[jcor,:,lpant]))
                R[jcor,:,lpant] = (R[jcor,:,lpant]+(1.0-medR)) * ratioATM

        # LM notes:

        # first crude tests:
        # applying the correction directly as the ratio only between Tar_ATM/BP_ARM as
        # a multiplication accross the entire SPW will act to change the scaling
        # from real astronomical lines. e.g.,
        # - imagine an scaling spectrum (Tar_AutoCorr/BP_AutoCorr) with a baseline at ~0.9, an astronomical line that peaks at 1.2 
        # in the middle of the SPW, and away from the ATM line that causes a dip down to 0.8 at the upper edge
        # if one were to fit this to obtain the scaling spectrum, the peak rescaling is 1.3, i.e. 1.0-0.9 = 0.1 + 1.2 peak.
        # - consider the ratio of ATM transmission (Tar_ATM/BP_ATM) is ~0.95 AWAY from the ATM line and 1.15 at the ATM feature
        # this means the baseline of the scaling spectrum * ratio ATM transmission becomes 0.9 * 0.95 -> 0.855
        # while the peak astronomical line will become 1.2 * 0.95 -> 1.14
        # the ATM line would be almost entirely corrected out, i.e 0.8 * 1.1 -> 0.88 - close to the new baseline
        # however now fitting this spectrum, the baseline to peak astronomical line is 1.0-0.855 + 1.14 = 1.285
        # thus the overall peak resacling as reduced by 1.3 - 1.285 = 0.015, 1.5%
        # -- the point is the ATM ratio, and the Tar_AC/BP_AC spectra are not baselined, the have 
        #    an aribtary baseline y scale.

        ## anyway, better to get average and apply
        # (i) shift all "R" scaling spectra to a baseline of ~1 (i.e. like first part of fitting code does anyway)
        # (ii) shift the ATM trans ratio correction to a baseline of 1
        # (iii) apply the multiplication, only regions close to the ATM line, i.e. not = 1 will
        #       but changed - i.e. more-or-less correcting out the ATM line residual between target and BP
        #  there is some low level 'shape' remainig in the ATM line channels -  but 
        #  this correction is made before the fitting, which goes 'bad/divergent' when ATM lines exist as
        # lorenzian like profiles and cannot be fitted out - this code doesn't need to fully correct 
        # the ATM profile, it should just remove most of the ATM residual differences so the rest of the 
        # renorm code can handle fitting the scaling spectrum
        
        # option, maybe set the ATM ratio spectrum to 1.0 i.e. no scaling to any channels
        # except where the ATM transmission is deeper than a given value ?
        
        # TESTING TO CODE - i.e. run with and without ATM on many data and see effect on % scaling

    # No return acts on the rescaling spectrum directly 

    

    # LM added - write to the history of the dataset upon application
    # of the renorm - thus a check will be made on subsequent runs that 
    # - if the renorlamization was made and history is present it will not apply again

    def recordApply(self, scanout=None, spwout=None, fldout=None):

        # crude check if we also add more to the message - not that the extended string is checked anyway
        if (scanout is not None) and (spwout is not None) and (fldout is not None):
            messageIn = 'ReNormalization correction applied to spw'+str(spwout)+' scan'+str(scanout)+' field'+str(fldout)+' '+self.RNversion
        else:
            messageIn = 'ReNormalization correction applied '+self.RNversion
        myms.open(self.msname)
        myms.writehistory(messageIn)
        myms.close()

        # nothing to return


    # LM added - check the history
    def checkApply(self):
        # just need to get the history column
        # appears that list history is useless and only writes to the logger ?
        mytb.open(self.msname+'/HISTORY')
        messageOut = list(mytb.getcol('MESSAGE'))
        applyStatus = False # i.e. assume not applied
        for messLine in messageOut:
            # only need to check for this statement
            if 'ReNormalization' in messLine:
                applyStatus = True
        mytb.close()

        return applyStatus

    # LM ADDED extra function for Tsys flag to be written out  
    # this uses the output of the plotRelTsysSpectra 
    # these are to be used with caution and are currently not very robust
    # for 12m Tsys data TDM if trigger is low and ATM are not correctly 
    # accounted for - these are to HELP the DR only




    def getband(self,freq):
        ''' Identify the Band for specific frequency (in GHz)
        '''
        lo=pl.array([0,0,84,125,157,211,275,385,602,787])*1e9
        hi=pl.array([0,0,116,163,212,275,373,500,720,950])*1e9

        return pl.arange(1,len(lo)+1)[(freq>lo)&(freq<hi)][0]

    def writeTsysTemps(self, dictIn=None, rettemplist=False):
        """
        Uses the dictionary from the Tsys Plotting code
        and will return a list with each element being a line
        to be entered into the tsystemplate in PL style

        optinally input discrionry in correct syntax can be passed 
        - note syntax is not currently explicity checked -
        - using retflchan = True in the plotRelTsysSpectra code will
          save the discrioney to the argument of the call if you 
          want a manual view of the dict or to see and edit and input 
          manually into this flag_lines code
        """

        if not dictIn:
            dictIn = self.TsysReturn
                
        listToKeep=[]
        TDM=True
        for keyuse in dictIn.keys():
            spwKeep = keyuse.split('=')[-1]
            # then sub layer is the scan key - not intent based its 
            # code based on Tsys TARGET intent scans 
            scanKeys = dictIn[keyuse].keys()
            # check if the Tsys are in same SPW setup, i.e. in the fdmspws, then ACA 
            # and uses a slightly different option in channel_ranges funct.
            if int(spwKeep) in self.fdmspws:
                TDM=False
            for scankeyuse in scanKeys:
                scanKeep= scankeyuse.split('=')[-1]
                # pass to channel_ranges which will do some consolidation and buffering (i.e. few extra channels)
                chanRans = self.channel_ranges(dictIn[keyuse][scankeyuse], TDM=TDM)
                for chanranlp in chanRans:
                    spwStrUse = str(spwKeep)+':'+str(chanranlp[0])+'~'+str(chanranlp[1])

                    listToKeep.append("mode=\'manual\' spw=\'{}\' scan=\'{}\' reason=\'QA2:tsysflag_tsys_channel\'".format(spwStrUse,scanKeep))
            

        # already write the file here for the logs
        self.writeTsysFlags(listToKeep)

        # passes out template list to argument as option
        if rettemplist:
            return listToKeep  

    def channel_ranges(self, channels, TDM=True):
        """
        Given a list of channels will return a list of 
        ranges that describe them accounding for a buffer
        and gap in input that can be assumed as consecutive
        """
        channels.sort()
        
        if TDM:
            addChan = 1
            gapChan = 3
        else:
            # ACA data are much finer resolution, to combat piece wise flags due to channels
            # triggering just around the threshold, set extra buffers 
            addChan = 5
            gapChan = 10 

        channel_range = [channels[0]-addChan, channels[0]] # gives a wider flag buffer 

        for i, chan in enumerate(channels):
            if chan <= channel_range[1] + gapChan: # checks if a gap of 3 and assumes continous
                channel_range[1] = chan+addChan  # gives a wider buffer to written flag
            else:
                # for discountinuty will call this funct again and 'appends' 
                return [channel_range] + self.channel_ranges(channels[i:])

        # get here if last channel reached
        return [channel_range]

    def writeTsysFlags(self, tsysFline):
        """
        Given the list from the flag_lines func that
        made a list of flag template compatible strings, 
        this code simply writes a PL style flag temp at
        the most detailed level, i.e. spw & chan per scan triggered  
        """

        # open file
        file_tsys = open(self.msname+'_ReNormflagtsystemplate.txt','w')

        # loop over list and write
        for tsyslinew in tsysFline:
            file_tsys.write('\n')
            file_tsys.write(tsyslinew)

        # close
        file_tsys.close()



## to remove any dependance on analysisUtils which was (Pre-JUNE) required
## in order to run the ATMtrans function, I have copied and modified here
## the required functions that Todd had written in aU
## put them in the class so we don't need to pass or make estensive tests
## as per the usual aU input checks

    def renormcomputeAzElFromRADecMJD(self, raDec, mjd):
        """
        Computes the az/el for a specified RA/Dec, MJD for ALMA observations using
        the CASA measures tool.
        
        raDec must either be a tuple in radians: [ra,dec],
        mjd must either be in days
        degrees is output
        - Todd Hunter - aU version
        - Luke Maud - copied and modified into almarenorm.py to make analysisUtils 
        independent.
        """
        
        #raDec im passing in rad already 
        #mjd is a float input in mjd already
        myme = metool() 
        myqa = qatool() 
        frame='AZEL'
        raQuantity = myqa.quantity(raDec[0], 'rad')
        decQuantity = myqa.quantity(raDec[1], 'rad')
        mydir = myme.direction('J2000', raQuantity, decQuantity)
        myme.doframe(myme.epoch('mjd', myqa.quantity(mjd, 'd')))
        observatory='ALMA'
        myme.doframe(myme.observatory(observatory))  # will not throw an exception if observatory not recognized
        myqa.done()
        myazel = myme.measure(mydir,frame)
        myme.done()
        myaz = myazel['m0']['value']
        myel = myazel['m1']['value']
        # want output in Degrees 
        myaz *= 180/pl.pi
        myel *= 180/pl.pi
        return([myaz,myel])



    def renormradec2rad(self, radecstring):
        """
        Convert a position from a single RA/Dec sexagesimal string to RA and
        Dec in radians.
        radecstring: any leading 'J2000' string is removed before consideration
        The RA and Dec portions can be separated by a comma or a space.
        The RA portion of the string must be colon-delimited, space-delimited,
        or 'h/m/s' delimited.
        The Dec portion of the string can be either ":", "." or space-delimited.
        If it is "." delimited, then it must have degrees, minutes, *and* seconds.
        Returns: a tuple
        returnList: if True, then return a list of length 2
        See also rad2radec.
        -Todd Hunter - aU version
        -Luke Maud added to almarenorm.py to avoid analysisUtils dependence
        """
        if radecstring.find('J2000')==0:
            radecstring = radecstring.replace('J2000','')
        if (radecstring.find('h')>0 and radecstring.find('d')>0):
            radecstring = radecstring.replace('h',':').replace('m',':').replace('d',':').replace('s','')
        radec1 = radecstring.replace(',',' ')
        tokens = radec1.split()
        if (len(tokens) == 2):
            (ra,dec) = radec1.split()
        elif (len(tokens) == 6):
            h,m,s,d,dm,ds = radec1.split()
            ra = '%s:%s:%s' % (h,m,s)
            dec = '%+f:%s:%s' % (float(d), dm, ds)
        else:
            print("Invalid format for RA/Dec string: ", radec1)
            return
        tokens = ra.strip().split(':')
        hours = 0
        for i,t in enumerate(tokens):
            hours += float(t)/(60.**i)
        if (dec.find(':') > 0):
            tokens = dec.lstrip().split(':')
        elif (dec.find('.') > 0):
            try:
                (d,m,s) = dec.lstrip().split('.')
            except:
                (d,m,s,sfraction) = dec.lstrip().split('.')
                s = s + '.' + sfraction
            tokens = [d,m,s]
        else:  # just an integer
            tokens = [dec]
        dec1 = 0
        for i,t in enumerate(tokens):
            dec1 += abs(float(t)/(60.**i))
        if (dec.lstrip().find('-') == 0):
            dec1 = -dec1
        decrad = dec1*pl.pi/180.
        ra1 = hours*15
        rarad = ra1*pl.pi/180.
  
        return(rarad,decrad)


    def renormdirection2radec(self, direction):
        """
        Convert a direction dictionary to a sexagesimal string of format:
        HH:MM:SS.SSSSS, +DDD:MM:SS.SSSSSS
        Todd Hunter - aU version
        Luke Maud edited and added to almarenorm.py for analysis utils independence 
        """
        ra  = direction['m0']['value']
        dec = direction['m1']['value']
        myqa = qatool()
        prec = 5
        mystring = '%s, %s' % (myqa.formxxx('%.12frad'%ra,format='hms',prec=prec),
                               myqa.formxxx('%.12frad'%dec,format='dms',prec=prec).replace('.',':',2))
        myqa.done()
   
        return(mystring)



    def renormWeather(self, scan, verbose=False): 
        ''' edited from Todd Hunter's analysisUtils code to get only what is
        required for the ATMtrans plotting as part of the ALMA renormalization 
        code

        - Luke Maud - copied and stripped down version from analysisUtils 
        written my Todd Hunter to make almarenorm aU independent
        '''

        preferredStation = 'TB2'
        conditions = {}
        conditions['pressure']=conditions['temperature']=conditions['humidity'] = 0
        myTimes = self.msmeta.timesforscan(scan)

        try:
            mytb.open(self.msname+'/WEATHER') # mytb is global tool instance already 
        except:
            print("Could not open the WEATHER table for this ms, default returned.")
            conditions['pressure']=563.0
            conditions['temperature']=0.0 # in deg C
            conditions['humidity'] = 20.0
            return([conditions,myTimes])

        mjdsec = mytb.getcol('TIME')
        indices = pl.argsort(mjdsec)
        mjd = mjdsec/86400.
        pressure = mytb.getcol('PRESSURE')
        relativeHumidity = mytb.getcol('REL_HUMIDITY')
        temperature = mytb.getcol('TEMPERATURE')
        if (pl.mean(temperature) > 100):
            # must be in units of Kelvin, so convert to C
            temperature = temperature-273.15 

        mjdsec = pl.array(mjdsec)[indices]
        pressure = pl.array(pressure)[indices]
        relativeHumidity = pl.array(relativeHumidity)[indices]
        temperature = pl.array(temperature)[indices]
        if 'NS_WX_STATION_ID' in mytb.colnames():
            stations = mytb.getcol('NS_WX_STATION_ID')
        else:
            stations = None
        mytb.close()
        if preferredStation != '':
            wsdict = self.renormWeatherStationNames()
            if wsdict is not None:
                preferredStationID = None
                for w in list(wsdict.keys()):
                    if wsdict[w].find(preferredStation) >= 0:
                        preferredStationID = w
                if preferredStationID is None:
                    print("Preferred station (%s) not found in this dataset. Using all." % (preferredStation))
                else:
                    indices = pl.where(stations == preferredStationID)
                    mjdsec = pl.array(mjdsec)[indices]
                    pressure = pl.array(pressure)[indices]
                    relativeHumidity = pl.array(relativeHumidity)[indices]
                    temperature = pl.array(temperature)[indices]
                    stations = pl.array(stations)[indices]
    

        # find the overlap of weather measurement times and scan times
        matches = pl.where(mjdsec>=min(myTimes))[0]  ## PL doesnt have min and max !!!
        matches2 = pl.where(mjdsec<=max(myTimes))[0]
        noWeatherData = False
        if (len(matches)>0 and len(matches2) > 0):
            # average the weather points enclosed by the scan time range
            selectedValues = range(matches[0], matches2[-1]+1)
            if (len(selectedValues) == 0):
                # there was a either gap in the weather data, or an incredibly short scan duration
                selectedValues = self.renormfindClosestTime(mjdsec, myTimes[0])  
        elif (len(matches)>0):
            # all points are greater than myTime, so take the first one
            selectedValues = matches[0]
        elif (len(matches2)>0):
            # all points are less than myTime, so take the last one
            selectedValues = matches2[-1]
        else:
            # table has no weather data!
            noWeatherData = True
        if (noWeatherData):
            conditions['pressure'] = 563.0
            conditions['temperature'] = 0  # Celsius is expected
            conditions['humidity'] = 20.0
            print("WARNING: No weather data found in the WEATHER table!")
        else:
            if (type(selectedValues)) is not range:
                conditions['readings'] = 1
                if (verbose):
                    print("selectedValues=%d, myTimes[0]=%.0f, len(matches)=%d, len(matches2)=%d" % (selectedValues,
                                                 myTimes[0], len(matches), len(matches2)))
                    if (len(matches) > 0):
                        print("matches[0]=%f, matches[-1]=%f" % (matches[0], matches[-1]))
                    if (len(matches2) > 0):
                        print("matches2[0]=%f, matches2[-1]=%d" % (matches2[0], matches2[-1]))
            else:
                conditions['readings'] = len(selectedValues)
            conditions['pressure'] = pl.mean(pressure[selectedValues])
            if (conditions['pressure'] != conditions['pressure']):
                # A nan value got through, due to no selected values (should be impossible)"
                if (verbose):
                    print(">>>>>>>>>>>>>>>>>>>>>>>>  selectedValues = ", selectedValues)
                    print("len(matches)=%d, len(matches2)=%d" % (len(matches), len(matches2)))
                    print("matches[0]=%f, matches[-1]=%f, matches2[0]=%f, matches2[-1]=%d" % (matches[0], matches[-1], matches2[0], matches2[-1]))
            conditions['temperature'] = pl.mean(temperature[selectedValues])
            conditions['humidity'] = pl.mean(relativeHumidity[selectedValues])
            if (verbose and noWeatherData==False):
                print("  Pressure = %.2f mb" % (conditions['pressure']))
                print("  Temperature = %.2f C" % (conditions['temperature']))
                print("  Relative Humidity = %.2f %%" % (conditions['humidity']))

        return([conditions,myTimes])



    def renormMedianPWV(self, verbose=False):
        """
        Extracts the PWV measurements from the WVR on all antennas all times.  
        First, it tries to find the ASDM_CALWVR
        table in the ms.  If that fails, it then tries to find the 
        ASDM_CALATMOSPHERE table in the ms.  
        Returns:
        The median PWV
        For further help and examples, see https://safe.nrao.edu/wiki/bin/view/ALMA/GetMedianPWV
        -- Todd Hunter - aU version
        -- Luke Maud - copied and stripped down for almarenorm to make independent of 
        the analysis utilities
        """
        pwvmean = 0  ## actually is the median 
        myTimes = [0,99999999999]
        if (verbose):
            print("in renormMedianPWV with myTimes = ", myTimes)
        try:
            mytb.open(self.msname+'/ASDM_CALWVR')
            pwvtime = mytb.getcol('startValidTime')  # mjdsec
            antenna = mytb.getcol('antennaName')
            pwv = mytb.getcol('water')
            mytb.close()
            # if read but somehow nothing comes back
            if (len(pwv) < 1):
                if verbose:
                    print("Found no data in ASDM_CALWVR table")
                return pwvmean
        except:
            pwv = 0
        if len(pwv)==1:
            try:
                pwvtime, antenna, pwv = renormPWVFromASDM_CALATMOSPHERE()
                if (len(pwv) < 1):
                    if verbose:
                        print("Found no data in ASDM_CALATMOSPHERE table")
                    return pwvmean
            except:
                pwv = 0

        # i.e. didnt get anything from above tables at all
        if len(pwv) == 1:
            if verbose:
                print("Found no data in ASDM_CALWVR nor ASDM_CALATMOSPHERE tables")
            return pwvmean
 
        # my times is hardcoded so should find something
        try:
            matches = pl.where(pl.array(pwvtime)>myTimes[0])[0]
        except:
            if verbose:
                print("Found no times > %d" % (myTimes[0]))
            return pwvmean

        # for testing 
        #print("%d matches = " % (len(matches)), matches)
        #print("%d pwv = " % (len(pwv)), pwv)
        ptime = pl.array(pwvtime)[matches]
        matchedpwv = pl.array(pwv)[matches]
        matches2 = pl.where(ptime<=myTimes[-1])[0]
        # for testing 
        #print("matchedpwv = %s" % (matchedpwv))
        #print("pwv = %s" % (pwv))
        if (len(matches2) < 1):
            # look for the value with the closest start time
            mindiff = 1e12
            for i in range(len(pwvtime)):
                if (abs(myTimes[0]-pwvtime[i]) < mindiff):
                    mindiff = abs(myTimes[0]-pwvtime[i])
                    #                pwvmean = pwv[i]*1000
            matchedpwv = []
            for i in range(len(pwvtime)):
                if (abs(abs(myTimes[0]-pwvtime[i]) - mindiff) < 1.0):
                    matchedpwv.append(pwv[i])
            pwvmean = 1000*pl.median(matchedpwv)
            if (verbose):
                print("Taking the median of %d pwv measurements from all antennas = %.3f mm" % (len(matchedpwv),pwvmean))
        else:
            pwvmean = 1000*pl.median(matchedpwv[matches2])
            if (verbose):
                print("Taking the median of %d pwv measurements from all antennas = %.3f mm" % (len(matches2),pwvmean))
        return pwvmean
        # end of getMedianPWV


    def renormPWVFromASDM_CALATMOSPHERE(self):
        """
        Reads the PWV via the water column of the ASDM_CALATMOSPHERE table in MS.
        - Todd Hunter - aU version
        - Luke Maud copied and edited for almarenorm so independent of analysisUtils
        """
        try:
            mytb.open(self.msname+'/ASDM_CALATMOSPHERE')
            pwvtime = mytb.getcol('startValidTime')  # mjdsec
            antenna = mytb.getcol('antennaName')
            pwv = mytb.getcol('water')[0]  # There seem to be 2 identical entries per row, so take first one.
            mytb.close()
        except:
            return(0,0,0)
        return(pwvtime, antenna, pwv)




    def renormWeatherStationNames(self):
        """
        Returns a dictionary keyed by ALMA weather station ID, with the value
        equal to the station name (e.g. 'WSTBn').
        vis: single measurement set
        -Todd Hunter - aU version
        -Luke Maud copied and modified/stripped down
        from analysisUtils to make almarenorm.py independent
        """
        prefix=['WSTB','Meteo','OSF']
        asdmStation = self.msname+'/ASDM_STATION'

        try:
            mytb.open(asdmStation)
            
            names = mytb.getcol('name')
            mydict = {}
            for i,name in enumerate(names):
                for p in prefix:
                    #            print "Checking if %s contains %s" % (name.lower(),p.lower())
                    if (name.lower().find(p.lower()) >= 0):
                        mydict[i] = name
            mytb.close()
        except:
            print("This measurement set does not have an ASDM_STATION table.")
            return
        return(mydict)
        

    def renormfindClosestTime(self, mytimes, mytime):
        myindex = 0
        mysep = pl.absolute(mytimes[0]-mytime)
        for m in range(1,len(mytimes)):
            if (pl.absolute(mytimes[m] - mytime) < mysep):
                mysep = pl.absolute(mytimes[m] - mytime)
                myindex = m
        return(myindex)

    # AL added - PIPE 1168 (2)
    def convert_plots_pdf(self, target, spw, include_heuristics=False):
        """
        Super hacky way to create PDFs of created plots so that we can display them in the weblog.
        Simply calls the bash commands "montage" (to create super plots of pngs), "convert" (to
        then convert those super plots into pdfs), and "pdfunite" (to combine all pdfs into one).

        Imports: 
            target : string
                Name of the target field that matches the filename target
            spw : str (or int, the type is forced)
                The spectral window of the files that need to be converted to a PDF.
            include_heuristics : boolean (OPTIONAL)
                If set to True, the per antenna heuristics plots will be included in the PDFs.
                Default: False

        Output:
            The target/spw matched plots are montaged and transformed into a PDF and placed into the
            ./RN_plots directory. The name of the PDF is put into the self.rnpipestats dictionary. 
        """
        import glob

        def diagnostic_sort(fn):
            # All files names are deterministic, we want to sort on the scan number, then on field number.
            return int(fn.split('scan')[-1].split('_')[0]), int(fn.split('field')[-1].split('.')[0])

        # First create the montaged files of the ReNormDiagnosticCheck plots. Defaults stolen from AU tools.
        tile = '2x4'
        geometry = '1000x800+2+2'
        pngs = glob.glob('./RN_plots/'+self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_scan*_field*.png')
        if len(pngs) == 0:
            print('No PNGs found! Exiting without creating a PDF.')
            self.logReNorm.write('No PNGs found! Exiting without creating a PDF.\n')
            return
        pngs.sort(key=diagnostic_sort) # sort file names by scan number, then by field to get the right order
        # Add the summary spectra plot to the beginning of the file
        pngs = ['./RN_plots/'+self.msname+'_'+target+'_spw'+str(spw)+'_ReNormSpectra.png']+pngs
        # Add the outlier antenna plots
        fields = pl.intersect1d(self.msmeta.fieldsforintent('*TARGET*'),self.msmeta.fieldsforname(target))
        if include_heuristics:
            ant_pngs = glob.glob('./RN_plots/'+self.msname+'_ReNormHeuristicOutlierAnt_*_spw'+str(spw)+'_scan*field'+str(fields)+'*.png')
            if len(ant_pngs) != 0:
                ant_pngs.sort()
                pngs = pngs+ant_pngs
        pages = int(ceil(len(pngs)/8.)) # Figure out how many pages are needed. We will create tiles of 2 columns x 4 rows
        j = 0
        montaged_pngs = [] # Keep the list of newly created montages
        for i in range(pages):
            figs = pngs[j:j+8]
            figs = ' '.join([fig for fig in figs])
            j+=8
            montage = './RN_plots/'+self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_montage_'+str(i)+'.png'
            os.system('montage -geometry '+geometry+' -tile '+tile+' '+figs+' '+montage)
            montaged_pngs.append(montage)

        # Now convert all the PNGs into PDFs
        for mfile in montaged_pngs:
            os.system('convert '+mfile+' '+mfile.split('.png')[0]+'.pdf')
        pdflist = ' '.join([fname.split('.png')[0]+'.pdf' for fname in montaged_pngs])

        # Finally, create the summary PDF containing all the plots
        outfile = self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_summary.pdf'
        os.system('pdfunite '+pdflist+' ./RN_plots/'+outfile)

        # Put the name of the new PDF into the self.rnpipestats dictionary.
        if self.rnpipestats[target][str(spw)]:
            self.rnpipestats[target][str(spw)]['pdf_summary'] = outfile

    # AL added - PIPE-1188
    def getImageSBFreqs(self,spwin):
        """
        Purpose:
            Given the spectral window, find the LO frequency and return the frequencies of
            the image sideband. Specifically, return the inputs needed for ATMtrans() in 
            order to properly show the atmospheric transmission for bands 9 and 10 where the
            image sideband atomospheric window also contributes to the spectral window. 

        Inputs:
            spwin : int
                The spectral window for which the image sideband frequencies are needed.

        Outputs:
            fSB : numpy.array
                The frequencies of the image sideband. These will be properly arranged 
                (i.e. they will be opposite in direction to the input spectral window 
                frequencies). 

            chansepSB : float
                The separation between channels (channel width).

            fCenterSB : float
                The mean frequency (center) of the image sideband.
                
            fWidthSB : float
                The total bandwidth of the image sideband (will be equal to the input
                spectral window). 
        
        Note: This has been taken from Todd Hunter's AU tools, specifically au.interpretLOs,
              au.getLOs, and plotbandpass3.py - CalcAtmTranmission.
        """
        # Get the information we need from the MS table ASDM_RECEIVER which will give us
        # the spw numbers, the LOs, and the "names" (more like an intent) of the spws.
        mytb.open(self.msname+'/ASDM_RECEIVER')
        numLO = mytb.getcol('numLO')
        freqLO = []
        spws = []
        names = []
        for i in range(len(numLO)):
            spw = int((mytb.getcell('spectralWindowId',i).split('_')[1]))
            if (spw not in spws):
                spws.append(spw)
                freqLO.append(mytb.getcell('freqLO',i))
                names.append(mytb.getcell('name',i))
        mytb.close()
        
        # We want to ignore the superfluous WVR windows and find the right index for our
        # input spectral window.
        sawWVR=False
        indices = []
        for i in range(len(spws)):
            if (names[i].find('WVR') >= 0):
                if (not sawWVR):
                    indices.append(i)
                    sawWVR = True
            else:
                indices.append(i)

        # This is quite clever (taken from Todd's bandpass3), the LO is the frequency that 
        # is exactly between the spw and the image sideband. Therefore, 
        #   2*(spw_freq - LO_freq) - spw_freq
        #   2 * spw_freq - 2 * LO_freq - spw_freq
        #   spw_freq - 2 * LO_freq
        # this results in each channel getting the correctly matched image sideband frequency
        # such that the array counts in the right direction which is opposite the input spectral
        # window. 
        fSB = pl.array(2*freqLO[indices[spwin]][0] - self.msmeta.chanfreqs(spwin))*1e-9
        fCenterSB = pl.mean(fSB)
        chansepSB = (fSB[-1]-fSB[0])/(len(fSB)-1)
        fWidthSB = chansepSB*len(fSB)

        return fSB, chansepSB, fCenterSB, fWidthSB


        
