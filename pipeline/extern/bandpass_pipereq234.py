import getopt
import sys
import os
import pickle
sys.path.append("/home/casa/contrib/AIV/science/analysis_scripts/")
import glob
import analysisUtils as myau
import numpy as np
import numpy.ma as ma
import matplotlib.pyplot as plt
import scipy.stats as st
from scipy import signal
from scipy import ndimage

#from casatools import table as tbtool
from casatools import table
from casatools import msmetadata as msmdtool

WVR_LO=[38.1927,45.83125,91.6625,183.325,259.7104,274.9875,366.650,458.3125,641.6375]


def fitAtmLines(ATMprof, freq):
         """
         Purpose:
             Given an atmospheric profile, find the features caused by water/ozone/etc
             and fit a Lorentizian profile to it, returning the center and gamma scale.

         Inputs:
             ATMprof : numpy.ndarray
                 An atmospheric profile as returned by ATMtrans()

             verbose : boolean : OPTIONAL
                 If True, then information about the fit will be printed to the terminal.

         Outputs:
             centers : list of floats
                 The center channel of found features.

             scales : list of floats
                 The gamma scale factor of found features.

         Note:
             This utilizes the scipy package, specifically scipy.signal.find_peaks and
             scipy.optimize.curve_fit.
         """
         from scipy.optimize import curve_fit
         from scipy.signal import find_peaks

         def get_atm_peaks(ATMprof):
             """
             Purpose: Use scipy's peak finding algorithm to find ATM dips >1%.

             Inputs:
                 ATMprof : array
                     An atmospheric profile which is simply a 1 dimensional array.
             """
             normATM = -ATMprof+np.median(ATMprof)
             peaks, _ = find_peaks(normATM, height=0.01)
             return peaks

         def lorentzian(x, x0, a, gam, off):
             """
             Purpose: Standard definition of a Lorentizian to optimize.

             Inputs:
                 x : array
                     The x data to fit to
                 x0 : float
                     The line center.
                 a : float
                     The amplitude of the feature.
                 gam : float
                     The width of the feature (2*gamma = FWHM).
                 off : float
                     The offset from 0.
             """
             return a * gam**2 / (gam**2 + ( x - x0 )**2) + off

         def get_gamma_bounds(center, width=50.):
             """
             Purpose: Assuming that atmospheric features are typically about 50km/s
                      wide, return the frequency width for a given center frequency
                      using the standard radio convention for velocity width.

                         delta_V = delta_nu / nu_0 * c
             Inputs:
                 center : float
                     Freqency location of the line center, nu_0 in the above equation.

                 width : float : OPTIONAL
                     The velocity width in km/s.
                     Default: 50.
             """
             ckms = 299792.4580
             return width*center/ckms

         # Find atmospheric features in the profile
         atm_feature_idxs = get_atm_peaks(ATMprof)

         # Define our x and y data.
         xData = np.arange(0,len(ATMprof))
         yData = ATMprof

         # Loop over each feature, fitting a Lorentizian and reporting the fits.
         centers, scales = [], []
         for i in range(len(atm_feature_idxs)):
             x0_guess = atm_feature_idxs[i]
             a_guess = yData[x0_guess] - max(yData)
             gamma_guess = 1.0
             off_guess = np.median(yData)

             # center must be +/- 20 channels,
             # amp must be between 0 and -1 (atm dips are always negative here)
             # gamma must be between 1 channel and 50 km/s (in channel space).
             # offset is between 0 (no transmission at all) and 1 (no opacity issues).
             x0_bounds = [x0_guess-20, x0_guess+20]
             a_bounds = [-1, 0]
             gamma_bounds = [1,get_gamma_bounds(freq[x0_guess]/abs(freq[1]-freq[0]))]
             off_bounds = [0,1]

             popt, cov = curve_fit(
                             f=lorentzian,
                             xdata=xData,
                             ydata=yData,
                             p0=[x0_guess,a_guess,gamma_guess, off_guess],
                             bounds=([x0_bounds[0], a_bounds[0], gamma_bounds[0], off_bounds[0]],[x0_bounds[1], a_bounds[1], gamma_bounds[1], off_bounds[1]])
                         )
             x2=int(np.ceil(popt[0]))
             x1=int(np.floor(popt[0]))
             
             centers.append((popt[0]-x1)*(float(freq[x2])-float(freq[x1]))/(x2-x1)+float(freq[x1]))
             scales.append(popt[2]*abs(float(freq[1])-float(freq[0])))
         return centers, scales


def getInfoFromTable(caltable):
    myvis=myau.getMeasurementSetFromCaltable(caltable)
    fieldIds = myau.getFieldIDsFromCaltable(caltable)
    fieldNames = myau.getFieldsFromCaltable(caltable,asnames=True)
    spwIds = myau.getSpwsFromCaltable(caltable)
    antennaNames = myau.getAntennaNamesFromCaltable(caltable)
    antIds = myau.getAntennaIDsFromCaltable(caltable)
    visname=caltable[0:caltable.find('.ms')+3]
    pwv,pwv_sigma=myau.getMedianPWV(visname)
    return fieldIds, fieldNames, spwIds, antennaNames, antIds, pwv


def extractValues(data,caltable):
    tabname=caltable.split('/')[-1]
    bandpass_amp=[]; bandpass_phase=[]; bandpass_amp2=[]; bandpass_phase2=[]; bandpass_freq=[]; bandpass_flag=[]
    fieldIds, fieldNames, spwIds, antennaNames, antIds, pwv = getInfoFromTable(caltable)

    fieldname=fieldNames[0]

    # create lists, with indexes antenna, poln, spw
    bandpass_phase=[[] for i in range(len(spwIds))]
    bandpass_amp=[[] for i in range(len(spwIds))]
    bandpass_phase2=[[] for i in range(len(spwIds))]
    bandpass_amp2=[[] for i in range(len(spwIds))]
    bandpass_flag=[[] for i in range(len(spwIds))]
    for i, ispw in enumerate(spwIds):
        bandpass_phase[i]=[[] for iant in range(len(antIds))]
        bandpass_amp[i]=[[] for iant in range(len(antIds))]
        bandpass_phase2[i]=[[] for iant in range(len(antIds))]
        bandpass_amp2[i]=[[] for iant in range(len(antIds))]
        bandpass_flag[i]=[[] for iant in range(len(antIds))]
        for j, iant in enumerate(antennaNames):
            bandpass_phase[i][j]=[[] for ipol in range(2)]
            bandpass_amp[i][j]=[[] for ipol in range(2)]
            bandpass_phase2[i][j]=[[] for ipol in range(2)]
            bandpass_amp2[i][j]=[[] for ipol in range(2)]
            bandpass_flag[i][j]=[[] for ipol in range(2)]


    for i, ifield in enumerate(list(data[tabname].keys())[1:]):
        for j, ispw in enumerate(list(data[tabname][fieldname].keys())):
            for k, iant in enumerate(list(data[tabname][fieldname][ispw].keys())[3:]):
                for l, ipol in enumerate(list(data[tabname][fieldname][ispw][iant].keys())):
                    bandpass_phase[j][k][l]=(data[tabname][ifield][ispw][iant][ipol]['phase'])
                    bandpass_amp[j][k][l]=(data[tabname][ifield][ispw][iant][ipol]['amp'])
                    bandpass_phase2[j][k][l]=(data[tabname][ifield][ispw][iant][ipol]['phase2'])
                    bandpass_amp2[j][k][l]=(data[tabname][ifield][ispw][iant][ipol]['amp2'])
                    bandpass_flag[j][k][l]=(data[tabname][ifield][ispw][iant][ipol]['flag'])

    return bandpass_phase, bandpass_amp, bandpass_phase2, bandpass_amp2, bandpass_flag


def evalPerAntBP_Platform(pickle_file,inputpath):
    print('Doing Platforming evaluation')
    if glob.glob('*platform.txt'):
        os.system("rm -rf *platform.txt")  # PLANS: Update these to overwrite if present rather than rm -rf 

    if glob.glob('*platform_value.txt'):
        os.system("rm -rf *platform_value.txt")  # PLANS: Update these to overwrite if present rather than rm -rf
    
    if glob.glob('*flagging.txt'):
        os.system("rm -rf *flagging.txt")  # PLANS: Update these to overwrite if present rather than rm -rf

    with open(pickle_file, 'rb') as f:
        data=pickle.load(f)
    
    note_platform_return = ''
    flagnote_return = ''
    note_platform_start_return = ''
    spws_affected = {}  # Format spw: [ant1...n]

    # Iterate through tables
    for i, itab in enumerate(list(data.keys())):
        pldir=inputpath.split('/')[-1]
#        caltable=glob.glob(inputpath+'/S*/G*/M*/working/'+itab)[0]
        caltable=glob.glob(os.path.abspath(os.path.join('../../', itab)))[0]

        print(caltable+ ' in Platforming evaluation')

        # Get the meta data from caltable
        fieldIds, fieldNames, spwIds, antennaNames, antIds, pwv = getInfoFromTable(caltable)
        # Construct the multidimensional array containing the bandpass solution (amp and phase) from pickle file and caltable
        # Bandpass_{amp/phase}[spwid][antid][polz]
        bandpass_phase, bandpass_amp, bandpass_phase2, bandpass_amp2, bandpass_flag=extractValues(data,caltable)

        eb=itab.split('.')[0]

        outfile=open(itab+'_platform.txt','a')
        outfile_val=open(eb+'_platform_value.txt','a')
        outfile_flag=open(eb+'_flagging.txt','a')

        # Bandpass calibrator field name
        fieldname=list(data[itab].keys())[1]
        refAnt=list(data[itab].keys())[0]

        # Taking statistical summary values for heuristics
        # per spw, ant, and pol
        spwIds = myau.getSpwsFromCaltable(caltable)
        antennaNames = myau.getAntennaNamesFromCaltable(caltable)
        antIds = myau.getAntennaIDsFromCaltable(caltable)

        # Appended string containing the heuristics values 
        note_platform_start=''
        # Appended string containing the flagging commands
        flagnote=''


        ##############################
        # Heurstics evaluation is done per ant, per spw, per pol
        # Therefore we loop through antennas, spwids, polz
        ##############################

        #Loop 1: antenna
        for j, iant in enumerate(antennaNames):
            note_platform_start=''
            flagnote=''
            
            #Loop 2: spw
            for k, ispw in enumerate(spwIds):
                note_platform_start=''
                flagnote=''
                #################################
                #naming plot files
                #bandpass amp   pol0 pol1
                #bandpass phase pol0 pol1
                #################################
                figure_name=itab+'_ant'+iant+'_spw'+str(ispw)+'_platforming.png'
                fig,((ax1,ax2),(ax3,ax4)) = plt.subplots(2,2,figsize=(18,15))
                fig.suptitle(pldir+' '+eb+' '+'ant '+iant+' spw '+str(ispw),fontsize=20)
                
                
                ################################
                #read ancillary information
                ################################
                spw_bandwidth=data[itab][fieldname][ispw]['bw']
                spw_nchan=data[itab][fieldname][ispw]['nchan']
                spw_freq=data[itab][fieldname][ispw]['freq']
                subb_bw = 62.5e6 *15./16.   # for edge channels
                subb_num = abs(int(round ( spw_bandwidth / subb_bw )))    # number of subband chunks
                subb_nchan = int ( spw_nchan / subb_num )    # number of channels per subband


 
                ################################
                #now generating atmospheric transmission model using median pwv value
                #this has two purposes
                #1. the heuristics skips if the subband center frequency is within the frequency range (+/-2FWMH) affected by atmpospheric absorption line
                #2. the heuristics skips if the transmisison value is less than 0.3 at the subband center frequency even if the subband center frequency is 
                #   outside the atmopspheric absorptione line
                ###############################
                if abs(spw_bandwidth) < 1.9e9:
                   chans=range(len(spw_freq))
                   frequency, channel, transmission, Tebbsky, tau = myau.CalcAtmosphere(chans, spw_freq, pwv)
                   centers,scales=fitAtmLines(transmission, spw_freq)   #FWHM=2xscale
                   bounds=[]
                   for b in range(len(centers)):
                       bounds.append([centers[b]-2*scales[b],centers[b]+2*scales[b]])
                ###############################

                #this is a container to keep the value: value[i+4]-value[i]
                bp_amp_diff=[]
                bp_phs_diff=[]
        
                #Loop 3: Polarization
                for ipol in range(2):
                    note_platform_start=''
                    flagnote=''
                
                    #this is a container to keep the value: value[i+4]-value[i]
                    bp_amp_diff=[]
                    bp_phs_diff=[]

                    #bandpass amp and phase from a given antenna, spw, pol
                    bp_phs=(bandpass_phase[k][j][ipol])
                    bp_amp=(bandpass_amp[k][j][ipol])

                    #bp_amp2 and bp_phs2 are used for plotting 
                    bp_phs2=(bandpass_phase2[k][j][ipol])
                    bp_amp2=(bandpass_amp2[k][j][ipol])

                    for ichan in range(spw_nchan-4):
                      if bp_amp[ichan+4] > 0.0:
                        bp_amp_diff.append(bp_amp[ichan+4]-bp_amp[ichan])
                        bp_phs_diff.append(bp_phs[ichan+4]-bp_phs[ichan])
                    bp_amp_rms = min(np.nanstd(bp_amp_diff),np.nanstd(bp_amp))
                    bp_phs_rms = min(np.nanstd(bp_phs_diff),np.nanstd(bp_phs))   # median of all values !=0
                    
                    note_platform=''
                    this_note_platform=''
                    note_platform_phsrms=''
                    note_platform_amprms=''
                    note_platform_phsjump=''
                    note_platform_ampjump=''
                    note_platform_subbspk=''
                    
                    flagchan_range_amp=[]
                    flagchan_range_phs=[]
                    
                    ################################
                    #Heuristics are evaluated only if the data is from BLC mode
                    #Following aoscheck, we check this by spw_bandwidth < 1.9GHz
                    #But we need to refer to PL meta data information or other information 
                    #to idenfity the BLC 
                    ################################
                    if abs(spw_bandwidth) < 1.9e9:
                       #########################
                       #quantities measure for each subband
                       #########################
                       subb_phs_rms=[]; subb_amp_rms=[]; subb_phs=[];  subb_amp=[]; subb_phs_sobel_rms=[]; subb_amp_sobel_rms=[]
                       if subb_num>1:
                            note_platform_start = eb +' '+str(ispw)+' '+iant+' '+str(ipol)+' '
                            #####################
                            #Sobel filter applied to the amp and phase 
                            #####################
                            kernel=np.array([-1,0,1])
                            check_phs=np.copy(bp_phs)
                            check_amp=np.copy(bp_amp)
                            sobel_phs=ndimage.convolve(check_phs[:,0], kernel, mode='constant')
                            sobel_amp=ndimage.convolve(check_amp[:,0], kernel, mode='constant')
                            sobel_phs[0]=sobel_phs[1]
                            sobel_phs[-1]=sobel_phs[-2]
                            sobel_amp[0]=sobel_amp[1]
                            sobel_amp[-1]=sobel_amp[-2]
                            #####################

                            #####################
                            #For each subband, estimate 
                            #    standard deviation of amp and phase: subb_{amp/phs}_rms
                            #    mean of amp and phase: subb_{amp/phs}
                            #    standard deviation of 'sobel' filtered amp and phase: subb_{amp/phs}_sobel_rms
                            #####################
                            for isubb in range(subb_num):
                                                              
                                subb_phs_rms.append(np.nanstd(bp_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp_rms.append(np.nanstd(bp_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_phs.append(np.nanmean(bp_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp.append(np.nanmedian(bp_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_phs_sobel_rms.append(np.nanstd(sobel_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp_sobel_rms.append(np.nanstd(sobel_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))


                            ######################
                            #Heuristics 1: find the subband with anomalously large phase RMS   
                            ######################

                            #estimate the median value of the subband RMS by excluding the largest value
                            subb_phs_rms_sort=np.sort(subb_phs_rms)
                            subb_phs_rms_med = np.nanmedian(subb_phs_rms_sort[:-1])
                            subb_phs_sobel_rms_sort=np.sort(subb_phs_sobel_rms)
                            subb_phs_sobel_rms_med=np.nanmedian(subb_phs_sobel_rms_sort[:-1])

                            ######################
                            # string that indicates the detection of platform "yes" or "no"
                            # maxvalue keeps the maximum heuristics value
                            ######################
                            yesorno='NO'
                            maxvalue=-999
                            for isubb in range(subb_num):
                              ###########################
                              # Do atmospheric modeling and find
                              # 1. the absorption line peaks and their FWHM
                              # 2. transmission 
                              # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                              # transimpact measure the transmission value and check whether the transmission is less than 30%
                              # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                              ###########################
                              atmimpact=False
                              transimpact=False
                              subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                              for b, bound in enumerate(bounds):
                                  if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                      atmimpact=True
                              tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                              if (transmission[tid]<0.3):
                                  transimpact=True
                              ############################    

                              ###################################
                              # Pre-check:
                              # Sobel filtered phase values in a given subband.
                              # 1.if the standard deviation of the Sobel filtered phase is larger than 3x
                              # the median subband RMS of the Sobel filtered phase value
                              # -OR-
                              # 2.if the maximum value of the Sobel filtered phase value is larger than 7x
                              # the median subband RMS of the Sobel filtered phase value,
                              # set check_subb_phs_var="YES"
                              ###################################

                              check_subb_phs_var='NO'
                              if (np.nanstd(sobel_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>3.0*subb_phs_sobel_rms_med) or (np.max(np.abs(sobel_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))>7.0*subb_phs_sobel_rms_med):
                                    check_subb_phs_var='YES'

                              ###################################
                              # update the maxvalue if the new subb_phs_rms is larger
                              ###################################
                              if maxvalue<subb_phs_rms[isubb]:
                                    maxvalue=subb_phs_rms[isubb]
                              ###################################

                              ###################################
                              # Now evaluation 
                              # check, 
                              #  1. subband RMS is 5 x larger than the median subband RMS 
                              #  2. subband RMS is > 10 degree
                              #  3. subband is not affected by atmospheric absorption and low transmission
                              ###################################
                              if ((subb_phs_rms[isubb] > 5.0*subb_phs_rms_med and subb_phs_rms[isubb] > 10.0) and (not atmimpact) and (not transimpact)):
                                ###########################
                                #  check, the subband is neither of the first and the last subband, and the Sobel filtered RMS pre-check
                                ###########################
                                if (isubb != 0 and isubb != subb_num-1) and (check_subb_phs_var == 'YES'): 
                                       yesorno='YES'   
                                       #########################
                                       # this is verbose message, which can be skipped for PL
                                       #########################
                                       this_note_platform = ' QA0_High_phase_spectral_rms subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_phs_rms[isubb]) + 'deg ('+"%.2f" %(subb_phs_rms[isubb]/subb_phs_rms_med)+'sigma)'
                                       note_platform += (this_note_platform+'\n')
                                       if ispw in spws_affected:
                                           spws_affected[ispw].append(iant)
                                       else: 
                                            spws_affected[ispw] = [iant]
                                       return spws_affected
                                       #########################

                                       #########################
                                       # this list contains the frequency range of the affected subband
                                       # it is necessary for plotting
                                       #########################
                                       this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                       flagchan_range_phs.append(this_flagchan_range)
                                       #########################
                                
                                #############################
                                # check, the subband is either the first or the last subband
                                #############################
                                elif (isubb == 0 or isubb == subb_num-1):
                                       ############################
                                       # check if the standard deviation of the Sobel filtered value is 10 x larger than
                                       # the median value of the subbands with the Sobel filtered phase value
                                       ############################
                                       if (np.nanstd(sobel_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>10.0*subb_phs_sobel_rms_med):
                                          yesorno='YES'
                                          ###########################
                                          # this verbose message, which can be skipped for PL
                                          ###########################
                                          this_note_platform = ' QA0_High_phase_spectral_rms subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_phs_rms[isubb]) + 'deg ('+"%.2f" %(subb_phs_rms[isubb]/subb_phs_rms_med)+'sigma)'
                                          note_platform += (this_note_platform+'\n')

                                          if ispw in spws_affected:
                                            spws_affected[ispw].append(iant)
                                          else: 
                                            spws_affected[ispw] = [iant]
                                          continue
                                          #########################

                                          #########################
                                          # this list contains the frequency range of the affected subband
                                          # it is necessary for plotting
                                          #########################
                                          this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                          flagchan_range_phs.append(this_flagchan_range)
                                          ###########################


                            #######################
                            # this string is important and appends the heuristics values for each heuristics
                            #######################
                            note_platform_phsrms = 'Platform(HighPhaseRMS)'+' '+yesorno+' max phs RMS: '+"%.6f"%(maxvalue)+' degrees'+' subb median RMS: '+"%.6f"%(subb_phs_rms_med)+' degrees'+' ' 
                            note_platform_start+=note_platform_phsrms 
                            #######################

                            ######################
                            #Heuristics 2: find the subband with anomalously large amplitude RMS   
                            ######################
                            
                            #estimate the median value of the subband RMS by excluding the largest value
                            subb_amp_rms_sort=np.sort(subb_amp_rms)
                            subb_amp_rms_med = np.nanmedian(subb_amp_rms_sort[:-1])
                            subb_amp_sobel_rms_sort=np.sort(subb_amp_sobel_rms)
                            subb_amp_sobel_rms_med=np.nanmedian(subb_amp_sobel_rms_sort[:-1])
                            
                            ######################
                            # string that indicates the detection of platform "yes" or "no"
                            # maxvalue keeps the maximum heuristics value
                            ######################
                            yesorno='NO'
                            maxvalue=-999
                            for isubb in range(subb_num):
                              ###########################
                              # Do atmospheric modeling and find
                              # 1. the absorption line peaks and their FWHM
                              # 2. transmission 
                              # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                              # transimpact measure the transmission value and check whether the transmission is less than 30%
                              # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                              ###########################
                              atmimpact=False
                              transimpact=False
                              subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                              for b, bound in enumerate(bounds):
                                  if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                      atmimpact=True
                              tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                              if (transmission[tid]<0.3):
                                  transimpact=True


                              ###################################
                              # Pre-check:
                              # Sobel filtered phase values in a given subband.
                              # 1.if the standard deviation of the Sobel filtered amp is larger than 3x
                              # the median subband RMS of the Sobel filtered amp value
                              # -OR-
                              # 2.if the maximum value of the Sobel filtered amp value is larger than 7x
                              # the median subband RMS of the Sobel filtered amp value,
                              # set check_subb_phs_var="YES"
                              ###################################
                              check_subb_amp_var='NO'
                              if (np.nanstd(sobel_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>3.0*subb_amp_sobel_rms_med) or (np.max(np.abs(sobel_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))>7.0*subb_amp_sobel_rms_med):
                                 check_subb_amp_var='YES'
                              
                              ###################################
                              # update the maxvalue if the new subb_amp_rms is larger
                              ###################################
                              if maxvalue<subb_amp_rms[isubb]:
                                    maxvalue=subb_amp_rms[isubb]
                              
                              ###################################
                              # Now evaluation 
                              # check, 
                              #  1. subband RMS is 5 x larger than the median subband RMS 
                              #  2. subband is not affected by atmospheric absorption and low transmission
                              ###################################                              
                              if ((subb_amp_rms[isubb] > 5.0*subb_amp_rms_med) and (not atmimpact) and (not transimpact)):
                                ###########################
                                #  check, the subband is neither of the first and the last subband, and the Sobel filtered RMS pre-check
                                ###########################
                                if (isubb != 0 and isubb != subb_num-1) and (check_subb_amp_var == 'YES'): 
                                    yesorno='YES'
                                    #########################
                                    # this is verbose message, which can be skipped for PL
                                    #########################
                                    this_note_platform = ' QA0_High_amp_spectral_rms  subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_amp_rms[isubb]) + 'amp ('+"%.2f" %(subb_amp_rms[isubb]/subb_amp_rms_med)+'sigma)'
                                    note_platform += (this_note_platform+'\n')
                                    if ispw in spws_affected:
                                           spws_affected[ispw].append(iant)
                                    else: 
                                        spws_affected[ispw] = [iant]
                                    #########################

                                    #########################
                                    # this list contains the frequency range of the affected subband
                                    # it is necessary for plotting
                                    #########################
                                    this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                    flagchan_range_amp.append(this_flagchan_range)
                                    #########################
                                
                                #############################
                                # check, the subband is either the first or the last subband
                                #############################
                                elif (isubb == 0 or isubb == subb_num-1):
                                       ############################
                                       # check if the standard deviation of the Sobel filtered value is 10 x larger than
                                       # the median value of the subbands with the Sobel filtered amp value
                                       ############################
                                       if (np.nanstd(sobel_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>10.0*subb_amp_sobel_rms_med):
                                          yesorno='YES'
                                          ###########################
                                          # this verbose message, which can be skipped for PL
                                          ###########################
                                          this_note_platform = ' QA0_High_amp_spectral_rms  subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_amp_rms[isubb]) + 'amp ('+"%.2f" %(subb_amp_rms[isubb]/subb_amp_rms_med)+'sigma)'
                                          note_platform += (this_note_platform+'\n')
                                          if ispw in spws_affected:
                                            spws_affected[ispw].append(iant)
                                          else:
                                            spws_affected[ispw] = [iant]
                                          #########################

                                          #########################
                                          # this list contains the frequency range of the affected subband
                                          # it is necessary for plotting
                                          #########################
                                          this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                          flagchan_range_amp.append(this_flagchan_range)
                                          ######################### 

                            #######################
                            # this string is important and appends the heuristics values for each heuristics
                            #######################
                            note_platform_amprms = 'Platform(HighAmplitudeRMS)'+' '+yesorno+' max amp RMS: '+"%.6f"%(maxvalue)+' amp'+' subb median RMS: '+"%.6f"%(subb_amp_rms_med)+ ' amp'+' ' 
                            note_platform_start+=note_platform_amprms 
                            #######################


                            ######################
                            # Heuristics 3: find the subband with anomalously large phase offset
                            # Each subband is compared against the two adjacent subbands, using 
                            # several thresholds below
                            ######################
                            
                            bp_jump_sigma=3.0     # jump threshold for 2,...,N-1 th subbands
                            bp_jump_sigma2=6.0    # jump threshold for the first and the last subband
                            ch_step_sigma=3.0     # step threshold at the border of two subbands 

                            ######################
                            # variables with string and numerical values of heuristics 
                            ######################
                            yesorno='NO'
                            maxvalue=-999
                            maxvalue_ch=-999
                            ######################
                            
                            #######################
                            # The first and the second subband are evaluated separately if the total number of subband is >3
                            #######################
                            if subb_num>3:
                               ########################### 
                               # The first subband
                               ###########################
                               ###########################
                               # Do atmospheric modeling and find
                               # 1. the absorption line peaks and their FWHM
                               # 2. transmission 
                               # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                               # transimpact measure the transmission value and check whether the transmission is less than 30%
                               # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                               ###########################

                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(0)*subb_nchan], spw_freq[(1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True

                               
                               ###########################
                               # measure the phase jump for the first subband
                               # and save the measurement into maxvalue if it is larger 
                               # than the current measurement
                               ###########################
                               subb_jump = abs((subb_phs[1]+subb_phs[2])/2.0 - subb_phs[0])    # jump of the first subband
                               if maxvalue<subb_jump:
                                    maxvalue=subb_jump
  
                               ##########################
                               # measure the phase step between the first and the second subband
                               # and save the measurement into maxvalue_ch if it is larger 
                               # than the current measurement
                               ##########################
                               ishift=1
                               ch_step = abs(np.nanmedian(bp_phs[((0+1)*subb_nchan-1):((0+1)*subb_nchan)]) - np.nanmedian(bp_phs[((0+1)*subb_nchan):((0+1)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                    ch_step = abs(np.nanmedian(bp_phs[((0+1)*subb_nchan-ishift):((0+1)*subb_nchan)]) - np.nanmedian(bp_phs[((0+1)*subb_nchan):((0+1)*subb_nchan+ishift)]))
                                    ishift+=1

                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step

                               ##########################
                               # check if the subband jump and subband step is larger than the threshold 
                               #       and if the subband is not affected by low transmission
                               ##########################
                               checkif=False
                               if subb_jump > bp_jump_sigma2 * bp_phs_rms and subb_jump > 5.0 and ch_step > ch_step_sigma * bp_phs_rms  and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                  #########################
                                  # this is verbose message, which can be skipped for PL
                                  #########################
                                  freq_max=(spw_freq[0*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  phase  subband : ' + str(0) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_jump/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_jump) +' degrees'
                                  note_platform += (this_note_platform+'\n')
                                  if ispw in spws_affected:
                                    spws_affected[ispw].append(iant)
                                  else: 
                                    spws_affected[ispw] = [iant]
                                  
                                  #########################
                                  # this list contains the frequency range of the affected subband
                                  # it is necessary for plotting
                                  #########################

                                  this_flagchan_range=[spw_freq[0*subb_nchan], spw_freq[(1)*subb_nchan-1]]
                                  flagchan_range_phs.append(this_flagchan_range)
                               
                               #############################
                               # The last subband
                               #############################
                               ###########################
                               # Do atmospheric modeling and find
                               # 1. the absorption line peaks and their FWHM
                               # 2. transmission 
                               # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                               # transimpact measure the transmission value and check whether the transmission is less than 30%
                               # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                               ###########################
                            
                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True
                               ###########################
                               
                               ###########################
                               # measure the phase jump for the last subband
                               # and save the measurement into maxvalue if it is larger 
                               # than the current measurement
                               ###########################
                               subb_jump = abs((subb_phs[subb_num-3]+subb_phs[subb_num-2])/2.0 - subb_phs[subb_num-1])    # jump of the last subband
                               if maxvalue<subb_jump:
                                    maxvalue=subb_jump
                               

                               ##########################
                               # measure the phase step between the N and N-1 th subband
                               # and save the measurement into maxvalue_ch if it is larger 
                               # than the current measurement
                               ##########################
                               ishift=1
                               ch_step = abs(np.nanmedian(bp_phs[((subb_num-2)*subb_nchan-1):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_phs[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                    ch_step = abs(np.nanmedian(bp_phs[((subb_num-2)*subb_nchan-ishift):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_phs[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+ishift)]))
                                    ishift+=1
                               
                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step
                           

                               ##########################
                               # check if the subband jump and subband step is larger than the threshold 
                               #       and if the subband is not affected by low transmission
                               ##########################

                               checkif=False
                               if subb_jump > bp_jump_sigma2 * bp_phs_rms and subb_jump > 5.0 and ch_step > ch_step_sigma * bp_phs_rms and (not transimpact) :    # platform step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                  
                                  #########################
                                  # this is verbose message, which can be skipped for PL
                                  #########################
                                  freq_max=(spw_freq[(subb_num-1)*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  phase  subband : ' + str(subb_num-1) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_jump/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_jump) +' degrees'
                                  note_platform += (this_note_platform+'\n')
                                  if ispw in spws_affected:
                                   spws_affected[ispw].append(iant)
                                  else: 
                                    spws_affected[ispw] = [iant]
                                  
                                  #########################
                                  # this list contains the frequency range of the affected subband
                                  # it is necessary for plotting
                                  #########################
                                  this_flagchan_range=[spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]]
                                  flagchan_range_phs.append(this_flagchan_range)

                            ########################### 
                            # From the 2nd to Nth subband
                            ###########################

                            for isubb in range(1,subb_num-1): 
                                ###########################
                                # Do atmospheric modeling and find
                                # 1. the absorption line peaks and their FWHM
                                # 2. transmission 
                                # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                                # transimpact measure the transmission value and check whether the transmission is less than 30%
                                # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                                ###########################
                                atmimpact=False
                                transimpact=False
                                subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                                for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                                tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                                if (transmission[tid]<0.3):
                                    transimpact=True

                                ###########################
                                # measure the phase jump for the given subband
                                # and save the measurement into maxvalue if it is larger 
                                # than the current measurement
                                ###########################

                                subb_jump = abs((subb_phs[isubb-1]+subb_phs[isubb+1])/2.0 - subb_phs[isubb])    # jump of this subband                    
                                if maxvalue<subb_jump:
                                    maxvalue=subb_jump
                                
                                ##########################
                                # measure the phase step at the edge of subband (left and right)
                                # and save the measurement into maxvalue_ch (left) and maxvalue_ch1 (right) 
                                # if they are larger than the current measurement
                                ##########################
                                ishift=1
                                ch_step = abs(np.nanmedian(bp_phs[(isubb*subb_nchan-1):(isubb*subb_nchan)]) - np.nanmedian(bp_phs[(isubb*subb_nchan):(isubb*subb_nchan+1)]))
                                while np.isnan(ch_step) and ishift<subb_nchan:
                                    ch_step = abs(np.nanmedian(bp_phs[(isubb*subb_nchan-ishift):(isubb*subb_nchan)]) - np.nanmedian(bp_phs[(isubb*subb_nchan):(isubb*subb_nchan+ishift)]))
                                    ishift+=1

                                ishift=1
                                ch_step1 = abs(np.nanmedian(bp_phs[((isubb+1)*subb_nchan-1):((isubb+1)*subb_nchan)]) - np.nanmedian(bp_phs[((isubb+1)*subb_nchan):((isubb+1)*subb_nchan+1)]))
                                while np.isnan(ch_step1) and ishift<subb_nchan:
                                    ch_step1 = abs(np.nanmedian(bp_phs[((isubb+1)*subb_nchan-ishift):((isubb+1)*subb_nchan)]) - np.nanmedian(bp_phs[((isubb+1)*subb_nchan):((isubb+1)*subb_nchan+ishift)]))
                                    ishift+=1
                               

                                if maxvalue_ch<np.max([ch_step,ch_step1]):
                                    maxvalue_ch=np.max([ch_step,ch_step1])
                                ###########################


                                ##########################
                                # check if the subband jump and subband step are larger than the threshold 
                                #       and if the subband is not affected by low transmission
                                ##########################
                                checkif=False

                                if subb_jump > bp_jump_sigma * bp_phs_rms and subb_jump > 5.0 and ch_step > ch_step_sigma * bp_phs_rms and ch_step1 > ch_step_sigma*bp_phs_rms and (not transimpact) :    # platform offset AND step too large
                                        checkif=True


                                if checkif:    # platform offset AND step too large
                                   yesorno='YES'
                                   #########################
                                   # this is verbose message, which can be skipped for PL
                                   #########################
                                   freq_max=(spw_freq[isubb*subb_nchan]) #GHz
                                   this_note_platform = ' QA0_Platforming  phase  subband : ' + str(isubb) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_jump/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_jump) +' degrees'
                                   note_platform += (this_note_platform+'\n')
                                   if ispw in spws_affected:
                                      spws_affected[ispw].append(iant)
                                   else: 
                                      spws_affected[ispw] = [iant]
                                   
                                   #########################
                                   # this list contains the frequency range of the affected subband
                                   # it is necessary for plotting
                                   #########################
                                   this_flagchan_range=[spw_freq[isubb*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                   flagchan_range_phs.append(this_flagchan_range)
                            
                            #######################
                            # this string is important and appends the heuristics values for each heuristics
                            #######################
                            note_platform_phsjump = 'Platform(PhaseJump)'+' '+yesorno+' max phs Jump: '+"%.6f"%(maxvalue)+' degree'+' max phs Step: '+"%.6f"%(maxvalue_ch)+' degree'+'  subb diff RMS: '+"%.6f"%(bp_phs_rms)+ ' degree'+' ' 
                            note_platform_start+=note_platform_phsjump 
                            #######################

                            ######################
                            # Heuristics 4: find the subband with anomalously large amp offset
                            # Each subband is compared against the two adjacent subbands, using 
                            # several thresholds below
                            #
                            # Heuristics 5: find the subband with anomalously large amp spike at edge
                            # The first and the last subband are not evaluated
                            # Each subband is compared against the two adjacent subbands, using 
                            # several thresholds below
                            ######################

                            bp_jump_sigma=3.0     # jump threshold for 2,...,N-1 th subbands
                            bp_jump_sigma2=6.0    # jump threshold for the first and the last subband
                            ch_step_sigma=5.0     # step threshold at the border of two subbands
                            spk_step_sigma=6.0    

                            ######################
                            # variables with string and numerical values of heuristics 
                            ######################
                            yesorno='NO'
                            maxvalue=-999
                            maxvalue_ch=-999
                            subb_spike=-999
                            subb_base=-999

                            #######################
                            # The first and the second subband are evaluated separately if the total number of subband is >3
                            #######################
                            if subb_num>3:
                               ########################### 
                               # The first subband
                               ###########################
                               ###########################
                               # Do atmospheric modeling and find
                               # 1. the absorption line peaks and their FWHM
                               # 2. transmission 
                               # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                               # transimpact measure the transmission value and check whether the transmission is less than 30%
                               # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                               ###########################

                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(0)*subb_nchan], spw_freq[(1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True

                               ###########################
                               # measure the amp jump for the first subband
                               # and save the measurement into maxvalue if it is larger 
                               # than the current measurement
                               ###########################

                               subb_jump = abs((subb_amp[1]+subb_amp[2])/2.0 - subb_amp[0])    # step or offset of the first subband
                               if maxvalue<subb_jump:
                                    maxvalue=subb_jump
                               
                              
                               ##########################
                               # measure the amp step between the first and the second subband
                               # and save the measurement into maxvalue_ch if it is larger 
                               # than the current measurement
                               ##########################

                               ishift=1
                               ch_step = abs(np.nanmedian(bp_amp[((0+1)*subb_nchan-1):((0+1)*subb_nchan)]) - np.nanmedian(bp_amp[((0+1)*subb_nchan):((0+1)*subb_nchan+1)]))
                               
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                  ch_step = abs(np.nanmedian(bp_amp[((0+1)*subb_nchan-ishift):((0+1)*subb_nchan)]) - np.nanmedian(bp_amp[((0+1)*subb_nchan):((0+1)*subb_nchan+ishift)]))
                                  ishift+=1
                               
                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step
                               

                               ##########################
                               # check if the subband jump and subband step is larger than the threshold 
                               #       and if the subband is not affected by low transmission
                               ##########################

                               checkif=False
                               if subb_jump > bp_jump_sigma2 * bp_amp_rms and ch_step > ch_step_sigma * bp_amp_rms and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                  #########################
                                  # this is verbose message, which can be skipped for PL
                                  #########################
                                  freq_max=(spw_freq[0*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(0) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_jump/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)
                                  note_platform += (this_note_platform+'\n')
                                  if ispw in spws_affected:
                                    spws_affected[ispw].append(iant)
                                  else: 
                                    spws_affected[ispw] = [iant]
                                  
                                  #########################
                                  # this list contains the frequency range of the affected subband
                                  # it is necessary for plotting
                                  #########################                                  
                                  this_flagchan_range=[spw_freq[0*subb_nchan], spw_freq[(0+1)*subb_nchan-1]]
                                  flagchan_range_amp.append(this_flagchan_range)


                               ########################### 
                               # The last subband
                               ###########################
                               ###########################
                               # Do atmospheric modeling and find
                               # 1. the absorption line peaks and their FWHM
                               # 2. transmission 
                               # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                               # transimpact measure the transmission value and check whether the transmission is less than 30%
                               # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                               ###########################
                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True
                               ###########################

                               ###########################
                               # measure the amp jump for the last subband
                               # and save the measurement into maxvalue if it is larger
                               # than the current measurement
                               ###########################
                               subb_jump = abs((subb_amp[subb_num-3]+subb_amp[subb_num-2])/2.0 - subb_amp[subb_num-1])    # step or offset of the first subband
                               if maxvalue<subb_jump:
                                   maxvalue=subb_jump
                               
                               ##########################
                               # measure the phase step between the N and N-1 th subband
                               # and save the measurement into maxvalue_ch if it is larger 
                               # than the current measurement
                               ##########################
                               ishift=1
                               ch_step = abs(np.nanmedian(bp_amp[((subb_num-2)*subb_nchan-1):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_amp[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                  ch_step = abs(np.nanmedian(bp_amp[((subb_num-2)*subb_nchan-ishift):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_amp[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+ishift)]))
                                  ishift+=1
                               if maxvalue_ch<ch_step:
                                    maxvalue_ch=ch_step


                               ##########################
                               # check if the subband jump and subband step is larger than the threshold 
                               #       and if the subband is not affected by low transmission
                               ##########################
                               checkif=False
                               if subb_jump > bp_jump_sigma2 * bp_amp_rms and ch_step > ch_step_sigma * bp_amp_rms and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                 
                                  #########################
                                  # this is verbose message, which can be skipped for PL
                                  #########################
                                  freq_max=(spw_freq[(subb_num-1)*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(subb_num-1) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_jump/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)
                                  note_platform += (this_note_platform+'\n')
                                  if ispw in spws_affected:
                                    spws_affected[ispw].append(iant)
                                  else: 
                                    spws_affected[ispw] = [iant]
                                  
                                  #########################
                                  # this list contains the frequency range of the affected subband
                                  # it is necessary for plotting
                                  #########################
                                  this_flagchan_range=[spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]]
                                  flagchan_range_amp.append(this_flagchan_range)

                            ########################### 
                            # From the 2nd to Nth subband
                            # Additionally, we also evaluate anomalous spikes in the subband
                            ###########################
                            countsubspike=0
                            for isubb in range(1,subb_num-1):
                                ###########################
                                # Do atmospheric modeling and find
                                # 1. the absorption line peaks and their FWHM
                                # 2. transmission 
                                # atmimpact checks whether the subband center frequency is affected by the atmospheric absorption line 
                                # transimpact measure the transmission value and check whether the transmission is less than 30%
                                # It is performed in several places in the code, so ideally it should be isolated as a seperate function
                                ###########################
                                atmimpact=False
                                transimpact=False
                                subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                                for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                                tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                                if (transmission[tid]<0.3):
                                    transimpact=True
                             
                                ###########################
                                # measure the phase jump for the given subband
                                # and save the measurement into maxvalue if it is larger 
                                # than the current measurement
                                ###########################

                                subb_jump = abs((subb_amp[isubb-1]+subb_amp[isubb+1])/2.0 - subb_amp[isubb])    # step or offset of this subband
                                if maxvalue<subb_jump:
                                     maxvalue=subb_jump


                                ##########################
                                # measure the phase step at the edge of subband (left and right)
                                # and save the measurement into maxvalue_ch (left) and maxvalue_ch1 (right) 
                                # if they are larger than the current measurement
                                ##########################
                                ishift=1
                                ch_step=np.nanmean(bp_amp[(isubb*subb_nchan-ishift):(isubb*subb_nchan)]) - np.nanmean(bp_amp[(isubb*subb_nchan):(isubb*subb_nchan+ishift)])
                                while np.isnan(ch_step) and ishift<subb_nchan:
                                      ch_step = np.nanmean(bp_amp[(isubb*subb_nchan-ishift):(isubb*subb_nchan)]) - np.nanmean(bp_amp[(isubb*subb_nchan):(isubb*subb_nchan+ishift)])   # start of subband
                                      ishift+=1
                                
                                ishift=1
                                ch_step1=np.nanmean(bp_amp[((isubb+1)*subb_nchan-ishift):((isubb+1)*subb_nchan)]) - np.nanmean(bp_amp[((isubb+1)*subb_nchan):((isubb+1)*subb_nchan+ishift)])
                                while np.isnan(ch_step1) and ishift<subb_nchan:
                                      ch_step1 = np.nanmean(bp_amp[((isubb+1)*subb_nchan-ishift):((isubb+1)*subb_nchan)]) - np.nanmean(bp_amp[((isubb+1)*subb_nchan):((isubb+1)*subb_nchan+ishift)])   # start of subband
                                      ishift+=1

                                if maxvalue_ch<np.max([ch_step,ch_step1]):
                                    maxvalue_ch=np.max([ch_step,ch_step1])
                                ###########################
                                
                                ###########################
                                # measure a spike for the given subband
                                # and count it if the spike is significant
                                ###########################

                                #spectral channel segment for spike estimate
                                ishift_spk=np.max([int(subb_nchan*0.3),3])

                                left=np.nanmean(bp_amp[isubb*subb_nchan-2*ishift_spk:isubb*subb_nchan-ishift_spk])
                                right=np.nanmean(bp_amp[isubb*subb_nchan+ishift_spk:isubb*subb_nchan+2*ishift_spk])
                                subb_spk=np.abs(np.nanmean([left,right]) - bp_amp[isubb*subb_nchan-ishift_spk:isubb*subb_nchan+ishift_spk])
                                subb_spkmax_id=np.argmax(subb_spk)
                                spk_step=subb_spk[subb_spkmax_id]

                                if subb_spike<abs(spk_step):
                                        subb_spike=abs(spk_step)
                                        subb_base=abs(np.nanmean([left,right]))
                                
                                ##########################
                                # if the spike channel is within atmospheric absorption band,
                                # we consider that this is not a genuine spike
                                ##########################
                                spk_atmimpact=False
                                for b, bound in enumerate(bounds):
                                  if (spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk] < bound[1] and spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk] > bound[0]):
                                      spk_atmimpact=True

                                if (abs(spk_step)>0.1*np.nanmean([left,right])) and (abs(spk_step) > spk_step_sigma*bp_amp_rms) and (not spk_atmimpact) and (not transimpact):
                                    countsubspike+=1
  
                                    #########################
                                    # this is verbose message, which can be skipped for PL
                                    #########################
                                    freq1_max=(spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk]) #GHz
                                    this_note_platform = ' QA0_Platforming  amp subband spike: ' + str(isubb) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq1_max +'GHz  P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_jump/bp_amp_rms)+ ' '+"%.1f"%(spk_step/bp_amp_rms)
                                    note_platform += (this_note_platform+'\n')
                                    if ispw in spws_affected:
                                        spws_affected[ispw].append(iant)
                                    else:
                                        spws_affected[ispw] = [iant]

                                    #########################
                                    # this list contains the frequency range of the affected subband
                                    # it is necessary for plotting
                                    #########################
                                    this_flagchan_range1=[spw_freq[subb_spkmax_id+isubb*subb_nchan-1-ishift_spk], spw_freq[subb_spkmax_id+isubb*subb_nchan+1-ishift_spk]]
                                    flagchan_range_amp.append(this_flagchan_range1)

                                #############################
                              

                                ##########################
                                # check if the subband jump and subband step are larger than the threshold 
                                #       and if the subband is not affected by low transmission
                                ##########################
                                checkif=False
                                if (subb_jump > bp_jump_sigma * bp_amp_rms and abs(ch_step) > ch_step_sigma*bp_amp_rms and abs(ch_step1) > ch_step_sigma*bp_amp_rms and np.sign(ch_step) == -np.sign(ch_step1) and (not transimpact)):
                                      checkif=True

                                if checkif: 
                                   yesorno='YES'
                                   freq_max=(spw_freq[isubb*subb_nchan]) #GHz
                                   this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(isubb) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_jump/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)+ ' '  +"%.1f"%(ch_step1/bp_amp_rms)
                                   note_platform += (this_note_platform+'\n')
                                   this_flagchan_range=[spw_freq[isubb*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                   flagchan_range_amp.append(this_flagchan_range)
                                   if ispw in spws_affected:
                                        spws_affected[ispw].append(iant)
                                   else: 
                                        spws_affected[ispw] = [iant]
                            
                            #######################
                            # this string is important and appends the heuristics values for each heuristics
                            #######################
                            note_platform_ampjump = 'Platform(AmplitudeJump)'+' '+yesorno+' max amp Jump: '+"%.6f"%(maxvalue)+' amp'+' max amp Step: '+"%.6f"%(maxvalue_ch)+' amp'+' subb diff RMS: '+"%.6f"%(bp_amp_rms)+ ' amp'+' ' 
                           
                            ##############################
                            # after counting spikes acorss the subband (from 2 to N-1 th subbands)
                            # if more then one spikes, we report it
                            ##############################
                            if countsubspike>0:
                                note_platform_subbspk = 'Platform(AmpSubbSpike)'+' '+'YES'+' max amp Spike: '+"%.6f"%(subb_spike)+' amp'+' subb base: '+"%.6f"%(subb_base)+ ' amp'+' subb diff RMS: '+"%.6f"%(bp_amp_rms)+' amp'+' '
                            else:
                                note_platform_subbspk = 'Platform(AmpSubbSpike)'+' '+'NO'+' max amp Spike: '+"%.6f"%(subb_spike)+' amp'+' subb base: '+"%.6f"%(subb_base)+ ' amp'+' subb diff RMS: '+"%.6f"%(bp_amp_rms)+' amp'+' '
                            #############################


                            #############################
                            # aggregate the verbose note
                            #############################
                            note_platform_start+=note_platform_ampjump 
                            note_platform_start+=note_platform_subbspk
                            note_platform_start+='\n'

                            #############################
                            # aggregate the outlier note
                            #############################
                            flag_note_oneline=note_platform_phsrms+note_platform_amprms+note_platform_phsjump+note_platform_ampjump+note_platform_subbspk

                            ############################
                            # creating the flagging command 
                            # if there is at least one "YES" in the outlier note
                            ############################
                            if 'YES' in flag_note_oneline:
                                flagnote+="# mode='manual' antenna='"+iant+"' spw='"+str(ispw)+"' pol='"+str(ipol)+"' reason='BP platforming'"+"\n"
                       
                       
                       ####################### 
                       # plotting
                       #######################
                       freq_range=np.max(spw_freq)-np.min(spw_freq)
                       margin=freq_range*0.05
                       if ipol==0:
                           amp_range=np.max(bp_amp2)-np.min(bp_amp2)
                           amargin=amp_range*0.1
                           transmission2=(transmission-1.0)*0.1+np.max(bp_amp2)+amargin
                           pcolor='blue'
                           ax1.plot(spw_freq,bp_amp2,color=pcolor)
                           ax1.plot(spw_freq,transmission2,color='black',alpha=0.5)
                           ax1.set_ylabel('amplitude')
                           ax1.set_xlabel('frequency [GHz]')
                           ax1.set_xlim(np.min(spw_freq)-margin,np.max(spw_freq)+margin)
                           if (len(flagchan_range_amp)>0):
                             for p in range(len(flagchan_range_amp)):
                                ax1.hlines(y=np.max(bp_amp2)*1.05,xmin=flagchan_range_amp[p][0], xmax=flagchan_range_amp[p][1],color='black', linewidth=4)
                                ax1.axvspan(flagchan_range_amp[p][0], flagchan_range_amp[p][1],color='black', alpha=0.4)
                           ax3.plot(spw_freq,bp_phs2,color=pcolor)
                           ax3.set_ylabel('degree')
                           ax3.set_xlabel('frequency [GHz]')
                           ax3.set_xlim(np.min(spw_freq)-margin,np.max(spw_freq)+margin)
                           if (len(flagchan_range_phs)>0):
                             for p in range(len(flagchan_range_phs)):
                                ax3.hlines(y=np.max(bp_phs2)*1.05,xmin=flagchan_range_phs[p][0], xmax=flagchan_range_phs[p][1],color='black', linewidth=4)
                                ax3.axvspan(flagchan_range_phs[p][0], flagchan_range_phs[p][1],color='black', alpha=0.4)

                       else:
                           amp_range=np.max(bp_amp2)-np.min(bp_amp2)
                           amargin=amp_range*0.1
                           transmission2=(transmission-1.0)*0.1+np.max(bp_amp2)+amargin
                           pcolor='green'
                           ax2.plot(spw_freq,bp_amp2,color=pcolor)
                           ax2.plot(spw_freq,transmission2,color='black',alpha=0.5)
                           ax2.set_ylabel('amplitude')
                           ax2.set_xlabel('frequency [GHz]')
                           ax2.set_xlim(np.min(spw_freq)-margin,np.max(spw_freq)+margin)
                           if (len(flagchan_range_amp)>0):
                             for p in range(len(flagchan_range_amp)):
                                ax2.hlines(y=np.max(bp_amp2)*1.05,xmin=flagchan_range_amp[p][0], xmax=flagchan_range_amp[p][1],color='black', linewidth=4)
                                ax2.axvspan(flagchan_range_amp[p][0], flagchan_range_amp[p][1],color='black', alpha=0.4)
                           ax4.plot(spw_freq,bp_phs2,color=pcolor)
                           ax4.set_ylabel('degree')
                           ax4.set_xlabel('frequency [GHz]')
                           ax4.set_xlim(np.min(spw_freq)-margin,np.max(spw_freq)+margin)
                           if (len(flagchan_range_phs)>0):
                             for p in range(len(flagchan_range_phs)):
                                ax4.hlines(y=np.max(bp_phs2)*1.05,xmin=flagchan_range_phs[p][0], xmax=flagchan_range_phs[p][1],color='black', linewidth=4)
                                ax4.axvspan(flagchan_range_phs[p][0], flagchan_range_phs[p][1],color='black', alpha=0.4)

                    outfile.write(note_platform)
                    outfile_val.write(note_platform_start)
                    outfile_flag.write(flagnote)
                    
                    note_platform_return += note_platform
                    flagnote_return += flagnote
                    note_platform_start_return += note_platform_start

                plt.savefig(figure_name)
                plt.close()
               
        outfile.close()
        outfile_val.close()
        outfile_flag.close()
        return spws_affected#note_platform_return, note_platform_start_return, flagnote_return #not strictly correct, missing all contents

def bandpass_platforming(inputpath='.', outputpath='./bandpass_platforming_qa'):
   mkdirstr='mkdir '+outputpath

   if os.path.isdir(outputpath) == False:
      os.system(mkdirstr)

   # Pull out table name and associated ms name
   vislist=[]
   tabkey=[]
   tablelist=[]

   # Path to the bandpass tables for analysis
#    mytablelist=glob.glob(inputpath+'/S*/G*/M*/working/uid*bandpass.s*channel.solintinf.bcal.tbl')
#    myworkdir=glob.glob(inputpath+'/S*/G*/M*/working/')
   mytablelist=glob.glob('uid*bandpass.s*channel.solintinf.bcal.tbl')
   myworkdir=glob.glob(inputpath)
   inputpath = os.path.abspath(inputpath)
   print('mytablelist', mytablelist)
   print('myworkdir', myworkdir)

   # Read the bandpass tables and save the table name, the associated MS name, the path to table into a list
   # table name is a key of the dictionary saved into pickle file
   tb=table()
   for mytab in mytablelist:
       tb.open(mytab)
       tb_summary=tb.info()
       if tb_summary['subType']=='B Jones':                           #checking whether this is bandpass gain table (bandtype='B Jones')
          tabkey.append(mytab.split('/')[-1])                         #bandpass table name 
          vislist.append(myau.getMeasurementSetFromCaltable(mytab))   #associated MS name
          tablelist.append(os.path.abspath('./' + mytab))                                     #bandpass table paths 
       tb.close()

   print('tabkey', tabkey)
   print('vislist', vislist)
   print('tablelist', tablelist)

   #go into the output directory to dump the analysis result 
   #products of the analysis result: bandpass data pickle file, text files, and plots
   os.chdir(outputpath)
   
   #creating PL directory to save the data (text files and figures)
   outdir='./bandpass_qa' #mytablelist[0].split('/')[-6]
   if os.path.isdir(outdir) == False:
      os.system('mkdir '+outdir)
   os.chdir(outdir)
   print("changed dir to:", outdir)

   #define structure of pickle file, bandpass_library
   #bandpass_library[mytab][myfield][myspw][myant][mypol]['amp']=amp2
   #bandpass_library[key1][key2][key3][key4][key5][key6]
   #key1: table names
   #key2: reference antenna name and field names
   #key3: spw ID
   #key4: bandwidth, num of channels, frequency, antennas
   #key5: polarization, 0 and 1
   #key6: 'amp'(original data with the PL flag applied, WVR LO checked additionally)
   #      'phase'(original data with the PL flag applied, WVR LO checked additionally)
   #      'amp2' (copy of the original with the PL flag only): not used for the analysis but used for plotting only
   #      'phase2'(copy of the original with the PL flag only): not used for the analyis but used for plotting only 
   #      'flag'
   #Overall structure of the dictionary, for example
   #bandpass_library.keys(['table1','table2','table3'])
   #bandpass_library['table1'].keys(['refAnt','J0821+1234','J1234-0234'])
   #bandpass_library['refAnt'] = 'DA41'
   #bandpass_library['J0821+1234'].keys(['17,19,21,23'])
   #bandpass_library['table1']['J0821+1234']['19'].keys(['bw','nchan','freq','DA41','DA42',...,])
   #bandpass_library['table1']['J0821+1234']['19']['bw']=1.9
   #bandpass_library['table1']['J0821+1234']['19']['nchan']=512
   #bandpass_library['table1']['J0821+1234']['19']['freq']=[123.122,123.123,123.124,.....,]
   #bandpass_library['table1']['J0821+1234']['19']['freq']=[123.122,123.123,123.124,.....,]
   #bandpass_library['table1']['J0821+1234']['19']['DA41'].keys([0,1])
   #bandpass_library['table1']['J0821+1234']['19']['DA41'][0].keys(['amp','phase','amp2','phase2','flag'])
   #bandpass_library['table1']['J0821+1234']['19']['DA41'][0]['amp']=[1.01,0.92,1.03,0.97,...,0.98]
   #bandpass_library['table1']['J0821+1234']['19']['DA41'][0]['phase']=[0.15,0.12,0.10,0.23,...,0.23]
   #bandpass_library['table1']['J0821+1234']['19']['DA41'][0]['flag']=[0,1,1,....,0]
   #

   if os.path.isfile('bandpass_library.pickle')==False:
      bandpass_library={}

      for i, mytab in enumerate(tabkey):
         caltable=tablelist[i]
         tb=table()
         tb.open(caltable)
         print("doing table:",mytab)
         fieldIds, fieldNames, spwIds, antennaNames, antIds, pwv = getInfoFromTable(caltable)
         bandpass_library[mytab]={}

         tmp = tb.getcol('ANTENNA2')
         res=st.mode(tmp)
         refAnt = antennaNames[np.bincount(tb.getcol('ANTENNA2')).argmax()]
         print("FieldNames", fieldNames)
         print("RefAnt", refAnt)
         bandpass_library[mytab]['RefAnt']=refAnt

         #check bandwidth and nchan
         visname=myau.getMeasurementSetFromCaltable(caltable)
         myvis=os.path.join(inputpath,visname)
         print('myvis', myvis)
         spw_bandwidth=myau.getScienceSpwBandwidths(myvis)

         for j, myfield in enumerate(fieldNames):
            print("doing field:",myfield)
            myfieldid=fieldIds[j]
            bandpass_library[mytab][myfield]={}
            
            for m, myspw in enumerate(spwIds):
               print("doing spw:",myspw)

               bandpass_library[mytab][myfield][myspw]={}
               spw_nchan=myau.getNChanFromCaltable(caltable,myspw)
               spw_freq=myau.getChanFreqFromCaltable(caltable,myspw)       #GHz
               bandpass_library[mytab][myfield][myspw]['bw']=spw_bandwidth[m]
               bandpass_library[mytab][myfield][myspw]['nchan']=spw_nchan
               bandpass_library[mytab][myfield][myspw]['freq']=spw_freq

               for k, myant in enumerate(antennaNames):
                  bandpass_library[mytab][myfield][myspw][myant]={}
                  myantid=antIds[k]
                  mytb = tb.query('FIELD_ID == '+str(myfieldid)+' AND SPECTRAL_WINDOW_ID == '+str(myspw)+' AND ANTENNA1 == '+str(myantid))
                  gain=mytb.getcol('CPARAM')
                  err=mytb.getcol('PARAMERR')
                  time=mytb.getcol('TIME')
                  flag=mytb.getcol('FLAG')
                  snr=mytb.getcol('SNR')
               
                  for mypol in range(len(gain)):
                      bandpass_library[mytab][myfield][myspw][myant][mypol]={}
                      phase=np.angle(gain[mypol])
                      phase=np.unwrap(phase)
                      deg=np.degrees(phase)
                      amp=np.absolute(gain[mypol])
                      amp2=np.copy(amp)
                      deg2=np.copy(deg)
                      myflag=flag[mypol]
                      idx=np.where(myflag==True)[0]
                      for myid in range(len(myflag)):
                          if (myid in idx):
                              amp2[myid]=np.nan
                              deg2[myid]=np.nan
                      
                      amp3=np.copy(amp2)
                      deg3=np.copy(deg2)

                      for ifreq, lofreq in enumerate(WVR_LO):
                          freq1=lofreq-62.5/1000.0/2.0
                          freq2=lofreq+62.5/1000.0/2.0
                          if (np.min(spw_freq)<lofreq and np.max(spw_freq)>lofreq):
                             wvrlo_id=np.where((spw_freq>freq1) & (spw_freq<freq2))[0]
                             amp2[wvrlo_id]=np.nan
                             deg2[wvrlo_id]=np.nan

                      bandpass_library[mytab][myfield][myspw][myant][mypol]['amp']=amp2
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['phase']=deg2
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['amp2']=amp3
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['phase2']=deg3
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['flag']=myflag

         tb.close()

      with open('bandpass_library.pickle', 'wb') as f:
          pickle.dump(bandpass_library, f, protocol=pickle.HIGHEST_PROTOCOL)

   # Evaluate bandpass platform/spikes
   spws_affected = evalPerAntBP_Platform('bandpass_library.pickle',inputpath)
   os.chdir(inputpath)
   return spws_affected

def main(argv):
   # Output path needs to be modified 
   def_output_path = ""#/home/zuul07/kberry/repos/clean/pipeline/pipeline/extern/QA0_Bandpass/"

   # Input path need to be modified
   inputpath='./'

   outputpath=def_output_path

   try:
       opts, args = getopt.getopt(argv,"h:i:o:")
   except getopt.GetoptError:
       print("python3 bandpass_prototype.py -i <'input_dir'> -o <'output dir'>")
       sys.exit(2)
     
   for opt, arg in opts:
     if opt in ("-i"):
        inputpath = str(arg)
     elif opt in ("-o"):
        #outputpath = def_output_path+str(arg)
        outputpath = str(arg)

   bandpass_platforming(inputpath=inputpath, outputpath=outputpath)


if __name__ == "__main__":
   main(sys.argv[1:])

