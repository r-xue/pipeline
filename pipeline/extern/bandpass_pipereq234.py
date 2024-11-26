import getopt
import sys
import os
import pickle
sys.path.append("/users/thunter/toddTools/")
sys.path.append("/home/casa/contrib/AIV/science/analysis_scripts/")
import glob
import toddTools as mytt
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
             Given an atmospheric profile, find the features caused by 
water/ozone/etc
             and fit a Lorentizian profile to it, returning the center 
and gamma scale.

         Inputs:
             ATMprof : numpy.ndarray
                 An atmospheric profile as returned by ATMtrans()

             verbose : boolean : OPTIONAL
                 If True, then information about the fit will be printed 
to the terminal.

         Outputs:
             centers : list of floats
                 The center channel of found features.

             scales : list of floats
                 The gamma scale factor of found features.

         Note:
             This utilizes the scipy package, specifically 
scipy.signal.find_peaks and
             scipy.optimize.curve_fit.
         """
         from scipy.optimize import curve_fit
         from scipy.signal import find_peaks

         def get_atm_peaks(ATMprof):
             """
             Purpose: Use scipy's peak finding algorithm to find ATM 
dips >1%.

             Inputs:
                 ATMprof : array
                     An atmospheric profile which is simply a 1 
dimensional array.
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
             Purpose: Assuming that atmospheric features are typically 
about 50km/s
                      wide, return the frequency width for a given 
center frequency
                      using the standard radio convention for velocity 
width.

                         delta_V = delta_nu / nu_0 * c
             Inputs:
                 center : float
                     Freqency location of the line center, nu_0 in the 
above equation.

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
        os.system("rm -rf *platform.txt")

    if glob.glob('*platform_value.txt'):
        os.system("rm -rf *platform_value.txt")
    
    if glob.glob('*platform_maxvalue.txt'):
        os.system("rm -rf *platform_maxvalue.txt")


    with open(pickle_file,'rb') as f:
        data=pickle.load(f)

    for i, itab in enumerate(list(data.keys())):
        pldir=inputpath.split('/')[-1]
        caltable=glob.glob(inputpath+'/S*/G*/M*/working/'+itab)[0]
        print(caltable+ ' in Platforming evaluation')

        fieldIds, fieldNames, spwIds, antennaNames, antIds, pwv = getInfoFromTable(caltable)
        bandpass_phase, bandpass_amp, bandpass_phase2, bandpass_amp2, bandpass_flag=extractValues(data,caltable)

        outfile=open(itab+'_platform.txt','a')

        eb=itab.split('.')[0]
        outfile_val=open(eb+'_platform_value.txt','a')
        outfile_max=open(eb+'_platform_maxvalue.txt','a')

        fieldname=list(data[itab].keys())[1]
        refAnt=list(data[itab].keys())[0]
        bp_med_amp_spw=[]; bp_med_phase_spw=[]
        #taking statistical summary values for heuristics
        #per spw, ant, and pol
        spwIds = myau.getSpwsFromCaltable(caltable)
        antennaNames = myau.getAntennaNamesFromCaltable(caltable)
        antIds = myau.getAntennaIDsFromCaltable(caltable)

        note_platform_start=''

        for j, iant in enumerate(antennaNames):
            note_platform_start=''
            for k, ispw in enumerate(spwIds):
                note_platform_start=''

                figure_name=itab+'_ant'+iant+'_spw'+str(ispw)+'_platforming.png'
                fig,((ax1,ax2),(ax3,ax4)) = plt.subplots(2,2,figsize=(18,15))
                fig.suptitle(pldir+' '+eb+' '+'ant '+iant+' spw '+str(ispw),fontsize=20)
        #precompute the values for comparison
                bp_med_amp_spw=np.nanmedian(bandpass_amp[k],axis=(0,1))
                bp_med_phase_spw=np.nanmedian(bandpass_phase[k],axis=(0,1))
                bp_phase_rms=np.nanmedian(np.nanstd(bandpass_phase[k],axis=2)) #median of the phase RMS (over frequency) for all ants and pols
        #computing 
                spw_bandwidth=data[itab][fieldname][ispw]['bw']
                spw_nchan=data[itab][fieldname][ispw]['nchan']
                spw_freq=data[itab][fieldname][ispw]['freq']
                subb_bw = 62.5e6 *15./16.   # for edge channels
                subb_num = abs(int(round ( spw_bandwidth / subb_bw )))    # number of subband chunks
                subb_nchan = int ( spw_nchan / subb_num )    # number of channels per subband
                bp_amp_diff=[]
                bp_phs_diff=[]

                #now generating ATM
                if abs(spw_bandwidth) < 1.9e9:
                   chans=range(len(spw_freq))
                   frequency, channel, transmission, Tebbsky, tau = myau.CalcAtmosphere(chans, spw_freq, pwv)
                   centers,scales=fitAtmLines(transmission, spw_freq)   #FWHM=2xscale
                   bounds=[]
                   for b in range(len(centers)):
                       bounds.append([centers[b]-2*scales[b],centers[b]+2*scales[b]])
                     
                for ipol in range(2):
                    note_platform_start=''

                    bp_phs=(bandpass_phase[k][j][ipol])
                    bp_amp=(bandpass_amp[k][j][ipol])
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
                    if abs(spw_bandwidth) < 1.9e9:

                       subb_phs_rms=[]; subb_amp_rms=[]; subb_phs=[];  subb_amp=[]; subb_amp_sobel_rms=[]; subb_phs_sobel_rms=[]
                       if subb_num>1:
                            note_platform_start = eb +' '+str(ispw)+' '+iant+' '+str(ipol)+' '
                            #check in-subband variation by Sobel filter
                            kernel=np.array([-1,0,1])
                            check_phs=np.copy(bp_phs)
                            check_amp=np.copy(bp_amp)
                            sobel1_phs=ndimage.convolve(check_phs[:,0], kernel, mode='constant')
                            sobel1_amp=ndimage.convolve(check_amp[:,0], kernel, mode='constant')
                            sobel1_phs[0]=sobel1_phs[1]
                            sobel1_phs[-1]=sobel1_phs[-2]
                            sobel1_amp[0]=sobel1_amp[1]
                            sobel1_amp[-1]=sobel1_amp[-2]

                            sobel2_phs=ndimage.convolve(sobel1_phs, kernel, mode='constant')
                            sobel2_amp=ndimage.convolve(sobel1_amp, kernel, mode='constant')
                            sobel2_phs[0]=sobel2_phs[1]
                            sobel2_phs[-1]=sobel2_phs[-2]
                            sobel2_amp[0]=sobel2_amp[1]
                            sobel2_amp[-1]=sobel2_amp[-2]

                            sobel_phs=ndimage.convolve(sobel2_phs, kernel, mode='constant')
                            sobel_amp=ndimage.convolve(sobel2_amp, kernel, mode='constant')
                            sobel_phs[0]=sobel_phs[1]
                            sobel_phs[-1]=sobel_phs[-2]
                            sobel_amp[0]=sobel_amp[1]
                            sobel_amp[-1]=sobel_amp[-2]


                            for isubb in range(subb_num):
                                subb_phs_rms.append(np.nanstd(bp_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp_rms.append(np.nanstd(bp_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_phs.append(np.nanmean(bp_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp.append(np.nanmedian(bp_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                #check in-subband variation by Sobel filter
                                subb_phs_sobel_rms.append(np.nanstd(sobel1_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))
                                subb_amp_sobel_rms.append(np.nanstd(sobel1_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))


                            #Platforming: high phase RMS
                            subb_phs_max_rms = subb_phs_rms.index(max(subb_phs_rms))    # index of max rms subband
                            subb_phs_rms_sort=np.sort(subb_phs_rms)
                            subb_phs_rms_1 = np.array(subb_phs_rms_sort[:-1])   # removed highest rms subband to estimate median (to avoid with even numbers high & low)
                            subb_phs_rms_med = np.nanmedian(subb_phs_rms_1[subb_phs_rms_1 > 1e-3])
                            subb_phs_sort=np.sort(subb_phs)
                            subb_phs_1 = subb_phs_sort[:-1]
                            subb_phs_med = np.nanmedian(subb_phs_1)

                            subb_phs_sobel_rms_sort=np.sort(subb_phs_sobel_rms)
                            subb_phs_sobel_rms_med=np.nanmedian(subb_phs_sobel_rms_sort[:-1])

                            yesorno='NO'
                            maxvalue=-999
                            for isubb in range(subb_num):
                              #check ib-subband variation
                              check_subb_phs_var='NO'
                              if (np.nanstd(sobel1_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>3.0*subb_phs_sobel_rms_med) or (np.max(np.abs(sobel1_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))>7.0*subb_phs_sobel_rms_med):
                                    check_subb_phs_var='YES'

                              if maxvalue<subb_phs_rms[isubb]:
                                    maxvalue=subb_phs_rms[isubb]

                              atmimpact=False
                              transimpact=False
                              subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                              for b, bound in enumerate(bounds):
                                  if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                      atmimpact=True
                              tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                              if (transmission[tid]<0.3):
                                  transimpact=True
                              
                              if ((subb_phs_rms[isubb] > 5.0*subb_phs_rms_med and subb_phs_rms[isubb] > 10.0) and (not atmimpact) and (not transimpact)):
                                if (isubb != 0 and isubb != subb_num-1) and (check_subb_phs_var == 'YES'): 
                                       yesorno='YES'
                                       this_note_platform = ' QA0_High_phase_spectral_rms subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_phs_rms[isubb]) + 'deg ('+"%.2f" %(subb_phs_rms[isubb]/subb_phs_rms_med)+'sigma)'
                                       note_platform += (this_note_platform+'\n')
                                       this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                       flagchan_range_phs.append(this_flagchan_range)
                                elif (isubb == 0 or isubb == subb_num-1):
                                       if (np.nanstd(sobel1_phs[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>10.0*subb_phs_sobel_rms_med):
                                          yesorno='YES'
                                          this_note_platform = ' QA0_High_phase_spectral_rms subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_phs_rms[isubb]) + 'deg ('+"%.2f" %(subb_phs_rms[isubb]/subb_phs_rms_med)+'sigma)'
                                          note_platform += (this_note_platform+'\n')
                                          this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                          flagchan_range_phs.append(this_flagchan_range)



                            note_platform_phsrms = 'Platform(HighPhaseRMS)'+' '+yesorno+' '+"%.6f"%(maxvalue)+' degrees'+' '+"%.6f"%(subb_phs_rms_med)+ ' degrees'+' ' 
                            note_platform_start+=note_platform_phsrms 

                            #Platforming: high amp RMS
                            subb_amp_max_rms = subb_amp_rms.index(max(subb_amp_rms))    # index of max rms subband
                            subb_amp_rms_sort=np.sort(subb_amp_rms)
                            subb_amp_rms_1 = np.array(subb_amp_rms_sort[:-1])   # removed highest rms subband to estimate median (to avoid with even numbers high & low)
                            subb_amp_rms_med = np.nanmedian(subb_amp_rms_1[subb_amp_rms_1 > 1e-3])
                            subb_amp_sort=np.sort(subb_amp)
                            subb_amp_1 = subb_amp_sort[:-1]
                            subb_amp_med = np.nanmedian(subb_amp_1)

                            subb_amp_sobel_rms_sort=np.sort(subb_amp_sobel_rms)
                            subb_amp_sobel_rms_med=np.nanmedian(subb_amp_sobel_rms_sort[:-1])
                            
                            yesorno='NO'
                            maxvalue=-999
                            for isubb in range(subb_num):
                              check_subb_amp_var='NO'
                              if (np.nanstd(sobel1_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>3.0*subb_amp_sobel_rms_med) or (np.max(np.abs(sobel1_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)]))>7.0*subb_amp_sobel_rms_med):
                                 check_subb_amp_var='YES'
                              
                              if maxvalue<subb_amp_rms[isubb]:
                                    maxvalue=subb_amp_rms[isubb]
                              
                              atmimpact=False
                              transimpact=False
                              subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                              for b, bound in enumerate(bounds):
                                  if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                      atmimpact=True
                              tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                              if (transmission[tid]<0.3):
                                  transimpact=True
                              
                              if ((subb_amp_rms[isubb] > 5.0*subb_amp_rms_med) and (not atmimpact) and (not transimpact)):
                                if (isubb != 0 and isubb != subb_num-1) and (check_subb_amp_var == 'YES'): 
                                    yesorno='YES'
                                    this_note_platform = ' QA0_High_amp_spectral_rms  subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_amp_rms[isubb]) + 'amp ('+"%.2f" %(subb_amp_rms[isubb]/subb_amp_rms_med)+'sigma)'
                                    note_platform += (this_note_platform+'\n')
                                    this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                    flagchan_range_amp.append(this_flagchan_range)
                                elif (isubb == 0 or isubb == subb_num-1):
                                    if (np.nanstd(sobel1_amp[(isubb*subb_nchan):((isubb+1)*subb_nchan)])>10.0*subb_amp_sobel_rms_med):
                                       yesorno='YES'
                                       this_note_platform = ' QA0_High_amp_spectral_rms  subband: '+str(isubb)+' Spw '+str(ispw)+' Ant '+iant+'  P:'+str(ipol)+' BB:'+' TBD'+'  '+ "%.2f"%(subb_amp_rms[isubb]) + 'amp ('+"%.2f" %(subb_amp_rms[isubb]/subb_amp_rms_med)+'sigma)'
                                       note_platform += (this_note_platform+'\n')
                                       this_flagchan_range=[spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                       flagchan_range_amp.append(this_flagchan_range)
                                        

                            note_platform_amprms = 'Platform(HighAmplitudeRMS)'+' '+yesorno+' '+"%.6f"%(maxvalue)+' amp'+' '+"%.6f"%(subb_amp_rms_med)+ ' amp'+' ' 
                            note_platform_start+=note_platform_amprms 
                    
                    #Platforming: phase jump
                            bp_step_sigma=3.0    #!#!#!
                            bp_step_sigma2=6.0    #for the first and the last subb
                            ch_step_sigma=3.0
                            bp_diff_step_last = 0.0
                            ichan=subb_nchan
                            nplatform = 0

                            yesorno='NO'
                            maxvalue=-999
                            maxvalue_ch=-999
                            if subb_num>3:
                            #first subband
                               subb_step = abs((subb_phs[1]+subb_phs[2])/2.0 - subb_phs[0])    # step or offset of the first subband
                               ishift=1
                               if maxvalue<subb_step:
                                    maxvalue=subb_step

                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(0)*subb_nchan], spw_freq[(1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True

                               ch_step = abs(np.nanmedian(bp_phs[((0+1)*subb_nchan-1):((0+1)*subb_nchan)]) - np.nanmedian(bp_phs[((0+1)*subb_nchan):((0+1)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                    ch_step = abs(np.nanmedian(bp_phs[((0+1)*subb_nchan-ishift):((0+1)*subb_nchan)]) - np.nanmedian(bp_phs[((0+1)*subb_nchan):((0+1)*subb_nchan+ishift)]))
                                    ishift+=1

                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step

                               checkif=False
                               if subb_step > bp_step_sigma2 * bp_phs_rms and subb_step > 5.0 and ch_step > ch_step_sigma * bp_phs_rms  and (not transimpact) :    # platform offset AND step too large
                                  checkif=True


                               if checkif:
                                  yesorno='YES'
                                  freq_max=(spw_freq[0*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  phase  subband : ' + str(0) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_step/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_step) +' degrees'
                                  note_platform += (this_note_platform+'\n')
                                  this_flagchan_range=[spw_freq[0*subb_nchan], spw_freq[(1)*subb_nchan-1]]
                                  flagchan_range_phs.append(this_flagchan_range)


                            #from the first+1 to the last-1 subband

                            for isubb in range(1,subb_num-1):
                                subb_step = abs((subb_phs[isubb-1]+subb_phs[isubb+1])/2.0 - subb_phs[isubb])    # step or offset of this subband
#                    
                                if maxvalue<subb_step:
                                    maxvalue=subb_step
                                
                                atmimpact=False
                                transimpact=False
                                subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                                for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                                tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                                if (transmission[tid]<0.3):
                                    transimpact=True

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
                                
                                checkif=False

                                if subb_step > bp_step_sigma * bp_phs_rms and subb_step > 5.0 and ch_step > ch_step_sigma * bp_phs_rms and ch_step1 > ch_step_sigma*bp_phs_rms and (not transimpact) :    # platform offset AND step too large
                                        checkif=True


                                if checkif:    # platform offset AND step too large
                                   yesorno='YES'
                                   freq_max=(spw_freq[isubb*subb_nchan]) #GHz
                                   this_note_platform = ' QA0_Platforming  phase  subband : ' + str(isubb) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_step/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_step) +' degrees'
                                   note_platform += (this_note_platform+'\n')
                                   this_flagchan_range=[spw_freq[isubb*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                   flagchan_range_phs.append(this_flagchan_range)
                            
                            #last subband
                            if subb_num>3:
                               subb_step = abs((subb_phs[subb_num-3]+subb_phs[subb_num-2])/2.0 - subb_phs[subb_num-1])    # step or offset of the first subband
                               ishift=1
                               if maxvalue<subb_step:
                                    maxvalue=subb_step
                               
                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True

                               ch_step = abs(np.nanmedian(bp_phs[((subb_num-2)*subb_nchan-1):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_phs[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                    ch_step = abs(np.nanmedian(bp_phs[((subb_num-2)*subb_nchan-ishift):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_phs[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+ishift)]))
                                    ishift+=1
                               
                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step
                            
                               checkif=False
                               if subb_step > bp_step_sigma2 * bp_phs_rms and subb_step > 5.0 and ch_step > ch_step_sigma * bp_phs_rms and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                  freq_max=(spw_freq[(subb_num-1)*subb_nchan]) #GHz
                                  this_note_platform = ' QA0_Platforming  phase  subband : ' + str(subb_num-1) +'Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P'+str(ipol)+' BB'+'TBD'+' ' +"%.1f" %(subb_step/bp_phs_rms)+ ' '  +"%.1f" %(ch_step/bp_phs_rms)+'sigma   '+ "%.1f" %(subb_step) +' degrees'
                                  note_platform += (this_note_platform+'\n')
                                  this_flagchan_range=[spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]]
                                  flagchan_range_phs.append(this_flagchan_range)
                            note_platform_phsjump = 'Platform(PhaseJump)'+' '+yesorno+' '+"%.6f"%(maxvalue)+' degree'+' '+"%.6f"%(maxvalue_ch)+' degree'+' '+"%.6f"%(bp_phs_rms)+ ' degree'+' ' 
                            note_platform_start+=note_platform_phsjump 


                    #Platforming: amp jump
                            bp_step_sigma=3.0    #!#!#!
                            bp_step_sigma2=6.0    #!#!#!
                            ch_step_sigma=5.0   #!#!
                            spk_step_sigma=6.0   #!#!
                            bp_diff_step_last = 0.0
                            ichan=subb_nchan
                            nplatform = 0

                            yesorno='NO'
                            maxvalue=-999
                            maxvalue_ch=-999
                            subb_spike=-999
                            subb_base=-999

                            if subb_num>3:
                            #first subband
                               subb_step = abs((subb_amp[1]+subb_amp[2])/2.0 - subb_amp[0])    # step or offset of the first subband
                               ishift=1
                               ch_step = abs(np.nanmedian(bp_amp[((0+1)*subb_nchan-1):((0+1)*subb_nchan)]) - np.nanmedian(bp_amp[((0+1)*subb_nchan):((0+1)*subb_nchan+1)]))
                               if maxvalue<subb_step:
                                    maxvalue=subb_step
                               
                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(0)*subb_nchan], spw_freq[(1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True

                               while np.isnan(ch_step) and ishift<subb_nchan:
                                  ch_step = abs(np.nanmedian(bp_amp[((0+1)*subb_nchan-ishift):((0+1)*subb_nchan)]) - np.nanmedian(bp_amp[((0+1)*subb_nchan):((0+1)*subb_nchan+ishift)]))
                                  ishift+=1
                               
                               if maxvalue_ch<ch_step:
                                   maxvalue_ch=ch_step
                               
                               checkif=False
                               if subb_step > bp_step_sigma2 * bp_amp_rms and ch_step > ch_step_sigma * bp_amp_rms and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                  yesorno='YES'
                                  freq_max=(spw_freq[0*subb_nchan]) #GHz

 
                                  this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(0) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_step/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)
                                  note_platform += (this_note_platform+'\n')
                                  this_flagchan_range=[spw_freq[0*subb_nchan], spw_freq[(0+1)*subb_nchan-1]]
                                  flagchan_range_amp.append(this_flagchan_range)



                            #from the first+1 subband to the last-1 subband
                            countsubspike=0
                            for isubb in range(1,subb_num-1):
                                subb_step = abs((subb_amp[isubb-1]+subb_amp[isubb+1])/2.0 - subb_amp[isubb])    # step or offset of this subband
                                if maxvalue<subb_step:
                                     maxvalue=subb_step

                                atmimpact=False
                                transimpact=False
                                subb_center_freq=np.mean([spw_freq[(isubb)*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]])
                                for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                                tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                                if (transmission[tid]<0.3):
                                    transimpact=True
                              
                               
                                ishift_spk=np.max([int(subb_nchan*0.3),3])

                                left=np.nanmean(bp_amp[isubb*subb_nchan-2*ishift_spk:isubb*subb_nchan-ishift_spk])
                                right=np.nanmean(bp_amp[isubb*subb_nchan+ishift_spk:isubb*subb_nchan+2*ishift_spk])
                                subb_spk=np.abs(np.nanmean([left,right]) - bp_amp[isubb*subb_nchan-ishift_spk:isubb*subb_nchan+ishift_spk])
                                subb_spkmax_id=np.argmax(subb_spk)
                                spk_step=subb_spk[subb_spkmax_id]

                                if subb_spike<abs(spk_step):
                                        subb_spike=abs(spk_step)
                                        subb_base=abs(np.nanmean([left,right]))

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

                                
                                spk_atmimpact=False
                                for b, bound in enumerate(bounds):
                                  if (spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk] < bound[1] and spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk] > bound[0]):
                                      spk_atmimpact=True

                                if (abs(spk_step)>0.1*np.nanmean([left,right])) and (abs(spk_step) > spk_step_sigma*bp_amp_rms) and (not spk_atmimpact) and (not transimpact):
                                    countsubspike+=1
                                    
                                    freq1_max=(spw_freq[subb_spkmax_id+isubb*subb_nchan-ishift_spk]) #GHz
                                    this_note_platform = ' QA0_Platforming  amp subband spike: ' + str(isubb) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq1_max +'GHz  P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_step/bp_amp_rms)+ ' '+"%.1f"%(spk_step/bp_amp_rms)
                                    note_platform += (this_note_platform+'\n')
                                    this_flagchan_range1=[spw_freq[subb_spkmax_id+isubb*subb_nchan-1-ishift_spk], spw_freq[subb_spkmax_id+isubb*subb_nchan+1-ishift_spk]]
                                    flagchan_range_amp.append(this_flagchan_range1)

 
                              
                                checkif=False
                                if (subb_step > bp_step_sigma * bp_amp_rms and abs(ch_step) > ch_step_sigma*bp_amp_rms and abs(ch_step1) > ch_step_sigma*bp_amp_rms and np.sign(ch_step) == -np.sign(ch_step1) and (not transimpact)):
                                      checkif=True

                                if checkif: 
                                   yesorno='YES'
                                   freq_max=(spw_freq[isubb*subb_nchan]) #GHz
                                   this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(isubb) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_step/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)+ ' '  +"%.1f"%(ch_step1/bp_amp_rms)
                                   note_platform += (this_note_platform+'\n')
                                   this_flagchan_range=[spw_freq[isubb*subb_nchan], spw_freq[(isubb+1)*subb_nchan-1]]
                                   flagchan_range_amp.append(this_flagchan_range)
                            
                            if subb_num>3:
                            #last subband
                               subb_step = abs((subb_amp[subb_num-3]+subb_amp[subb_num-2])/2.0 - subb_amp[subb_num-1])    # step or offset of the first subband
                               ishift=1
                               if maxvalue<subb_step:
                                   maxvalue=subb_step
                               
                               atmimpact=False
                               transimpact=False
                               subb_center_freq=np.mean([spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]])
                               for b, bound in enumerate(bounds):
                                   if (subb_center_freq<bound[1] and subb_center_freq>bound[0]):
                                       atmimpact=True
                               tid=np.argmax(frequency[np.where(frequency<subb_center_freq)[0]])            
                               if (transmission[tid]<0.3):
                                   transimpact=True
                               
                               ch_step = abs(np.nanmedian(bp_amp[((subb_num-2)*subb_nchan-1):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_amp[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+1)]))
                               while np.isnan(ch_step) and ishift<subb_nchan:
                                  ch_step = abs(np.nanmedian(bp_amp[((subb_num-2)*subb_nchan-ishift):((subb_num-2)*subb_nchan)]) - np.nanmedian(bp_amp[((subb_num-2)*subb_nchan):((subb_num-2)*subb_nchan+ishift)]))
                                  ishift+=1


                               if maxvalue_ch<ch_step:
                                    maxvalue_ch=ch_step
                               
                               checkif=False
                               if subb_step > bp_step_sigma2 * bp_amp_rms and ch_step > ch_step_sigma * bp_amp_rms and (not transimpact) :    # platform offset AND step too large
                                  checkif=True

                               if checkif:
                                 yesorno='YES'
                                 freq_max=(spw_freq[(subb_num-1)*subb_nchan]) #GHz
                                 this_note_platform = ' QA0_Platforming  amplitude subband: ' + str(subb_num-1) +' Spw '+str(ispw)+' Ant '+iant+'  '+"%9.6f"%freq_max + ' GHz   P: '+str(ipol)+' BB:'+' TBD'+ \
                                                      ' sigmas: ' +"%.1f"%(subb_step/bp_amp_rms)+ ' '+"%.1f"%(ch_step/bp_amp_rms)
                                 note_platform += (this_note_platform+'\n')
                                 this_flagchan_range=[spw_freq[(subb_num-2)*subb_nchan], spw_freq[(subb_num-1)*subb_nchan-1]]
                                 flagchan_range_amp.append(this_flagchan_range)
                            
                            note_platform_ampjump = 'Platform(AmplitudeJump)'+' '+yesorno+' '+"%.6f"%(maxvalue)+' amp'+' '+"%.6f"%(maxvalue_ch)+' amp'+' '+"%.6f"%(bp_amp_rms)+ ' amp'+' ' 
                           
                            
                            if countsubspike>0:
                                note_platform_subbspk = 'Platform(AmpSubbSpike)'+' '+'YES'+' '+"%.6f"%(subb_spike)+' amp'+' '+"%.6f"%(subb_base)+ ' amp'+' '+"%.6f"%(bp_amp_rms)+' amp'+' '
                            else:
                                note_platform_subbspk = 'Platform(AmpSubbSpike)'+' '+'NO'+' '+"%.6f"%(subb_spike)+' amp'+' '+"%.6f"%(subb_base)+ ' amp'+' '+"%.6f"%(bp_amp_rms)+' amp'+' '
                            

                            note_platform_start+=note_platform_ampjump 
                            note_platform_start+=note_platform_subbspk
                            note_platform_start+='\n'


                        #plotting
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
                plt.savefig(figure_name)
                plt.close()
               
        outfile.close()
        outfile_val.close()

def main(argv):

   #def_output_path = "/lustre/cv/projects/iyoon/QA_Heuristics/Bandpass/Data/result_max_v4/"
   def_output_path = "/lustre/cv/projects/iyoon/QA_Heuristics/Bandpass/Data/result_plbm_fix/"

   inputpath='/lustre/naasc/sciops/comm/iyoon/pipeline/root/2017.1.01562.S_2023_10_30T18_09_18.158'

   outputpath=def_output_path


   try:
     opts, args = getopt.getopt(argv,"h:i:o:")
   except getopt.GetoptError:
     print("python3 bandpass_check.py -i <'path_to_ms'> -o <'output dir'>")
     sys.exit(2)
   for opt, arg in opts:
     if opt in ("-i"):
        inputpath = str(arg)
     elif opt in ("-o"):
        outputpath = def_output_path+str(arg)

   mkdirstr='mkdir '+outputpath
   print(mkdirstr)
   print(inputpath)

   if os.path.isdir(outputpath) == False:
      os.system(mkdirstr)

#pull out table name and associated ms name
   vislist=[]
   tabkey=[]
   tablelist=[]

   mytablelist=glob.glob(inputpath+'/S*/G*/M*/working/uid*bandpass.s*channel.solintinf.bcal.tbl')
   myworkdir=glob.glob(inputpath+'/S*/G*/M*/working/')

   tb=table()
   for mytab in mytablelist:
       tb.open(mytab)
       tb_summary=tb.info()
       if tb_summary['subType']=='B Jones':
          tabkey.append(mytab.split('/')[-1])
          vislist.append(myau.getMeasurementSetFromCaltable(mytab))
          tablelist.append(mytab)
       tb.close()
   print('Tabkey',tabkey)
   print('Tablelist',tablelist)
   print('vislist',vislist)

#going into the output directory
   os.chdir(outputpath)

#creating plot directory
   outdir=mytablelist[0].split('/')[-6]
   if os.path.isdir(outdir) == False:
      os.system('mkdir '+outdir)
   os.chdir(outdir)
#define structure of pickle file
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
         myvis=myworkdir[0]+visname
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
                  if (len(flag[0])!=len(flag[1])):
                      if (len(flag[0]==1)):
                              flag[0]=flag[1]
                      elif (len(flag[1]==1)):
                              flag[1]=flag[0]

               
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

                      #Find out Platform: high rms, phase jump, and amp jump
                      #also find out spike
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['amp']=amp2
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['phase']=deg2
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['amp2']=amp3
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['phase2']=deg3
                      bandpass_library[mytab][myfield][myspw][myant][mypol]['flag']=myflag

         tb.close()

      with open('bandpass_library.pickle', 'wb') as f:
          pickle.dump(bandpass_library, f, protocol=pickle.HIGHEST_PROTOCOL)

#evaluate bandpass platform/spikes

   evalPerAntBP_Platform('bandpass_library.pickle',inputpath)



if __name__ == "__main__":
   main(sys.argv[1:])

