#!/usr/bin/env python
# coding: utf-8

# In[2]:


#%%timeit -r 1 -n 1
Database_selector = 0
_bl_selector = False
all_files =True            # if False, hand-made list is used for debugging
deb_detect_lines = False    # Debugging mode of detect_lines()
deb_detect_lines2 = False   # Debugging mode of detect_lines2()

# if Database_selector == 0 or Database_selector == 1:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\line-forest')
# elif Database_selector == 2:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\multi-wide-line')
# elif Database_selector == 3:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\weak-wide-line')
# elif Database_selector == 4:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\wide-line-at-spw-edge')
# elif Database_selector == 5:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\multi_source')
# elif Database_selector == 6:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\multi_source2')
# elif Database_selector == 7:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\singlepolLW')
# elif Database_selector == 8:
#     get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\VenusLW')
#
#lf_g_gm.py (v.4.1)
#   Line finder based on Gaussian filter along with baseline estimator using Geman-McClure function
#   0)Rough estimation of mean and std by Recursive 2sigma cut as follows:
#      *0 Regard the original set as SET
#      *1 Calculate mean and std of SET
#      *2 Devide SET into Upperset(more than mean+2std) and Lowerset(the rest)
#      *3 Calculate means and stds in the two sets
#      *4 Calculate relative_mean_gap = (mean_upper - mean_lower)/ std_lower
#      *5 If relative_mean_gap < thr(2.25), mean_lower and std_lower indicate rough estimation of mean and std in neighborhood of the baseline
#        (The std_lower is used as STD in the following  (No correction is made in the current version.)
#        Otherwise, regard the Lowerset as SET, and goto *2
#   1)Baseline estimation by Geman-McClure function (lf_gm.py)
#      Initial value is given by the above estimation
#      Parameter c of Geman-McClure fuction is given by STD/4
#      Baseline is provided by minimization of mean Geman-McClure errors
#   2)Wide line detection by zero-crossing of first derivative of Gaussian average on the baseline subtracted signal(SIGNAL_BL)
#     * Zero-crossing are detected on the first derivative of Gaussian average(\sigma = 8) of SIGNAL_BL
#     * Zero-crossings are validated by both the second derivative of the Gaussian average and SIGNAL_BL
#     * Line width is esimated using median of Gaussian of SIGNAL_BL or SIGNAL_BL itself
#     * Wide lines are validated if the mean-height, max-height ans area satisfied requirements.
#   3)Narrow line detection by zero-crossing of first derivative of Gaussian average on SIGNAL_BL
#     * Zero-crossings are detected in parallel for different Gaussian \sigma's (sgzxmax(=3) kinds of sigma are used)
#     * Zero-crossings are validated by both the second derivative of the Gaussian average and SIGNA_BL
#        ** Empirically tuned threshold is used for the second derivative of Gaussian average
#        ** Fixed threshold * STD is used for SIGNAL_BL
#     * Line width is esimated using median of Gaussian of SIGNAL_BL.or SIGNAL_BL itself
#
#lf_gm.py
#   Linear Regression for a given signal,
#
#   lfr.GM() is called from line_finder() in this script
#   parameter tuning for lfr.GM() is shown in line_finder()

# Rough estimation of STD and BL
e_std_max = 12        #12
thr_rmg = 2.25        # 2.25 thr_tmg = 2.25 is empirically tuned.   # relative_mean_gap converges to 2.25 ??
thr_rpg = 0.5
thr_lstd = 3         # 3
thr_dsp = 128        #1023
forced_level_down = 0.1
initial_setup_of_local_std = True
sigma_sp = 1.0        # STD, estimated in line_finder()

# Baseline estimation by Geman-McClure function
gm_param = 4
regression_dim = 0    # 0,1  In current version, 0 is recommended.
Baseline_inclination = True
Gaussian_subtraction = False   # Not used in this version

# Line/WideLine detection
sgzxmax=3             # 3: 1 See details in line_finder()
wl_mask_index = 1     # 1: 0 through 3 are effective, wl_mask_index indicates a number of effective sigma's
thr_c = 0.01          # 0.01, fixed empirically, used in detect_lines()
thr_d = 4.0           # automatically tuned to 4.0 or 4.5 by Gaussian_sp_bl in detect_lines2(),
thr_e = thr_c         # 0.01, fixed empirically, used in detect_lines2()
thr_f1 = 1.50         # automatically tuned by sign_dist in detect_lines2()
thr_f2 = 1.50         # automatically tuned by sign_dist in detect_line
thr_g = thr_d         # automatically tuned to thr_d in detect_lines2()
thr_h = 40            # 50, fixed empirically, used in detect_lines2()
thr_fgh = 100         # 150, fixed empirically, used in detect_lines2()

# Line Width estimation
# Gaussian_sp_bl      # automatically set to (max_height/sigma_sp < thr_Gsb) in line_finder
# sp_bl_a, median_iw and thr_d are controlled by Gaussian_sp_bl
# Median of Gaussian is selected for noisy signal, while original edge is selected for the others in v4.1
Gaussian_sp_bl = True   # True:Median of Gaussian  # False : Median edge  # True : Gaussian edge,  # False : Original edge
median_iw = 0            # 1                        # 1                    # 0                      # 0
thr_Gsb = 20.0        # 10.0 5.0

# Other global parameters
dmax = 1000
min_X = 0
max_X = dmax
max_height = 100
min_height = -100
sign_dist = 1.0       # automatically set to max_height/abs(min_height) in line_finder()

# Parameters for Graphic Option
Draw_graph = False
Savefig_only_detected_lines = True    # True: Save figures if lines are detected   # False: Save all figures
Baseline_estimation = True
Show_details = False             # False: only Fig-0,  # True: All Figs   # True : only Fig-i
Show_each = 0 # 0 1 2 3 4 5       # 0                   # 0                # i(1..5)
Show_label = True                #effective only when Show_details = False
Zoom_in = 0 # if Zoom_in=1,narrow band arround the highest peak is shown in the graph, if Zoom_in==2, given band.

import numpy as np
from math import pi, sqrt, exp, log, isnan
import scipy.stats as ss
import copy
import py_compile
from . import lf_gmb as lfr      # version-4 uses lf_gma module
#py_compile.compile('lf_gma.py')

def gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [1 / (sigma * sqrt(2*pi)) *exp(-float(x)**2/(2*sigma**2)) for x in r]
def derivative_of_gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [-x / (sigma**3 * sqrt(2*pi)) *exp(-float(x)**2/(2*sigma**2)) for x in r]
def second_floor_derivative_of_gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [1 / (sigma**5 * sqrt(2*pi)) *(x**2 - sigma**2) * exp(-float(x)**2/(2*sigma**2)) for x in r]
def third_floor_derivative_of_gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [1 / (sigma**7 * sqrt(2*pi)) *(-x**2 + 3*sigma**2) * x * exp(-float(x)**2/(2*sigma**2)) for x in r]

def gaussian_average(_spectrum,sigma):
    k_edge = int(sigma * 3)
    ksize= 2 * k_edge + 1
    _g = np.array(gaussian(ksize,sigma))      # normalization
    _g_total = np.sum(_g)
    _g /= _g_total
    return(np.ma.convolve(_spectrum, _g, propagate_mask=False)[k_edge:-k_edge])

def med_bl_a(sig, ib,ibmin,ibmax):        # median of sig[ib0:ib1] is calculated    # when ibd==0,  sig[ib] is returned.
    #if sig[ib]<0:
    #    return sig[ib]
    ib0 = max(ibmin,ib-median_iw)
    ib1 = min(ibmax,ib+median_iw+1)
    return np.ma.median(sig[ib0:ib1])

def detect_lines(sp_bl,sigma,g_ave,signal,coeff,wl_flag,wl_analysis):
    global sigma_sp, Gaussian_sp_bl, thr_c, thr_d
    global min_X,max_X,max_height,min_height
    detected_lines = []
    k_edge = int(sigma * 3)
    ksize= 2 * k_edge + 1
    _g1 = np.array(gaussian(ksize,sigma))
    _dg1 = np.array(derivative_of_gaussian(ksize,sigma))
    _cf1 = np.array(second_floor_derivative_of_gaussian(ksize,sigma))

    #print(sp_bl[min_X:max_X].shape, _cf2.shape, np.ma.convolve(sp_bl[min_X:max_X], _cf2).shape)
    g_ave[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _g1)[k_edge:-k_edge]     #length is adjusted after the convolution
    signal[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _dg1)[k_edge:-k_edge]
    coeff[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _cf1)[k_edge:-k_edge]

    std_c = np.std(coeff[min_X:max_X])
    coeff[min_X:max_X] /= std_c

    if Gaussian_sp_bl:                     # when sp_bl is noisy. Gaussian average is used for adjusting edges
        k_edge2 = 1
        g_ave2 = np.ma.arange(sp_bl.shape[0], dtype=np.float64)
        g_ave2[min_X:max_X] = gaussian_average(sp_bl[min_X:max_X],k_edge2)
        sp_bl_a = g_ave2
        median_iw = 1
        thr_d = 4.5
    else:
        sp_bl_a = sp_bl
        median_iw = 0
        thr_d = 4.0

    thrt_d1 = thr_d
    #if wl_analysis:
    #    thrt_d1 /= 2.0
    thrt_d2 = thr_d * 1.5

    zero_crossings = np.where(np.diff(np.sign(signal[min_X:max_X])))[0]     # Simple implementation of zero-crossings
    zero_crossings += min_X
    for j in range(len(zero_crossings)):
        i = zero_crossings[j]
        if abs(signal[i+1]) < abs(signal[i]):    # The nearer point should be selected for the following processings
            i += 1
    if deb_detect_lines:
        print ('zero_crossings',zero_crossings)

    for j in range(len(zero_crossings)):
        i = zero_crossings[j]
        dl_on = not wl_flag[i] or wl_analysis
        emissional = (signal[i] > signal[i+1])
        if deb_detect_lines:
            print('dl_on',i,dl_on,wl_flag[i],wl_analysis,emissional)
        if dl_on and emissional:
            if deb_detect_lines:
                print('emissional*',i,coeff[i],thr_c,sp_bl[i],sigma_sp*thrt_d1,sigma_sp,thrt_d1)
            if (-coeff[i] > thr_c and (sp_bl[i] > sigma_sp*thrt_d1)):          # Qualification by coeff and sp_bl
                if deb_detect_lines:
                    print('**',sp_bl[i], max_height*0.90-sigma_sp, max_height*0.05)
                #if max_height > -min_height or (sp_bl[i] < max_height*0.90-sigma_sp and sp_bl[i] > max_height*0.05):
                if max_height > -min_height:
                    ib_lim = min_X if j == 0 else zero_crossings[j-1]          # lower/upper limits of the line
                    ie_lim = max_X if j == (len(zero_crossings)-1) else zero_crossings[j+1]
                    #print('ib_lim,ie_lim',i,coeff[i],ib_lim,ie_lim)
                    ib = i
                    ie = i                    # lower/upper bounds of the line
                    while med_bl_a(sp_bl_a, ib, min_X, max_X) > 0 and ib > ib_lim:  # refined version using median function
                    #while sp_bl_a[ib]>0 and ib > ib_lim: # original version
                        #print('sp_bl_a,med_bl_a,ib',ib,sp_bl_a[ib], med_bl_a(sp_bl_a, ib, min_X, max_X))
                        ib -= 1;
                    while med_bl_a(sp_bl_a, ie, min_X, max_X) > 0 and ie < ie_lim:
                    #while sp_bl_a[ie]>0 and ie < ie_lim:
                        #print('sp_bl_a,med_bl_a,ie',ie,sp_bl_a[ie], med_bl_a(sp_bl_a, ie, min_X, max_X))
                        ie += 1
                    detected_lines.append([i,ib,ie])
                    if deb_detect_lines:
                        print ('reg line1a', i,ib,ie,detected_lines)
        elif dl_on:  # absorption
            if deb_detect_lines:
                print('absorption*',i,coeff[i],thr_c,sp_bl[i],-sigma_sp*thrt_d2,sigma_sp,thrt_d2)
            if coeff[i] > thr_c and sp_bl[i] < -sigma_sp *thrt_d2 :      # Qualification by coeff and sp_bl
                if deb_detect_lines:
                    print('**',sp_bl[i], min_height*0.90, min_height*0.05)
                #if min_height < -max_height or (sp_bl[i] > min_height*0.90+sigma_sp and sp_bl[i] < min_height*0.05):
                if min_height < -max_height:
                    ib_lim = min_X if j == 0 else zero_crossings[j-1]               # lower/upper limits of the line
                    ie_lim = max_X if j == (len(zero_crossings)-1) else zero_crossings[j+1]
                    #ib_lim = min_X
                    #ie_lim = max_X
                    #print('ib_lim,ie_lim',i,coeff[i],ib_lim,ie_lim,sp_bl[i])
                    ib = i
                    ie = i
                    while med_bl_a(sp_bl_a, ib, min_X, max_X) < 0 and ib > ib_lim:    # lower/upper bounds of the line
                    #while sp_bl_a[ib]<0 and ib > ib_lim:
                        #print('sp_bl_a,med_bl_a,ib',ib,sp_bl_a[ib], med_bl_a(sp_bl_a, ib, min_X, max_X))
                        ib -= 1;
                    while med_bl_a(sp_bl_a, ie, min_X, max_X) < 0 and ie < ie_lim:
                    #while sp_bl_a[ie]<0 and ie < ie_lim:
                        #print('sp_bl_a,med_bl_a,ib',ib,sp_bl_a[ib], med_bl_a(sp_bl_a, ie, min_X, max_X))
                        ie += 1
                    detected_lines.append([i,ib,ie])
                    if deb_detect_lines:
                        print('reg line2a',i,ib,ie,detected_lines)
    return(detected_lines)
def register_line(lines_list,new_line):
    #print('new_line*',new_line,lines_list)
    il = len(lines_list)
    if il ==0:
        #print('reg:append0',lines_list,new_line)
        lines_list.append(new_line)
    elif new_line[2] > lines_list[il-1][2]:     # When the ending points are different
        if new_line[1] == lines_list[il-1][1]:  # Rewrite the line when the starting points are same
            #print('reg:replace',lines_list,new_line)
            lines_list[il-1]=new_line
        else:
            #print('reg:append1',lines_list,new_line)
            lines_list.append(new_line)
def last_line(lines_list):
    il = len(lines_list)
    if il == 0:
        return [-1,-1,0]
    else:
        return lines_list[il-1]
def detect_lines2(sp_bl,sigma,g_ave,signal,coeff):
    global sigma_sp, Gaussian_sp_bl,thr_d,thr_e,thr_f1,thr_f2,thr_g,thr_h,thr_fgh,sign_dist
    global min_X,max_X

    k_edge = int(sigma * 3)
    ksize= 2 * k_edge + 1
    _g1 = np.array(gaussian(ksize,sigma))
    _dg1 = np.array(derivative_of_gaussian(ksize,sigma))
    _cf1 = np.array(second_floor_derivative_of_gaussian(ksize,sigma))

    #print(sp_bl[min_X:max_X].shape, _cf2.shape, np.ma.convolve(sp_bl[min_X:max_X], _cf2).shape)
    g_ave[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _g1)[k_edge:-k_edge]     #length is adjusted after the convolution
    signal[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _dg1)[k_edge:-k_edge]
    coeff[min_X:max_X] = np.ma.convolve(sp_bl[min_X:max_X], _cf1)[k_edge:-k_edge]

    std_c = np.std(coeff[min_X:max_X])
    coeff[min_X:max_X] /= std_c

    zero_crossings = np.where(np.diff(np.sign(signal[min_X:max_X])))[0]     # Simple implementation of zero-crossings
    zero_crossings += min_X
    for j in range(len(zero_crossings)):
        i = zero_crossings[j]
        if abs(signal[i+1]) < abs(signal[i]):       # The nearer point should be selected for the following processings
            i += 1
    if deb_detect_lines2:
        print ('zero_crossings2',zero_crossings)
    #print ('Gaussian_sp_bl',Gaussian_sp_bl)

    if Gaussian_sp_bl:                         # when sp_bl is noisy. Gaussian average is used for adjusting edges
        k_edge2 = 1
        g_ave2 = np.ma.arange(sp_bl.shape[0], dtype=np.float64)
        g_ave2[min_X:max_X] = gaussian_average(sp_bl[min_X:max_X],k_edge2)
        sp_bl_a = g_ave2                       # Gausian average is used
        thr_d = 4.5                            # empirically tuned
    else:
        sp_bl_a = sp_bl                        # original signal is used
        thr_d = 4.0                            # empirically tuned
    thr_f0 = 1.5     #1.30
    thr_g = thr_d
    if sign_dist > 10.0:   # Rnaga
        thr_f1 = thr_f0
        thr_f2 = 3
    elif sign_dist > 2.0:
        thr_f1 = 2.5  # 2 1.5  0.5   0.75
        thr_f2 = 3  # 1.5
    elif sign_dist > 0.5:
        thr_f1 = 2.5 #2 3 1.5
        thr_f2 = 2.5 #2 3 1.5
    elif sign_dist > 0.1:
        thr_f1 = 3  # 1.5
        thr_f2 = 2.5  #1.5   # 0.5   0.75
    else:
        thr_f1 = 3  # 1.5
        thr_f2 = thr_f0  #1.5   # 0.5   0.75

    #sgzx = sgzxmax

    print('sign_dist,thr_f1,thr_f2',sign_dist,thr_f1,thr_f2)
    print('thr_e,f1,f2,g,h,fgh',thr_e,thr_f1,thr_f2,thr_g,thr_h,thr_fgh)

    first_spbl = sp_bl[min_X]
    detected_lines = []

    #First_zone = True
    ib_lim = min_X
    ie_lim = max_X
    #if (abs(first_spbl/sigma_sp) < 0.5):
    #    First_zone = False
    last_registered = [-1, -1, 0]
    for j in range(len(zero_crossings)):
        i = zero_crossings[j]
        last_registered = last_line(detected_lines)
        #print ('last',i,last_registered)
        if (i > last_registered[1] and i < last_registered[2]):
            continue
        emissional = (signal[i] > signal[i+1])
        #if (first_spbl*sp_bl[i] < 0):
        #    First_zone = False

        ib = i
        ie = i+1   # corrected 2023/09/26
        if deb_detect_lines2:
            print('zero_crossing', i, coeff[i],emissional)
        if emissional and (-coeff[i] > thr_e):
            if deb_detect_lines2:
                print('emission', i, coeff[i],emissional)
            while med_bl_a(sp_bl_a, ib, min_X, max_X) > 0 and ib > ib_lim:
                #print('sp_bl_a,med_bl_a2,ib',ib,sp_bl_a[ib],med_bl_a(sp_bl_a, ib, min_X, max_X))
                ib -= 1;
            while med_bl_a(sp_bl_a, ie, min_X, max_X) > 0 and ie < ie_lim:
                #print('sp_bl_a,med_bl_a2,ie',ie,sp_bl_a[ie],med_bl_a(sp_bl_a, ie, min_X, max_X))
                ie += 1
            if deb_detect_lines2:
                print('ib,ie+',i,coeff[i],ib,ie)

            if ib != ie:                            # Select a wide line by mean, max and area of sp_bl[ib:ie]
                mean_g = np.mean(sp_bl[ib:ie]) if ie > ib+2 else 0
                thr_f = thr_f1
                thr_g0 = thr_g
                if sign_dist > 2:
                    if ie > ib + 10:
                        thr_f -= 0.2
                        thr_g0 -= 1.0
                    elif ie > ib + 5:
                        thr_f -= 0.1
                max_g = np.max(sp_bl[ib:ie])
                area_g = mean_g *min(ie-ib,100) #if mean_g > 1.0 else 0
                prod_coeff = (mean_g * max_g * area_g)/sigma_sp**3
                if deb_detect_lines2:
                    print('mean_g1',ib,ie,mean_g/sigma_sp,thr_f, max_g/sigma_sp,thr_g0, area_g/sigma_sp, thr_h, prod_coeff, thr_fgh, sigma_sp)
                if (mean_g > sigma_sp*thr_f) or (area_g > thr_h*sigma_sp) or (max_g > thr_g0*sigma_sp) or (prod_coeff > thr_fgh):  ##>> ?logic
                    ic = ib + np.argmax(sp_bl[ib:ie])  #(ib+ie)//2
                    register_line(detected_lines,[ic,ib,ie])
                    last_registered = [ic,ib,ie]
                    if deb_detect_lines2:
                        print ('reg line1', ic,ib,ie,detected_lines)
        elif (not emissional) and (coeff[i] > thr_e):
            if deb_detect_lines2:
                print('absorption', i, coeff[i],emissional)
            while med_bl_a(sp_bl_a, ib, min_X, max_X) < 0 and ib > ib_lim:
                #print('sp_bl_a,med_bl_a2,ib',ib,sp_bl_a[ib],med_bl_a(sp_bl_a, ib, min_X, max_X))
                ib -= 1;
            while med_bl_a(sp_bl_a, ie, min_X, max_X) < 0 and ie < ie_lim:
                #print('sp_bl_a,med_bl_a2,ie',ie,sp_bl_a[ie],med_bl_a(sp_bl_a, ie, min_X, max_X))
                ie += 1
            if deb_detect_lines2:
                print('ib,ie-',i,coeff[i],ib,ie)

            if ib != ie:                          # Select a wide line by mean, max and area of sp_bl[ib:ie]
                mean_g = np.mean(sp_bl[ib:ie]) if ie > ib+2 else 0
                thr_f = thr_f2
                thr_g0 = thr_g
                if sign_dist < 0.5:
                    if ie > ib + 10:
                        thr_f -= 0.2
                        thr_g0 -= 1.0
                    elif ie > ib + 5:
                        thr_f -= 0.1
                min_g = np.min(sp_bl[ib:ie])
                area_g = -mean_g * min(ie-ib,100) if -mean_g > 1.0 else 0
                prod_coeff = (mean_g * min_g * area_g)/sigma_sp**3
                if deb_detect_lines2:
                    print('mean_g2',ib,ie,-mean_g/sigma_sp,thr_f, -min_g/sigma_sp,thr_g0, area_g/sigma_sp, thr_h, prod_coeff, thr_fgh, sigma_sp)
                if (mean_g < -sigma_sp*thr_f) or (area_g > thr_h*sigma_sp) or (min_g < -thr_g0*sigma_sp) or (prod_coeff > thr_fgh):
                    ic = ib + np.argmin(sp_bl[ib:ie])  #(ib+ie)//2
                    register_line(detected_lines,[ic,ib,ie])
                    last_registered = [ic,ib,ie]
                    if deb_detect_lines2:
                        print ('reg line2', ic,ib,ie,detected_lines)
    return(detected_lines)

def merge_detected_lines(lines_lists):            # merge sorted lists that were generated by detect_lines() and detect_lines2()
    num_lists = len(lines_lists)
    merged_lines_list = [] if num_lists == 0 else copy.copy(lines_lists[num_lists-2])
    if num_lists > 1:
        for i in range(1,num_lists-1):              # merge line lists generated by detect_lines()
            ii = num_lists - i-2
            jmax = len(lines_lists[ii])             # index of the new line list
            mmax = len(merged_lines_list)           # index of the merged line list
            j = 0
            m = 0
            #print(i,ii,m,mmax,j,jmax)
            while j < jmax:                        # while loop is used for easy imlementation
                #print(i,ii,m,mmax,j)
                if m == mmax:
                    for jt in range(j,jmax):                           # append the rest of the line lists
                        merged_lines_list.append(lines_lists[ii][jt])
                    break
                k = lines_lists[ii][j][0]
                km = merged_lines_list[m][0]
                #print(k,km)
                if k < km-1:                                           # new line appears before the m-th line
                    merged_lines_list.insert(m,lines_lists[ii][j])
                    m += 1
                    mmax += 1
                    j += 1
                elif k >= km-1 and k <= km+1:                        # same line appears
                    if 1:                                          #_#_# if 1: the m-th line is replaced by the new line
                        merged_lines_list[m]=lines_lists[ii][j]
                    m += 1
                    j += 1
                else: # elif k > km+1:
                    while k > merged_lines_list[m][0]+1 and m <= mmax-1:  # skip merged lines to the appropreate position
                        m += 1
                        if m == mmax:
                            break
            #print('mdl',i,ii,merged_lines_list)
        ii = num_lists - 1                            # Finally, lines detected by detect_lines2() are merged.
        jmax = len(lines_lists[ii])
        mmax = len(merged_lines_list)
        j = 0
        m = 0
        while j < jmax:
            if m == mmax:
                for jt in range(j,jmax):
                    merged_lines_list.append(lines_lists[ii][jt])
                break
            k = lines_lists[ii][j][0]
            km = merged_lines_list[m][0]
            if k < km:
                merged_lines_list.insert(m,lines_lists[ii][j])
                m += 1
                mmax += 1
                j += 1
            elif k == km:                                          # if peak positions are same
                if lines_lists[ii][j] != merged_lines_list[m]:       # the m-th line is replaced by the new (wide) line.
                    merged_lines_list.insert(m,lines_lists[ii][j])
                    m += 1
                    mmax += 1
                j += 1
            else: # elif k > km:
                while k > merged_lines_list[m][0] and m <= mmax-1:  # skip merged lines to the appropreate position
                    m += 1
                    if m == mmax:
                        break
        #print('mdl',i,ii,merged_lines_list)
    return(merged_lines_list)

def pipeline_format_lines(lines_merged):
    num_list = len(lines_merged)
    formatted_list = [] if num_list == 0 else copy.copy(lines_merged[0][1:])
    if num_list > 1:
        for i in range(1,num_list):
            new_member = lines_merged[i][1:3]
            last_member = formatted_list[-2:]
            #print(new_member,last_member)
            if last_member[0] > new_member[0]:
                formatted_list.insert(-2,new_member[0])
                formatted_list.insert(-2,new_member[1])
            elif last_member == new_member:
                continue
            else:
                formatted_list.extend(new_member)
            #formatted_list.append(new_member[1])
    return(formatted_list)

def draw_graph_gmr(_spectrum,baseline,_spectrum_bl,signal,coeff,g_ave,lines_detected,lines_merged,sgzx_list,mean_level,cut_level):
    import matplotlib.pyplot as plt
    global thr_c,thr_d,thr_e,thr_f1,thr_f2,thr_rmg,gm_param,sigma_sp
    global dmax,min_X,max_X
    global Zoom_in, Show_details, Show_each, Show_label,Savefig_only_detected_lines

    X = np.ma.arange(dmax)
    sgzxmax = len(sgzx_list)
    if 1:
        sgzxmax += 1

    str1 = format(Baseline_estimation,'01d')
    str1a = format(Baseline_inclination,'01d')
    str2c = format(thr_c,'3.2f')
    str2d = format(thr_d,'3.2f')
    str2e = format(thr_e,'3.2f')
    str2f1 = format(thr_f1,'3.2f')
    str2f2 = format(thr_f2,'3.2f')
    #str3 = format(regression_dim,'01d')
    str3a = format(thr_rmg,'3.2f')
    str3b = format(gm_param,'3.1f')
    str4 = format(Show_each,'01d')
    opt_sigma = '('
    for sgzx in range(sgzxmax-1):
        if sgzx > 0 :
            opt_sigma += ','
        opt_sigma += format(sgzx_list[sgzx],'3.2f')
    opt_sigma += ')'
    resultfile='gm_result_'+data_file+'_('+str1+str1a+'_'+str2c+','+str2d+','+str2e+','+str2f1+                ','+str2f2+','+str3a+','+str3b+')-'+opt_sigma+'-'+str4+'.png'

    plt.style.use("seaborn-darkgrid")
    if Show_details and Show_each == 0:
        plt.rcParams["figure.figsize"] = [20, 20]#[20,20]#ax.set_box_aspect(0.5)  # 0.2     1
    else:
        plt.rcParams["figure.figsize"] = [20, 6]#[20, 6] [20,200]#ax.set_box_aspect(0.5)  # 0.2     1
    parameters = {'axes.labelsize': 25,'axes.titlesize': 35,'xtick.labelsize':20,'ytick.labelsize':20}

    if Show_details and Show_each == 0:
        fig, ax = plt.subplots(sgzxmax+2, 1)
    else:
        fig, ax = plt.subplots(1, 1)

    #print(str(fig), str(ax[0]))

    if Zoom_in == 1:
        peak_i = int((min_X+max_X)/2)
        max_line_height = 0
        for sgzx in range(sgzxmax):
            for j in range(len(lines_detected[sgzx])):
                i = lines_detected[sgzx][j][0]
                if _spectrum_bl[i] > max_line_height:
                    max_line_height = _spectrum_bl[i]
                    peak_i = i
        min_Xd=max(min_X,peak_i-250)
        max_Xd=min(max_X,peak_i+250)
    elif Zoom_in == 0:
        min_Xd=min_X
        max_Xd=max_X  # max_X - 30
    else:
        rate1=0.15   #0.40
        rate2=0.4    #0.85
        min_Xd=int(min_X*(1-rate1)+max_X*rate1)
        max_Xd=int(min_X*(1-rate2)+max_X*rate2)

    if Show_details and Show_each == 0:
        ax[0].plot(X[min_Xd:max_Xd], _spectrum[min_Xd:max_Xd],c="darkgray")
        ax[0].plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd], c="black")
        ax[0].plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd]+sigma_sp, c="lightgreen")    # t_param/gm_param
        ax[0].plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd]-sigma_sp, c="lightgreen")
    elif not Show_details :
        if Show_label:
            plt.xlabel(resultfile)
        plt.plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd], c="black")  #"black"  "red"
        plt.plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd]+sigma_sp, c="lightgreen")    # t_param/gm_param
        plt.plot(X[min_Xd:max_Xd], baseline[min_Xd:max_Xd]-sigma_sp, c="lightgreen")
        plt.plot(X[min_Xd:max_Xd], _spectrum[min_Xd:max_Xd],c="darkgray")
    if Show_details:
        print('Show_each',Show_each)
        if Show_each == 0:
            ax[sgzxmax+1].plot(X[min_Xd:max_Xd], _spectrum[min_Xd:max_Xd],c="gray")
            for sgzx in range(sgzxmax):
                if 1:
                    std_sig = np.std(signal[sgzx,min_X:max_X])
                    signal[sgzx,min_X:max_X] /= std_sig
                ax[sgzx+1].plot(X[min_Xd:max_Xd], coeff[sgzx,min_Xd:max_Xd]*10,c="green")
                ax[sgzx+1].plot(X[min_Xd:max_Xd], signal[sgzx,min_Xd:max_Xd]*10,c="orange")
                ax[sgzx+1].plot(X[min_Xd:max_Xd], g_ave[sgzx,min_Xd:max_Xd],c="brown")
                ax[sgzx+1].plot(X[min_Xd:max_Xd], _spectrum_bl[min_Xd:max_Xd],c="black")

                #print('gzx,len(lines_detected)',sgzx,len(lines_detected[sgzx]))

                jmax = len(lines_detected[sgzx])
                for j in range(jmax):
                    #print('sgzx,j,len(lines_detected)',sgzx,j,len(lines_detected[sgzx]))
                    i=lines_detected[sgzx][j][0]
                    ib=lines_detected[sgzx][j][1]
                    ie=lines_detected[sgzx][j][2]
                    #print('i,ib,ie',i,ib,ie,min_Xd,max_Xd)
                    if i>=min_Xd and i<max_Xd:
                        #print('green line1')
                        #ax[0].plot([i, i],[baseline[i],_spectrum[i]] , c="black")
                        ax[sgzx+1].plot([i, i],[0,_spectrum_bl[i]] , c="blue")
                        if sgzx < sgzxmax-1:
                            #print('graph',sgzx,sgzxmax-1)
                            ax[sgzx+1].plot([i, i],[0,signal[sgzx,i]] , c="blue")
                        if ib>=min_Xd:
                            ax[sgzx+1].plot([ib, ib],[0,_spectrum_bl[ib]] , c="green")
                        #print('green line3')
                        if ie<max_Xd:
                            ax[sgzx+1].plot([ie, ie],[0,_spectrum_bl[ie]] , c="green")
                        #print('green line3')
                        ax[sgzx+1].plot([max(ib,min_Xd), min(ie,max_Xd)],[0,0] , c="red", linewidth=4)
                        #ax[0].plot([max(ib,min_Xd), min(ie,max_Xd)],[baseline[max(ib,min_Xd)],baseline[min(ie,max_Xd)]] , c="yellow")
            print ('mean_level',mean_level,len(mean_level))
            print ('cut_level',cut_level,len(cut_level))
            for icut in range(len(cut_level)):
                ax[sgzxmax+1].plot(X[min_Xd:max_Xd], X[min_Xd:max_Xd]*0+cut_level[icut],c="blue", linewidth=0.5)
            for imean in range(len(mean_level)):
                ax[sgzxmax+1].plot(X[min_Xd:max_Xd], X[min_Xd:max_Xd]*0+mean_level[imean],c="red", linewidth=0.5)
        elif Show_each < 5:
            sgzx = Show_each -1
            if 1:
                std_sig = np.std(signal[sgzx,min_X:max_X])
                signal[sgzx,min_X:max_X] /= std_sig
            plt.plot(X[min_Xd:max_Xd], coeff[sgzx,min_Xd:max_Xd]*10,c="green")
            plt.plot(X[min_Xd:max_Xd], signal[sgzx,min_Xd:max_Xd]*10,c="orange")
            plt.plot(X[min_Xd:max_Xd], g_ave[sgzx,min_Xd:max_Xd],c="brown")
            plt.plot(X[min_Xd:max_Xd], _spectrum_bl[min_Xd:max_Xd],c="black")

            #print('gzx,len(lines_detected)',sgzx,len(lines_detected[sgzx]))

            jmax = len(lines_detected[sgzx])
            for j in range(jmax):
                #print('sgzx,j,len(lines_detected)',sgzx,j,len(lines_detected[sgzx]))
                i=lines_detected[sgzx][j][0]
                ib=lines_detected[sgzx][j][1]
                ie=lines_detected[sgzx][j][2]
                #print('i,ib,ie',i,ib,ie,min_Xd,max_Xd)
                if i>=min_Xd and i<max_Xd:
                    #print('green line1')
                    #ax[0].plot([i, i],[baseline[i],_spectrum[i]] , c="black")
                    plt.plot([i, i],[0,_spectrum_bl[i]] , c="blue")
                    if sgzx < sgzxmax-1:
                        #print('graph',sgzx,sgzxmax-1)
                        plt.plot([i, i],[0,signal[sgzx,i]] , c="blue")
                    if ib>=min_Xd:
                        plt.plot([ib, ib],[0,_spectrum_bl[ib]] , c="green")
                    #print('green line3')
                    if ie<max_Xd:
                        plt.plot([ie, ie],[0,_spectrum_bl[ie]] , c="green")
                    #print('green line3')
                    plt.plot([max(ib,min_Xd), min(ie,max_Xd)],[0,0] , c="red", linewidth=4)
                    #ax[0].plot([max(ib,min_Xd), min(ie,max_Xd)],[baseline[max(ib,min_Xd)],baseline[min(ie,max_Xd)]] , c="yellow")
        else:
            print ('Show_each5:mean_level',mean_level,len(mean_level))
            print ('Show_each5:cut_level',cut_level,len(cut_level))
            plt.plot(X[min_Xd:max_Xd], _spectrum[min_Xd:max_Xd],c="gray")
            for imean in range(len(mean_level)):
                plt.plot(X[min_Xd:max_Xd], X[min_Xd:max_Xd]*0+mean_level[imean],c="red", linewidth=0.5)
            for icut in range(len(cut_level)):
                plt.plot(X[min_Xd:max_Xd], X[min_Xd:max_Xd]*0+cut_level[icut],c="blue", linewidth=0.5)
    for j in range(len(lines_merged)):
        #print('sgzx,j,len(lines_merged)',sgzx,j,len(lines_merged))
        i=lines_merged[j][0]
        ib=lines_merged[j][1]
        ie=lines_merged[j][2]
        #print('i,ib,ie',i,ib,ie)
        if i>=min_Xd and i<max_Xd:
            if Show_details and Show_each == 0:
                ax[0].plot([i, i],[baseline[i],_spectrum[i]] , c="blue")
                ax[0].plot([max(ib,min_Xd), min(ie,max_Xd)],[baseline[max(ib,min_Xd)],baseline[min(ie,max_Xd)]] , c="red", linewidth=4)  ##temp
            elif not Show_details:
                plt.plot([i, i],[baseline[i],_spectrum[i]] , c="blue")
                plt.plot([max(ib,min_Xd), min(ie,max_Xd)],[baseline[max(ib,min_Xd)],baseline[min(ie,max_Xd)]] , c="red", linewidth=4)  ##temp
    print(resultfile)
    if not Savefig_only_detected_lines or len(lines_merged) > 0:
        plt.savefig(resultfile)
    else:
        plt.close()
    #plt.close()
    return

def local_sigma(_spectrum,gaussian_sgm):
    global min_X,max_X
    delta=gaussian_sgm*3
    #print(min_X,max_X,delta)
    #print(_spectrum[min_X+delta:max_X-delta])
    #print(gaussian_average(_spectrum,gaussian_sgm)[min_X+delta:max_X-delta])
    return(np.ma.std(_spectrum[min_X+delta:max_X-delta]-gaussian_average(_spectrum,gaussian_sgm)[min_X+delta:max_X-delta]))

def line_finder(sp, sp_mask = False):
    global sigma_sp, Gaussian_sp_bl, thr_rmg, sign_dist
    global dmax, min_X, max_X, max_height, min_height
    global Draw_graph, Baseline_estimation, Baseline_inclination
    dmax=len(sp)
    #print(sp_mask)
    _spectrum = np.ma.array(sp,mask=sp_mask)

    min_X = 0
    max_X = dmax - 1
    if _spectrum.mask.shape != ():         ### even when == (), this code works
        while _spectrum.mask[min_X] :     ### otherwise, min_X and max_X are adjusted to mask data
            if min_X == dmax - 1:
                break
            min_X += 1
        while _spectrum.mask[max_X] :
            if max_X == 0:
                break
            max_X -= 1
    print('min_X,max_X',min_X,max_X)
    if (min_X > max_X):
        print ('No effective data')
        return [],0,0
    baseline = np.ma.arange(dmax,dtype=float)
    # baseline estimation by Geman-McClure function
    if 1:
        X_ = np.ma.arange(dmax).reshape(-1,1)

        mean_sp = np.ma.mean(_spectrum)
        height_sp = np.ma.max(_spectrum)-mean_sp
        depth_sp = mean_sp - np.ma.min(_spectrum)
        sigma_sp = np.ma.std(_spectrum)
        mean_level = np.arange(e_std_max,dtype=float)
        cut_level = np.arange(e_std_max,dtype=float)
        #skew_sp = ss.skew(_spectrum)
        _spectrum1 = _spectrum
        if initial_setup_of_local_std:
            gaussian_sgm=5
            local_std = local_sigma(_spectrum,gaussian_sgm)
            print ('**local_std',local_std)
            local_std_check_enable = True
        else:
            local_std_check_enable = False
        local_std_check_invalid = False
        for e_std in range(e_std_max-1):
            if height_sp > depth_sp:
                mean_level[e_std]=mean_sp
                cut_level[e_std]=mean_sp+sigma_sp*2
                upper_part = np.where(_spectrum1>cut_level[e_std])
                #if e_std == 0 or local_std_check_enable:                              # For initial division
                if 1:
                    thr_level = 2.0
                    while upper_part[0].shape[0] == 0:      # Very rare cases, thr_level is tuned for the set
                        thr_level -= forced_level_down
                        print ('*forced thr_level down1',thr_level)
                        cut_level[e_std]=mean_sp+sigma_sp*thr_level
                        upper_part = np.where(_spectrum1>cut_level[e_std])
                lower_part = np.where(_spectrum1<=cut_level[e_std])
            else:
                mean_level[e_std]=mean_sp
                cut_level[e_std]=mean_sp-sigma_sp*2
                lower_part = np.where(_spectrum1<cut_level[e_std])
                #if local_std_check_invalid:                              # For initial division
                #if e_std == 0 or local_std_check_enable:                              # For initial division
                if 1:
                    thr_level = 2.0
                    while lower_part[0].shape[0] == 0:      # Very rare cases, thr_level is tuned for the set
                        thr_level -= forced_level_down
                        print ('*forced thr_level down2',thr_level)
                        cut_level[e_std]=mean_sp-sigma_sp*thr_level
                        lower_part = np.where(_spectrum1<cut_level[e_std])
                upper_part = np.where(_spectrum1>=cut_level[e_std])
            upper_mean = np.ma.mean(_spectrum1[upper_part])
            lower_mean = np.ma.mean(_spectrum1[lower_part])
            mean_gap = upper_mean - lower_mean
            relative_mean_gap = mean_gap/sigma_sp
            #print('statistics0:',e_std,mean_gap,sigma_sp,relative_mean_gap,local_std,mean_level[e_std],cut_level[e_std])
            print('statistics0:',e_std,mean_gap,sigma_sp,relative_mean_gap,mean_level[e_std],cut_level[e_std],local_std_check_enable,local_std_check_invalid)
            if height_sp > depth_sp:
                sigma_sp = np.ma.std(_spectrum1[lower_part])
                #skew_sp = ss.skew(_spectrum1[lower_part])
                mean_sp = lower_mean
            else:
                #print('**negative')
                sigma_sp = np.ma.std(_spectrum1[upper_part])
                #skew_sp = ss.skew(_spectrum1[upper_part])
                mean_sp = upper_mean
            if local_std_check_enable:
                local_std_check_invalid = (sigma_sp > 2 * local_std)
                if local_std_check_invalid:
                    if (e_std == e_std_max-2):
                        print ('**final_local_std_check_invalid')
            if relative_mean_gap <= thr_rmg and not local_std_check_invalid: # thr_tmg = 2.25  # relative_mean_gap converges to 2.25
                if not local_std_check_enable and e_std < thr_lstd:
                    gaussian_sgm=5
                    local_std = local_sigma(_spectrum,gaussian_sgm)
                    print ('**local_std,e_std',local_std,e_std)
                    local_std_check_enable = True
                    local_std_check_invalid = (sigma_sp > 2 * local_std)
                if not local_std_check_invalid:
                    mean_level[e_std+1]=mean_sp
                    break
                else:
                    print ('**local_std_check works')
            if height_sp > depth_sp:
                _spectrum1 = _spectrum1[lower_part]             # This substitution is important for convergence
            else:
                _spectrum1 = _spectrum1[upper_part]             # This substitution is important for convergence
                #print('statistics1:',e_std,mean_gap,sigma_sp,relative_mean_gap)
        print('sigma_sp,mean_sp',sigma_sp,mean_sp)
    # baseline estimation by Geman-McClure function
    displacement_effective = False
    if Baseline_estimation:
        if Baseline_inclination and (dmax > thr_dsp):
            half_pos = len(_spectrum)//2
            mean_left= np.ma.mean(_spectrum1[:half_pos])
            mean_right=np.ma.mean(_spectrum1[half_pos:])
            relative_pos_gap = 2*(mean_right-mean_left)/sigma_sp
            print ('relative_pos_gap',half_pos,mean_sp,mean_left,mean_right,relative_pos_gap)
            displacement_effective = (abs(relative_pos_gap) > thr_rpg)
            if displacement_effective:
                displacement = np.ma.arange(dmax,dtype=float)
                #print ('displacement',displacement)
                slope = 2*(mean_right-mean_left)/len(_spectrum)
                mean_displacement =slope*(displacement[min_X]+displacement[max_X-1])/2
                displacement = displacement * slope - mean_displacement
                print ('slope,mean_displacement',slope,mean_displacement)
                _spectrum2 = _spectrum - displacement
                _spectrum_ = _spectrum2.reshape(-1,1)
            else:
                _spectrum_ = _spectrum.reshape(-1,1)
        else:
            _spectrum_ = _spectrum.reshape(-1,1)
            print ('**horizontal baseline assumed')

        t_param = 3.0 * sigma_sp
        d_param = len(_spectrum)//1.5         # when d_param is not effective for lfr.GM, d_param is gradually reduced in lfr.GM

        ycmax = 1
        yc_list = np.ma.arange(ycmax,dtype=float).reshape(-1,1)
        yc_list[0] = mean_sp

        regressor =lfr.GM(t=t_param, d=d_param, g=gm_param, y_list=yc_list,                                  model=lfr.LinearRegressor0(), loss=lfr.gm_error_loss, metric=lfr.mean_gm_error)

        print ('t,d,g:',t_param,d_param,gm_param)
        #print (X_.shape,_spectrum_.shape)
        ones = np.ones((len(X_), 1))
        aaa=regressor.fit(X_,_spectrum_,ones)
        #print ('aaa',vars(aaa.model))
        baseline = regressor.predict(X_)[:,0]
        if displacement_effective:
            baseline += displacement
        #print ('baseline=',baseline)
    _spectrum_bl = np.ma.arange(_spectrum.shape[0], dtype=np.float64)
    if Gaussian_subtraction or Baseline_estimation:
        _spectrum_bl[min_X:max_X] = _spectrum[min_X:max_X] -baseline[min_X:max_X]
    else:
        _spectrum_bl[min_X:max_X] = _spectrum[min_X:max_X]
        baseline[min_X:max_X] = 0

    sgzx_list = []
    if sgzxmax >= 1:
        for ii in range(sgzxmax):
            sgzx_list += [2 ** (ii+1)]
    #print ('sgzx_list',sgzx_list)
    sgzxmaxp1= sgzxmax + 1
    g_ave = np.ma.arange(sgzxmaxp1*dmax,dtype=float).reshape(sgzxmaxp1,dmax)
    signal = np.ma.arange(sgzxmaxp1*dmax,dtype=float).reshape(sgzxmaxp1,dmax)
    coeff = np.ma.arange(sgzxmaxp1*dmax,dtype=float).reshape(sgzxmaxp1,dmax)

    # Zero crossing analysis on the second derivative of Gaussian averages
    max_height = np.ma.max(_spectrum_bl[min_X:max_X])
    min_height = np.ma.min(_spectrum_bl[min_X:max_X])
    sign_dist = max_height/abs(min_height)
    #print ('sign_dist', np.ma.max(_spectrum_bl[min_X:max_X]),np.ma.min(_spectrum_bl[min_X:max_X]), sign_dist)
    Gaussian_sp_bl = (max_height/sigma_sp < thr_Gsb)
    print ('Gaussian_sp_bl',Gaussian_sp_bl,max_height/sigma_sp)

    sigma_p = sgzx_list[0] #[0]
    #skew_sp = ss.skew(_spectrum)

    wide_lines = detect_lines2(_spectrum_bl,sigma_p,g_ave[sgzxmax,:],signal[sgzxmax,:],coeff[sgzxmax,:])
    wl_flag= np.arange(dmax,dtype='int')
    wl_flag[min_X:max_X] = 0
    for il in range(len(wide_lines)):
        ib = wide_lines[il][1]
        ie = wide_lines[il][2]
        wl_flag[ib:ie] = 1

    lines_detected = []
    # Zero crossing analyses on the first derivative of Gaussian averages
    for sgzx in range(sgzxmax):
        sigma = sgzx_list[sgzx]
        wl_analysis = (sgzx >= sgzxmax - wl_mask_index) # wl_analysis is activated for the last one when wl_mask_index = 1
        new_lines = detect_lines(_spectrum_bl,sigma,g_ave[sgzx,:],signal[sgzx,:],coeff[sgzx,:],wl_flag,wl_analysis)
        lines_detected.append(new_lines)
        print ('lines_detected',sgzx,len(lines_detected[sgzx]),lines_detected[sgzx])

    lines_detected.append(wide_lines)
    print ('lines_detected2',sgzxmax,len(lines_detected[sgzxmax]),lines_detected[sgzxmax])

    lines_merged = merge_detected_lines(lines_detected)
    print ('lines_merged',len(lines_merged),lines_merged)
    lines_final = pipeline_format_lines(lines_merged)
    #print ('lines_final',len(lines_final)/2,lines_final)
    if Draw_graph:
        draw_graph_gmr(_spectrum,baseline,_spectrum_bl,signal,coeff,g_ave,                       lines_detected,lines_merged,sgzx_list, mean_level[0:e_std+2], cut_level[0:e_std+1]) #sigma_rest #t_param
    return(lines_final,baseline[0],baseline[0]/sigma_sp) #sigma_rest  #t_param

if __name__ == "__main__":
    loc_all = 1
    if Database_selector == 0:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field1','simplegrid_stage11_spw19_field1',
                                       'simplegrid_stage11_spw21_field1','simplegrid_stage11_spw23_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field1','simplegrid_stage9_spw19_field1',
                                       'simplegrid_stage9_spw21_field1','simplegrid_stage9_spw23_field1']
    elif Database_selector == 1:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw25_field1','simplegrid_stage11_spw27_field1',
                                       'simplegrid_stage11_spw29_field1','simplegrid_stage11_spw31_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw25_field1','simplegrid_stage9_spw27_field1',
                                       'simplegrid_stage9_spw29_field1','simplegrid_stage9_spw31_field1']
    elif Database_selector == 2:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field2','simplegrid_stage11_spw19_field2',
                                       'simplegrid_stage11_spw21_field2','simplegrid_stage11_spw23_field2']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field2','simplegrid_stage9_spw19_field2',
                                       'simplegrid_stage9_spw21_field2','simplegrid_stage9_spw23_field2']
    elif Database_selector == 3 or Database_selector == 4:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field1','simplegrid_stage11_spw19_field1',
                                       'simplegrid_stage11_spw21_field1','simplegrid_stage11_spw23_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field1','simplegrid_stage9_spw19_field1',
                                       'simplegrid_stage9_spw21_field1','simplegrid_stage9_spw23_field1']
    elif Database_selector == 5:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw23_field3']
        else:
            file_name_list_original = ['simplegrid_stage9_spw23_field3']
    elif Database_selector == 6:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field6','simplegrid_stage11_spw19_field6']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field6','simplegrid_stage9_spw19_field6']
    elif Database_selector == 7:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw19_field7']
        else:
            file_name_list_original = ['simplegrid_stage9_spw19_field7']
    elif Database_selector == 8:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field1','simplegrid_stage11_spw23_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field1','simplegrid_stage9_spw23_field1']
    file_name_list = file_name_list_original
    if not all_files:                              # if 1, select one spw field
        file_name_list = []
        file_name_list.append(file_name_list_original[1])
        loc_all = 1                   # if 0, select locations by [loc_b loc_e]
        if loc_all == 0:
            loc_b =  120 #51 #139 #72
            loc_e = loc_b + 1 #30  #15
    for file_name in file_name_list:
        data_file =file_name+'.npz'
        data = np.load(data_file)                         # np.load('data1_17_1.npz')
        #data = np.load(directory_name+data_file)         # np.load('data1_17_1.npz')
        full_spectrum = data['data'][:,:]
        fs_mask = data['mask'][:,:]
        fs = np.ma.array(full_spectrum,mask=fs_mask)
        #print(fs_mask,np.sum(fs_mask))
        #print(fs.shape)

        _spectrum = np.ma.arange(fs.shape[1], dtype=np.float64)

        loc_max = fs.shape[0]
        if loc_all:
            loc_b = 0 #loc_max // 4 #loc_max//4 #//4
            loc_e = loc_max #loc_b + 3 #loc_max // 2 #loc_max // 2 #int(loc_max//2) #loc_max//2 #loc_b + 1
        print('loc_b,loc_e',loc_b,loc_e,loc_max,fs.shape[1])
        base_l = np.arange(loc_e-loc_b, dtype=np.float64)
        base_lr = np.arange(loc_e-loc_b, dtype=np.float64)
        for loc in range(loc_b,loc_e):
            str1 = format(loc,'03d')
            data_file = file_name + '.npz#' + str1
            #_spectrum = fs[loc,:]
            for j in range(_spectrum.shape[0]):
                #_spectrum[j] = np.ma.average(fs[loc,j])
                _spectrum[j] = fs[loc,j]

            ## setting for line_finder()
            if _bl_selector:             # spectrum data before baseline subtraction
                Baseline_estimation = False         # Ransac is called for baseline estimation
            else:
                Baseline_estimation = True         # Ransac is called for baseline estimation
            Gaussian_subtraction = False
            if 1:   # line_finder works for spetrum with mask
                #print (_spectrum.shape,_spectrum.mask.shape)
                lines_final,base_l[loc-loc_b],base_lr[loc-loc_b] = line_finder(_spectrum,_spectrum.mask)
            else:   # line_finder also works for spectrum without mask
                lines_final,base_l[loc-loc_b],base_lr[loc-loc_b] =line_finder(_spectrum)
            print('lines_final',data_file,len(lines_final)/2,lines_final)
        #print ('*** baseline statistics **', gm_param,np.mean(base_l),np.std(base_l),max(base_l),min(base_l))
        #print ('*** baseline statistics **', gm_param,np.mean(base_lr),np.std(base_lr),max(base_lr),min(base_lr))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




