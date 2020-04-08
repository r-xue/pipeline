#!/usr/bin/env python
# coding: utf-8
#
# This code is originally provided by Yoshito Shimajiri.
# See PIPE-251 for detail about this.

# # 1. Import modules

# In[ ]:


#from astropy.io import fits
#import pandas as pd
#import os
import numpy as np
import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')
#import time
#import termcolor

# # 2. Function for getting fits file names

# In[ ]:

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools

LOG = infrastructure.get_logger(__name__)


#Project = ["2019.2.00052.Sa", "2019.2.00052.S", "2019.1.00915.S", "2019.2.00037.S"]

# def read_input(project_name):
#     if project_name == "2019.1.00915.S":
#         fits_list = ["uid___A001_X1465_X1ffd.s12_57.Ridge-N4_sci.spw23.cube.I.iter0.image.sd.fits",
#                      "uid___A001_X1465_X1ffd.s12_27.Ridge-N4_sci.spw19.cube.I.iter0.image.sd.fits",
#                      "uid___A001_X1465_X1ffd.s12_42.Ridge-N4_sci.spw21.cube.I.iter0.image.sd.fits",
#                      "uid___A001_X1465_X1ffd.s12_12.Ridge-N4_sci.spw17.cube.I.iter0.image.sd.fits"]

#     if project_name == "2019.2.00037.S":
#         fits_list=["uid___A001_X14c3_X1dc.s12_16.NGC_1365_sci.spw17.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X1dc.s12_35.NGC_1365_sci.spw19.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X1dc.s12_54.NGC_1365_sci.spw21.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X1dc.s12_73.NGC_1365_sci.spw23.cube.I.iter0.image.sd.fits"]

#     if project_name == "2019.2.00052.S":
#         fits_list=["uid___A001_X14c3_X2f4.s12_12.NGC4725_sci.spw17.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X2f4.s12_27.NGC4725_sci.spw19.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X2f4.s12_42.NGC4725_sci.spw21.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14c3_X2f4.s12_57.NGC4725_sci.spw23.cube.I.iter0.image.sd.fits"]

#     if project_name == "2019.2.00052.Sa":
#         fits_list=["uid___A001_X14d8_X309.s12_12.NGC3384_sci.spw17.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14d8_X309.s12_27.NGC3384_sci.spw19.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14d8_X309.s12_42.NGC3384_sci.spw21.cube.I.iter0.image.sd.fits",
#                    "uid___A001_X14d8_X309.s12_57.NGC3384_sci.spw23.cube.I.iter0.image.sd.fits"]
#     return fits_list


# # 3. Function for finding the emission free channels rougly

# In[ ]:


# To find the emission free channels roughly for estimating RMS
def decide_rms(naxis3, cube_regrid):
    start_rms_ch, end_rms_ch = int(naxis3 * 2 / 10), int(naxis3 * 3 / 10)
    rms_map1 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5
    start_rms_ch, end_rms_ch = int(naxis3 * 3 / 10), int(naxis3 * 4 / 10)
    rms_map2 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5
    start_rms_ch, end_rms_ch = int(naxis3 * 4 / 10), int(naxis3 * 5 / 10)
    rms_map3 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5
    start_rms_ch, end_rms_ch = int(naxis3 * 5 / 10), int(naxis3 * 6 / 10)
    rms_map4 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5
    start_rms_ch, end_rms_ch = int(naxis3 * 6 / 10), int(naxis3 * 7 / 10)
    rms_map5 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5
    start_rms_ch, end_rms_ch = int(naxis3 * 7 / 10), int(naxis3 * 8 / 10)
    rms_map6 = ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5

    rms_check = np.array([np.nanmean(rms_map1),
                          np.nanmean(rms_map2),
                          np.nanmean(rms_map3),
                          np.nanmean(rms_map4),
                          np.nanmean(rms_map5),
                          np.nanmean(rms_map6)])
    rms_check_min = np.argmin(rms_check)
    if rms_check_min == 0:
        rms_map = rms_map1
        start_rms_ch, end_rms_ch = int(naxis3 * 2 / 10), int(naxis3 * 3 / 10)
    if rms_check_min == 1:
        rms_map = rms_map2
        start_rms_ch, end_rms_ch = int(naxis3 * 3 / 10), int(naxis3 * 4 / 10)
    if rms_check_min == 2:
        rms_map = rms_map3
        start_rms_ch, end_rms_ch = int(naxis3 * 4 / 10), int(naxis3 * 5 / 10)
    if rms_check_min == 3:
        rms_map = rms_map4
        start_rms_ch, end_rms_ch = int(naxis3 * 5 / 10), int(naxis3 * 6 / 10)
    if rms_check_min == 4:
        rms_map = rms_map5
        start_rms_ch, end_rms_ch = int(naxis3 * 6 / 10), int(naxis3 * 7 / 10)
    if rms_check_min == 5:
        rms_map = rms_map6
        start_rms_ch, end_rms_ch = int(naxis3 * 7 / 10), int(naxis3 * 8 / 10)
    LOG.info("RMS: {}".format(np.nanmean(rms_map)))
    return rms_map


# # 4. Function for making figures

# In[ ]:


# Function for making fiures
def make_figures(peak_sn, mask_map, rms_threshold, rms_map,
                 masked_average_spectrum, all_average_spectrum,
                 naxis3, peak_sn_threshold, spectrum_at_peak,
                 idy, idx, output_name):

    std_value = np.nanstd(masked_average_spectrum)
    plt.figure(figsize=(20, 5))
    plt.subplot(1, 3, 1)
    plt.title("Peak SN map")
    plt.xlabel("RA [pixel]")
    plt.ylabel("DEC [pixel]")
    plt.imshow(peak_sn, cmap="rainbow")
    plt.colorbar(shrink=0.9)
    plt.scatter(idx, idy, s=300, marker="o", facecolors='none', edgecolors='grey', linewidth=5)
    plt.subplot(1, 3, 2)
    plt.title("Mask map (1: SN<" + str(peak_sn_threshold) + ")")
    plt.xlabel("RA [pixel]")
    plt.ylabel("DEC [pixel]")
    plt.imshow(mask_map, vmin=0, vmax=1, cmap="rainbow")
    plt.colorbar(shrink=0.9)
    plt.subplot(1, 3, 3)
    plt.title("Masked-averaged spectrum")
    plt.xlabel("Channel")
    plt.ylabel("Intensity [K]")
    plt.ylim(std_value * (-7.), std_value * 7.)
    plt.plot(spectrum_at_peak, "-", color="grey", label="spectrum at peak", alpha=0.5)
    plt.plot(masked_average_spectrum, "-", color="red", label="masked averaged")
    plt.plot([0, naxis3], [0, 0], "-", color="black")
    plt.plot([0, naxis3], [-4. * std_value, -4. * std_value], "--", color="red")
    plt.plot([0, naxis3], [np.nanmean(rms_map) * 1., np.nanmean(rms_map) * 1], "--", color="blue")
    plt.plot([0, naxis3], [np.nanmean(rms_map) * (-1.), np.nanmean(rms_map) * (-1.)], "--", color="blue")
    if std_value * (7.) >= np.nanmean(rms_map) * peak_sn_threshold:
        plt.plot([0, naxis3], [np.nanmean(rms_map) * peak_sn_threshold, np.nanmean(rms_map) * peak_sn_threshold], "--", color="green")
        plt.text(naxis3 * 0.5, np.nanmean(rms_map) * peak_sn_threshold, "lower 10% level", fontsize=18, color="green")
    plt.text(naxis3 * 0.1, np.nanmean(rms_map) * 1., "1.0 x rms", fontsize=18, color="blue")
    plt.text(naxis3 * 0.1, np.nanmean(rms_map) * (-1.), "-1.0 x rms", fontsize=18, color="blue")
    plt.text(naxis3 * 0.6, -4. * std_value, "-4.0 x std", fontsize=18, color="red")
    plt.legend()
    std_threshold = 4.
    if np.nanmin(masked_average_spectrum) <= (-1) * std_value * std_threshold:
        plt.text(naxis3 * 2. / 5., -5. * std_value, "Warning!!", fontsize=25, color="Orange")
    plt.savefig(output_name, bbox_inches="tight")
    plt.show()
    if np.nanmin(masked_average_spectrum) <= (-1) * std_value * std_threshold:
        warning_sentence = '#### Warning ####'
        warning_sentence_mark = '###############'
        LOG.warn(warning_sentence_mark)
        LOG.warn(warning_sentence)
        LOG.warn(warning_sentence_mark)
    return


# # 5. Function for reading fits

# In[ ]:


# Function for reading FITS and its header
# def read_fits(input):
#         print("FITS:", input)
#         hdu          =  fits.open(input)[0]
#         cube        = hdu.data
#         naxis1     = hdu.header['NAXIS1']
#         naxis2     = hdu.header['NAXIS2']
#         naxis3     = hdu.header['NAXIS4']
#         cdelt2     = hdu.header['CDELT2']
#         cdelt3     = abs(hdu.header['CDELT4'])
#         cube_regrid = cube[:,0,:,:]
#         return cube_regrid, naxis1, naxis2, naxis3,cdelt2, cdelt3
# print("END")


# In[ ]:


# Function for reading FITS and its header (CASA version)
def read_fits(input):
    LOG.info("FITS: {}".format(input))
    with casatools.ImageReader(input) as ia:
        cube = ia.getchunk()
        csys = ia.coordsys()
        increments = csys.increment()
        csys.done()

    cube_regrid = np.swapaxes(cube[:, :, 0, :], 2, 0)
    naxis1 = cube.shape[0]
    naxis2 = cube.shape[1]
    naxis3 = cube.shape[3]
    cdelt2 = increments['numeric'][1]
    cdelt3 = abs(increments['numeric'][3])

    return cube_regrid, naxis1, naxis2, naxis3, cdelt2, cdelt3


# # 6. Main part

# In[ ]:


# number_of_spw = 0
# for project_loop in range(len(Project)):
#     project_name = Project[project_loop]
#     fits_list = read_input(project_name)

#     for fits_loop in range(len(fits_list)):


def detect_contamination(imagename):
    LOG.info("=================")
    #input = "./" + project_name + "/" + fitsimage
    #fitsimage = os.path.basename(imagename.rstrip('/'))
    #output_name = str(project_name) + "." + str(fitsimage) + ".png"
    # TODO: adapt output_name for pipeline naming scheme
    output_name = imagename.rstrip('/') + '.contamination.png'
    LOG.info(output_name)
    #number_of_spw = number_of_spw + 1
    # Read FITS and its header
    cube_regrid, naxis1, naxis2, naxis3, cdelt2, cdelt3 = read_fits(imagename)

    # Making rms 　& Peak SN maps
    rms_map = decide_rms(naxis3, cube_regrid)
    peak_sn = (np.nanmax(cube_regrid, axis=0)) / rms_map
    idy, idx = np.unravel_index(np.argmax(peak_sn), peak_sn.shape)
    spectrum_at_peak = cube_regrid[:, idy, idx]

    # Making averaged spectra and masked average spectrum
    all_average_spectrum = np.zeros([naxis3])

    mask_map = np.zeros([naxis2, naxis1])
    count_map = np.zeros([naxis2, naxis1])
    #min_value = np.nanmin(cube_regrid)
    #max_value = np.nanmax(cube_regrid)
    rms_threshold = 2.

    for i in range(naxis1):
        for j in range(naxis2):
            if np.isnan(np.nanmax(cube_regrid[:, j, i])) == False:
                all_average_spectrum = all_average_spectrum + cube_regrid[:, j, i]
                count_map[j, i] = 1.0

    all_average_spectrum = all_average_spectrum / np.nansum(count_map)

    # In the case that pixel number is fewer than the mask threshold (mask_num_thresh).
    #mask_num_thresh = 0.
    peak_sn_threshold = 0.
    peak_sn2 = (np.nanmax(cube_regrid, axis=0)) / rms_map
    peak_sn_1d = np.ravel(peak_sn2)
    peak_sn_1d.sort()
    parcent_threshold = 10. #%
    total_pix = np.sum(count_map)
    pix_num_threshold = int(total_pix * parcent_threshold / 100.)
    peak_sn_threshold = peak_sn_1d[pix_num_threshold]

    mask_map2 = np.zeros([naxis2, naxis1])
    masked_average_spectrum2 = np.zeros([naxis3])
    peak_sn2_judge = (peak_sn < peak_sn_threshold)
    for i in range(naxis1):
        for j in range(naxis2):
            if str(peak_sn2_judge[j, i]) == "True":
                masked_average_spectrum2 = masked_average_spectrum2 + cube_regrid[:, j, i]
                mask_map2[j, i] = 1.0

    masked_average_spectrum2 = masked_average_spectrum2 / np.nansum(mask_map2)
    mask_map = mask_map2
    masked_average_spectrum = masked_average_spectrum2

    # Make figures
    make_figures(peak_sn, mask_map, rms_threshold, rms_map,
                 masked_average_spectrum, all_average_spectrum,
                 naxis3, peak_sn_threshold, spectrum_at_peak, idy, idx, output_name)

#LOG.info("Total spw:", number_of_spw)


# In[ ]:




