# coding: utf-8
#
# This code is originally provided by Yoshito Shimajiri.
# See PIPE-251 for detail about this.

from math import ceil
import os
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import collections

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.displays.pointing as pointing
from pipeline.infrastructure import casa_tools
from ..common import display as sd_display

LOG = infrastructure.get_logger(__name__)


# global parameters
MATPLOTLIB_FIGURE_NUM = 6666
std_threshold = 4.


# Frequency Spec
FrequencySpec = collections.namedtuple('FrequencySpec', ['unit', 'data'])


# Direction Spec
DirectionSpec = collections.namedtuple('DirectionSpec', ['ref', 'minra', 'maxra', 'mindec', 'maxdec', 'resolution'])


# To find the emission free channels roughly for estimating RMS
def decide_rms(naxis3: int, cube_regrid, inverted: bool=False):
    rms_maps = [__slice_cube_regrid(naxis3, cube_regrid, x, inverted) for x in range(2, 8)]
    rms_check = np.array([np.nanmean(rms_maps[x]) for x in range(6)])
    rms_map = rms_maps[np.argmin(rms_check)]
    LOG.info("RMS: {}".format(np.nanmean(rms_map)))
    return rms_map


# Slice cube_regrid from position to position+1
def __slice_cube_regrid(naxis3: int, cube_regrid, pos, inverted: bool):
    if inverted:
        start_rms_ch, end_rms_ch = ceil(naxis3 * pos / 10), ceil(naxis3 * (pos + 1) / 10)
    else:
        start_rms_ch, end_rms_ch = int(naxis3 * pos / 10), int(naxis3 * (pos + 1) / 10)
    return ((np.nanstd(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2. + (np.nanmean(cube_regrid[start_rms_ch:end_rms_ch, :, :], axis=0))**2.)**0.5


# Function for making fiures
def make_figures(peak_sn, mask_map, rms_threshold, rms_map,
                 masked_average_spectrum, all_average_spectrum,
                 naxis3, peak_sn_threshold, spectrum_at_peak,
                 idy, idx, output_name, fspec=None, dspec=None):

    std_value = np.nanstd(masked_average_spectrum)
    plt.figure(MATPLOTLIB_FIGURE_NUM, figsize=(20, 5))
    a1 = plt.subplot(1, 3, 1)
    a2 = plt.subplot(1, 3, 2)
    kw = {}
    #dspec = None
    if dspec is not None:
        Extent = (dspec.maxra + dspec.resolution / 2, dspec.minra - dspec.resolution / 2,
                  dspec.mindec - dspec.resolution / 2, dspec.maxdec + dspec.resolution / 2)
        span = max(dspec.maxra - dspec.minra + dspec.resolution, dspec.maxdec - dspec.mindec + dspec.resolution)
        (RAlocator, DEClocator, RAformatter, DECformatter) = pointing.XYlabel(span, dspec.ref)
        for a in [a1, a2]:
            a.xaxis.set_major_formatter(RAformatter)
            a.yaxis.set_major_formatter(DECformatter)
            a.xaxis.set_major_locator(RAlocator)
            a.yaxis.set_major_locator(DEClocator)
            xlabels = a.get_xticklabels()
            plt.setp(xlabels, 'rotation', pointing.RArotation)
            ylabels = a.get_yticklabels()
            plt.setp(ylabels, 'rotation', pointing.DECrotation)
        kw['extent'] = Extent
        dunit = dspec.ref
        mdec = (dspec.mindec + dspec.maxdec) / 2 * np.pi / 180
        dx = (Extent[0] - Extent[1]) / peak_sn.shape[1]
        dy = (Extent[3] - Extent[2]) / peak_sn.shape[0]
        # Pixel coordinate -> Axes coordinate
        scx = (idx + 0.5) / peak_sn.shape[1]
        scy = (idy + 0.5) / peak_sn.shape[0]
        # aspect ratio based on DEC correction factor
        aspect = 1.0 / np.cos((dspec.mindec + dspec.maxdec) / 2 / 180 * np.pi)
        kw['aspect'] = aspect
    else:
        dunit = 'pixel'
        scx = idx
        scy = peak_sn.shape[0] - 1 - idy
    LOG.debug(f'scx = {scx}, scy = {scy}')
    plt.sca(a1)
    plt.title("Peak SN map")
    plt.xlabel(f"RA [{dunit}]")
    plt.ylabel(f"DEC [{dunit}]")
    LOG.debug('peak_sn.shape = {}'.format(peak_sn.shape))
    plt.imshow(np.flipud(peak_sn), cmap="rainbow", **kw)
    ylim = plt.ylim()
    LOG.debug('ylim = {}'.format(list(ylim)))
    plt.colorbar(shrink=0.9)
    trans = plt.gca().transAxes if dspec is not None else None
    plt.scatter(scx, scy, s=300, marker="o", facecolors='none', edgecolors='grey', linewidth=5,
                transform=trans)
    plt.sca(a2)
    plt.title("Mask map (1: SN<" + str(peak_sn_threshold) + ")")
    plt.xlabel(f"RA [{dunit}]")
    plt.ylabel(f"DEC [{dunit}]")
    plt.imshow(np.flipud(mask_map), vmin=0, vmax=1, cmap="rainbow", **kw)
    formatter = matplotlib.ticker.FixedFormatter(['Masked', 'Unmasked'])
    plt.colorbar(shrink=0.9, ticks=[0, 1], format=formatter)

    plt.subplot(1, 3, 3)
    plt.title("Masked-averaged spectrum")
    if fspec is not None:
        abc = fspec.data
        assert len(abc) == len(spectrum_at_peak)
        plt.xlabel('Frequency [{}]'.format(fspec.unit))
    else:
        abc = np.arange(len(spectrum_at_peak), dtype=int)
        plt.xlabel("Channel")
    w = np.abs(abc[-1] - abc[0])
    minabc = np.min(abc)
    plt.ylabel("Intensity [K]")
    plt.ylim(std_value * (-7.), std_value * 7.)
    plt.plot(abc, spectrum_at_peak, "-", color="grey", label="spectrum at peak", alpha=0.5)
    plt.plot(abc, masked_average_spectrum, "-", color="red", label="masked averaged")
    plt.plot([abc[0], abc[-1]], [0, 0], "-", color="black")
    plt.plot([abc[0], abc[-1]], [-4. * std_value, -4. * std_value], "--", color="red")
    plt.plot([abc[0], abc[-1]], [np.nanmean(rms_map) * 1., np.nanmean(rms_map) * 1], "--", color="blue")
    plt.plot([abc[0], abc[-1]], [np.nanmean(rms_map) * (-1.), np.nanmean(rms_map) * (-1.)], "--", color="blue")
    if std_value * (7.) >= np.nanmean(rms_map) * peak_sn_threshold:
        plt.plot([abc[0], abc[-1]], [np.nanmean(rms_map) * peak_sn_threshold, np.nanmean(rms_map) * peak_sn_threshold], "--", color="green")
        plt.text(minabc + w * 0.5, np.nanmean(rms_map) * peak_sn_threshold, "lower 10% level", fontsize=18, color="green")
    plt.text(minabc + w * 0.1, np.nanmean(rms_map) * 1., "1.0 x rms", fontsize=18, color="blue")
    plt.text(minabc + w * 0.1, np.nanmean(rms_map) * (-1.), "-1.0 x rms", fontsize=18, color="blue", va='top')
    plt.text(minabc + w * 0.6, -4. * std_value, "-4.0 x std", fontsize=18, color="red")
    plt.legend()
    if np.nanmin(masked_average_spectrum) <= (-1) * std_value * std_threshold:
        plt.text(minabc + w * 2. / 5., -5. * std_value, "Warning!!", fontsize=25, color="Orange")

    # disable use of offset on axis label
    plt.gca().get_xaxis().get_major_formatter().set_useOffset(False)
    plt.gca().get_yaxis().get_major_formatter().set_useOffset(False)

    plt.savefig(output_name, bbox_inches="tight")
    plt.clf()

    return


def warn_deep_absorption_feature(masked_average_spectrum, imageitem=None):
    std_value = np.nanstd(masked_average_spectrum)
    if np.nanmin(masked_average_spectrum) <= (-1) * std_value * std_threshold:
        if imageitem is not None:
            field = imageitem.sourcename
            spw = ','.join(map(str, np.unique(imageitem.spwlist)))
            warning_sentence = f'Field {field} Spw {spw}: ' \
                              'Absorption feature is detected ' \
                              'in the lower S/N area. ' \
                              'Please check calibration result in detail.'
        LOG.warning(warning_sentence)


# Function for reading FITS and its header (CASA version)
def read_fits(input):
    LOG.info("FITS: {}".format(input))
    with casa_tools.ImageReader(input) as ia:
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


def detect_contamination(context, imageitem, inverted=False):
    imagename = imageitem.imagename
    LOG.info("=================")
    stage_number = context.task_counter
    stage_dir = os.path.join(context.report_dir, f'stage{stage_number}')
    if not os.path.exists(stage_dir):
        os.mkdir(stage_dir)
    output_name = os.path.join(stage_dir, imagename.rstrip('/') + '.contamination.png')
    LOG.info(output_name)
    # Read FITS and its header
    cube_regrid, naxis1, naxis2, naxis3, cdelt2, cdelt3 = read_fits(imagename)
    image_obj = sd_display.SpectralImage(imagename)
    (refpix, refval, increment) = image_obj.spectral_axis(unit='GHz')
    frequency = np.array([refval + increment * (i - refpix) for i in range(naxis3)])
    fspec = FrequencySpec(unit='GHz', data=frequency)
    qa = casa_tools.quanta
    minra = qa.convert(image_obj.ra_min, 'deg')['value']
    maxra = qa.convert(image_obj.ra_max, 'deg')['value']
    mindec = qa.convert(image_obj.dec_min, 'deg')['value']
    maxdec = qa.convert(image_obj.dec_max, 'deg')['value']
    grid_size = image_obj.beam_size / 3
    dirref = image_obj.direction_reference
    dspec = DirectionSpec(ref=dirref, minra=minra, maxra=maxra, mindec=mindec, maxdec=maxdec,
                          resolution=grid_size)

    # Making rms 　& Peak SN maps
    rms_map = decide_rms(naxis3, cube_regrid, inverted)
    peak_sn = (np.nanmax(cube_regrid, axis=0)) / rms_map
    idy, idx = np.unravel_index(np.nanargmax(peak_sn), peak_sn.shape)
    LOG.debug(f'idx {idx}, idy {idy}')
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
                 naxis3, peak_sn_threshold, spectrum_at_peak, idy, idx, output_name,
                 fspec, dspec)

    # warn if absorption feature is detected
    warn_deep_absorption_feature(masked_average_spectrum, imageitem)
