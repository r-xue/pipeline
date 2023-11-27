"""
This module provides functionality to detect contamination in spectral data.

Original code provided by Yoshito Shimajiri.
For more details, refer to PIPE-251.
"""

from collections import namedtuple
from math import ceil
import os
from typing import Dict, Optional, Tuple, TYPE_CHECKING, Union

import matplotlib
import matplotlib.figure as figure
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.displays.pointing as pointing
from pipeline.infrastructure import casa_tools
from ..common import display as sd_display
from ..common import sdtyping

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.axis import Axis
    from matplotlib.ticker import Formatter, Locator
    from pipeline.infrastructure import Context
    from pipeline.infrastructure.imagelibrary import ImageItem


# Initialize logger for this module
LOG = infrastructure.get_logger(__name__)

# Global parameters
MATPLOTLIB_FIGURE_NUM = 6666  # Unique identifier for matplotlib figures
STD_THRESHOLD = 4.  # Threshold for standard deviation to detect absorption features

# Parameters for slicing the cube image and determining the scope of processing
N_SLICES = 10  # Total number of slices
N_EDGE = 2  # Number of edge slices to be excluded from processing
N_REMAINING = N_SLICES - N_EDGE * 2  # Number of slices remaining after excluding edges

# Default colormap of graphs
DEFAULT_COLORMAP = "rainbow"

# Define a named tuple to represent the frequency specification.
#  unit: Represents the unit of the frequency (e.g., pixel, Hz, MHz).
#  data: Represents the actual frequency data or values.
FrequencySpec = namedtuple('FrequencySpec', ['unit', 'data'])

# Define a named tuple to represent the direction specification in astronomical images.
#  ref: Represents the reference frame (e.g., J2000, B1950).
#  minra: Represents the minimum right ascension value.
#  maxra: Represents the maximum right ascension value.
#  mindec: Represents the minimum declination value.
#  maxdec: Represents the maximum declination value.
#  resolution: Represents the resolution of the image in the direction axes.
DirectionSpec = namedtuple('DirectionSpec', ['ref', 'minra', 'maxra', 'mindec', 'maxdec', 'resolution'])

# Define a named tuple to represent the sizes of each axis in a image cube.
#  x: Represents the size of the X-axis (typically the RA direction in astronomical images).
#  y: Represents the size of the Y-axis (typically the DEC direction in astronomical images).
#  sp: Represents the size of the spectral axis (e.g., frequency or velocity).
NAxis = namedtuple('NAxis', ['x', 'y', 'sp'])


def detect_contamination(context: 'Context',
                         item: 'ImageItem',
                         is_frequency_channel_reversed: Optional[bool]=False):
    """
    Detect contamination in the given image item.

    This function is the main routine of the module. It detects contamination in the provided image item
    and generates a contamination report.

    Args:
        context (Context): The pipeline context object.
        item (ImageItem): The image item object to be analyzed.
        is_frequency_channel_reversed (bool, optional): True if frequency channels are in reversed order. Defaults to False.
    """
    LOG.info("=================")

    # Create a directory for the current stage
    stage_dir = os.path.join(context.report_dir, f'stage{context.task_counter}')
    os.makedirs(stage_dir, exist_ok=True)

    # Define the output name for the contamination report
    output_name = os.path.join(stage_dir, item.imagename.rstrip('/') + '.contamination.png')
    LOG.info(output_name)

    # Read FITS and its header
    cube_regrid, naxis = _read_fits(item.imagename)
    image_obj = sd_display.SpectralImage(item.imagename)
    freq_spec = _get_frequency_spec(naxis, image_obj)
    dir_spec = _get_direction_spec(image_obj)

    # Calculate RMS and Peak SN maps
    rms_map, peak_sn, spectrum_at_peak, idx, idy = \
        _calculate_rms_and_peak_sn(cube_regrid, naxis, is_frequency_channel_reversed)

    # Determine peak SN threshold
    # In the case that pixel number is fewer than the mask threshold (mask_num_thresh).
    peak_sn_threshold = _determine_peak_sn_threshold(cube_regrid, rms_map)

    # Update the mask map and calculate the masked average spectrum
    mask_map, masked_average_spectrum = _get_mask_map_and_average_spectrum(cube_regrid, naxis,
                                                                           peak_sn, peak_sn_threshold)

    # Generate the contamination report figures
    _make_figures(peak_sn, mask_map, rms_map, masked_average_spectrum,
                  peak_sn_threshold, spectrum_at_peak, idy, idx, output_name,
                  freq_spec, dir_spec)

    # Issue a warning if an absorption feature is detected
    _warn_deep_absorption_feature(masked_average_spectrum, item)


def _decide_rms(naxis: NAxis,
                cube_regrid: 'sdtyping.NpArray3D',
                is_frequency_channel_reversed: bool) -> 'sdtyping.NpArray2D':
    """
    Find the emission free channels roughly for estimating RMS.

    Slice the cube image into N_SLICES frequeucy-wise (or AXIS3 wise), and return the slice
    which has the smallest rms value among them.  Each N_EDGE slices on both edges are
    excluded from the estimate.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        cube_regrid (NpArray3D) : data chunk loaded from image cube
        is_frequency_channel_reversed (bool) : True if frequency channels are in reversed order. False if not.

    Returns:
        RMS map of the part of the cube.
    """
    # Calculate RMS maps for the slices
    sliced_rms_maps = [
        _slice_and_calc_RMS_of_cube_regrid(naxis, cube_regrid, x, is_frequency_channel_reversed)
        for x in range(N_EDGE, N_SLICES - N_EDGE)
    ]

    # Calculate mean RMS values for each slice
    mean_rms_values = np.array([np.nanmean(sliced_rms_maps[x]) for x in range(N_REMAINING)])

    # Select the RMS map with the smallest mean RMS value
    rms_map = sliced_rms_maps[np.argmin(mean_rms_values)]

    LOG.info("RMS: {}".format(np.nanmean(rms_map)))
    return rms_map


def _slice_and_calc_RMS_of_cube_regrid(naxis: NAxis,
                                       cube_regrid: 'sdtyping.NpArray3D',
                                       pos: int,
                                       is_frequency_channel_reversed: bool) -> 'sdtyping.NpArray2D':
    """
    Get one chunk from N_SLICES chunks of cube_regrid, and calculate RMS of it.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        cube_regrid (NpAdday3D): data chunk loaded from image cube
        pos (int): position to slice
        is_frequency_channel_reversed (bool): True if frequency channels are in reversed order. False if not.

    Returns:
        RMS array of the squared standard deviation and mean for the sliced cube.
    """
    # calculate start and end positions for slicing the cube
    _func = ceil if is_frequency_channel_reversed else int
    start_rms_ch = _func(naxis.sp * pos / N_SLICES)
    end_rms_ch = _func(naxis.sp * (pos + 1) / N_SLICES)

    # extract a slice from the cube based on the calculated positions
    sliced_cube = cube_regrid[start_rms_ch:end_rms_ch, :, :]

    # calculate the squared standard deviation and mean for the sliced cube
    stddevsq = np.nanstd(sliced_cube, axis=0) ** 2.
    meansq = np.nanmean(sliced_cube, axis=0) ** 2.

    # compute and return the RMS using the squared standard deviation and mean
    return np.sqrt(stddevsq + meansq)


def _make_figures(peak_sn: 'sdtyping.NpArray2D',
                  mask_map: 'sdtyping.NpArray2D',
                  rms_map: 'sdtyping.NpArray2D',
                  masked_average_spectrum: 'sdtyping.NpArray1D',
                  peak_sn_threshold: float,
                  spectrum_at_peak: 'sdtyping.NpArray1D',
                  idy: np.int64,
                  idx: np.int64,
                  output_name: str,
                  freq_spec: Optional[FrequencySpec]=None,
                  dir_spec: Optional[DirectionSpec]=None) -> None:
    """
    Create figures to visualize contamination.

    Args:
        peak_sn (NpArray2D): Array representing the peak SN ratio.
        mask_map (NpArray2D): Array representing the mask map.
        rms_map (NpArray2D): Array representing the RMS map.
        masked_average_spectrum (NpArray1D): Array representing the masked average spectrum.
        peak_sn_threshold (float): Threshold value for the peak SN ratio.
        spectrum_at_peak (NpArray1D): Array representing the spectrum at the peak.
        idy (int64): Y-axis index of the pixel (spatial direction).
        idx (int64): X-axis index of the pixel (spatial direction).
        output_name (str): Name of the output file.
        freq_spec (FrequencySpec, optional): Frequency specification. Defaults to None.
        dir_spec (DirectionSpec, optional): Direction specification. Defaults to None.
    """
    # Initialize the figure with a specified size
    _figure = figure.Figure(figsize=(20, 5))

    # Create subplots(Axes) for peak SN map, mask map, and masked averaged spectrum
    peak_sn_plot = _figure.add_subplot(1, 3, 1)
    mask_map_plot = _figure.add_subplot(1, 3, 2)
    kw = {}

    # Check if direction specification is provided
    if dir_spec is not None:
        _set_plot_spec(peak_sn_plot, dir_spec)
        _set_plot_spec(mask_map_plot, dir_spec)
        dir_unit = dir_spec.ref

        # Convert pixel coordinates to axes coordinates
        scx = (idx + 0.5) / peak_sn.shape[1]
        scy = (idy + 0.5) / peak_sn.shape[0]

        # Set aspect ratio based on DEC correction factor
        kw['aspect'] = 1.0 / np.cos((dir_spec.mindec + dir_spec.maxdec) / 2 / 180 * np.pi)

        # Set extent for the plot
        _half_resolution = dir_spec.resolution / 2
        kw['extent'] = (dir_spec.maxra + _half_resolution, dir_spec.minra - _half_resolution,
                        dir_spec.mindec - _half_resolution, dir_spec.maxdec + _half_resolution)
    else:
        dir_unit = 'pixel'
        scx = idx
        scy = peak_sn.shape[0] - 1 - idy

    # Plot the peak SN map, mask map, and masked averaged spectrum
    _plot_peak_SN_map(_figure, peak_sn_plot, peak_sn, dir_unit, dir_spec is not None, scx, scy, kw)
    _plot_mask_map(_figure, mask_map_plot, mask_map, peak_sn_threshold, dir_unit, kw)
    _plot_masked_averaged_spectrum(_figure, rms_map, masked_average_spectrum, peak_sn_threshold,
                                   spectrum_at_peak, freq_spec)

    # Disable the use of offset on axis labels
    _figure.gca().get_xaxis().get_major_formatter().set_useOffset(False)
    _figure.gca().get_yaxis().get_major_formatter().set_useOffset(False)

    # Save the figure to the specified output file
    _figure.savefig(output_name, bbox_inches="tight")
    _figure.clf()


def _plot_peak_SN_map(fig: 'matplotlib.Figure',
                      plot: 'Axes',
                      peak_sn: 'sdtyping.NpArray2D',
                      dir_unit: str,
                      has_dir_spec: bool,
                      scx: float,
                      scy: float,
                      kw: Dict[str, Union[float, Tuple[float, float]]]) -> None:
    """
    Plot the Peak Signal-to-Noise (SN) map with specified parameters.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        peak_sn (NpArray2D): The data representing the peak signal-to-noise ratio.
        dir_unit (str): The unit for the RA (Right Ascension) and DEC (Declination) axis labels.
        has_dir_spec (bool): Flag indicating if a direction specification is provided.
        scx (float): The x-coordinate for the scatter plot marker.
        scy (float): The y-coordinate for the scatter plot marker.
        kw (Dict[str, Union[float, Tuple[float, float]]]): Additional keyword arguments for the imshow().
    """
    # Log the scatter plot coordinates
    LOG.debug(f'scx = {scx}, scy = {scy}')

    # Set the current Axes object to the provided plot
    fig.sca(plot)

    # Set the title and axis labels for the plot
    plot.set_title("Peak SN map")
    plot.set_xlabel(f"RA [{dir_unit}]")
    plot.set_ylabel(f"DEC [{dir_unit}]")

    # Log the shape of the peak SN data
    LOG.debug('peak_sn.shape = {}'.format(peak_sn.shape))

    # Display the peak SN data as an image with the specified colormap and keyword arguments
    plot.imshow(np.flipud(peak_sn), cmap=DEFAULT_COLORMAP, **kw)

    # Log the y-axis limits
    ylim = plot.get_ylim()
    LOG.debug('ylim = {}'.format(list(ylim)))

    # Display a colorbar for the image
    plot.colorbar = fig.colorbar(
        matplotlib.cm.ScalarMappable(
            matplotlib.colors.Normalize(np.nanmin(peak_sn), np.nanmax(peak_sn)),
            DEFAULT_COLORMAP),
        shrink=0.9, ax=plot)

    # Determine the transformation for the scatter plot marker based on the presence of a direction specification
    trans = fig.gca().transAxes if has_dir_spec else None

    # Plot a scatter marker at the specified coordinates
    plot.scatter(scx, scy, s=300, marker="o", facecolors='none',
                 edgecolors='grey', linewidth=5, transform=trans)


def _plot_mask_map(fig: 'matplotlib.Figure',
                   plot: 'Axes',
                   mask_map: 'sdtyping.NpArray2D',
                   peak_sn_threshold: float,
                   dir_unit: str,
                   kw: Dict[str, Union[float, Tuple[float, float]]]):
    """
    Plot the mask map with specified parameters.

    Args:
        plot (Axes): The matplotlib Axes object to be used for plotting.
        mask_map (NpArray2D): The data representing the mask map.
        peak_sn_threshold (float): The threshold for the peak SN ratio.
        dir_unit (str): The unit for the RA (Right Ascension) and DEC (Declination) axis labels.
        kw (Dict[str, Union[float, Tuple[float, float]]]): Additional keyword arguments for the imshow().
    """
    # Set the current Axes object to the provided plot
    fig.sca(plot)

    # Set the title and axis labels for the plot
    plot.set_title(f"Mask map (1: SN<{peak_sn_threshold})")
    plot.set_xlabel(f"RA [{dir_unit}]")
    plot.set_ylabel(f"DEC [{dir_unit}]")

    # Display the mask map data as an image with the specified colormap and keyword arguments
    plot.imshow(np.flipud(mask_map), vmin=0, vmax=1, cmap=DEFAULT_COLORMAP, **kw)

    # Define a custom formatter for the colorbar labels
    formatter = matplotlib.ticker.FixedFormatter(['Masked', 'Unmasked'])

    # Display a colorbar for the image with the custom formatter
    plot.colorbar = fig.colorbar(
        matplotlib.cm.ScalarMappable(
            matplotlib.colors.Normalize(np.nanmin(mask_map), np.nanmax(mask_map)),
            DEFAULT_COLORMAP),
        shrink=0.9, ticks=[0, 1], format=formatter, ax=plot)


def _plot_masked_averaged_spectrum(fig: 'matplotlib.Figure',
                                   rms_map: 'sdtyping.NpArray2D',
                                   masked_average_spectrum: 'sdtyping.NpArray1D',
                                   peak_sn_threshold: float,
                                   spectrum_at_peak: 'sdtyping.NpArray1D',
                                   freq_spec: Optional[FrequencySpec]=None):
    """
    Plot the masked-averaged spectrum with specified parameters.

    Args:
        rms_map (NpArray2D): The data representing the RMS map.
        masked_average_spectrum (NpArray1D): The masked averaged spectrum data.
        peak_sn_threshold (float): The threshold for the peak signal-to-noise ratio.
        spectrum_at_peak (NpArray1D): The spectrum data at the peak.
        freq_spec (Optional[FrequencySpec]): Frequency specifications. Defaults to None.
    """
    # Calculate the standard deviation of the masked averaged spectrum
    stddev = np.nanstd(masked_average_spectrum)

    # Create a subplot for the masked-averaged spectrum
    plot = fig.add_subplot(1, 3, 3)
    plot.set_title("Masked-averaged spectrum")

    if freq_spec is not None:
        abc = freq_spec.data
        assert len(abc) == len(spectrum_at_peak)
        plot.set_xlabel(f'Frequency [{freq_spec.unit}]')
    else:
        abc = np.arange(len(spectrum_at_peak), dtype=int)
        plot.set_xlabel("Channel")

    # Get the width and the minimum value of the frequency or channel range
    w = np.abs(abc[-1] - abc[0])
    minabc = np.min(abc)

    # Set y-axis label and limits
    plot.set_ylabel("Intensity [K]")
    plot.set_ylim(stddev * (-7.), stddev * 7.)

    # Plot the spectrum at the peak and the masked averaged spectrum
    plot.plot(abc, spectrum_at_peak, "-", color="grey", label="spectrum at peak", alpha=0.5)
    plot.plot(abc, masked_average_spectrum, "-", color="red", label="masked averaged")

    # Define the edges for horizontal lines
    _edge = [abc[0], abc[-1]]

    # Plot horizontal lines for reference
    plot.plot(_edge, [0, 0], "-", color="black")
    plot.plot(_edge, [-4. * stddev, -4. * stddev], "--", color="red")
    plot.plot(_edge, [np.nanmean(rms_map) * 1., np.nanmean(rms_map) * 1], "--", color="blue")
    plot.plot(_edge, [np.nanmean(rms_map) * (-1.), np.nanmean(rms_map) * (-1.)], "--", color="blue")

    # Check if the standard deviation is above the threshold and plot additional lines and annotations
    if stddev * (7.) >= np.nanmean(rms_map) * peak_sn_threshold:
        plot.plot(_edge,
                  [np.nanmean(rms_map) * peak_sn_threshold, np.nanmean(rms_map) * peak_sn_threshold],
                  "--", color="green")
        plot.text(minabc + w * 0.5, np.nanmean(rms_map) * peak_sn_threshold,
                  "lower 10% level", fontsize=18, color="green")

    # Add text annotations for the plotted lines
    plot.text(minabc + w * 0.1, np.nanmean(rms_map) * 1., "1.0 x rms", fontsize=18, color="blue")
    plot.text(minabc + w * 0.1, np.nanmean(rms_map) * (-1.), "-1.0 x rms", fontsize=18, color="blue", va='top')
    plot.text(minabc + w * 0.6, -4. * stddev, "-4.0 x std", fontsize=18, color="red")

    # Display the legend
    plot.legend()

    # Add a warning text if the minimum of the masked averaged spectrum is below the threshold
    if np.nanmin(masked_average_spectrum) <= (-1) * stddev * STD_THRESHOLD:
        plot.text(minabc + w * 2. / 5., -5. * stddev, "Warning!!", fontsize=25, color="Orange")


def _set_plot_spec(plot: 'Axes',
                   dir_spec: DirectionSpec):
    """
    Configure the plot specifications based on the provided direction specifications.

    This function adjusts the x and y axis labels, ticks, and rotations based on the provided
    direction specifications. It uses the pointing module to determine the appropriate formatting
    and rotation for the RA and DEC labels.

    Args:
        plot (Axes): The plot object to be configured.
        dir_spec (DirectionSpec): The direction specifications containing details about RA and DEC.
    """
    # Calculate the span based on the maximum and minimum RA and DEC values, and the resolution
    _span = max(dir_spec.maxra - dir_spec.minra + dir_spec.resolution,
                dir_spec.maxdec - dir_spec.mindec + dir_spec.resolution)

    # Get the appropriate locators and formatters for RA and DEC based on the span and reference
    RAlocator, DEClocator, RAformatter, DECformatter = pointing.XYlabel(_span, dir_spec.ref)

    # Configure the x-axis (RA) and y-axis (DEC) with the obtained formatters, locators, and rotations
    _configure_axis(plot.xaxis, RAformatter, RAlocator, pointing.RArotation)
    _configure_axis(plot.yaxis, DECformatter, DEClocator, pointing.DECrotation)


def _configure_axis(axis: 'Axis',
                    formatter: 'Formatter',
                    locator: 'Locator',
                    rotation: Union['pointing.RArotation', 'pointing.DECrotation']):
    """
    Configure the given axis with the specified formatter, locator, and rotation.

    Args:
        axis (Axis): The axis to be configured.
        formatter (Formatter): The formatter to be set for the axis.
        locator (Locator): The locator to be set for the axis.
        rotation (Union[pointing.RArotation, pointing.DECrotation]): The rotation to be set for the axis labels.
    """
    axis.set_major_formatter(formatter)
    axis.set_major_locator(locator)
    axis.set_tick_params(rotation=rotation)


def _warn_deep_absorption_feature(masked_average_spectrum: 'sdtyping.NpArray1D',
                                  imageitem: Optional['ImageItem']=None):
    """
    Warn if a strong absorption feature is detected in the spectrum.

    This function checks the masked average spectrum for any strong absorption features.
    If detected, it logs a warning message with details about the image item (if provided).

    Args:
        masked_average_spectrum (NpArray1D): Array representing the masked average spectrum.
        imageitem (Optional['ImageItem']): ImageItem object containing details about the image. Defaults to None.
    """
    # Calculate the standard deviation of the masked average spectrum
    std_value = np.nanstd(masked_average_spectrum)

    # Determine if the spectrum has a strong absorption feature
    _has_strong_absorption = np.nanmin(masked_average_spectrum) <= (-1) * std_value * STD_THRESHOLD

    # If a strong absorption feature is detected, log a warning message
    if _has_strong_absorption:
        if imageitem is not None:
            field = imageitem.sourcename
            spw = ','.join(map(str, np.unique(imageitem.spwlist)))
            warning_sentence = (f'Field {field} Spw {spw}: '
                                'Absorption feature is detected in the lower SN area. '
                                'Please check calibration result in detail.')
        else:
            warning_sentence = ('Absorption feature detected but an image item is not specificated. '
                                'Please check calibration result in detail.')
        LOG.warning(warning_sentence)


def _read_fits(input: str) -> Tuple['sdtyping.NpArray3D', NAxis]:
    """
    Read FITS file and extract its header information using casatools.

    Args:
        input (str): Path to the FITS image file.

    Returns:
        Tuple containing:
        - Data chunk extracted from the FITS file.
        - Sizes of axes of 3D image cube.
    """
    LOG.info(f"FITS: {input}")

    # Extract data chunk and coordinate system from the FITS file
    with casa_tools.ImageReader(input) as ia:
        cube = ia.getchunk()

    # Reorder the axes of the cube for further processing
    cube_regrid = np.swapaxes(cube[:, :, 0, :], 2, 0)

    # Extract dimensions and increments from the cube and coordinate system
    naxis = NAxis(x=cube.shape[0], y=cube.shape[1], sp=cube.shape[3])

    return cube_regrid, naxis


def _get_direction_spec(image_obj: 'sd_display.SpectralImage') -> DirectionSpec:
    """
    Extract direction specifications from a given SpectralImage object.

    Args:
        image_obj: SpectralImage object containing image metadata.

    Returns:
        DirectionSpec object.
    """
    # Convert RA and DEC values from the image object to degrees
    minra, maxra = _convert_to_degrees(image_obj.ra_min, image_obj.ra_max)
    mindec, maxdec = _convert_to_degrees(image_obj.dec_min, image_obj.dec_max)

    # The grid size is obtained by dividing the beam size by 3.
    grid_size = image_obj.beam_size / 3

    return DirectionSpec(ref=image_obj.direction_reference,
                         minra=minra, maxra=maxra,
                         mindec=mindec, maxdec=maxdec,
                         resolution=grid_size)


def _convert_to_degrees(min_value: float,
                        max_value: float) -> Tuple[float, float]:
    """
    Convert given values to degrees using CASA quanta tool.

    Args:
        min_value (float): Minimum value to convert.
        max_value (float): Maximum value to convert.

    Returns:
        Tuple containing the converted minimum and maximum values in degrees.
    """
    # Initialize CASA quanta tool for unit conversion
    qa = casa_tools.quanta

    return qa.convert(min_value, 'deg')['value'], qa.convert(max_value, 'deg')['value']


def _get_frequency_spec(naxis: NAxis,
                        image_obj: 'sd_display.SpectralImage') -> FrequencySpec:
    """
    Extract frequency specifications from a given SpectralImage object.

    Args:
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        image_obj (SpectralImage) : SpectralImage object containing image metadata.

    Returns:
        FrequencySpec object.
    """
    # Retrieve spectral axis details in GHz from the image object
    refpix, refval, increment = image_obj.spectral_axis(unit='GHz')

    # Calculate the frequency values based on the spectral axis details
    frequency = np.array([refval + increment * (i - refpix) for i in range(naxis.sp)])

    return FrequencySpec(unit='GHz', data=frequency)


def _calculate_rms_and_peak_sn(cube_regrid: 'sdtyping.NpArray3D',
                               naxis: NAxis,
                               is_frequency_channel_reversed: bool) -> Tuple[np.array, np.array, np.array, int, int]:
    """Calculate the RMS (Root Mean Square) and Peak SN maps for the given data cube.

    This function computes the RMS map for the data cube and then calculates the peak SN for each pixel.
    It also identifies the location (idx, idy) of the maximum peak SN in the image.

    Args:
        cube_regrid (NpArray3D): 3D data cube containing the image data.
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        is_frequency_channel_reversed (bool): Indicates if the frequency channels are in reversed order.

    Returns:
        Tuple[np.array, np.array, np.array, int, int]:
            - rms_map: 2D array containing the RMS values for each pixel.
            - peak_sn: 2D array containing the peak SN values for each pixel.
            - spectrum_at_peak: 1D array containing the spectrum at the location of the maximum peak SN.
            - idx: x-coordinate of the maximum peak SN location.
            - idy: y-coordinate of the maximum peak SN location.
    """
    # Calculate the RMS map for the data cube
    rms_map = _decide_rms(naxis, cube_regrid, is_frequency_channel_reversed)

    # Calculate the peak SN for each pixel
    peak_sn = (np.nanmax(cube_regrid, axis=0)) / rms_map

    # Identify the location of the maximum peak SN in the image
    idy, idx = np.unravel_index(np.nanargmax(peak_sn), peak_sn.shape)
    LOG.debug(f'idx {idx}, idy {idy}')

    # Extract the spectrum at the location of the maximum peak SN
    spectrum_at_peak = cube_regrid[:, idy, idx]

    return rms_map, peak_sn, spectrum_at_peak, idx, idy


def _count_valid_pixels(cube_regrid: 'sdtyping.NpArray3D') -> int:
    """
    Count valid pixels for the given data cube.

    This function counts amount of pixels in the data cube has a valid value (not NaN).

    Args:
        cube_regrid (sdtyping.NpArray3D): 3D data cube containing the image data.

    Returns:
        amount of pixels in the data cube has a valid value.
    """
    total_pix = 0
    _, ny, nx = cube_regrid.shape

    # Iterate over the data cube and count valid pixels
    for i in range(nx):
        for j in range(ny):
            if not np.isnan(np.nanmax(cube_regrid[:, j, i])):
                total_pix += 1

    return total_pix


def _determine_peak_sn_threshold(cube_regrid: 'sdtyping.NpArray3D',
                                 rms_map: np.array) -> float:
    """Determine the peak SN threshold.

    This function calculates the peak SN for each pixel in the image and then determines a threshold
    based on a certain percentage of the total number of valid pixels.

    Args:
        cube_regrid (sdtyping.NpArray3D): 3D data cube containing the image data.
        rms_map (np.array): 2D array containing the root mean square (RMS) values for each pixel.

    Returns:
        The calculated peak SN threshold.
    """
    # Calculate the peak SN for each pixel
    peak_sn = (np.nanmax(cube_regrid, axis=0)) / rms_map

    # Flatten the 2D peak SN array to a 1D array and sort it
    peak_sn_1d = np.ravel(peak_sn)
    peak_sn_1d.sort()

    # Define the percentage threshold
    percent_threshold = 10.  # %

    # Calculate the number of pixels corresponding to the percentage threshold
    total_pix = _count_valid_pixels(cube_regrid)
    pix_num_threshold = int(total_pix * percent_threshold / 100.)

    # Return the peak SN value corresponding to the pixel number threshold
    if peak_sn_1d.shape[0] > 0:
        return peak_sn_1d[pix_num_threshold]
    else:
        return 0.


def _get_mask_map_and_average_spectrum(cube_regrid: np.array,
                                       naxis: NAxis,
                                       peak_sn: np.array,
                                       peak_sn_threshold: float) -> Tuple[np.array, np.array]:
    """
    Get mask map and masked average spectrum based on peak SN threshold.

    This function creates a mask map and a masked average spectrum using the provided peak SN threshold.
    Pixels in the mask map are set to 1.0 if the corresponding peak SN value is below the threshold.

    Args:
        cube_regrid (np.array): 3D data cube containing the image data.
        naxis (NAxis) : namedtuple of the sizes of each axis in a image cube.
        peak_sn (np.array): 2D array containing the peak SN ratio for each pixel.
        peak_sn_threshold (float): Threshold value for the peak SN.

    Returns:
        Tuple[np.array, np.array]:
            mask_map: 2D array where each pixel is set to 1.0 if the corresponding peak SN value is below the threshold.
            masked_average_spectrum: 1D array representing the average spectrum of the masked regions.
    """
    # Initialize the mask map and masked average spectrum with zeros
    mask_map = np.zeros([naxis.y, naxis.x])
    masked_average_spectrum = np.zeros([naxis.sp])

    # Create a boolean mask based on the peak SN threshold
    peak_sn2_judge = (peak_sn < peak_sn_threshold)

    # Iterate over the data cube and update the mask map and masked average spectrum
    for i in range(naxis.x):
        for j in range(naxis.y):
            if str(peak_sn2_judge[j, i]) == "True":
                masked_average_spectrum = masked_average_spectrum + cube_regrid[:, j, i]
                mask_map[j, i] = 1.0

    # Normalize the masked average spectrum by the sum of the mask map
    masked_average_spectrum /= np.nansum(mask_map)

    return mask_map, masked_average_spectrum
