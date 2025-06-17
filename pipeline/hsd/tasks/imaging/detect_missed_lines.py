"""
Diagnoze possible missed lines

Original code provided by Andres Guzman.
See PIPE-2416 / PIPEREQ-182 for details.
"""

from typing import List, Tuple, TYPE_CHECKING
import math
import os

from astropy.stats import sigma_clip
from matplotlib import figure
import numpy as np
from scipy.ndimage import convolve, label
from scipy.stats import median_abs_deviation

import pipeline.infrastructure as infrastructure
from pipeline.hsd.tasks.common import display as sd_display

if TYPE_CHECKING:
    from pipeline.domain import MeasurementSet
    from pipeline.infrastructure import Context
    from pipeline.infrastructure.imagelibrary import ImageItem
    from pipeline.hsd.tasks.common import sdtyping

# Initialize logger for this module
LOG = infrastructure.get_logger(__name__)

# Parameters
DEVIATION_THRESHOLD_SINGLE_BEAM = 7.0
DEVIATION_THRESHOLD_MOMENT_MASK = 5.0
MASK_LIMIT = 7.0
SIGMA_CLIPPING_THRESHOLD = 6.0
SIGMA_CLIPPING_MAX_ITERATIONS = 3


class DetectMissedLines( object ):
    """Class to find lines missed during line identification"""
    def __init__( self,
                  context: 'Context',
                  msobj_list: List['MeasurementSet'],
                  spwid_list: List[int],
                  fieldid_list: List[int],
                  item: 'ImageItem',
                  frequency_channel_reversed: bool = False,
                  edge: List[int] = [0, 0],
                  do_plot: bool = True ):
        """
        Construct DetectMissedLines instance

        Args:
            context: Pipeline context
            msobj_list: List of MeasurementSet objects
            spwid_list: List of spectral window ids
            fieldid_list: List of field ids
            item: The image item object to be analyzed
            frequency_channel_reversed: True if frequency channel is in reversed order (for LSB)
            edge: Edge parameter
            do_plot: Set True to make figure. Default is True
        Raises:
            ValueError if
              - dimensions of image and weight do not match
              - number of channels in spectral window object and image cube are inconsistent
        """
        self.context = context
        self.msobj_list = msobj_list
        self.spwid_list = spwid_list
        self.fieldid_list = fieldid_list
        self.item = item
        self.frequency_channel_reversed = frequency_channel_reversed
        self.edge = edge
        self.do_plot = do_plot

        # set field name
        self.field_name = self.msobj_list[0].get_fields( field_id=self.fieldid_list[0] )[0].name

        # read image and weight files (assume weight=1.0 if associated weight file is not found)
        self.image = sd_display.SpectralImage( self.item.imagename )
        weightname = self.item.imagename + ".weight"
        if os.path.exists( weightname ):
            self.weight = sd_display.SpectralImage( weightname )
            # check array shapes of image and weight
            if self.image.data.shape != self.weight.data.shape:
                raise ValueError(
                    "Demensions of image ({}) and weight ({}) do not match".format(self.item.imagename, weightname )
                )
        else:
            LOG.warning( "Weight file {} not found. Assuming weight=1.0 for all pixels.".format( weightname ) )
            self.weight = copy.deepcopy( self.image )
            self.weight.data = np.ones( np.shape( self.image.data ) )

        # number of spectral channels
        self.nchan = self.msobj_list[0].spectral_windows[self.spwid_list[0]].num_channels

        # 4th axis of image cube is the spectrum, 'edge' channels excluded.
        if self.image.data.shape[3] != self.nchan - sum(self.edge):
            # this vaiolates the assumption of the procdure
            raise ValueError(
                "Number of spectral channels of spectral window object and image cube does not match"
            )

        # frequency and channel conversion
        (refpix, refval, increment) = self.image.spectral_axis(unit='GHz')
        self.frequency = np.array([refval + increment * (ch - refpix) for ch in range(self.nchan - sum(self.edge))] )  # in GHz
        self.frequency_frame = self.image.frequency_frame

        # pixel scale and beam size
        self.pixel_scale = np.abs( self.image.direction_axis( 1, unit='deg' )[2]  )  # 1: Declination
        self.beam_size = self.image.beam_size  # in degrees

    def analyze( self,
                 valid_lines: List[List[float]],
                 linefree_ranges: List[List[int]] ) -> Tuple[bool, str | None]:
        """
        Analyze the image cube to diagnoze missed lines

        Args:
            valid_lines: List of valid lines (channels in float)
            linefree_ranges: List of line-free ranges (channels in int)
        Returns:
            True if possible missed-line is detected, False if not
            string indicating the detection process (None if no detection)
        """
        # convert valid_lines to line_ranges (int) and revert channels if LSB
        line_ranges = []
        for line in valid_lines:
            # 'valid_lines' holds the line center/width in channels BEFORE removing 'edge' channels,
            # while image cube comes with the 'edge' channels already dropped.
            # Also note that 'edge' parameters are in the order BEFORE flipping for sidebands.
            # (NOT in frequency order)
            if self.frequency_channel_reversed:  # LSB
                ( line_center, line_width ) = ( self.nchan - line[0] - 1.0, line[1] )
                line_center -= self.edge[1]
            else:  # USB
                ( line_center, line_width ) = ( line[0], line[1] )
                line_center -= self.edge[0]
            line_ranges.append(
                [ math.floor(line_center - line_width/2.0),
                  math.ceil(line_center + line_width/2.0) ] )

        # first try 'single_beam' method
        single_beam_detection = self._detect_over_deviation_threshold( line_ranges,
                                                                       linefree_ranges,
                                                                       mask_mode='single_beam',
                                                                       dev_threshold=DEVIATION_THRESHOLD_SINGLE_BEAM )
        if single_beam_detection:
            LOG.info( "Field {} spw {}: Significant off-line-range emission detected at peak.".format( self.field_name, self.spwid_list[0] ))
            return True, "single_beam"

        # next try 'moment_mask' method
        LOG.info( "Field {} spw {}: No significant off-line-range emission detected at peak. Search for extended emission with 'moment_mask'.".format( self.field_name, self.spwid_list[0] ))
        moment_mask_detection = self._detect_over_deviation_threshold( line_ranges,
                                                                       linefree_ranges,
                                                                       mask_mode='moment_mask',
                                                                       dev_threshold=DEVIATION_THRESHOLD_MOMENT_MASK )
        if moment_mask_detection:
            LOG.info( "Field {} spw {}: Significant off-line-range extended emission detected.".format( self.field_name, self.spwid_list[0] ))
            return True, "moment_mask"

        # Return False if no missed-lines are detected
        LOG.info( "Field {} spw {}: No significant off-line-range extended emission detected.".format( self.field_name, self.spwid_list[0] ))
        return False, None

    def _sigma_estimation( self, data: np.ndarray, sigma: float, maxiters: int ) -> float:
        """
        Estimate the standard deviation of a sigma clipped 'data'

        Args:
            data : Image or spectrum
            sigma : Sigma clipping threshold
            maxiters : Limit of iterations
        Returns:
            estimated sigma value
        """
        clipped_data = sigma_clip( data, sigma=sigma, maxiters=maxiters,
                                   cenfunc='median', stdfunc='mad_std',
                                   axis=None, masked=False, return_bounds=False )
        sigma = median_abs_deviation( clipped_data, nan_policy='omit', scale='normal' )

        return sigma

    def _mask_spec( self, weighted_cube: 'sdtyping.NpArray3D',
                    mask_limit: float = MASK_LIMIT ) -> Tuple['sdtyping.NpArray1D', float]:
        """
        Calculate the metric with moment_mask method

        Args:
            weighted_cube : Weighted cube
            mask_limit : Threshold to mask the weighted cube data w.r.t. its standartd deviation
        Returns:
            Projected 1-D spectrum
            Standard deviation of the spectrum
        """
        sigma = self._sigma_estimation( weighted_cube,
                                        SIGMA_CLIPPING_THRESHOLD, SIGMA_CLIPPING_MAX_ITERATIONS )
        mask_cube = weighted_cube > mask_limit * sigma

        beamsize_pix = self.beam_size / self.pixel_scale
        ksize = 2 * int( 2.5 * beamsize_pix ) + 1

        x, y = np.indices( ( ksize, ksize ) )
        x0 = y0 = ( ksize - 1 ) / 2.0
        r2 = ( y - y0 ) * ( y - y0 ) + ( x - x0 ) * ( x - x0 )
        s2 = beamsize_pix * beamsize_pix / (8.0 * math.log(2.0))     # FWHM -> sigma conversion
        kbeam = np.exp( -r2 / ( 2 * s2 ) )

        cmask = convolve(input=mask_cube, weights=kbeam[np.newaxis, :], mode='constant')
        image_mask = np.sum( np.array( cmask > .5, dtype=int ), axis=0 )
        masked_spectrum = np.nanmean( weighted_cube * ( image_mask[np.newaxis, :] > 0 ), axis=(1, 2) )
        sigma_mm = self._sigma_estimation( masked_spectrum,
                                           SIGMA_CLIPPING_THRESHOLD, SIGMA_CLIPPING_MAX_ITERATIONS )

        return masked_spectrum, sigma_mm

    def _beam_weight( self, center: Tuple[float, float] ) -> 'sdtyping.NpArray2D':
        """
        Calculate a 2D-gaussian beam for beam weighting

        Args:
            center : Tuple indicating the beam center
        Returns:
            2D gaussian beam with a beam size
        """
        x, y = np.indices( (self.image.ny, self.image.nx) )
        beamsize_pix = self.beam_size / self.pixel_scale
        r2 = ( y - center[1] ) * ( y - center[1] ) + ( x - center[0] ) * ( x - center[0] )
        s2 = beamsize_pix * beamsize_pix / (8.0 * math.log(2.0))     # FWHM -> sigma conversion

        return np.exp( -r2 / ( 2 * s2 ) )

    def _extract_beam_spec( self, center: Tuple[float, float] ) -> 'sdtyping.NpArray1D':
        """
        Project the beam weighted image cube to a 1-D spectrum

        Args:
            center : Tuple indicating the beam center
        Returns:
            projected 1-D spectrum
        """
        beam_weight = self._beam_weight( center )
        cube = self.image.data[:, :, 0, :].transpose(2, 1, 0)
        product = beam_weight[np.newaxis, :, :] * cube

        return np.nanmean( product, axis=(1, 2) ) / np.nanmean( beam_weight )

    def _max_spec( self,
                   weighted_cube: 'sdtyping.NpArray3D',
                   center: Tuple[float, float] | None = None ) -> Tuple['sdtyping.NpArray1D', float]:
        """
        Calculate the metric with single_beam detection

        This method calculates the beam weighted spectrum of a specific position (center) in the weighted cube,
        and determine the standard deviation (sigma) of it.
        If no specific position is spcifid, the method picks the peak in the weighted cube.

        Args:
            center : Tuple indicating the beam center
                     If None is given the peak position of the weighted cube is used.
        Returns
            Projected 1-D spectrum
            Sigma of the spectrum
        """
        if center is None:
            chmax, x0, y0 = np.unravel_index(np.nanargmax(weighted_cube), np.shape(weighted_cube))
        sb = self._extract_beam_spec( ( x0, y0 ) )
        sigma_sb = median_abs_deviation(sb, nan_policy='omit', scale='normal')

        return sb, sigma_sb

    def _detect_over_deviation_threshold( self,
                                          line_ranges: List[List[int]],
                                          linefree_ranges: List[List[int]],
                                          mask_mode: str = 'single_beam',
                                          dev_threshold: float = DEVIATION_THRESHOLD_SINGLE_BEAM,
                                          width_threshold: int = 2 ) -> bool:
        """
        Search for the missed lines and create the diagnostic plot

        Args:
            line_ranges: List of deteced lines in spectral channels
            linefree_ranges: Line-free ranges in spectral channels
            mask_mode: Mask mode
            dev_threshold: Deviation threshold
            width_threshold: Threshold of chunk size in pixels

        Returns:
            True if wide enough missed lines are detected, False if not

        Raises:
            ValueError for unkown mask_mode
        """
        # width_threshold is 2 or more
        width_threshold = max( width_threshold, 2 )

        # calculate the weighted cube
        cube = self.image.data[:, :, 0, :].transpose(2, 1, 0)
        weight = self.weight.data[:, :, 0, :].transpose(2, 1, 0)
        weighted_cube = cube * weight

        if mask_mode == 'single_beam':
            f2, sigma = self._max_spec( weighted_cube )
        elif mask_mode == 'moment_mask':
            f2, sigma = self._mask_spec( weighted_cube )
        else:
            raise ValueError( "Unknown mask_mode {}".format(mask_mode) )

        # deviation/sigma (Z-scores)
        z_all = f2 / sigma

        # deviation/sigma of line-free ranges
        z_linefree = np.full( z_all.shape[0], np.nan )
        for linefree in linefree_ranges:
            z_linefree[ linefree[0]:linefree[1]+1 ] = z_all[ linefree[0]:linefree[1]+1 ]

        # deviation/sigma of line ranges
        z_line = np.full( z_all.shape[0], np.nan )
        for line in line_ranges:
            z_line[ line[0]:line[1]+1 ] = z_all[ line[0]:line[1]+1 ]

        # deviation/sigma of other ranges
        z_other = np.where( np.isnan( z_line ) & np.isnan( z_linefree ), z_all, np.nan )

        # create the stage_dir if needed but does not yet exist
        if self.do_plot:
            stage_dir = os.path.join(self.context.report_dir,
                                     f'stage{self.context.task_counter}')
            os.makedirs( stage_dir, exist_ok=True )

        # find channels which exceed deviation_threshold
        if np.nanmax( z_linefree ) > dev_threshold:
            labeling_half_maximum, n_labels = label(z_linefree > dev_threshold / 2)

            # search for wide 'chunks', which are wider than width_threshold
            for idx in range( 1, n_labels ):
                if np.sum( labeling_half_maximum == idx ) > width_threshold:
                    if self.do_plot:
                        self._plot( stage_dir,
                                    line_ranges,
                                    z_all, z_line, z_linefree, z_other,
                                    dev_threshold, mask_mode )
                    return True
        return False

    def _plot( self,
               stage_dir: str,
               line_ranges: List[List[int]],
               z_all: 'sdtyping.NpArray1D',
               z_line: 'sdtyping.NpArray1D',
               z_linefree: 'sdtyping.NpArray1D',
               z_other: 'sdtyping.NpArray1D',
               dev_threshold: float,
               mask_mode: str ):
        """
        Create the plot to diagnose the missed-lines

        Args:
            stage_dir: Stage directory of weblog
            line_range: List of spectral channels of line ranges
            z_all: deviation/sigma of all frequency channels
            z_line: deviation/sigma of line ranges
            z_linefree: deviation/sigma of line-free ranges
            z_other: deviation/sigma of other ranges
            dev_threshold: Deviation threshold for excess detection
            mask_mode: Mask mode
        Raises:
            ValueError for unkown mask_mode
        """
        # prepare the figure
        fig = figure.Figure()
        ax = fig.add_axes( (0.1, 0.1, 0.85, 0.83) )

        # calculate the boundaries of the frequency bins (to prepare for axes.stairs())
        increment = self.frequency[1] - self.frequency[0]
        frequency_boundaries = [ freq - increment/2.0 for freq in self.frequency ] \
            + [ self.frequency[-1] + increment/2.0 ]

        # draw the full spectrum in gray
        ax.stairs( z_linefree, frequency_boundaries, color='gray', label='continuum range' )

        # overdraw the line part in orange
        ax.stairs( z_line, frequency_boundaries, color='orange', label='line range' )

        # overdraw the other part in magenta (masked range)
        ax.stairs( z_other, frequency_boundaries, color='magenta', label='masked range' )

        # mark the excesses
        z_excess = np.where( z_linefree > dev_threshold, z_linefree, np.nan )
        ax.scatter( self.frequency, z_excess, color='red', marker='.',
                    label='above deviation threshold in\ncontinuum range' )

        # draw the threshold line
        ax.hlines( dev_threshold, np.min(self.frequency), np.max(self.frequency),
                   linestyle='dotted', label='threshold')

        # paint the line ranges
        for line in line_ranges:
            ax.axvspan( self.frequency[line[0]], self.frequency[line[1]], color='cyan', alpha=0.3 )

        # figure parameters
        ax.set_ylim( 1.1 * np.nanmin( z_linefree ),
                     1.1 * np.nanmax( z_linefree ) )
        ax.set_xlabel( 'frequency (GHz) {}'.format(self.frequency_frame) )
        ax.set_ylabel( 'deviation / sigma' )
        ax.tick_params( direction='in' )
        ax.legend( fontsize='small' )
        ax.grid()

        if mask_mode == 'single_beam':
            mode = "at peak"
        elif mask_mode == 'moment_mask':
            mode = "extended"
        else:
            raise ValueError( "Unknown mask_mode {}".format(mask_mode) )

        # figure title
        plot_title = "Field:{} spw:{} ({})".format( self.field_name, self.spwid_list[0], mode )
        ax.set_title( plot_title )

        # save figure to file
        plot_outfile = os.path.join( stage_dir, "{}.missedlines.png".format( self.item.imagename ))
        LOG.info( "Saving diagnistic plot for missed-lines to {}".format(plot_outfile) )
        fig.savefig( plot_outfile )
