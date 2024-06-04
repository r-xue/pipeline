"""
Heuristic for identifying and/or defining the SPW designation, either as a continuum window or for spectral line analysis
"""
from itertools import chain
from typing import Type

import pipeline.infrastructure.logging as logging
from pipeline.domain.spectralwindow import SpectralWindow
from pipeline.domain.measurementset import MeasurementSet
from pipeline.infrastructure.utils.conversion import range_to_list

LOG = logging.get_logger(__name__)


def detect_spectral_lines(mset: Type[MeasurementSet], specline_spws: str='auto') -> None:
    """
    Args:
        mset(object): MeasurementSet object containing MS information
        specline_spws(str): tells pipeline how to handle spectral line assignment; 'auto', 'none', or
            user-defined spectral line spws in CASA format (e.g. '2,3,4~9,23')
    """

    spws = mset.get_all_spectral_windows()
    if specline_spws == 'auto':
        LOG.info("Spectral line detection set to {}.".format(specline_spws))
        for spw in spws:
            auto_detector(spw)
    elif specline_spws == 'none':
        LOG.info("Spectral line assignment is turned off. All spws will be regarded as continuum.")
    else:
        spec_windows = range_to_list(specline_spws)
        LOG.debug('User-defined spectral windows for spectral lines: {}'.format(spec_windows))
        if not all([type(x) == int for x in spec_windows]):
            raise Exception("Invalid input for user-defined spws.")
        LOG.info("Spectral line detection defined by user.")
        LOG.info('The user identified the following spws for spectral line analysis: {}. '
                    'All other spws will be regarded as continuum.'.format(specline_spws))
        for spw in spws:
            if int(spw.id) in spec_windows:
                spw.specline_window = True
            

def auto_detector(spw: Type[SpectralWindow]) -> None:
    """
    Args:
        spw(object): spectral window object stored in pipeline context
    """

    spectral_line_spw = False
    if spw.min_frequency.value < 1000000000.0:
        LOG.info("Frequencies below 1GHz are not currently supported. Regarding spw {} as a continuum window.".format(spw.id))
    else:
        if spw.bandwidth.value < 64000000.0 or spw.num_channels > 128 or (spw.bandwidth.value / spw.num_channels) < 500000.0:
            LOG.info("Spw {} has been identified as a spectral line window.".format(spw.id))
            spectral_line_spw = True
        else:
            LOG.info("Spw {} has been identified as a continuum window.".format(spw.id))
    spw.specline_window = spectral_line_spw
        
