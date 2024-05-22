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


class SpectralLineDetector(object):


    def __init__(self, mset: Type[MeasurementSet], spws: str='auto') -> None:
        """
        Args:
            mset(object): MeasurementSet object containing MS information
            spws(str): tells pipeline how to handle spectral line assignment; 'auto', 'none', or
                       user-defined spectral line spws in CASA format (e.g. '2,3,4~9,23')
        """
        self.mset = mset
        self.spws = spws
    
    def execute(self):
        spws = self.mset.get_all_spectral_windows()
        if self.spws == 'auto':
            LOG.info("Spectral line detection set to {}.".format(self.spws))
            for spw in spws:
                self.auto_detector(spw)
        elif self.spws == 'none':
            LOG.info("Spectral line assignment is turned off. All spws will be regarded as continuum.")
        else:
            LOG.info("Spectral line detection defined by user.")
            spec_windows = self.spws.split(',')
            spec_windows = list(chain.from_iterable([range_to_list(x) if '~' in x else [x] for x in spec_windows]))
            LOG.info('The user identified the following spws for spectral line analysis: {}. '
                     'All other spws will be regarded as contiuum.'.format(self.spws))
            for spw in spws:
                if str(spw.id) in spec_windows:
                    spw.specline_window = True
            

    def auto_detector(self, spw: Type[SpectralWindow]) -> None:
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
        