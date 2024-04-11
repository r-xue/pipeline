"""
Heuristic for identifying and/or defining SPW intent, either as a continuum window or for spectral line analysis
"""
from itertools import chain
from typing import Type

import pipeline.infrastructure.logging as logging
from pipeline.domain.spectralwindow import SpectralWindow
from pipeline.domain.measurementset import MeasurementSet

LOG = logging.get_logger(__name__)


def casa_ranges(rng):
    low, high = rng.split('~')
    return [str(x) for x in range(int(low), int(high) + 1)]


class SpectralLineDetector(object):


    def __init__(self, mset: Type[MeasurementSet], auto: str='none', user_windows: str=None) -> None:
        """
        Args:
            mset(object): MeasurementSet object containing MS information
            auto(str): tells pipeline how to handle spectral line assignment; 'auto', 'none', or 'user'
            user_windows(str): user-defined spectral line spws in CASA format (e.g. '2,3,4~9,23')
        """
        self.mset = mset
        self.auto = auto
        self.user_windows = user_windows
    
    def execute(self):
        LOG.info("Spectral line detection set to {}.".format(self.auto))
        spws = self.mset.get_all_spectral_windows()
        if self.auto == 'auto':
            for spw in spws:
                self.auto_detector(spw)
        elif self.auto == 'user':
            if not self.user_windows:
                LOG.error("User-defined spectral window assignment selected without providing spw definitions.")
                raise Exception("User-defined spectral window assignment selected without providing spw definitions.")
            spec_windows = self.user_windows.split(',')
            spec_windows = list(chain.from_iterable([casa_ranges(x) if '~' in x else [x] for x in spec_windows]))
            LOG.info('The user identified the following spws for spectral line analysis: {}'.format(self.user_windows))
            for spw in spws:
                if str(spw.id) in spec_windows:
                    spw.specline_window = True
        else:
            LOG.info("Spectral line assignment is turned off. All spws will be regarded as continuum.")
            

    def auto_detector(self, spw: Type[SpectralWindow]) -> None:
        """
        Args:
            spw(object): spectral window object stored in pipeline context
        """

        spw_type = False
        if spw.min_frequency.value < 1000000000.0:
            LOG.info("Frequencies below 1GHz are not currently supported. Regarding spw {} as a continuum window.".format(spw.id))
        else:
            if spw.bandwidth.value < 64000000.0 or spw.num_channels > 128 or (spw.bandwidth.value / spw.num_channels) < 500000.0:
                LOG.info("Spw {} has been identified as a spectral line window.".format(spw.id))
                spw_type = True
            else:
                LOG.info("Spw {} has been identified as a continuum window.".format(spw.id))
        spw.specline_window = spw_type
        