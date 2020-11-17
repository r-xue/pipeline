"""
Utility module to calculate the atmospheric transmission.

Examples:
    Calculate using metadata in MeasurementSet
    >>> (freq, trans) = atmutil.get_transmission('M100.ms', antenna_id=1, spw_id=18, doplot=True)
"""
import math
import os
from typing import Union, Tuple

import numpy as np
import pylab as pl

import casatools

import pipeline.extern.adopted as adopted


class AtmType(object):
    """Atmosphere type enum class."""
    
    tropical = 1
    midLatitudeSummer = 2
    midLatitudeWinter = 3
    subarcticSummer = 4
    subarcticWinter = 5


def init_at(at: casatools.atmosphere, humidity: float=20.0,
            temperature: float=270.0, pressure: float=560.0,
            atmtype: AtmType=AtmType.midLatitudeWinter, altitude: float=5000.0,
            fcenter: float=100.0, nchan: float=4096, resolution: float=0.001):
    """
    Initialize atmospheric profile and spectral window setting.
     
    Initialize atmospheric profile and spectral window setting in CASA
    atmosphere tool using input antenna site parameters and spectral window
    frequencies.

    Args:
        at: CASA atmosphere tool instance to initialize.
        humidity: The relative humidity at the ground (unit: %).
        temperature: The temperature at the ground (unit: K).
        pressure: The pressure at the ground (unit: mbar).
        atmtype: An AtmType enum that defines a type of atmospheric profile.
        altitude: The altitude of antenna site to calculate atmospheric
            transmission (unit: m).
        fcenter: The center frequency of spectral window (unit: GHz).
        nchan: The number of channels in spectral window.
        resolution: The channel width of spectral window (unit: GHz).
    """
    myqa = casatools.quanta()
    at.initAtmProfile(humidity=humidity,
                      temperature=myqa.quantity(temperature, 'K'),
                      altitude=myqa.quantity(altitude, 'm'),
                      pressure=myqa.quantity(pressure, 'mbar'),
                      atmType=atmtype)
    fwidth = nchan * resolution
    at.initSpectralWindow(nbands=1,
                          fCenter=myqa.quantity(fcenter, 'GHz'),
                          fWidth=myqa.quantity(fwidth, 'GHz'),
                          fRes=myqa.quantity(resolution, 'GHz'))


def calc_airmass(elevation: float=45.0) -> float:
    """
    Calculate the relative airmass of a given elevation angle.
    
    Args:
        elevation: An angle of elevation (unit: degree).

    Returns:
        The relative airmass to the one at zenith.
    """
    return 1.0 / math.cos((90.0 - elevation) * math.pi / 180.) 


def calc_transmission(airmass: float, dry_opacity: Union[float, np.ndarray],
                      wet_opacity: Union[float, np.ndarray]) -> Union[float, np.ndarray]:
    """
    Calculate total atmospheric transmission.
    
    Calculate total atmospheric transmission from the zenith opacities and
    relative airmass.
    
    Args:
        arimass: The relative airmass to the zenith one.
        dry_opacity: The integrated zenith dry opacity.
        wet_opacity: The integrated zenith wet opacity.
    
    Returns:
        The atmospheric transmission.
    """
    return np.exp(-airmass * (dry_opacity + wet_opacity))


def get_dry_opacity(at: casatools.atmosphere) -> np.ndarray:
    """
    Obtain the integrated zenith opacity of dry species.
    
    Args:
        at: Atmosphere tool instance initialized by a spectral window and site
            parameter settings.
    
    Returns:
        An array of the zenith integrated opacity of dry species for each
        channel of the first spectral window.
    """
    dry_opacity_result = at.getDryOpacitySpec(0)
    dry_opacity = np.asarray(dry_opacity_result[1])
    return dry_opacity


def get_wet_opacity(at: casatools.atmosphere) -> np.ndarray:
    """
    Obtain the integrated zenith opacity of wet species.
    
    Args:
        at: Atmosphere tool instance initialized by a spectral window and site
            parameter settings.
    
    Returns:
        An array of the zenith integrated opacity of wet species for each
        channel of the first spectral window.
    """
    wet_opacity_result = at.getWetOpacitySpec(0)
    wet_opacity = np.asarray(wet_opacity_result[1]['value'])
    return wet_opacity


def test(pwv: float=1.0, elevation: float=45.0) -> np.ndarray:
    """
    Calculate atmospheric transmission and generate a plot.
    
    Calculate atmospheric transmission of a given PWV and elevation angle.
    The default parameter values of init_at function are used to initialize
    atmospheric and spectral window settings. A plot of atmospheric
    transmission, wet and dry zenith opacities of each channel of the spectral
    window is also generated.
    
    Args:
        pwv: The zenith water vapor column for forward radiative transfer
            calculation (unit: mm).
        elevation: The angle of elevation (unit: degree).
    
    Returns:
        An array of atmospheric transmission of each channel of the spectral window.
    """
    myat = casatools.atmosphere()
    myqa = casatools.quanta()
    init_at(myat)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

    airmass = calc_airmass(elevation)

    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)

    plot(dry_opacity, wet_opacity, transmission)

    return transmission


def plot(frequency: np.ndarray, dry_opacity: np.ndarray,
         wet_opacity: np.ndarray, transmission: np.ndarray):
    """
    Generate a plot of atmospheric transmission, wet and dry opacities.
    
    Generate a twin axes plot of atmospheric transmission, wet and dry
    opacities by matplotlib.
    
    Args:
        frequency: An array of frequency values.
        dry_opacity: The integrated dry opacity at each frequency.
        wet_opacity: The integrated wet opacity at each frequency.
        transmission: The atmospheric transmission at each frequency.
    """
    pl.clf()
    a1 = pl.gcf().gca()
    pl.plot(frequency, dry_opacity, label='dry')
    pl.plot(frequency, wet_opacity, label='wet')
    pl.legend(loc='upper left', bbox_to_anchor=(0., 0.5))
    a2 = a1.twinx()
    a2.yaxis.set_major_formatter(pl.NullFormatter())
    a2.yaxis.set_major_locator(pl.NullLocator())
    pl.gcf().sca(a2)
    pl.plot(frequency, transmission, 'm-')
    M = transmission.min()
    Y = 0.8
    ymin = (M - Y) / (1.0 - Y)
    ymax = transmission.max() + (1.0 - transmission.max()) * 0.1
    pl.ylim([ymin, ymax])


def get_spw_spec(vis: str, spw_id: int) -> Tuple[float, int, float]:
    """
    Calculate spectral setting of a spectral window.
    
    Calculate the center frequency, number of channels, and channel resolution
    of a spectral windown in a MeasurementSet. The values can be passed to
    init_at function to initialize spectral window setting in atmosphere tool.
    
    Args:
        vis: Path to MeasurementSet.
        spw_id: A spectral window ID to select.

    Returns:
        A three element tuple of the center frequency in GHz, number of
        channels, and resolution in GHz.
    """
    mytb = casatools.table()
    mytb.open(os.path.join(vis, 'SPECTRAL_WINDOW'))
    nrow = mytb.nrows()
    if spw_id < 0 or spw_id >= nrow:
        raise RuntimeError('spw_id {} is out of range'.format(spw_id))
    try:
        nchan = mytb.getcell('NUM_CHAN', spw_id)
        chan_freq = mytb.getcell('CHAN_FREQ', spw_id)
    finally:
        mytb.close()

    center_freq = (chan_freq.min() + chan_freq.max()) / 2.0
    resolution = chan_freq[1] - chan_freq[0]

    # Hz -> GHz
    toGHz = 1.0e-9
    center_freq *= toGHz
    resolution *= toGHz

    return center_freq, nchan, resolution


def get_median_elevation(vis: str, antenna_id: int) -> float:
    """
    Calculate the median elevation of pointing directions of an antenna.
    
    Calculate the median elevation of pointing directions of an antenna in a
    MeasurementSet. The pointing directions are obtained from the DIRECTION
    column in POINTING subtable. Only supports DIRECTION column in AZELGEO
    coordinate frame and the unit in radian.
    
    Args:
        vis: Path to MeasurementSet.
        antenna_id: The antenna ID to select.
    
    Returns:
        The median of elevation of selected antenna (unit: degree).
    
    Raises:
        RuntimeError: An error when DIRECTION column has unsupported coodinate
            frame or unit. 
    """
    mytb = casatools.table()
    mytb.open(os.path.join(vis, 'POINTING'))
    tsel = mytb.query('ANTENNA_ID == {}'.format(antenna_id))
    # default elevation
    elevation = 45.0
    try:
        if tsel.nrows() > 0:
            colkeywords = tsel.getcolkeywords('DIRECTION')
            if colkeywords['MEASINFO']['Ref'] != 'AZELGEO':
                raise RuntimeError('The coordinate frame must be AZELGEO. Got {}'.format(colkeywords['MEASINFO']['Ref']))
            elif not (colkeywords['QuantumUnits'] == 'rad').all():
                raise RuntimeError('The unit must be radian. Got {}'.format(str(colkeywords['QuantumUnits'])))
            elevation_list = tsel.getcol('DIRECTION')[1][0]
            elevation = np.median(elevation_list) * 180.0 / math.pi
    finally:
        tsel.close()
        mytb.close()

    return elevation        


def get_transmission(vis: str, antenna_id: int=0, spw_id: int=0,
                     doplot: bool=False) -> Tuple[np.ndarray, np.ndarray]:
    """
    Calculate atmospheric transmission of an antenna and spectral window.
    
    Caldulate the atmospheric transmission of a given spectral window for an
    elevation angle corresponding to pointings of a given antenna in a
    MeasurementSet.
    The median of elevations in all pointings of the selected antenna is used
    in calulation. The atmospheric profile is constructed by default site
    parameters of the function, init_at. The median of zenith water vapor
    column (pwv) is used to calculate the transmission.

    Args:
        vis: Path to MeasurementSet.
        spw_id: A spectral window ID to select.
        antenna_id: The antenna ID to select.
        doplot: If True, plot the atmospheric transmission and opacities.

    Returns:
        A tuple of 2 arrays. The first one is an array of frequencies in the
        unit of GHz, and the other is the atmospheric transmission at each
        frequency.
    """
    center_freq, nchan, resolution = get_spw_spec(vis, spw_id)
    elevation = get_median_elevation(vis, antenna_id)

    # set pwv to 1.0 
    #pwv = 1.0
    # get median PWV using Todd's script
    (pwv, pwvmad) = adopted.getMedianPWV(vis=vis)

    myat = casatools.atmosphere()
    myqa = casatools.quanta()
    init_at(myat, fcenter=center_freq, nchan=nchan, resolution=resolution)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

    airmass = calc_airmass(elevation)

    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)
    frequency = myqa.convert(myat.getSpectralWindow(0), "GHz")['value']

    if doplot:
        plot(frequency, dry_opacity, wet_opacity, transmission)

    return frequency, transmission
