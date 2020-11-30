import math
import os

import numpy
import matplotlib.pyplot as plt

import pipeline.extern.adopted as adopted
from pipeline.infrastructure import casatools


class AtmType(object):
    tropical = 1
    midLatitudeSummer = 2
    midLatitudeWinter = 3
    subarcticSummer = 4
    subarcticWinter = 5


def init_at(at, humidity=20.0, temperature=270.0, pressure=560.0,
            atmtype=AtmType.midLatitudeWinter, altitude=5000.0,
            fcenter=100.0, nchan=4096, resolution=0.001):
    """
    at: atmosphere tool
    humidity: relative humidity [%]
    temperature: temperature [K]
    pressure: pressure [mbar]
    atmtype: AtmType enum
    altitude: altitude [m]
    fcenter: center frequency [GHz]
    nchan: number of channels
    resolution: channel width [GHz]
    """
    myqa = casatools.quanta
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


def calc_airmass(elevation=45.0):
    """
    elevation: elevation [deg]
    """
    return 1.0 / math.cos((90.0 - elevation) * math.pi / 180.) 


def calc_transmission(airmass, dry_opacity, wet_opacity):
    """
    """
    return numpy.exp(-airmass * (dry_opacity + wet_opacity))


def get_dry_opacity(at):
    """
    at: atmosphere tool
    """
    dry_opacity_result = at.getDryOpacitySpec(0)
    dry_opacity = numpy.asarray(dry_opacity_result[1])
    return dry_opacity


def get_wet_opacity(at):
    """
    at: atmosphere tool
    """
    wet_opacity_result = at.getWetOpacitySpec(0)
    wet_opacity = numpy.asarray(wet_opacity_result[1]['value'])
    return wet_opacity


def test(pwv=1.0, elevation=45.0):
    """
    pwv: water vapor content [mm]
    elevation: elevation [deg]
    """
    myat = casatools.atmosphere
    myqa = casatools.quanta
    init_at(myat)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

    airmass = calc_airmass(elevation)

    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)

    plot(dry_opacity, wet_opacity, transmission)

    return transmission


def plot(frequency, dry_opacity, wet_opacity, transmission):
    plt.clf()
    a1 = plt.gcf().gca()
    plt.plot(frequency, dry_opacity, label='dry')
    plt.plot(frequency, wet_opacity, label='wet')
    plt.legend(loc='upper left', bbox_to_anchor=(0., 0.5))
    a2 = a1.twinx()
    a2.yaxis.set_major_formatter(plt.NullFormatter())
    a2.yaxis.set_major_locator(plt.NullLocator())
    plt.gcf().sca(a2)
    plt.plot(frequency, transmission, 'm-')
    M = transmission.min()
    Y = 0.8
    ymin = (M - Y) / (1.0 - Y)
    ymax = transmission.max() + (1.0 - transmission.max()) * 0.1
    plt.ylim([ymin, ymax])


def get_spw_spec(vis, spw_id):
    """
    vis: MS name
    spw_id: spw id

    return: center frequency [GHz], number of channels, and resolution [GHz]
    """
    with casatools.TableReader(os.path.join(vis, 'SPECTRAL_WINDOW')) as mytb:
        nrow = mytb.nrows()
        if spw_id < 0 or spw_id >= nrow:
            raise RuntimeError('spw_id {} is out of range'.format(spw_id))
        nchan = mytb.getcell('NUM_CHAN', spw_id)
        chan_freq = mytb.getcell('CHAN_FREQ', spw_id)

    center_freq = (chan_freq.min() + chan_freq.max()) / 2.0
    resolution = chan_freq[1] - chan_freq[0]

    # Hz -> GHz
    toGHz = 1.0e-9
    center_freq *= toGHz
    resolution *= toGHz

    return center_freq, nchan, resolution


def get_median_elevation(vis, antenna_id):
    with casatools.TableReader(os.path.join(vis, 'POINTING')) as mytb:
        tsel = mytb.query('ANTENNA_ID == {}'.format(antenna_id))
        # default elevation
        elevation = 45.0
        try:
            if tsel.nrows() > 0:
                colkeywords = tsel.getcolkeywords('DIRECTION')
                if colkeywords['MEASINFO']['Ref'] == 'AZELGEO':
                    elevation_list = tsel.getcol('DIRECTION')[1][0]
                    elevation = numpy.median(elevation_list) * 180.0 / math.pi
        finally:
            tsel.close()

    return elevation        


def get_transmission(vis, antenna_id=0, spw_id=0, doplot=False):
    """
    calculate atmospheric transmission assuming PWV=1mm.

    vis -- MS name
    antenna_id -- antenna ID
    spw_id -- spw ID

    Returns:
        (frequency array [GHz], atm transmission)
    """
    center_freq, nchan, resolution = get_spw_spec(vis, spw_id)
    elevation = get_median_elevation(vis, antenna_id)

    # set pwv to 1.0 
    #pwv = 1.0
    # get median PWV using Todd's script
    (pwv, pwvmad) = adopted.getMedianPWV(vis=vis)

    myat = casatools.atmosphere
    myqa = casatools.quanta
    init_at(myat, fcenter=center_freq, nchan=nchan, resolution=resolution)
    myat.setUserWH2O(myqa.quantity(pwv, 'mm'))

    airmass = calc_airmass(elevation)

    dry_opacity = get_dry_opacity(myat)
    wet_opacity = get_wet_opacity(myat)
    transmission = calc_transmission(airmass, dry_opacity, wet_opacity)
    #frequency = numpy.fromiter((center_freq + (float(i) - 0.5 * nchan) * resolution for i in xrange(nchan)), dtype=numpy.float64)
    frequency = myqa.convert(myat.getSpectralWindow(0), "GHz")['value']

    if doplot:
        plot(frequency, dry_opacity, wet_opacity, transmission)

    return frequency, transmission
