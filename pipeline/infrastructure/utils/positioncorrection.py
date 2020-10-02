"""
Utilities used for correcting image center coordinates.
"""
import os
import pyfits
import numpy as np
from typing import Union, Dict, Tuple

from .. import casatools
from .. import logging

LOG = logging.get_logger(__name__)

__all__ = ['do_wide_field_pos_cor']


def do_wide_field_pos_cor(fitsname: str, date_time: Union[Dict, None] = None,
                          obs_long: Union[Dict[str, Union[str, float]], None] = None,
                          obs_lat: Union[Dict[str, Union[str, float]], None] = None) -> None:
    """Applies mean wide field position correction to FITS WCS in place.

    Apply a mean correction to the FITS WCS reference position as a function
    of mean hour angle of observation and mean declination (see PIPE-587,
    PIPE-700, SRDP-412, and VLASS Memo #14).

    The correction is intended for VLASS-QL images. It is performed as part of
    the hifv_exportvlassdata task call in the VLASS-QL pipeline run. It can also
    be executed outside of the pipeline.

    CRVAL1, CUNIT1, CRVAL2, CUNIT2 and HISTORY keywords are updated *in place*
    in the input FITS image header.

    Args:
        fitsname: name (and path) of FITS file to be processed.
        date_time: Mean date and time of observation in casatools.measure.epoch
            format, if None use DATE-OBS FITS header value.
            e.g. {'m0': {'unit': 'd', 'value': 58089.83550347222},
                'refer': 'UTC', 'type': 'epoch'}
        obs_long: Geographic longitude of observatory, casatools.quanta.quantity
            format. If None, then use VLA coordinate.
            e.g. {'value': -107.6, 'unit': 'deg'}.
        obs_lat: Geographic latitude of observatory, casatools.quanta.quantity
            format. If None, then use VLA coordinate.
            e.g. {'value': 34.1, 'unit': 'deg'}.

    Example:
        >>> file = "VLASS1.1.ql.T19t20.J155950+333000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits"
        Mean time of observation
        >>> datetime = pipeline.infrastructure.casatools.measures.epoch('utc', '2017-12-02T20:03:07.500')
        VLA coordinates
        >>> obslong = {'unit':'deg','value':-107.6}
        >>> obslat = {'unit':'deg','value':34.1}
        >>> do_wide_field_pos_cor(file, date_time=datetime, obs_long=obslong, obs_lat=obslat)
    """
    # Obtain observatory geographic coordinates
    if (obs_long is None) or (obs_lat is None):
        observatory = casatools.measures.observatory('VLA')
        obs_long = observatory['m0']
        obs_lat = observatory['m1']

    if os.path.exists(fitsname):
        # Open FITS image and obtain header
        with pyfits.open(fitsname, mode='update') as hdulist:
            header = hdulist[0].header

            # Check whether position correction was already applied
            if 'Position correction ' in str(header['history']):
                message = "Positions are already corrected in  {}".format(fitsname)
                try:
                    LOG.warn(message)
                except NameError:
                    print(message)
                return None

            # Get original coordinates
            ra_head = {'unit': header['cunit1'], 'value': header['crval1']}
            dec_head = {'unit': header['cunit2'], 'value': header['crval2']}

            # Mean observing time
            if date_time is None:
                date_obs = header['date-obs']
                timesys = header['timesys']
                date_time = casatools.measures.epoch(timesys, date_obs)

            # Compute correction
            offset = calc_wide_field_pos_cor(ra=ra_head, dec=dec_head, obs_long=obs_long,
                                             obs_lat=obs_lat, date_time=date_time)

            # Apply corrections
            ra_rad_fixed = casatools.quanta.sub(
                ra_head, casatools.quanta.div(offset[0], casatools.quanta.cos(dec_head)))
            dec_rad_fixed = casatools.quanta.sub(dec_head, offset[1])

            # Update FITS image header, use degrees
            header['crval1'] = casatools.quanta.convert(ra_rad_fixed, 'deg')['value']
            header['cunit1'] = 'deg'
            header['crval2'] = casatools.quanta.convert(dec_rad_fixed, 'deg')['value']
            header['cunit2'] = 'deg'

            # Update history, "Position correction..." message should remain the last record in list.
            messages = ['Uncorrected CRVAL1 = {:.12E} deg'.format(casatools.quanta.convert(ra_head, 'deg')['value']),
                        'Uncorrected CRVAL2 = {:.12E} deg'.format(casatools.quanta.convert(dec_head, 'deg')['value']),
                        'Position correction ({:.3E}/cos(CRVAL2), {:.3E}) arcsec applied'.format(
                            casatools.quanta.convert(offset[0], 'arcsec')['value'] * -1.0,
                            casatools.quanta.convert(offset[1], 'arcsec')['value'] * -1.0)]
            for m in messages:
                header.add_history(m)

            # Save changes and inform log
            hdulist.flush()
            try:
                LOG.info("{} to {}".format(messages[-1], fitsname))
            except NameError:
                print("{} to {}".format(messages[-1], fitsname))
    else:
        message = 'Image {} does not exist. No position correction was done.'.format(fitsname)
        try:
            LOG.warn(message)
        except NameError:
            print(message)

    return None


def calc_wide_field_pos_cor(ra: Dict, dec: Dict, obs_long: Dict, obs_lat: Dict,
                            date_time: Dict) -> Tuple[Dict, Dict]:
    """Computes the wide field position correction.

    Args:
        ra: Uncorrected Right Ascension.
        dec: Uncorrected Declination.
        obs_long: Geographic longitude of observatory.
        obs_lat: Geographic latitude of observatory.
        date_time: Date and time of observation.

    The arguments are all in casatools.quanta format (dictionary containing
    value (float) and unit (str)). The function internally uses radian units for
    computation. The arguments may have any convertible units.

    Returns:
        A tuple containing RA and Dec offsets with units (in radians).
    """
    # Get original coordinates in radians
    ra_rad = casatools.quanta.convert(ra, 'rad')['value']
    dec_rad = casatools.quanta.convert(dec, 'rad')['value']

    # Mean geographic coordinates of antennas in radians
    obs_long_rad = casatools.quanta.convert(obs_long, 'rad')['value']
    obs_lat_rad = casatools.quanta.convert(obs_lat, 'rad')['value']

    # Greenwich Mean Sidereal Time
    GMST = casatools.measures.measure(date_time, 'GMST1')

    # Local Sidereal Time
    LST = casatools.quanta.convert(GMST['m0'], 'h')['value'] % 24.0 + np.rad2deg(obs_long_rad) / 15.0
    if LST < 0:
        LST = LST + 24
    LST_rad = np.deg2rad(LST * 15)  # in radians

    # Hour angle (in radians)
    ha_rad = LST_rad - ra_rad
    if ha_rad < 0.0:
        ha_rad = ha_rad + 2.0 * np.pi

    # Compute correction
    amp = np.deg2rad(0.25 / 3600.0)
    offset = np.zeros(2)
    zd = np.arccos(np.sin(obs_lat_rad) * np.sin(dec_rad) + np.cos(obs_lat_rad)
                   * np.cos(dec_rad) * np.cos(ha_rad))
    chi = np.arctan(np.sin(ha_rad) / (np.cos(dec_rad) * np.tan(obs_lat_rad) -
                                      np.sin(dec_rad) * np.cos(ha_rad)))
    deltatot = amp * np.tan(zd)

    # Restrict ha_rad to the -np.pi to +np.pi range in order to deal with
    # denominator of parallactic angle term going to zero at declination of
    # arctan(cos(obs_lat_rad)/cos(ha_rad)) and flipping sign of chi: for
    # positive ha_rad, maintain chi positive by adding np.pi if needed, and for
    # negative ha_rad, subtract np.pi to keep chi negative.
    if ha_rad > np.pi:
        ha_rad = ha_rad - 2.0 * np.pi
    if (ha_rad < 0.0) and (chi > 0.0):
        chi = chi - np.pi
    elif (ha_rad > 0.0) and (chi < 0.0):
        chi = chi + np.pi

    # Offset values
    offset[0] = deltatot * np.sin(chi)
    offset[1] = deltatot * np.cos(chi)

    return ({'value': offset[0], 'unit': 'rad'},
            {'value': offset[1], 'unit': 'rad'})
