"""
Utilities used for correcting image center coordinates.
"""
import os
import pyfits
import numpy as np

from .. import casatools
from .. import logging

LOG = logging.get_logger(__name__)

__all__ = ['do_wide_field_pos_cor']

def do_wide_field_pos_cor(fitsname, date_time=None, obs_long=None, obs_lat=None):
    """
    Calculate and apply a mean correction to the central position as a function of mean hour
    angle of observation and mean declination (see PIPE-587, PIPE-700, SRDP-412, and VLASS Memo #14).

    The correction is intended for VLASS-QL images. It is performed as part of the hifv_exportvlassdata task call in
    the VLASS-QL pipeline run and can also be executed outside of the pipeline.

    CRVAL1, CUNIT1, CRVAL2, CUNIT2 and HISTORY keywords are updated *in place* in the input FITS image header.

    Parameters
    ----------
    :param fitsname: name (and path) of FITS file to be processed.
    :type fitsname: string
    :param data_time: Mean date and time of observation, e.g. {'m0': {'unit': 'd', 'value': 58089.83550347222},
                                                              'refer': 'UTC', 'type': 'epoch'}
    :type data_time: dict, casatools.measure.epoch
    :param obs_long: Geographic longitude of observatory, e.g. {'value': -107.6, 'unit': 'deg'}.
    :type obs_long: dict, casatools.quanta.quantity
    :param obs_lat: Geographic latitude of observatory, e.g. {'value': 34.1, 'unit': 'deg'}.
    :type obs_lat: dict, casatools.quanta.quantity

    Example
    -------
    file = "VLASS1.1.ql.T19t20.J155950+333000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits"
    # Mean time of observation
    date_time = pipeline.infrastructure.casatools.measures.epoch('utc', '2017-12-02T20:03:07.500')
    # VLA coordinates
    obs_long = {'unit':'deg','value':-107.6}
    obs_lat = {'unit':'deg','value':34.1}
    do_wide_field_pos_cor(file, date_time=date_time, obs_long=obs_long, obs_lat=obs_lat)
    """
    # Obtain observatory geographic coordinates
    if (obs_long is None) or (obs_lat is None):
        observatory = casatools.measures.observatory('VLA')
        obs_long = observatory['m0']
        obs_lat = observatory['m1']

    if os.path.exists(fitsname):
        # Open FITS image and obtain header
        hdulist = pyfits.open(fitsname, mode='update')
        header = hdulist[0].header

        # Check whether position correction was already applied
        if 'Position correction ' in str(header['history']):
            message = "Positions are already corrected in  {}".format(fitsname)
            try:
                LOG.warn(message)
            except NameError:
                print(message)
        else:
            # Get original coordinates
            crval1, cunit1 = header['crval1'], header['cunit1']
            crval2, cunit2 = header['crval2'], header['cunit2']

            ra_rad = casatools.quanta.convert({'value': crval1, 'unit': cunit1}, 'rad')['value']
            dec_rad = casatools.quanta.convert({'value': crval2, 'unit': cunit2}, 'rad')['value']

            # Mean geographic coordinates of antennas in radians
            obs_long_rad = casatools.quanta.convert(obs_long, 'rad')['value']
            obs_lat_rad = casatools.quanta.convert(obs_lat, 'rad')['value']

            # Mean observing time
            if date_time is None:
                date_obs = header['date-obs']
                timesys = header['timesys']
                date_time = casatools.measures.epoch(timesys, date_obs)

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
            zd = np.arccos(
                np.sin(obs_lat_rad) * np.sin(dec_rad) + np.cos(obs_lat_rad) * np.cos(dec_rad) * np.cos(ha_rad))
            chi = np.arctan(np.sin(ha_rad) / (np.cos(dec_rad) * np.tan(obs_lat_rad) - np.sin(dec_rad) *
                                              np.cos(ha_rad)))
            deltatot = amp * np.tan(zd)

            # Restrict ha_rad to the -np.pi to +np.pi range in order to deal with denominator of parallactic
            # angle term going to zero at declination of arctan(cos(obs_lat_rad)/cos(ha_rad)) and flipping
            # sign of chi: for positive ha_rad, maintain chi positive by adding np.pi if needed, and for
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

            # Apply corrections
            ra_rad_fixed = ra_rad - offset[0] / np.cos(dec_rad)
            dec_rad_fixed = dec_rad - offset[1]

            # Update FITS image header
            header['crval1'] = np.rad2deg(ra_rad_fixed)
            header['cunit1'] = 'deg'
            header['crval2'] = np.rad2deg(dec_rad_fixed)
            header['cunit2'] = 'deg'

            # Update history, "Position correction..." message should remain the last record in list.
            messages = ['Uncorrected CRVAL1 = {:.12E} deg'.format(np.rad2deg(ra_rad)),
                        'Uncorrected CRVAL2 = {:.12E} deg'.format(np.rad2deg(dec_rad)),
                        'Position correction ({:.3E}/cos(CRVAL2), {:.3E}) arcsec applied'.format(
                            np.rad2deg(offset[0]) * -3600.0,
                            np.rad2deg(offset[1]) * -3600.0)]
            for m in messages:
                header.add_history(m)

            # Save changes and inform log
            hdulist.flush()
            try:
                LOG.info("{} to {}".format(messages[-1], fitsname))
            except NameError:
                print("{} to {}".format(messages[-1], fitsname))

        # Close FITS file
        hdulist.close()
    else:
        message = 'Image {} does not exist. No position correction was done.'.format(fitsname)
        try:
            LOG.warn(message)
        except NameError:
            print(message)
    return
