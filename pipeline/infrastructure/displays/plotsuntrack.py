import math

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

from pipeline.infrastructure.utils.conversion import mjd_seconds_to_datetime

LOG = infrastructure.get_logger(__name__)


def plot_suntrack(vis='', figfile='', elvstime=True):
    """
    Plots the solar az/el during a dataset, with one point per scan.  You
    can specify either the ms or the asdm.  It reads the observatory name
    from the ExecBlock.xml (ASDM) or the OBSERVATION table (ms).
    Returns the name of the figfile.
    figfile can be: True, False, or a string name
    elvstime: False=plot el vs. az;  True=plot el vs. time
          I have implemented lists to hold the times, but not plotted yet.
    -- Todd Hunter
    
    v1.4989
    """

    azimuth = []
    elevation = []
    pointingScans = []
    startmjd = []
    endmjd = []
    mjd = []

    vis = vis.split('/')[-1]

    with casa_tools.MSMDReader(vis) as msmd:
        scannumbers = msmd.scannumbers()
        t = []
        for snumber in scannumbers:
            mytimes = msmd.timesforscan(snumber)
            t.append([np.min(mytimes), np.max(mytimes)])
        firstScan = np.min(scannumbers)
        lastScan = np.max(scannumbers)
        if ('CALIBRATE_POINTING#ON_SOURCE' in msmd.intents()):
            pointingScans = msmd.scansforintent('CALIBRATE_POINTING#ON_SOURCE')
        else:
            pointingScans = []

    obsTable = vis + '/OBSERVATION'
    try:
        with casa_tools.TableReader(obsTable) as table:
            observatory = table.getcell('TELESCOPE_NAME')
    except:
        LOG.warn("Could not open OBSERVATION table to get the telescope name: %s" % obsTable)
        observatory = ''

    for scantime in t:
        az, el = sun(observatory=observatory, mjdsec=np.min(scantime))
        azimuth.append(az)
        elevation.append(el)
        mjd.append(np.min(scantime))
        startmjd.append(np.min(scantime))
        az, el = sun(observatory=observatory, mjdsec=np.max(scantime))
        azimuth.append(az)
        elevation.append(el)
        endmjd.append(np.max(scantime))
        mjd.append(np.max(scantime))

    plt.clf()
    adesc = plt.subplot(111)
    twilight = False
    if (elevation[0]*elevation[-1] < 0):
        twilight = True
    if (twilight):
        color = 'r'
    elif (elevation[0] < 0):  # nighttime
        color = 'k'
    else:  # daytime
        color = 'b'
    azimuthStart = np.array(azimuth)[range(0, len(azimuth), 2)]
    elevationStart = np.array(elevation)[range(0, len(elevation), 2)]
    azimuthEnd = np.array(azimuth)[range(1, len(azimuth), 2)]
    elevationEnd = np.array(elevation)[range(1, len(elevation), 2)]
    if (elvstime):
        list_of_date_times = mjd_seconds_to_datetime(mjd)
        timeplot = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot, elevation, '%s-' % color)
        timeplot = matplotlib.dates.date2num(mjd_seconds_to_datetime(startmjd))
        plt.plot_date(timeplot, elevationStart, '%so' % color)
        timeplot = matplotlib.dates.date2num(mjd_seconds_to_datetime(endmjd))
        plt.plot_date(timeplot, elevationEnd, 'wo', markeredgecolor=color)
        timeplot = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot, elevation, '%s.-' % color)
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=range(0, 60, 30)))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=range(0, 60, 10)))
        adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
        xlims = plt.xlim()
        myxrange = xlims[1]-xlims[0]
        plt.xlim([xlims[0]-0.06*myxrange, xlims[1]+0.06*myxrange])
        RescaleXAxisTimeTicks(plt.xlim(), adesc)
        adesc.xaxis.grid(True, which='major')
    else:
        plt.plot(azimuth, elevation, '%s-' % color)
        plt.plot(azimuthStart, elevationStart, '%so' % color)
        plt.plot(azimuthEnd, elevationEnd, 'wo', markeredgecolor=color)
        plt.plot(azimuth, elevation, '%s.-' % color)

    xlims = plt.xlim()
    plt.ylim([-92, 92])
    ylims = plt.ylim()
    azoff = (xlims[1]-xlims[0])*0.05
    for p in pointingScans:
        if (elvstime == False):
            plt.text(azimuth[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
        else:
            plt.text(timeplot[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
    if (elvstime == False):
        plt.text(azimuth[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        plt.text(azimuth[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    else:
        plt.text(timeplot[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        plt.text(timeplot[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    plt.axhline(0, ls='--', color='k')
    plt.ylabel('Elevation (deg)')
    if (elvstime):
        plt.xlabel('Time (UT on %s)' % (mjd_seconds_to_datetime([mjd[0]])[0].strftime('%Y-%m-%d')))
    else:
        plt.xlabel('Azimuth (deg)')
    plt.yticks(range(-90, 92, 15))
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')
    plt.title('Solar position from %s during %s' % (observatory, vis))

    if len(figfile) < 1:
        if (elvstime):
            figfile = vis + '.sun.elvstime.png'
        else:
            figfile = vis + '.sun.png'
    else:
        suntrack_file = figfile
    plt.savefig(figfile)
    plt.draw()

    LOG.debug("Mean elevation = %f deg" % (np.mean(elevation)))


def sun(observatory='ALMA', mjdsec=0.0):
    """
    Determines the az/el of the Sun for the specified observatory and specified
    time in MJD seconds (or MJD) or date/time string.  
       Any of these formats is valid: 2011/10/15 05:00:00
                                      2011/10/15-05:00:00
                                      2011-10-15 05:00:00
                                      2011-10-15T05:00:00
                                      2011-Oct-15T05:00:00
    If no information is given, it defaults to ALMA and 'now'.
    Returns az, el in degrees.
    Other observatories available:
      ARECIBO  ATCA  BIMA  CLRO  DRAO  DWL  GB  GBT  GMRT  IRAM PDB  IRAM_PDB
      JCMT  MOPRA  MOST  NRAO12M  NRAO_GBT  PKS  SAO SMA  SMA  VLA  VLBA  WSRT
      ATF  ATA  CARMA  ACA  OSF  OVRO_MMA  EVLA  ASKAP  APEX  SMT  NRO  ASTE
      LOFAR  MeerKAT  KAT-7  EVN  LWA1  PAPER_SA  PAPER_GB  e-MERLIN  MERLIN2

    For further help and examples, see https://safe.nrao.edu/wiki/bin/view/ALMA/Sun
    -- Todd Hunter
    """

    mjd = mjdsec/86400.

    me = casa_tools.measures
    qa = casa_tools.quanta

    observatory = me.observatory(observatory)
    obs_long = qa.convert(observatory['m0'], 'deg')['value']
    obs_lat = qa.convert(observatory['m1'], 'deg')['value']

    (az, el) = ComputeSolarAzElLatLong(mjdsec, obs_lat, obs_long)

    return (az, el)


def ComputeSolarAzElLatLong(mjdsec, latitude, longitude):
    """
    Computes the apparent Az,El of the Sun for a specified time and location
    on Earth.  Latitude and longitude must arrive in degrees, with positive
    longitude meaning east of Greenwich.
    -- Todd Hunter
    """
    DEG_TO_RAD = math.pi/180.
    RAD_TO_DEG = 180/math.pi
    HRS_TO_RAD = math.pi/12.
    [RA, Dec] = ComputeSolarRADec(mjdsec)
    LST = ComputeLST(mjdsec, longitude)

    phi = latitude*DEG_TO_RAD
    hourAngle = HRS_TO_RAD*(LST - RA)
    azimuth = RAD_TO_DEG*math.atan2(math.sin(hourAngle), (math.cos(hourAngle) *
                                                          math.sin(phi) - math.tan(Dec*DEG_TO_RAD)*math.cos(phi)))

    # the following is to convert from South=0 (which the French formula uses)
    # to North=0, which is what the rest of the world uses */
    azimuth += 180.0

    if (azimuth > 360.0):
        azimuth -= 360.0
    if (azimuth < 0.0):
        azimuth += 360.0

    argument = math.sin(phi)*math.sin(Dec*DEG_TO_RAD) + math.cos(phi)*math.cos(Dec*DEG_TO_RAD) * math.cos(hourAngle)
    elevation = RAD_TO_DEG*math.asin(argument)

    return ([azimuth, elevation])


def ComputeSolarRADec(mjdsec=None):
    """
    Computes the RA,Dec of the Sun (in hours and degrees) for a specified time 
    (default=now), or for the mean time of a measurement set.  
    apparent: if True, then report apparent position rather than J2000
    approximate: if True, then do not use the CASA measures tool
    See also: au.planet('sun',useJPL=True)
    -- Todd Hunter
    """
    RAD_TO_DEG = 180/math.pi
    RAD_TO_HRS = (1.0/0.2617993877991509)
    #ra,dec = ComputeSolarRADecRadians(mjdsec)

    mydict = planet('sun', useJPL=False, mjd=mjdsec/86400.)
    ra, dec = mydict['directionRadians']

    return(ra*RAD_TO_HRS, dec*RAD_TO_DEG)


def ComputeLST(mjdsec=None, longitude=0, ut=None, hms=False,
               observatory=None, date=None, verbose=False, prec=0, vis=''):

    if verbose:
        print("MJD seconds = ", mjdsec)
    #JD = mjdToJD(mjdsec/86400.)
    MJD = mjdsec/86400.
    JD = MJD + 2400000.5
    T = (JD - 2451545.0) / 36525.0
    sidereal = 280.46061837 + 360.98564736629*(JD - 2451545.0) + 0.000387933*T*T - T*T*T/38710000.

    # now we have LST in Greenwich, need to scale back to site

    sidereal += longitude
    sidereal /= 360.
    sidereal -= np.floor(sidereal)
    sidereal *= 24.0
    if (sidereal < 0):
        sidereal += 24
    if (sidereal >= 24):
        sidereal -= 24

    return(sidereal)


def planet(body='', date='', observatory='ALMA',
           verbose=False, help=False, mjd=None,
           beam='', useJPL=True, standard='Butler-JPL-Horizons 2012', subroutine=False,
           apparent=False, vis='', bodyForScan='', scan='',
           savefig='', showplot=False, antennalist='', frequency=345.0,
           symb=',', timeout=4, asdm='', getIlluminatedFraction=False):

    JPL_HORIZONS_ID = {'ALMA': '-7',
                       'VLA': '-5',
                       'GBT': '-9',
                       'MAUNAKEA': '-80',
                       'OVRO': '-81',
                       'geocentric': '500'
                       }

    foundObservatory = False
    if (type(frequency) == str):
        frequency = float(frequency)
    if (type(observatory) == int):
        observatory = str(observatory)
    elif (type(observatory) == str):
        if (len(observatory) < 1):
            observatory = 'ALMA'
        if (observatory.upper() == 'SMA'):
            observatory = 'MAUNAKEA'
    for n in JPL_HORIZONS_ID:
        if (n.find(observatory) >= 0 or (len(observatory) > 2 and observatory.find(n) >= 0)):
            observatory = JPL_HORIZONS_ID[n]
            if (verbose):
                print("Using observatory: %s = %s" % (n, JPL_HORIZONS_ID[n]))
            foundObservatory = True
            break
    if (body[-3:] == '.ms'):
        print("If you want to specify an ms, use the vis parameter.")
        return
    if (foundObservatory == False):
        if (observatory.lower().find('geocentric') >= 0):
            observatory = 500
        try:
            o = int(observatory)
            key = []
            try:
                key = find_key(JPL_HORIZONS_ID, observatory)
            except:
                if (key == []):
                    print("Using observatory: %s" % (observatory))
                else:
                    print("Using observatory: %s = %s" % (observatory, key))

        except:
            print("Unrecognized observatory = %s" % (observatory))
            print("For a list of codes, see http://ssd.jpl.nasa.gov/horizons.cgi#top")
            return
    #if len(date) < 1 and (vis is None or vis=='') :
    #    date = mjd_seconds_to_datetime([float(mjd)])

    if (len(body) > 0):
        while (body[-1] == ' ' and len(body) > 0):
            body = body[0:-1]
    else:
        print("You must specify body, or vis and bodyForScan")
        return
    data = None

    myme = casa_tools.measures
    myqa = casa_tools.quanta

    myme.doframe(myme.epoch('mjd', myqa.quantity(mjd, 'd')))

    phasedir = myme.direction(body.upper())
    myme.doframe(phasedir)
    mydir = myme.measure(phasedir, 'J2000')

    ra = mydir['m0']['value']
    dec = mydir['m1']['value']

    radec = '%s, %s' % (myqa.formxxx('%.12frad' % ra, format='hms', prec=5),
                           myqa.formxxx('%.12frad' % dec, format='dms', prec=5).replace('.', ':', 2))


    mydict = {'directionRadians': [mydir['m0']['value'], mydir['m1']['value']]}

    return mydict


def convertColonDelimitersToHMSDMS(mystring, s=True, usePeriodsForDeclination=False):
    """
    Converts HH:MM:SS.SSS, +DD:MM:SS.SSS  to  HHhMMmSS.SSSs, +DDdMMmSS.SSSs
          or HH:MM:SS.SSS +DD:MM:SS.SSS   to  HHhMMmSS.SSSs +DDdMMmSS.SSSs
          or HH:MM:SS.SSS, +DD:MM:SS.SSS  to  HHhMMmSS.SSSs, +DD.MM.SS.SSS
          or HH:MM:SS.SSS +DD:MM:SS.SSS   to  HHhMMmSS.SSSs +DD.MM.SS.SSS
    s: whether or not to include the trailing 's' in both axes
    -Todd Hunter
    """
    colons = len(mystring.split(':'))
    if (colons < 5 and (mystring.strip().find(' ') > 0 or mystring.find(',') > 0)):
        print("Insufficient number of colons (%d) to proceed (need 4)" % (colons-1))
        return
    if (usePeriodsForDeclination):
        decdeg = '.'
        decmin = '.'
        decsec = ''
    else:
        decdeg = 'd'
        decmin = 'm'
        decsec = 's'
    if (s):
        outstring = mystring.strip(' ').replace(':', 'h', 1).replace(':', 'm', 1).replace(
            ',', 's,', 1).replace(':', decdeg, 1).replace(':', decmin, 1) + decsec
        if (',' not in mystring):
            outstring = outstring.replace(' ', 's ', 1)
    else:
        outstring = mystring.strip(' ').replace(':', 'h', 1).replace(
            ':', 'm', 1).replace(':', decdeg, 1).replace(':', decmin, 1)
    return(outstring)


def RescaleXAxisTimeTicks(xlim, adesc):
    if xlim[1] - xlim[0] < 10/1440.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 1))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.SecondLocator(bysecond=list(range(0, 60, 30))))
    elif xlim[1] - xlim[0] < 0.5/24.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 5))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 1))))
    elif xlim[1] - xlim[0] < 1/24.:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 2))))
