
def plotSunDuringTrack(ms='', asdm='', plotfile='', figfiledir='', elvstime=False):
    """
    Plots the solar az/el during a dataset, with one point per scan.  You
    can specify either the ms or the asdm.  It reads the observatory name
    from the ExecBlock.xml (ASDM) or the OBSERVATION table (ms).
    Returns the name of the plotfile.
    plotfile can be: True, False, or a string name
    elvstime: False=plot el vs. az;  True=plot el vs. time
          I have implemented lists to hold the times, but not plotted yet.
    -- Todd Hunter
    
    v1.4989
    """
    if (ms == '' and asdm == ''):
        print("Need to specify either an ms or an asdm")
        return
    azimuth = []
    elevation = []
    pointingScans = []
    startmjd = []
    endmjd = []
    mjd = []
    if (asdm != ''):
        if (os.path.exists(asdm) == False):
            print("ASDM does not exist: %s" % (asdm))
            return
        track = os.path.basename(asdm)
        if (track == ''):
            track = os.path.basename(asdm[:-1])  # in case name ends in '/'
        observatory = getObservatoryNameFromASDM(asdm)
        scandict, sourcedict = readscans(asdm)
        for scan in scandict:
#            print "processing scan ", scan
            az,el = sun(observatory=observatory, mjd=dateStringToMJD(scandict[scan]['start'], verbose=False))
            azimuth.append(az)
            elevation.append(el)
            startmjd.append(scandict[scan]['startmjd']*86400)
            mjd.append(scandict[scan]['startmjd']*86400)
            az,el = sun(observatory=observatory, mjd=dateStringToMJD(scandict[scan]['end'], verbose=False))
            azimuth.append(az)
            elevation.append(el)
            endmjd.append(scandict[scan]['endmjd']*86400)
            mjd.append(scandict[scan]['endmjd']*86400)
            if (scandict[scan]['intent'].find('CALIBRATE_POINTING') >= 0):
                pointingScans.append(scan)
        firstScan = np.min(list(scandict.keys()))
        lastScan = np.max(list(scandict.keys()))
    else:
        track = os.path.basename(ms)
        if (casaVersion >= casaVersionWithMSMD):
            mymsmd = createCasaTool(msmdtool)
            mymsmd.open(ms)
            scannumbers = mymsmd.scannumbers()
            t = []
            for snumber in scannumbers:
                mytimes = mymsmd.timesforscan(snumber)
                t.append([np.min(mytimes), np.max(mytimes)])
            firstScan = np.min(scannumbers)
            lastScan = np.max(scannumbers)
            if ('CALIBRATE_POINTING#ON_SOURCE' in mymsmd.intents()):
                pointingScans = mymsmd.scansforintent('CALIBRATE_POINTING#ON_SOURCE')
            else:
                pointingScans = []
        else:
            print("Running ValueMapping because this is a pre-4.1 version of casa")
            vm = ValueMapping(ms)
            t = vm.getTimesForScans(vm.uniqueScans)
            firstScan = np.min(vm.uniqueScans)
            lastScan = np.max(vm.uniqueScans)
            pointingScans = getScansForIntentFast(vm,vm.uniqueScans,'CALIBRATE_POINTING#ON_SOURCE')
        observatory = getObservatoryName(ms)
        for scantime in t:
            az,el = sun(observatory=observatory, mjdsec=np.min(scantime))
            azimuth.append(az)
            elevation.append(el)
            mjd.append(np.min(scantime))
            startmjd.append(np.min(scantime))
            az,el = sun(observatory=observatory, mjdsec=np.max(scantime))
            azimuth.append(az)
            elevation.append(el)
            endmjd.append(np.max(scantime))
            mjd.append(np.max(scantime))
    pb.clf()
    adesc = pb.subplot(111)
    twilight = False
    if (elevation[0]*elevation[-1] < 0):
        twilight = True
    if (twilight):
        color = 'r'
    elif (elevation[0] < 0): # nighttime
        color = 'k'
    else:  # daytime
        color = 'b'
    azimuthStart = np.array(azimuth)[range(0,len(azimuth),2)]
    elevationStart = np.array(elevation)[range(0,len(elevation),2)]
    azimuthEnd = np.array(azimuth)[range(1,len(azimuth),2)]
    elevationEnd = np.array(elevation)[range(1,len(elevation),2)]
    if (elvstime):
        list_of_date_times = mjdSecondsListToDateTime(mjd)
        timeplot = pb.date2num(list_of_date_times)
        pb.plot_date(timeplot, elevation, '%s-'%color)
        timeplot = pb.date2num(mjdSecondsListToDateTime(startmjd))
        pb.plot_date(timeplot, elevationStart, '%so'%color)
        timeplot = pb.date2num(mjdSecondsListToDateTime(endmjd))
        pb.plot_date(timeplot, elevationEnd, 'wo', markeredgecolor=color)
        timeplot = pb.date2num(list_of_date_times)
        pb.plot_date(timeplot, elevation, '%s.-'%color)
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=range(0,60,30)))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=range(0,60,10)))
        adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
        xlims = pb.xlim()
        myxrange = xlims[1]-xlims[0]
        pb.xlim([xlims[0]-0.06*myxrange, xlims[1]+0.06*myxrange])
        RescaleXAxisTimeTicks(pb.xlim(), adesc)
        adesc.xaxis.grid(True,which='major')
    else:
        pb.plot(azimuth, elevation, '%s-'%color)
        pb.plot(azimuthStart, elevationStart, '%so'%color)
        pb.plot(azimuthEnd, elevationEnd, 'wo', markeredgecolor=color)
        pb.plot(azimuth, elevation, '%s.-'%color)
        
    xlims = pb.xlim()
    pb.ylim([-92,92])
    ylims = pb.ylim()
    azoff = (xlims[1]-xlims[0])*0.05
    for p in pointingScans:
        if (elvstime==False):
            pb.text(azimuth[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
        else:
            pb.text(timeplot[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
    if (elvstime==False):
        pb.text(azimuth[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        pb.text(azimuth[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    else:
        pb.text(timeplot[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        pb.text(timeplot[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    pb.axhline(0, ls='--', color='k')
    pb.ylabel('Elevation (deg)')
    if (elvstime):
        pb.xlabel('Time (UT on %s)' % (mjdsecToUT(mjd[0]).split()[0]))
    else:
        pb.xlabel('Azimuth (deg)')
    pb.yticks(range(-90,92,15))
    adesc.xaxis.grid(True,which='major')
    adesc.yaxis.grid(True,which='major')
    pb.title('Solar position from %s during %s' % (observatory,track))
    if (plotfile != ''):
        if (plotfile == True):
            if (elvstime):
                plotfile = track + '.sun.elvstime.png'
            else:
                plotfile = track + '.sun.png'
        if (figfiledir != ''):
            if (not os.path.exists(figfiledir)):
                os.makedirs(figfiledir)
            plotfile = os.path.join(figfiledir,plotfile)
        mydir = os.path.dirname(plotfile)
        if mydir != '':
            checkdir = mydir
        else:
            checkdir = '.'
        if not os.access(checkdir,os.W_OK):
            print("Cannot write to this directory, writing to /tmp instead.")
            plotfile = os.path.join('/tmp',os.path.basename(plotfile))
        pb.savefig(plotfile)
        print("Wrote plot = ", plotfile)
    pb.draw()
    print("Mean elevation = %f deg" % (np.mean(elevation)))
    if (plotfile == ''):
        return(np.mean(elevation))
    else:
        return(plotfile)