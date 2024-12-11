import os
import pickle

from pipeline.infrastructure import casa_tools
import numpy as np

#This file contains functions that applycalqa used to borrow from AnalysisUtils, but given how  unreliable that one huge file is,
#the very few functions we are actually using were moved here

#List of SSO objects
SSOfieldnames = ['Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa']


def getSpwList(msmd,intent='OBSERVE_TARGET#ON_SOURCE',tdm=True,fdm=True, sqld=False):
    spws = msmd.spwsforintent(intent)
    almaspws = msmd.almaspws(tdm=tdm,fdm=fdm,sqld=sqld)
    scienceSpws = np.intersect1d(spws,almaspws)
    return(list(scienceSpws))


def onlineChannelAveraging(msmd, spws=None):
    """
    For Cycle 3-onward data, determines the channel averaging factor from
    the ratio of the effective channel bandwidth to the channel width.
    spw: a single value, or a list; if Nonne, then uses science spws
    Returns: single value for a single spw, or a list for a list of spws
    -Todd Hunter
    """
    hanning_effBw = {1: 2.667, 2: 3.200, 4: 4.923, 8: 8.828, 16: 16.787}
    if spws is None:
        spws = getSpwList(msmd)
    if type(spws) != list:
        spws = [spws]
    Ns = list(hanning_effBw.keys())
    ratios = [hanning_effBw[i]/i for i in Ns]
    Nvalues = []
    for spw in spws:
        chanwidths = msmd.chanwidths(spw)
        nchan = len(chanwidths)
        if (nchan < 5):
            return 1
        chanwidth = abs(chanwidths[0])
        chaneffwidth = msmd.chaneffbws(spw)[0]
        ratio = chaneffwidth/chanwidth
        Nvalues.append(Ns[np.argmin(abs(ratios - ratio))])
    if (len(spws) == 1):
        return Nvalues[0]
    else:
        return Nvalues

def getSpecSetup(myms, spwlist = [], intentlist = ['*BANDPASS*', '*FLUX*', '*PHASE*', '*CHECK*', '*POLARIZATION*'], bfolder = None, applycalQAversion=""):
    '''Obtain spectral setup dictionary from MS.
    Positional Parameters:
    myms: MS folder name
    Keyword Parameters:
    spwlist: List of SPWs to include in the dictionary. Default is to use all science SPWs.
    intentlist: List of intents to include in the dictionary.
    bfolder: Use buffer folder to read dictionary from previous execution of this command, saved as a pickle file.
             Default is None. If some folder is given, but no pickle file for the requested MS is found, create
             it with current result.
    '''

    #If full path is given, take last bit for MS name
    mssplit = myms.split('/')
    if len(mssplit) > 1:
        msname = mssplit[-1]
    else:
        msname = myms

    #If the buffer folder bfolder is used, and there is a pickle file for the spwsetup, read it
    spwpkl = str(bfolder)+'/'+str(msname)+'_spwsetup'+'.v'+str(applycalQAversion)+'.pkl'
    if (bfolder is not None) and os.path.exists(spwpkl):
        print("Buffering existing spectral setup at : "+spwpkl)
        #Pickle file for spw setup
        spwpklfile = open(spwpkl, 'rb')
        spwsetup = pickle.load(spwpklfile)
        spwpklfile.close()
        # print("using bfolder")
        return spwsetup

    #Else read in the information from the MS
    #if spwlist is empty, get all science SPWs
    with casa_tools.MSMDReader(myms) as msmd:
        if len(spwlist) == 0:
            spwlist = getSpwList(msmd)
        spwsetup = {}
        spwsetup['spwlist'] = spwlist
        spwsetup['intentlist'] = intentlist
        spwsetup['scan'] = {}
        spwsetup['fieldname'] = {}
        spwsetup['fieldid'] = {}
        for intent in intentlist:
            spwsetup['scan'][intent] = list(msmd.scansforintent(intent))
            spwsetup['fieldname'][intent] = list(msmd.fieldsforintent(intent,asnames=True))
            spwsetup['fieldid'][intent] = list(msmd.fieldsforintent(intent,asnames=False))

        spwsetup['antids'] = msmd.antennasforscan(scan = spwsetup['scan'][intentlist[0]][0])

        #Get SPW info
        for spwid in spwlist:
            spwsetup[spwid] = {}
            #Get SPW information: frequencies of each channel, etc.
            chanfreqs = msmd.chanfreqs(spw=spwid)
            nchan = len(chanfreqs)
            spwsetup[spwid]['chanfreqs'] = chanfreqs
            spwsetup[spwid]['nchan'] = nchan
            #Get data descriptor for each SPW
            spwsetup[spwid]['ddi'] = msmd.datadescids(spw=spwid)[0]
            spwsetup[spwid]['npol'] = msmd.ncorrforpol(msmd.polidfordatadesc(spwsetup[spwid]['ddi']))

        #Save SPW setup in buffer folder
        if (bfolder is not None):
            spwpkl = str(bfolder)+'/'+msname+'_spwsetup'+'.v'+str(applycalQAversion)+'.pkl'
            print('Writing pickle dump of SPW setup to '+spwpkl)
            pklfile = open(spwpkl, 'wb')
            pickle.dump(spwsetup, pklfile, protocol=2)
            pklfile.close()

    return spwsetup

def get_intents_to_process(spwsetup, intents = None):
    #Define intents that need to be processed
    #avoiding intents with repeated scans

    if intents is None:
        intents = spwsetup['intentlist']
    intents2proc = []
    for intent in intents: 
        if intent in spwsetup['intentlist']: 
            alreadyincluded = any([(spwsetup['scan'][intent] == spwsetup['scan'][prevint]) for prevint in intents2proc])
            if (len(spwsetup['scan'][intent]) > 0) and (not alreadyincluded) and \
               (not (spwsetup['fieldname'][intent][0] in SSOfieldnames)):
                intents2proc.append(intent)

    return intents2proc

def getUnitsDicts(spwsetup):
    '''Return dictionaries with factor and unit strings from spwsetup dictionary.'''

    unitfactor = {}
    unitstr = {}
    for spw in spwsetup['spwlist']:
        #plot factors to get the right units, frequencies in GHz:
        frequencies = (1.e-09)*spwsetup[int(spw)]['chanfreqs']
        bandwidth = np.ma.max(frequencies) - np.ma.min(frequencies)
        band_midpoint = (np.ma.max(frequencies) + np.ma.min(frequencies)) / 2.0
        unitfactor[spw] = {'amp_slope': 1.0/bandwidth, 'amp_intercept': 1.0, 'phase_slope': (180.0/np.pi)/bandwidth, 'phase_intercept': (180.0/np.pi)}
        unitstr[spw] = {'amp_slope': '[Jy/GHz]', 'amp_intercept': '[Jy]', 'phase_slope': '[deg/GHz]', 'phase_intercept': '[deg]'}

    return (unitfactor, unitstr)
