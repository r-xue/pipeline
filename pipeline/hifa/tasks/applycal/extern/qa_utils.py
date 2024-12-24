import os
import pickle

import numpy as np

from pipeline.domain import MeasurementSet
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import conversion

#This file contains functions that applycalqa used to borrow from AnalysisUtils, but given how  unreliable that one huge file is,
#the very few functions we are actually using were moved here

#List of SSO objects
SSOfieldnames = {'Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa'}


def getSpwList(msmd,intent='OBSERVE_TARGET#ON_SOURCE',tdm=True,fdm=True, sqld=False):
    spws = msmd.spwsforintent(intent)
    almaspws = msmd.almaspws(tdm=tdm,fdm=fdm,sqld=sqld)
    scienceSpws = np.intersect1d(spws,almaspws)
    return(list(scienceSpws))


def getSpecSetup(myms: MeasurementSet, intents: list[str], bfolder = None, applycalQAversion=""):
    '''Obtain spectral setup dictionary from MS.
    Positional Parameters:
    myms: MeasurementSet domain object
    Keyword Parameters:
    spwlist: List of SPWs to include in the dictionary. Default is to use all science SPWs.
    intents: List of pipeline intents to include in the dictionary.
    bfolder: Use buffer folder to read dictionary from previous execution of this command, saved as a pickle file.
             Default is None. If some folder is given, but no pickle file for the requested MS is found, create
             it with current result.
    '''
    spwlist = []

    #If the buffer folder bfolder is used, and there is a pickle file for the spwsetup, read it
    spwpkl = f'{bfolder}/{myms.basename})+spwsetup.v{applycalQAversion}.pkl'
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
    with casa_tools.MSMDReader(myms.basename) as msmd:
        if len(spwlist) == 0:
            spwlist = getSpwList(msmd)
        spwsetup = {}
        spwsetup['spwlist'] = spwlist
        spwsetup['intentlist'] = intents
        spwsetup['scan'] = {}
        spwsetup['fieldname'] = {}
        spwsetup['fieldid'] = {}
        pipe_intents_in_ms = [i for i in intents if i in myms.intents]
        for pipe_intent in pipe_intents_in_ms:
            casa_intent = conversion.to_CASA_intent(myms, pipe_intent)
            spwsetup['scan'][pipe_intent] = list(msmd.scansforintent(casa_intent))
            spwsetup['fieldname'][pipe_intent] = list(msmd.fieldsforintent(casa_intent, asnames=True))
            spwsetup['fieldid'][pipe_intent] = list(msmd.fieldsforintent(casa_intent, asnames=False))

        spwsetup['antids'] = msmd.antennasforscan(scan = spwsetup['scan'][intents[0]][0])

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
            spwpkl = f'{bfolder}/{myms.basename}_spwsetup.v{applycalQAversion}.pkl'
            print('Writing pickle dump of SPW setup to '+spwpkl)
            pklfile = open(spwpkl, 'wb')
            pickle.dump(spwsetup, pklfile, protocol=2)
            pklfile.close()

    return spwsetup

def get_intents_to_process(ms: MeasurementSet, intents: list[str]) -> list[str]:
    """
    Optimise a list of intents so that scans with multiple intents are only
    processed once.

    :param ms: MeasurementSet domain object
    :param intents: list of intents to consider for processing
    :return: optimised list of intents to process
    """
    intents_to_process = []
    for intent in intents:
        for scan in ms.get_scans(scan_intent=intent):
            scan_included_via_other_intent = any(i in scan.intents for i in intents_to_process)
            field_is_not_sso = SSOfieldnames.isdisjoint({field.name for field in scan.fields})
            
            if not scan_included_via_other_intent and field_is_not_sso:
                intents_to_process.append(intent)

    return intents_to_process


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
