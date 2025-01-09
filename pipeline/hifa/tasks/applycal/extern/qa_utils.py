import numpy as np

from pipeline.domain import MeasurementSet
from pipeline.domain.measures import FrequencyUnits

#This file contains functions that applycalqa used to borrow from AnalysisUtils, but given how  unreliable that one huge file is,
#the very few functions we are actually using were moved here

#List of SSO objects
SSOfieldnames = {'Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa'}


def get_spec_setup(ms: MeasurementSet, intents: list[str]):
    """
    Obtain spectral setup dictionary from MS.

    @param ms: MeasurementSet domain object
    @param intents: List of pipeline intents to include in the dictionary.
    """
    science_spws = ms.get_spectral_windows(intent=','.join(intents))
    spwsetup = {}
    spwsetup['spwlist'] = [spw.id for spw in science_spws]
    spwsetup['intentlist'] = intents
    spwsetup['scan'] = {intent: [scan.id for scan in ms.get_scans(scan_intent=intent)]
                        for intent in intents
                        if len(ms.get_scans(scan_intent=intent)) > 0}
    spwsetup['fieldid'] = {intent: [field.id for field in ms.get_fields(intent=intent)]
                           for intent in intents
                           if len(ms.get_fields(intent=intent)) > 0}
    spwsetup['fieldname'] = {intent: [field.name for field in ms.get_fields(intent=intent)]
                             for intent in intents
                             if len(ms.get_fields(intent=intent)) > 0}
    spwsetup['antids'] = sorted([ant.id for ant in ms.get_scans(scan_intent=intents[0])[0].antennas])
    for spw in science_spws:
        spwsetup[spw.id] = {
            # prototype cached the channel centre frequencies
            'chanfreqs': np.array([float((c.high + c.low).to_units(FrequencyUnits.HERTZ) / 2) for c in spw.channels]),
            'nchan': len(spw.channels),
            'ddi': ms.get_data_description(spw=spw.id).id,
            'npol': ms.get_data_description(spw=spw.id).num_polarizations
        }

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


def get_unit_factor(ms: MeasurementSet):
    unitfactor = {}
    for spw in ms.spectral_windows:
        bandwidth = spw.bandwidth
        bandwidth_ghz = float(bandwidth.to_units(FrequencyUnits.GIGAHERTZ))

        #plot factors to get the right units, frequencies in GHz:
        unitfactor[spw.id] = {
            'amp_slope': 1.0 / bandwidth_ghz,
            'amp_intercept': 1.0,
            'phase_slope': (180.0 / np.pi) / bandwidth_ghz,
            'phase_intercept': (180.0 / np.pi)
        }

    return unitfactor
