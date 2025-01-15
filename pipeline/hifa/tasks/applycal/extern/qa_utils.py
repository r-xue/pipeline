import numpy as np

from pipeline.domain import MeasurementSet
from pipeline.domain.measures import FrequencyUnits

#This file contains functions that applycalqa used to borrow from AnalysisUtils, but given how  unreliable that one huge file is,
#the very few functions we are actually using were moved here

#List of SSO objects
SSOfieldnames = {'Ceres', 'Pallas', 'Vesta', 'Venus', 'Mars', 'Jupiter', 'Uranus', 'Neptune', 'Ganymede', 'Titan', 'Callisto', 'Juno', 'Europa'}


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
