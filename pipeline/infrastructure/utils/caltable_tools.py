import os
import numpy as np

from pipeline.infrastructure import casa_tools


# Adapted from analysisUtils.getNChanFromCaltable()
def nchan_from_caltable(caltable, spw) -> int:
    """
    Returns the number of channels of the specified spw in a caltable.
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    with casa_tools.TableReader(caltable) as mytb:
        spectralWindowTable = mytb.getkeyword('SPECTRAL_WINDOW').split()[1]

    with casa_tools.TableReader(spectralWindowTable) as mytb:
        nchan = mytb.getcell('NUM_CHAN', spw)

    return nchan


# Adapted from analysisUtils.getChanFreqFromCaltable()
def chan_freq_from_caltable(caltable, spw) -> np.array:
    """
    Returns the frequency (in GHz) of the specified spw channel in a caltable.
    Return array of all channel frequencies
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    with casa_tools.TableReader(caltable) as mytb:
        spectralWindowTable = mytb.getkeyword('SPECTRAL_WINDOW').split()[1]

    with casa_tools.TableReader(spectralWindowTable) as mytb:
        spws = range(len(mytb.getcol('MEAS_FREQ_REF')))
        chanFreqGHz = {}
        for i in spws:
            # The array shapes can vary, so read one at a time.
            spectrum = mytb.getcell('CHAN_FREQ', i)
            chanFreqGHz[i] = 1e-9 * spectrum

    return chanFreqGHz[spw]


def antenna_names_from_caltable(caltable) -> list[str]:
    """
    Returns the antenna names from the specified caltable's ANTENNA table.
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    mytable = os.path.join(caltable, 'ANTENNA')
    with casa_tools.TableReader(mytable) as mytb:
        names = mytb.getcol('NAME')  # an array

    return list(names)


def get_ant_ids_from_caltable(caltable) -> list[int]:
    """
    Returns a list of all unique antenna ids in the caltable
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    with casa_tools.TableReader(caltable) as tb:
        table_ants = set(tb.getcol('ANTENNA1'))

    caltable_antennas = [int(ant) for ant in table_ants]
    return caltable_antennas


def get_spws_from_table(caltable) -> list[int]:
    """
    Returns a list of all unique spws in the calibration table
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    with casa_tools.TableReader(caltable) as tb:
        table_spws = set(tb.getcol('SPECTRAL_WINDOW_ID'))
    caltable_spws = sorted([int(spw) for spw in table_spws])
    return caltable_spws


def field_ids_from_caltable(caltable) -> list[int]:
    """
    Returns a list of all unique field ids in the calibration table
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    with casa_tools.TableReader(caltable) as mytb:
        fields = list(set(mytb.getcol('FIELD_ID')))
    return fields


def field_names_from_caltable(caltable) -> list[str]:
    """
    Returns a list of all unique field names in the calibration table
    """
    if not os.path.exists(caltable):
        raise FileNotFoundError(f"Caltable {caltable} does not exist")

    fields = field_ids_from_caltable(caltable)

    with casa_tools.TableReader(caltable + '/FIELD') as mytb:
        names = mytb.getcol('NAME')
        fields = list(names[fields])

    return fields
