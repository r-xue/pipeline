import os

import numpy as np

# key is frequency coverage, (fmin, fmax)
# value is the filename of the cached data
CACHED_TRANSMISSION_DATA = {
    (0, 50): "transmission_SHARED_Band0-50GHz.npz",
    (67, 116): "transmission_ALMA_Band2.npz",
    (125, 163): "transmission_ALMA_Band4.npz",
    (158, 211): "transmission_ALMA_Band5.npz",
    (211, 275): "transmission_ALMA_Band6.npz",
    (275, 373): "transmission_ALMA_Band7.npz",
    (385, 500): "transmission_ALMA_Band8.npz",
    (602, 720): "transmission_ALMA_Band9.npz",
    (787, 950): "transmission_ALMA_Band10.npz",
}


def load_transmission_data(cache_file):
    data_dir = os.path.join(
        os.path.dirname(__file__),
        "compressed_transmission_only_2MHz_float16"
    )
    npzfile = np.load(os.path.join(data_dir, cache_file))
    transmission = npzfile["transmission"]
    return transmission


def calc_channel_freq(fcenter, bandwidth, resolution):
    nchan = int(np.ceil(bandwidth / resolution))
    refchan = (nchan - 1) // 2
    reffreq = fcenter
    chansep = resolution
    if nchan % 2 == 0:
        reffreq -= chansep / 2
    freqoffset = reffreq - chansep * refchan
    return np.array([freqoffset + i * chansep for i in range(nchan)], dtype=float)


def find_cache_key(fmin, fmax):
    """
    Find the cached transmission data file that covers the given frequency range.
    Returns the filename of the cached data or None if no suitable data is found.
    """
    for (cache_fmin, cache_fmax) in CACHED_TRANSMISSION_DATA.keys():
        if cache_fmin <= fmin and fmax <= cache_fmax:
            return cache_fmin, cache_fmax
    return None, None


def get_cached_transmission(fmin, fmax):
    """
    Get the cached transmission data for the given frequency range.
    Returns the transmission data if found, otherwise raises an error.
    """
    cache_fmin, cache_fmax = find_cache_key(fmin, fmax)
    if cache_fmin is None:
        raise ValueError(f"No cached transmission data available for range {fmin} - {fmax} GHz")

    cache_file = CACHED_TRANSMISSION_DATA[(cache_fmin, cache_fmax)]
    transmission = load_transmission_data(cache_file)

    resolution = 2e-3  # 2MHz
    if cache_fmin == 0:
        fcenter = cache_fmax / 2 + resolution / 2
        bandwidth = cache_fmax - cache_fmin
    else:
        fcenter = (cache_fmin + cache_fmax) / 2
        bandwidth = cache_fmax - cache_fmin + resolution
    freq = calc_channel_freq(fcenter, bandwidth, resolution)
    mask = np.logical_and(fmin <= freq, freq <= fmax)

    return freq[mask], transmission[mask]
