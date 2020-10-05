import pytest

from .imaging import spw_intersect
from .imaging import update_sens_dict
from .imaging import update_beams_dict

spw_intersect_test_params = (([4, 12],
                              [[7, 9]],
                              [[4, 7], [9, 12]]),
                             ([4, 12],
                              [[4, 5]],
                              [[5, 12]]),
                             ([4, 12],
                              [[11, 12]],
                              [[4, 11]]),
                             ([4, 12],
                              [[4, 5], [11, 12]],
                              [[5, 11]]),
                             ([4, 12],
                              [[5, 6], [10, 11]],
                              [[4, 5], [6, 10], [11, 12]]),
                             ([228.0, 232.0],
                              [[229.7, 229.9], [230.4, 230.6], [231.0, 231.5]],
                              [[228.0, 229.7], [229.9, 230.4], [230.6, 231.0], [231.5, 232.0]]))


@pytest.mark.parametrize("spw_range, line_regions, expected", spw_intersect_test_params)
def test_spw_intersect(spw_range, line_regions, expected):
    """
    Test spw_intersect()

    This utility function takes a frequency range (as unitless integers
    or doubles) and computes the intersection with a list of frequency
    ranges denoting the regions of spectral lines. It returns the remaining
    ranges excluding the line frequency ranges.

    """
    assert spw_intersect(spw_range, line_regions) == expected


update_sens_dict_test_params = (({'robust': 0.5,
                                  'uvtaper': [],
                                  'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {16:
                                         {'sensitivityAllChans': '0.000261 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 },
                                 {'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {18:
                                         {'sensitivityAllChans': '0.000248 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        20:
                                         {'sensitivityAllChans': '0.000281 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 },
                                 {'robust': 0.5,
                                  'uvtaper': [],
                                  'uid___A002_Xc46ab2_X15ae.ms':
                                   {'J0006-0623':
                                     {'BANDPASS':
                                       {16:
                                         {'sensitivityAllChans': '0.000261 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        18:
                                         {'sensitivityAllChans': '0.000248 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'},
                                        20:
                                         {'sensitivityAllChans': '0.000281 Jy/beam',
                                          'nchanUnflagged': 120,
                                          'effChanBW': '41671875.0 Hz',
                                          'sensBW': '2000000000.0 Hz'}
                                       }
                                     }
                                   }
                                 }),)

@pytest.mark.parametrize("dct, udct, rdct", update_sens_dict_test_params)
def test_update_sens_dict(dct, udct, rdct):
    """
    Test update_sens_dict()
    """

    sens_dct = dct
    update_sens_dict(sens_dct, udct)
    assert sens_dct == rdct

update_beams_dict_test_params = (({'robust': 0.5,
                                   'uvtaper': [],
                                   'J0006-0623':
                                    {'BANDPASS':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.52, 'unit': 'arcsec'},
                                           'minor': {'value': 4.1, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': -85.49505615234375}
                                          }
                                        }
                                      }
                                    }
                                  },
                                  {'Uranus':
                                    {'AMPLITUDE':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.49, 'unit': 'arcsec'},
                                           'minor': {'value': 4.56, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': 79.95410919189453}
                                          }
                                        }
                                      }
                                    }
                                  },
                                  {'robust': 0.5,
                                   'uvtaper': [],
                                   'J0006-0623':
                                    {'BANDPASS':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.52, 'unit': 'arcsec'},
                                           'minor': {'value': 4.1, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': -85.49505615234375}
                                          }
                                        }
                                      }
                                    },
                                   'Uranus':
                                    {'AMPLITUDE':
                                      {'20':
                                        {'beam':
                                          {'major': {'value': 6.49, 'unit': 'arcsec'},
                                           'minor': {'value': 4.56, 'unit': 'arcsec'},
                                           'positionangle': {'unit': 'deg', 'value': 79.95410919189453}
                                          }
                                        }
                                      }
                                    }
                                  }),)

@pytest.mark.parametrize("dct, udct, rdct", update_beams_dict_test_params)
def test_update_beams_dict(dct, udct, rdct):
    """
    Test update_beams_dict()
    """

    beams_dct = dct
    update_beams_dict(beams_dct, udct)
    assert beams_dct == rdct
