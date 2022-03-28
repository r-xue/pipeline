"""
Test for observatory_policy.py.

Currently, only primitive tests, which doesn't require
pipeline-specific instances such as context or domain objects,
are defined.
"""
import pytest

from . import observatory_policy


def test_call_abstract_methods():
    """Test abstract base class.

    All the methods should raise NotImplementedError.
    """
    policy_cls = observatory_policy.ObservatoryImagingPolicy
    with pytest.raises(NotImplementedError):
        beam_size_pixel = policy_cls.get_beam_size_pixel()

    with pytest.raises(NotImplementedError):
        convsupport = policy_cls.get_convsupport()

    with pytest.raises(NotImplementedError):
        margin = policy_cls.get_imaging_margin()


def test_alma_imaging_policy():
    """Test ALMA imaging policy.

    The beam size in pixel coordinate should be 9.
    The convolution support, convsupport, should be 6.
    """
    policy_cls = observatory_policy.ALMAImagingPolicy

    beam_size_pixel = policy_cls.get_beam_size_pixel()
    assert beam_size_pixel == 9

    convsupport = policy_cls.get_convsupport()
    assert convsupport == 6

    margin = policy_cls.get_imaging_margin()
    assert margin == 10


def test_nro_imaging_policy():
    """Test NRO imaging policy.

    The beam size in pixel coordinate should be 3.
    The convolution support, convsupport, should be 3.
    """
    policy_cls = observatory_policy.NROImagingPolicy

    beam_size_pixel = policy_cls.get_beam_size_pixel()
    assert beam_size_pixel == 3

    convsupport = policy_cls.get_convsupport()
    assert convsupport == 3

    margin = policy_cls.get_imaging_margin()
    assert margin == 0
