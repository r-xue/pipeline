"""Phase 1 plumbing tests for PIPE-2956.

These tests pin down the parameter-plumbing contract for ``examineCrossPolSum``
on ``hif_correctedampflag``. They are intentionally narrow: they do not exercise
the multi-scan averaging behavior (Phase 2) or any CASA/MS machinery.
"""

import inspect
from unittest.mock import Mock

import pytest

from pipeline.extern import XmlObjectifier
from pipeline.hif.cli.hif_correctedampflag import hif_correctedampflag
from pipeline.hif.tasks.correctedampflag.correctedampflag import CorrectedampflagInputs
from pipeline.infrastructure.launcher import Context

PARAM = 'examineCrossPolSum'


def test_inputs_descriptor_default_is_false():
    """Test whether the default of examineCrossPolSum is False."""
    descriptor = getattr(CorrectedampflagInputs, PARAM)
    assert descriptor.default is False


def test_inputs_init_signature_includes_param():
    """Test whether the examineCrossPolSum parameter is set in CorrectedampflagInputs."""
    params = inspect.signature(CorrectedampflagInputs.__init__).parameters
    assert PARAM in params
    assert params[PARAM].default is None


def test_inputs_default_resolves_to_false():
    """Test whether the default is False."""
    context = Mock(spec=Context)
    inputs = CorrectedampflagInputs(context=context, vis=None)
    assert getattr(inputs, PARAM) is False


def test_inputs_explicit_true_resolves_to_true():
    """Test whether the parameter can be set to True."""
    context = Mock(spec=Context)
    inputs = CorrectedampflagInputs(context=context, vis=None, **{PARAM: True})
    assert getattr(inputs, PARAM) is True


def test_inputs_explicit_false_resolves_to_false():
    """Test whether the parameter can be set to False."""
    context = Mock(spec=Context)
    inputs = CorrectedampflagInputs(context=context, vis=None, **{PARAM: False})
    assert getattr(inputs, PARAM) is False


def test_cli_signature_includes_param():
    """Test whether the parameter is included in the function signature."""
    params = inspect.signature(hif_correctedampflag).parameters
    assert PARAM in params
    assert params[PARAM].default is None


@pytest.mark.parametrize(
    'text,expected',
    [
        ('true', True),
        ('True', True),
        ('TRUE', True),
        ('false', False),
        ('False', False),
        ('FALSE', False),
    ],
)
def test_ppr_xml_castType_round_trips_boolean_strings(text, expected):
    """PPRs deliver values as XML text; verify the documented PPR path returns
    Python booleans for ``examineCrossPolSum`` rather than strings.
    """
    assert XmlObjectifier.castType(text) is expected
