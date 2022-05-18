"""Test for hsd_atmcor task.

Currently only test Inputs class.
"""
import os
import tempfile

import pytest

import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.launcher as launcher

from . import atmcor


@pytest.mark.parametrize(
    'value, expected',
    [
        ('auto', 'auto'),
        (0, '0'),
        ([0, 1], ['0', '1']),
    ]
)
def test_inputs_atmcor(value, expected):
    """Test atmcor parameter."""
    with tempfile.TemporaryDirectory() as tempdirname:
        ctx = launcher.Context(name=os.path.join(tempdirname, 'pipeline-test-atmcor'))
        # print(f'context name: {ctx.name}')
        print(f'atmtype: testing "{value}" (type {type(value)})...')
        inputs = atmcor.SDATMCorrectionInputs(ctx, atmtype=value)
        output = inputs.atmtype
        assert output == expected, f'Error atmtype: input {value} output {output} expected {expected}'


@pytest.mark.parametrize(
    'value, expected',
    [
        ('2', 2.0),
        ('2000.0m', 2.0),
        (2.0, 2.0),
        (['2km', 2.0], [2.0, 2.0]),
        (casa_tools.quanta.quantity(2.0, 'km'), 2.0),
    ]
)
def test_inputs_h0(value, expected):
    """Test h0 parameter."""
    with tempfile.TemporaryDirectory() as tempdirname:
        ctx = launcher.Context(name=os.path.join(tempdirname, 'pipeline-test-atmcor'))
        print(f'h0: testing "{value}" (type {type(value)})...')
        inputs = atmcor.SDATMCorrectionInputs(ctx, h0=value)
        output = inputs.h0
        assert output == expected, f'Error h0: input {value} output {output} expected {expected}'
