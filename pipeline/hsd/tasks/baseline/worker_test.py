import pytest
from pipeline.hsd.tasks.baseline.worker import (
    SerialBaselineSubtractionWorker as worker,
    BaselineFitParamConfig,
)

SPWS = [17, 19, 23]
DEF = "cspline"
AUTO = "automatic"

# Tests for build_fitting_configuration
@pytest.mark.parametrize(
    "inp, expected, should_raise",
    [
        # valid inputs
        ({17: DEF,      19: DEF,      23: DEF}, None, False),
        ({17: DEF,      19: DEF,      23: DEF}, {}, False),
        ({17: "cspline", 19: "cspline", 23: "cspline"}, "spline", False), #   SPLINE = CSPLINE in the FittingFunction class of fitrorder.py
        ({17: DEF, 19: DEF, 23: DEF}, {30: "poly"}, False),
        ({17: "poly",   19: "poly",   23: "poly"}, "poly", False),
        ({17: DEF,      19: DEF,      23: "poly"}, {"23":"poly"}, False),
        ({17:"poly",  19:"cspline",  23: DEF}, {"17":"poly", 19:"spline"}, False),
        ({17:"poly",    19:"cspline",  23:"poly"}, {17:"poly", 19:"spline", 23:"poly"}, False),
        # error inputs 
        (ValueError, "badfunc", True),
        (ValueError, {19:"invalid"}, True),
    ],
)
def test_build_fitting_configuration(inp, expected, should_raise):
    if should_raise:
        with pytest.raises(expected):
            worker.build_fitting_configuration(inp, SPWS)
        return
    cfg = worker.build_fitting_configuration(inp, SPWS)
    assert isinstance(cfg, dict) and set(cfg.keys()) == set(SPWS)
    assert all(isinstance(v, BaselineFitParamConfig) for v in cfg.values())
    plain = {k: v.fitfunc.blfunc for k, v in cfg.items()}
    assert plain == expected


# Tests for get_fit_order_dict
@pytest.mark.parametrize(
    "inp, expected, should_raise",
    [
        # valid inputs
        (None,             {17: AUTO, 19: AUTO, 23: AUTO},   False),
        (2,                {17: 2,    19: 2,    23: 2},      False),
        (-1,               {17: AUTO, 19: AUTO, 23: AUTO},   False),
        ({},               {17: AUTO, 19: AUTO, 23: AUTO},   False),
        ("automatic",      {17: AUTO, 19: AUTO, 23: AUTO},   False),
        ({"19": -1},       {17: AUTO, 19: AUTO, 23: AUTO},   False),
        ({17: 2, 19: -1},
                          {17: 2,     19: AUTO, 23: AUTO},   False),
        ({17: 0, 19: 3, 23: 5},
                          {17: 0,    19: 3,    23: 5},       False),
        # error inputs 
        ("bad",            ValueError,                       True),
        (1.5,              TypeError,                        True),
    ],
)
def test_get_fit_order_dict(inp, expected, should_raise):
    if should_raise:
        with pytest.raises(expected):
            worker.get_fit_order_dict(inp, SPWS)
        return
    cfg = worker.get_fit_order_dict(inp, SPWS)
    assert isinstance(cfg, dict) and set(cfg.keys()) == set(SPWS)
    assert cfg == expected