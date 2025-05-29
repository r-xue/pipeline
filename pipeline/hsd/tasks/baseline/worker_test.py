import pytest
from pipeline.hsd.tasks.baseline.worker import (
    get_fit_func_dict,
    get_fit_order_dict,
    BaselineFitParamConfig,
)

SPWS = [17, 19, 23]
DEF = "cspline"
AUTO = "automatic"

# Tests for get_fit_func_dict
@pytest.mark.parametrize(
    "inp, expected, should_raise",
    [
        # valid inputs
        (None,  {17: DEF,      19: DEF,      23: DEF},      False),
        ({},    {17: DEF,      19: DEF,      23: DEF},      False),
        ("spline",         
                 {17: "cspline", 19: "cspline", 23: "cspline"},False), #   SPLINE = CSPLINE in the FittingFunction class of fitrorder.py
        ({30: "poly"},     
                 {17: DEF, 19: DEF, 23: DEF},               False),
        ("poly",
                 {17: "poly",   19: "poly",   23: "poly"},  False),
        ({"23":"poly"},
                 {17: DEF,      19: DEF,      23: "poly"},  False),
        ({"17":"poly", 19:"spline"},
                 {17:"poly",  19:"cspline",  23: DEF},     False),
        ({17:"poly", 19:"spline", 23:"poly"},
                 {17:"poly",    19:"cspline",  23:"poly"},   False),
        # error inputs 
        ("badfunc",         ValueError,                     True),
        ({19:"invalid"},  ValueError,                     True),
    ],
)
def test_get_fit_func_dict(inp, expected, should_raise):
    if should_raise:
        with pytest.raises(expected):
            get_fit_func_dict(inp, SPWS)
        return
    cfg = get_fit_func_dict(inp, SPWS)
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
            get_fit_order_dict(inp, SPWS)
        return
    cfg = get_fit_order_dict(inp, SPWS)
    assert isinstance(cfg, dict) and set(cfg.keys()) == set(SPWS)
    assert cfg == expected