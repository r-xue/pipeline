'''Provide the old rounding behavior.'''

import decimal

__all__ = ['round_half_up']

def round_half_up(x):
    return float(decimal.Decimal(float(x)).to_integral_value(rounding=decimal.ROUND_HALF_UP))
