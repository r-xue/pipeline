'''Provide the old rounding behavior.'''

import decimal

__all__ = ['round_half_up']

def round_half_up(value, prec=0):
    return float(decimal.Decimal(float(value) * 10**prec).to_integral_value(rounding=decimal.ROUND_HALF_UP)) / 10**prec
