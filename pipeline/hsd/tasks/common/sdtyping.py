"""
This module defines types of objects used in single dish.
"""

from typing import NewType, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:

    # N-Dimension Array of float64 
    NpArray1D = NewType('NpArray1D', np.ndarray[np.float64])
    NpArray2D = NewType('NpArray2D', np.ndarray[np.ndarray[np.float64]])
    NpArray3D = NewType('NpArray3D', np.ndarray[np.ndarray[np.ndarray[np.float64]]])

    # Direction
    # type as a direction of the origin for moving target
    Direction = NewType('Direction', dict[str, str | float])

    # Angle
    Angle = NewType('Angle', dict[str, str | float])
