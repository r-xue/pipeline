"""
This module defines types of objects used in single dish.
"""

from typing import Dict, NewType, TYPE_CHECKING, Union

import numpy as np

if TYPE_CHECKING:

    # NDArray with dimension
    NDArray1D = NewType('NDArray1D', np.ndarray[np.float64])
    NDArray2D = NewType('NDArray2D', np.ndarray[np.ndarray[np.float64]])
    NDArray3D = NewType('NDArray3D', np.ndarray[np.ndarray[np.ndarray[np.float64]]])

    # Direction
    # type as a direction of the origin for moving target
    Direction = NewType('Direction', Dict[str, Union[str, float]])

    # Angle
    Angle = NewType('Angle', Dict[str, Union[str, float]])
