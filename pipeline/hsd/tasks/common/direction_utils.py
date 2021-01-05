import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

import typing
from typing import NewType, Dict, Union, Tuple

Quantity  = NewType( 'Quantity',  Dict )
Direction = NewType( 'Direction', Union[Dict, None] )
Epoch     = NewType( 'Epoch', Dict )
Position  = NewType( 'Position', Dict )

__all__ = { 'direction_shift', 'direction_offset', 'direction_recover', 'direction_convert' }


def direction_shift( direction:Direction, reference:Direction, origin:Direction ) -> Direction:
    """                                     
    Offsets the 'direction', following to shift 'reference' to 'origin'.              

    Offsets the 'direction', so that it follows the 'reference' shifted to 'origin',
    and returns the offseted-diretion in direction quantity.
    Example : This feature can be used to create an image centerized at 'origin':                            
              Providing the time-by-time position of the observing points ('direction')
              together with the time-by-time position of the moving-source ('reference'),
              the function shifts the "direction" so that the position of the moving-source
              is centerized to "origin".                  

    Args:
        direction: direction to be converted  (eg. time-by-time position of observing points)
        reference: reference direction (eg. time-by-time position of the moving source on the sky)
        origin:    direction of the origin (eg. where to centerized the new image)           
    Returns:                           
        shifted direction (reference centerized at origin)
    """
    # check if 'refer's are all identical for each directions
    if origin['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and origin should be identical" )
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casa_tools.measures
    offset = me.separation( reference, origin )
    posang = me.posangle( reference, origin )
    new_direction = me.shift( direction, offset=offset, pa=posang )

    return new_direction


def direction_offset( direction:Direction, reference:Direction ) -> Direction:
    """                               
    Offsets the 'direction', following to shift 'reference' to the coordinate-origin (0, 0).

    Offsets the 'direction', so that it follows the 'reference' shifted to the coordinate-origin (0, 0),
    and returns the offseted-diretion in direction quantity.     
    This is equivallent to calling direction_shift( direction, reference, origin ) with the coordinate-origin (0, 0) as 'origin'.

    Example : This feature can be used to create an image centerized at the coordinate-origin (0, 0):
              Providing the time-by-time position of the observing points ('direction')
              together with the time-by-time position of the moving-source ('reference'),
              the function shifts the 'direction' so that the position of the moving-source
              is centerized at the coordinate-origin (0, 0).
    Args:                                                 
        direction: direction to be converted  (eg. time-by-time position of observing points)
        reference: reference direction (eg. time-by-time position of the moving source on the sky)
    Returns:           
        shifted direction (reference centerized at the coordinate-origin)
    """
    # check if 'refer's are all identical for each directions
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casa_tools.measures
    offset = me.separation( reference, direction )
    posang = me.posangle( reference, direction )

    outref = direction['refer']
    zero_direction = me.direction( outref, '0.0deg', '0.0deg' )
    new_direction = me.shift( zero_direction, offset=offset, pa=posang )

    return new_direction


def direction_recover( ra:float, dec:float, org_direction:Direction ) -> Tuple[float, float]:
    """                                                                                                      
    Recovers the shift-coordinate from offset-coordinate

    Recovers the shift-coordinate values from the offset-coordinate values.

    Args:                                                                        
        ra:  ra of offset-corrdinate
        dec: dec of offset-coordinate
        org_direction: direction of the origin
    Returns:                                                               
        return value: ra, dec in shift-coordinate
    """
    me = casa_tools.measures
    qa = casa_tools.quanta

    direction = me.direction( org_direction['refer'],
                              str(ra)+'deg', str(dec)+'deg' )
    zero_direction  = me.direction( org_direction['refer'], '0deg', '0deg' )
    offset = me.separation( zero_direction, direction )
    posang = me.posangle( zero_direction, direction )
    new_direction = me.shift( org_direction, offset=offset, pa=posang )
    new_ra  = qa.convert( new_direction['m0'], 'deg' )['value']
    new_dec = qa.convert( new_direction['m1'], 'deg' )['value']

    return new_ra, new_dec


def direction_convert(direction:Direction, mepoch:Epoch, mposition:Position, outframe:str) -> Tuple[Quantity, Quantity]
    """  
    Convert the frame of the 'direction' to 'outframe'

    Convert the 'frame' of the direction to that specified as 'outframe'.
    If 'outframe' is identical to the frame of the 'direction', the original 'direction' will be returned.

    Args:
        direction:  original direction
        mepoch:     epoch
        mposition:  position
        outframe:   frame of output direction
    Returns:
        return values
    """
    direction_type = direction['type']
    assert direction_type == 'direction'
    inframe = direction['refer']

    # if outframe is same as input direction reference, just return
    # direction as it is
    if outframe == inframe:
        # return direction
        return direction['m0'], direction['m1']

    # conversion using measures tool
    me = casa_tools.measures
    me.doframe(mepoch)
    me.doframe(mposition)
    out_direction = me.measure(direction, outframe)
    return out_direction['m0'], out_direction['m1']
