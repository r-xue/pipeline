import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.logging as logging

LOG = infrastructure.get_logger(__name__)


def direction_shift( direction, reference, origin ):
    # check if 'refer's are all identical for each directions
    if origin['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and origin should be identical" )
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casatools.measures
    offset = me.separation( reference, origin )
    posang = me.posangle( reference, origin )
    new_direction = me.shift( direction, offset=offset, pa=posang )

    return new_direction


def direction_offset( direction, reference ):
    # check if 'refer's are all identical for each directions
    if direction['refer'] != reference['refer']:
        raise RuntimeError( "'refer' of reference and direction should be identical" )

    me = casatools.measures
    offset = me.separation( reference, direction )
    posang = me.posangle( reference, direction )

    outref = direction['refer']
    zero_direction = me.direction( outref, '0.0deg', '0.0deg' )
    new_direction = me.shift( zero_direction, offset=offset, pa=posang )

    return new_direction


def direction_recover( ra, dec, org_direction ):
    me = casatools.measures
    qa = casatools.quanta

    direction = me.direction( org_direction['refer'],
                              str(ra)+'deg', str(dec)+'deg' )
    zero_direction  = me.direction( org_direction['refer'], '0deg', '0deg' )
    offset = me.separation( zero_direction, direction )
    posang = me.posangle( zero_direction, direction )
    new_direction = me.shift( org_direction, offset=offset, pa=posang )
    new_ra  = qa.convert( new_direction['m0'], 'deg' )['value']
    new_dec = qa.convert( new_direction['m1'], 'deg' )['value']

    return new_ra, new_dec


def direction_convert(direction, mepoch, mposition, outframe):
    direction_type = direction['type']
    assert direction_type == 'direction'
    inframe = direction['refer']

    # if outframe is same as input direction reference, just return
    # direction as it is
    if outframe == inframe:
        # return direction
        return direction['m0'], direction['m1']

    # conversion using measures tool
    me = casatools.measures
    me.doframe(mepoch)
    me.doframe(mposition)
    out_direction = me.measure(direction, outframe)
    return out_direction['m0'], out_direction['m1']
