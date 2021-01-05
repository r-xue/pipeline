import pytest

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
from pipeline.hsd.tasks.common.direction_utils import direction_shift, direction_offset, direction_recover, direction_convert

LOG = infrastructure.get_logger(__name__)

me = casatools.measures
qa = casatools.quanta

# ----------------------------------------------------------------------------

test_params_shift   = [
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '30deg' ),
      me.direction( 'J2000', '10deg', '20deg' ),
      me.direction( 'J2000', '39.1575253282043deg', '29.836125182282498deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '30deg' ),
      me.direction( 'J2000', '-10deg', '20deg' ),
      me.direction( 'J2000', '17.658856653770037deg', '28.563899711023435deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '30deg' ),
      me.direction( 'J2000', '-10deg', '-20deg' ),
      me.direction( 'J2000', '21.373888777604098deg', '-11.274494798360799deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '30deg' ),
      me.direction( 'J2000', '10deg', '-20deg' ),
      me.direction( 'J2000', '40.458189684591325deg', '-10.144260059229625deg' )),
]

@pytest.mark.parametrize("direction, reference, origin, expected", test_params_shift)
def test_direction_shift( direction, reference, origin, expected ):
    """ 
    Test direction_shift()

    Args:
      direction :
      reference :
      origin    :
      expected  : expected result
    Returns:
      (none)
    Raises:
      AssertationError for tests failing
    """
    epsdeg = qa.quantity( '1.0E-8deg' )
    result = direction_shift( direction, reference, origin )
    separation = me.separation( result, expected )
    assert qa.lt( qa.abs(separation), epsdeg )


test_params_shift_raise = [
    (me.direction( 'J2000', '50deg', '40deg' ),
     me.direction( 'B1950', '20deg', '30deg' ),
     me.direction( 'J2000', '10deg', '20deg' )),
    (me.direction( 'J2000', '50deg', '40deg' ),
     me.direction( 'J2000', '20deg', '30deg' ),
     me.direction( 'B1950', '10deg', '20deg' ))
]

@pytest.mark.parametrize( "direction, reference, origin", test_params_shift_raise )
def test_direction_shift_raise( direction, reference, origin ):
    """
    Test direction_shift() to raise RuntimeError

    Args:
      direction :
      reference :
      origin    :
    Returns:
      (none)
    """
    with pytest.raises( RuntimeError ):
        direction_shift( direction, reference, origin )


# ----------------------------------------------------------------------------

test_params_offset  = [
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '30deg' ),
      me.direction( 'J2000', '23.147430773469846deg', '13.000727439793065deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '-20deg', '30deg' ),
      me.direction( 'J2000', '52.70408975138903deg', '25.19302160451274deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '-20deg', '-30deg' ),
      me.direction( 'J2000', '82.52164912863546deg', '43.44608629655966deg' )),
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'J2000', '20deg', '-30deg' ),
      me.direction( 'J2000', '56.53926935383829deg', '62.67005126630539deg' ))
]

@pytest.mark.parametrize("direction, reference, expected", test_params_offset)
def test_direction_offset( direction, reference, expected ):
    """ 
    Test direction_offset()

    Args:
      direction :
      reference :
      expected  : expected result
    Returns:
      none
    Raises:
      AssertationError for tests failing
    """
    epsdeg = qa.quantity( '1.0E-8deg' )
    result = direction_offset( direction, reference )
    ###
#    LOG.info( "##### result2={}".format(( separation )))
#    LOG.info( "##### result2={} {}".format(
#        qa.convert( result['m0'], 'deg' ),
#        qa.convert( result['m1'], 'deg' ) ))
#    raise RuntimeError
    ###
    separation = me.separation( result, expected )
    assert qa.lt( qa.abs(separation), epsdeg )


test_params_offset_raise  = [
    ( me.direction( 'J2000', '50deg', '40deg' ),
      me.direction( 'B1950', '20deg', '30deg' ))
]

@pytest.mark.parametrize("direction, reference", test_params_offset_raise)
def test_direction_offset_raise( direction, reference ):
    """ 
    Test direction_offset to raise RuntimeError()

    Args:
      direction :
      reference :
    Returns:
      none
    Raises:
      AssertationError for tests failing
    """
    with pytest.raises( RuntimeError ):
        direction_offset( direction, reference )

# ----------------------------------------------------------------------------

test_params_recover = [( 30.0, 20.0,  
                         me.direction( 'J2000', '5deg', '10deg' ),
                         37.34114334622996, 28.563899711023446
                     )]

@pytest.mark.parametrize("ra, dec, org_direction, expected_ra, expected_dec", test_params_recover)
def test_direction_recover( ra, dec, org_direction, expected_ra, expected_dec ):
    """ 
    Test direction_recover()

    Args:
      ra, dec :
      org_directiopn :
    Returns:
      none
    Raises:
      AssertationError for tests failing
    """
    eps = 1.0E-8 # in deg
    result_ra, result_dec = direction_recover( ra, dec, org_direction )
    assert abs(result_ra-expected_ra)<eps and abs(result_dec-expected_dec)<eps

# ----------------------------------------------------------------------------

test_params_convert = [( me.direction( 'J2000', '10deg', '60deg' ),
                         me.epoch( rf='UTC', v0=qa.quantity( '58000.0d' ) ),
                         me.observatory( 'ALMA' ),
                         'B1950',
                         qa.quantity( '9.279004120033214deg' ), 
                         qa.quantity( '59.7256831517683deg' ) 
                     )]

@pytest.mark.parametrize("direction, mepoch, mposition, outframe, expected_ra, expected_dec", test_params_convert)
def test_direction_convert( direction, mepoch, mposition, outframe, expected_ra, expected_dec ):
    """ 
    Test direction_convert()

    Args:
      direction :
      mepoch :
      mposition :
      outframe :
      expected_ra, expected_dec :
    Returns:
      none
    Raises:
      AssertationError for tests failing
    """

    epsdeg = qa.quantity( '1.0E-8deg' )
    result_ra, result_dec = direction_convert( direction, mepoch, mposition, outframe )
    assert qa.lt( qa.abs(qa.sub(result_ra, expected_ra)), epsdeg ) or qa.lt(  qa.abs(qa.sub(result_dec, expected_dec)), epsdeg ) 

# ----------------------------------------------------------------------------
