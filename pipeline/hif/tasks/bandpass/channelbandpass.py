from __future__ import absolute_import

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
from . import common
from . import bandpassworker

LOG = logging.get_logger(__name__)


class ChannelBandpassInputs(common.CommonBandpassInputs):
    minsnr = basetask.property_with_default('minsnr', 3.0)

    @property
    def bandtype(self):
        return 'B'

    def __init__(self, context, output_dir=None, run_qa2=None,
                 #
                 vis=None, caltable=None, 
                 # data selection arguments
                 field=None, spw=None, antenna=None, intent=None,
                 # solution parameters
                 solint=None, combine=None, refant=None, minblperant=None,
                 minsnr=None, solnorm=None, fillgaps=None, append=None,
                 # preapply calibrations
                 gaincurve=None, opacity=None, parang=None,
                 # calibration target
                 to_intent=None, to_field=None):
        self._init_properties(vars())


class ChannelBandpass(bandpassworker.BandpassWorker):
    Inputs = ChannelBandpassInputs
