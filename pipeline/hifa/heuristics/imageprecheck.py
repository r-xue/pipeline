import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


class ImagePreCheckHeuristics(object):
    def __init__(self, inputs):
        self.inputs = inputs
        self.context = inputs.context

    # Below maxBR is the maxAllowedBeamAxialRatio that will be in the SBSummary table as of cycle 7
    def compare_beams(self, beam_0p0, beam_0p5, beam_1p0, beam_2p0, minAR, maxAR, maxBR):

        cqa = casatools.quanta

        beams = {0.0: beam_0p0, 0.5: beam_0p5, 1.0: beam_1p0, 2.0: beam_2p0}
        robusts = sorted(beams.keys())

        # Define predicted beam areas and beam ratios
        beamArea_0p0 =  cqa.mul(beams[0.0]['minor'], beams[0.0]['major'])
        beamArea_0p5 =  cqa.mul(beams[0.5]['minor'], beams[0.5]['major'])
        beamArea_1p0 =  cqa.mul(beams[1.0]['minor'], beams[1.0]['major'])
        beamArea_2p0 =  cqa.mul(beams[2.0]['minor'], beams[2.0]['major'])
        beamRatio_0p0 =  cqa.div(beams[0.0]['major'], beams[0.0]['minor'])
        beamRatio_0p5 =  cqa.div(beams[0.5]['major'], beams[0.5]['minor'])
        beamRatio_1p0 =  cqa.div(beams[1.0]['major'], beams[1.0]['minor'])
        beamRatio_2p0 =  cqa.div(beams[2.0]['major'], beams[2.0]['minor'])

        # Define PI requested beam area range
        minARbeamArea = cqa.mul(minAR, minAR)
        maxARbeamArea = cqa.mul(maxAR, maxAR)

        # Define a default value of maxBR if none is available (i.e. Cycle 5, 6 data)
        if cqa.getvalue(maxBR) == 0.0:
            maxBR = cqa.quantity(2.5)

        # PI requested resolution range is not available, robust=0.5 (pre-Cycle 5 data and all 7m-array datasets)
        if (cqa.getvalue(minAR) == 0.0) and \
           (cqa.getvalue(maxAR) == 0.0):
            hm_robust = 0.5
            hm_robust_score_value = 1.0
            hm_robust_score_longmsg = 'No representative target info found'
            hm_robust_score_shortmsg = 'No representative target'

        # robust=0.5 beam area in range
        elif cqa.le(minARbeamArea, beamArea_0p5) and \
             cqa.le(beamArea_0p5, maxARbeamArea):
            hm_robust = 0.5
            # axial ratio less than max
            if cqa.le(beamRatio_0p5, maxBR):
                hm_robust_score_value = 1.0
                hm_robust_score_longmsg = 'Predicted robust=0.5 beam is within the PI requested range'
                hm_robust_score_shortmsg = 'Beam within range'
            # axial ratio greater than max
            else:
                hm_robust_score_value = 0.5
                hm_robust_score_longmsg = 'Predicted robust=0.5 beam is within the PI requested Beam Area, but the Axial Ratio exceeds the maximum allowed'
                hm_robust_score_shortmsg = 'Beam within range, BR too large'
            LOG.warn(hm_robust_score_longmsg)

        # robust=0.0 beam area in range
        elif cqa.le(minARbeamArea, beamArea_0p0) and \
             cqa.le(beamArea_0p0, maxARbeamArea):
            hm_robust = 0.0
            # axial ratio less than max
            if cqa.le(beamRatio_0p0, maxBR):
                hm_robust_score_value = 0.85
                hm_robust_score_longmsg = 'Predicted non-default robust=0.0 beam is within the PI requested range'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust'
            # axial ratio greater than max
            else:
                hm_robust_score_value = 0.5
                hm_robust_score_longmsg = 'Predicted non-default robust=0.0 beam is within the PI requested range, but the Axial Ratio exceeds the maximum allowed'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust, BR too large'
            LOG.warn(hm_robust_score_longmsg)

        # robust=1.0 beam area in range
        elif cqa.le(minARbeamArea, beamArea_1p0) and \
             cqa.le(beamArea_1p0, maxARbeamArea):
            hm_robust = 1.0
            # axial ratio less than max
            if cqa.le(beamRatio_1p0, maxBR):
                hm_robust_score_value = 0.85
                hm_robust_score_longmsg = 'Predicted non-default robust=1.0 beam is within the PI requested range'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust'
            # axial ratio greater than max
            else:
                hm_robust_score_value = 0.5
                hm_robust_score_longmsg = 'Predicted non-default robust=1.0 beam is within the PI requested range, but the Axial Ratio exceeds the maximum allowed'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust, BR too large'
            LOG.warn(hm_robust_score_longmsg)

        # robust=2.0 beam area in range
        elif cqa.le(minARbeamArea, beamArea_2p0) and \
             cqa.le(beamArea_2p0, maxARbeamArea):
            hm_robust = 2.0
            # axial ratio less than max
            if cqa.le(beamRatio_2p0, maxBR):
                hm_robust_score_value = 0.85
                hm_robust_score_longmsg = 'Predicted non-default robust=2.0 beam is within the PI requested range'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust'
            # axial ratio greater than max
            else:
                hm_robust_score_value = 0.5
                hm_robust_score_longmsg = 'Predicted non-default robust=2.0 beam is within the PI requested range, but the Axial Ratio exceeds the maximum allowed'
                hm_robust_score_shortmsg = 'Beam within range using non-default robust, BR too large'
            LOG.warn(hm_robust_score_longmsg)

        # robust=2.0 beam area out of range
        elif cqa.lt(beamArea_2p0, minARbeamArea):
            hm_robust = 2.0
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'The beam is too small, the predicted non-default robust=2.0 beam cannot achieve PI beam area'
            hm_robust_score_shortmsg = 'Beam is too small'
            LOG.warn(hm_robust_score_longmsg)
        # robust=0.0 beam area out of range
        elif cqa.gt(beamArea_0p0, maxARbeamArea):
            hm_robust = 0.0
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'The beam is too large, the predicted non-default robust=0.0 beam cannot achieve PI beam area'
            hm_robust_score_shortmsg = 'Beam is too large'
            LOG.warn(hm_robust_score_longmsg)
        else: 
            hm_robust = 0.5
            hm_robust_score_value = 0.25
            hm_robust_score_longmsg = 'Requested beam area range falls in robust gap'
            hm_robust_score_shortmsg = 'Requested beam falls in robust gap'
            LOG.warn(hm_robust_score_longmsg)
                
        return hm_robust, (hm_robust_score_value, hm_robust_score_longmsg, hm_robust_score_shortmsg)
