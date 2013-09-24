from __future__ import absolute_import
import collections
import copy
import os
import types

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.hif.tasks.common import commonresultobjects

LOG = infrastructure.get_logger(__name__)


class WvrgcalResult(basetask.Results):

    def __init__(self, vis, final=[], pool=[], preceding=[],
      wvrflag=[]):
        """
        Construct and return a new WvrgcalflagResult.
        """
        super(WvrgcalResult, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()

        # section for qa2 results
        self.qa2 = commonresultobjects.Qa2Result()

        # results used to calculate the qa2 results
        self.bandpass_result = None
        self.nowvr_result = None

        # views and flag operations
        self.flagging = []
        self.wvrflag = wvrflag
        self.view = collections.defaultdict(list)
        
        # record wvrgcal tie arguments for weblog 
        self.tie = ''

    def merge_with_context(self, context):
        if not self.final:
            LOG.warning('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
              '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):

        # Format the Wvrgcal results.
        s = 'WvrgcalResult:\n'
        if not self.final:
            s += '\tNo wvr caltables will be applied\n'

        for calapplication in self.final:
            s += '\tBest caltable for spw #{spw} field {field} in {vis} is {name}\n'.format(
              spw=calapplication.spw, field=calapplication.field,
              vis=os.path.basename(calapplication.vis),
              name=calapplication.gaintable)
        s += '\twvrflag is {wvrflag}'.format(wvrflag=self.wvrflag)

        return s


class WvrgcalflagResult(WvrgcalResult):

    def merge_with_context(self, context):
        if not self.final:
            LOG.warning('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
              '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)

        if self.wvrflag:
            ms = context.observing_run.get_ms(name=self.vis)
            if hasattr(ms, 'reference_antenna') and \
              type(ms.reference_antenna) == types.StringType:
                refant = ms.reference_antenna.split(',')
                bad_antennas = set(self.wvrflag).intersection(refant)
                if bad_antennas:
                    LOG.warning('%s antennas with bad wvr removed from refant list: %s' %
                      (os.path.basename(self.vis), list(bad_antennas)))
                    for antenna in list(bad_antennas):
                        refant.remove(antenna)
                    ms.reference_antenna = ','.join(refant)

    def addflags(self, flags):
        self.flagging += flags

    def addview(self, description, viewresult):
        self.view[description].append(viewresult)

    def descriptions(self):
        return self.view.keys()

    def first(self, description):
        return copy.deepcopy(self.view[description][0])

    def flagcmds(self):
        return copy.deepcopy(self.flagging)

    def last(self, description):
        return copy.deepcopy(self.view[description][-1])

