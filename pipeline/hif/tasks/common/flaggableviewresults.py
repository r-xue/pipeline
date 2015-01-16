from __future__ import absolute_import
import collections
import copy

import pipeline.infrastructure as infrastructure

LOG = infrastructure.get_logger(__name__)


class FlaggableViewResults(object):
    def __init__(self):
        """
        Construct and return a new FlaggableViewResults.
        """
        # views and associated results
        self.flagging = []
        # following are used instead of standard dictionaries so that
        # missing keys are created automatically as needed
        self.view = collections.defaultdict(list)

    def addview(self, description, viewresult):
        self.view[description].append(viewresult)

    def addflags(self, flags):
        self.flagging += flags

    def add_flag_reason_plane(self, flag_reason_plane, flag_reason_key):
        for description in self.descriptions():
            self.view[description][-1].flag_reason_plane = \
              flag_reason_plane[description]
            self.view[description][-1].flag_reason_key = \
              flag_reason_key

    def descriptions(self):
        return self.view.keys()

    def first(self, description):
        return copy.deepcopy(self.view[description][0])

    def flagcmds(self):
        return copy.deepcopy(self.flagging)

    def flagged(self):
        return len(self.flagging) > 0

    def last(self, description):
        return copy.deepcopy(self.view[description][-1])

