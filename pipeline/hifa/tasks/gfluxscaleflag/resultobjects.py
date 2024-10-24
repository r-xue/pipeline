"""
Created on 01 Jun 2017

@author: Vincent Geers (UKATC)
"""
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.hif.tasks.correctedampflag import resultobjects

LOG = infrastructure.get_logger(__name__)


class GfluxscaleflagResults(basetask.Results):

    def __init__(self):
        super().__init__()
        self.cafresult = resultobjects.CorrectedampflagResults()

        # records callibrary files used in applycal calls
        self.callib_map = {} 

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.api.Results.merge_with_context`
        """
        # This task has nothing to merge into the context. The
        # task is assumed to be followed by a call to
        # hifa_gfluxscale that will produce the final caltable
        # to be used.
        pass

    def __repr__(self):
        s = 'GfluxscaleflagResults'
        return s
