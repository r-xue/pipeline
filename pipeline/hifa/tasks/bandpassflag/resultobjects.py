import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.hif.tasks.correctedampflag import resultobjects

LOG = infrastructure.get_logger(__name__)


class BandpassflagResults(basetask.Results):

    def __init__(self, vis):
        super(BandpassflagResults, self).__init__()
        self.cafresult = resultobjects.CorrectedampflagResults()
        self.plots = {}
        self.vis = vis

        # Set of antennas that should be moved to the end of the refant list.
        self.refants_to_demote = set()

        # Set of entirely flagged antennas that should be removed from refants.
        self.refants_to_remove = set()

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.api.Results.merge_with_context`
        """
        # Update reference antennas for MS.
        ms = context.observing_run.get_ms(name=self.vis)
        ms.update_reference_antennas(ants_to_demote=self.refants_to_demote,
                                     ants_to_remove=self.refants_to_remove)

    def __repr__(self):
        return 'BandpassflagResults'
