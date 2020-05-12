import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask

LOG = infrastructure.get_logger(__name__)


class PolRefAntResults(basetask.Results):

    def __init__(self):
        super(PolRefAntResults, self).__init__()

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.api.Results.merge_with_context`
        """
        pass

    def __repr__(self):
        return 'PolRefAntResults'
