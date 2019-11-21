from casatasks.private.parallel.parallel_task_helper import ParallelTaskHelper

import pipeline.h.tasks.applycal.applycal as applycal
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class UVcontSubInputs(applycal.ApplycalInputs):
    applymode = vdp.VisDependentProperty(default='calflag')
    flagsum = vdp.VisDependentProperty(default=False)
    intent = vdp.VisDependentProperty(default='TARGET')

    def __init__(self, context, output_dir=None, vis=None, field=None, spw=None, antenna=None, intent=None, parang=None,
                 applymode=None, flagbackup=None, flagsum=None, flagdetailedsum=None):
        super(UVcontSubInputs, self).__init__(context, output_dir=output_dir, vis=vis, field=field, spw=spw,
                                              antenna=antenna, intent=intent, parang=parang, applymode=applymode,
                                              flagbackup=flagbackup, flagsum=flagsum, flagdetailedsum=flagdetailedsum)


# Register this as an imaging MS(s) preferred task
api.ImagingMeasurementSetsPreferred.register(UVcontSubInputs)


@task_registry.set_equivalent_casa_task('hif_uvcontsub')
class UVcontSub(applycal.Applycal):
    Inputs = UVcontSubInputs

    # Override prepare method with one which sets and unsets the VI1CAL
    # environment variable.
    def prepare(self):
        inputs = self.inputs

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result = UVcontSubResults()
                result.mitigation_error = True
                return result

        try:
            # Set cluster to serial mode for this applycal
            #if infrastructure.mpihelpers.is_mpi_ready():
            #    ParallelTaskHelper.bypassParallelProcessing(1)

            return super(UVcontSub, self).prepare()

        finally:
            # Reset cluster to parallel mode
            #if infrastructure.mpihelpers.is_mpi_ready():
            #    ParallelTaskHelper.bypassParallelProcessing(0)
            pass

        return UVcontSubResults()


# Simple results class to transport any mitigation error
class UVcontSubResults(basetask.Results):
    """
    UVcontSubResults is the results class for the pipeline UVcontSub task.
    """

    def __init__(self, applied=[]):
        super(UVcontSubResults, self).__init__()
        self.mitigation_error = False

# May need this full class in the future
#
#
#class UVcontSubResults(basetask.Results):
#    """
#    UVcontSubResults is the results class for the pipeline UVcontSub task.
#    """
#
#    def __init__(self, applied=[]):
#        """
#        Construct and return a new UVContSubResults.
#
#        The resulting object should be initialized with a list of
#        CalibrationTables corresponding to the caltables applied by this task.
#
#        :param applied: caltables applied by this task
#        :type applied: list of :class:`~pipeline.domain.caltable.CalibrationTable`
#        """
#        super(UVcontSubResults, self).__init__()
#        self.applied = set()
#        self.applied.update(applied)
#
#    def merge_with_context(self, context):
#        """
#        Merges these results with the given context by examining the context
#        and marking any applied caltables, so removing them from subsequent
#        on-the-fly calibration calculations.
#
#        See :method:`~pipeline.Results.merge_with_context`
#        """
#        if not self.applied:
#            LOG.error('No results to merge')
#
#        for calapp in self.applied:
#            LOG.trace('Marking %s as applied' % calapp.as_applycal())
#            context.callibrary.mark_as_applied(calapp.calto, calapp.calfrom)
#
#    def __repr__(self):
#        for caltable in self.applied:
#            s = 'UVcontSubResults:\n'
#            if isinstance(caltable.gaintable, list):
#                basenames = [os.path.basename(x) for x in caltable.gaintable]
#                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
#                    spw=caltable.spw, vis=os.path.basename(caltable.vis),
#                    name=','.join(basenames))
#            else:
#                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
#                    name=caltable.gaintable, spw=caltable.spw,
#                    vis=os.path.basename(caltable.vis))
#        return s
