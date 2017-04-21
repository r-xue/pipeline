from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
#from pipeline.hif.tasks.applycal import applycal
from pipeline.h.tasks.applycal import applycal

LOG = infrastructure.get_logger(__name__)

class UVcontSubInputs(applycal.ApplycalInputs):

    # Property overrides
    applymode = basetask.property_with_default('applymode','calflag')

    # Would like to set this to False in future but this causes
    # an issue with the results handling.
    flagsum = basetask.property_with_default('flagsum', False)
    #flagbackup = basetask.property_with_default('flagbackup', False)

    """
    Input for the UVcontSub task
    """
    @basetask.log_equivalent_CASA_call
    def __init__(self, context, output_dir=None,
                 #
                 vis=None,
                 # data selection arguments
                 field=None, spw=None, antenna=None, intent=None,
                 # preapply calibrations
                 opacity=None, parang=None, applymode=None, calwt=None,
                 flagbackup=None, flagsum=None, flagdetailedsum=None):
        self._init_properties(vars())

    @property
    def intent(self):
        return self._intent

    @intent.setter
    def intent(self, value):
        if value is None:
            value = 'TARGET'
        self._intent = value.replace('*', '')


# Register this as an imaging MS(s) preferred task
basetask.ImagingMeasurementSetsPreferred.register(UVcontSubInputs)

class UVcontSub(applycal.Applycal):
    Inputs = UVcontSubInputs

    # Override prepare method with one which sets and unsets the VI1CAL
    # environment variable.
    def prepare(self):

        try:
            vi1cal =  os.environ['VI1CAL']
            vi1cal_was_unset = False
        except:
            os.environ['VI1CAL'] = '1'
            vi1cal_was_unset = True

        # Set cluster to serial mode for this applycal
        if infrastructure.mpihelpers.is_mpi_ready():
            from parallel.parallel_task_helper import ParallelTaskHelper
            ParallelTaskHelper.bypassParallelProcessing(1)

        results = super(UVcontSub, self).prepare()

        # Reset cluster to parallel mode
        if infrastructure.mpihelpers.is_mpi_ready():
           ParallelTaskHelper.bypassParallelProcessing(0)

        if vi1cal_was_unset:
            del os.environ['VI1CAL']

        return results

# May need this in the future
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
#            if type(caltable.gaintable) is types.ListType:
#                basenames = [os.path.basename(x) for x in caltable.gaintable]
#                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
#                    spw=caltable.spw, vis=os.path.basename(caltable.vis),
#                    name=','.join(basenames))
#            else:
#                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
#                    name=caltable.gaintable, spw=caltable.spw,
#                    vis=os.path.basename(caltable.vis))
#        return s
#
