from __future__ import absolute_import
import os
import types

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure import casa_tasks
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils

from pipeline.hif.heuristics import fieldnames

LOG = infrastructure.get_logger(__name__)


class ApplycalInputs(basetask.StandardInputs,
                     basetask.OnTheFlyCalibrationMixin):
    """
    ApplycalInputs defines the inputs for the Applycal pipeline task.
    """
    def __init__(self, context, output_dir=None,
                 # 
                 vis=None, 
                 # data selection arguments
                 field=None, spw=None, antenna=None, intent=None,
                 # preapply calibrations
                 opacity=None, parang=None, calwt=None,
                 flagbackup=None):
        self._init_properties(vars())

    @property
    def antenna(self):
        if self._antenna is not None:
            return self._antenna
        
        if type(self.vis) is types.ListType:
            return self._handle_multiple_vis('antenna')

        antennas = self.ms.get_antenna(self._antenna)
        return ','.join([str(a.id) for a in antennas])

    @antenna.setter
    def antenna(self, value):
        if value is None:
            value = ''
        self._antenna = value

    @property
    def calstate(self):
        if type(self.vis) is types.ListType:
            return self._handle_multiple_vis('calstate')
        
        # The default calstate property implementation merges states that are 
        # identical apart from calwt. As applycal is a task that can handle
        # calwt, request that jobs not be merged when calwt differs by supplying
        # a null 'ignore' argument - or as in this case - omitting it.   
        return self.context.callibrary.get_calstate(self.calto)

    @property
    def flagbackup(self):
        return self._flagbackup

    @flagbackup.setter
    def flagbackup(self, value):
        if value is None:
            value = True
        elif value:
            value = True
        else:
            value = False
        self._flagbackup = value

    @property
    def calwt(self):
        return self._calwt

    @calwt.setter
    def calwt(self, value):
        if value is None:
            value = True
        elif value:
            value = True
        else:
            value = False
        self._calwt = value

    @property
    def field(self):
        if not callable(self._field):
            return self._field

        if type(self.vis) is types.ListType:
            return self._handle_multiple_vis('field')

        # this will give something like '0542+3243,0343+242'
        intent_fields = self._field(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))
        
        return ','.join(fields)

    @field.setter
    def field(self, value):
        if value is None:
            value = fieldnames.IntentFieldnames()
        self._field = value

    @property
    def intent(self):
        return self._intent

    @intent.setter
    def intent(self, value):
        if value is None:
            value = 'TARGET,PHASE,BANDPASS,AMPLITUDE'
        self._intent = value.replace('*', '')

    @property
    def spw(self):
        if self._spw is not None:
            return self._spw
        
        if type(self.vis) is types.ListType:
            return self._handle_multiple_vis('spw')

        science_spws = self.ms.get_spectral_windows(self._spw, with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @spw.setter
    def spw(self, value):
        self._spw = value


class ApplycalResults(basetask.Results):
    """
    ApplycalResults is the results class for the pipeline Applycal task.     
    """
    
    def __init__(self, applied=[]):
        """
        Construct and return a new ApplycalResults.
        
        The resulting object should be initialized with a list of
        CalibrationTables corresponding to the caltables applied by this task.

        :param applied: caltables applied by this task
        :type applied: list of :class:`~pipeline.domain.caltable.CalibrationTable`
        """
        super(ApplycalResults, self).__init__()
        self.applied = set()
        self.applied.update(applied)

    def merge_with_context(self, context):
        """
        Merges these results with the given context by examining the context
        and marking any applied caltables, so removing them from subsequent
        on-the-fly calibration calculations.

        See :method:`~pipeline.Results.merge_with_context`
        """
        if not self.applied:
            LOG.error('No results to merge')

        for calapp in self.applied:
            LOG.trace('Marking %s as applied' % calapp.as_applycal())
            context.callibrary.mark_as_applied(calapp.calto, calapp.calfrom)

    def __repr__(self):
        for caltable in self.applied:
            s = 'ApplycalResults:\n'
            if type(caltable.gaintable) is types.ListType:
                basenames = [os.path.basename(x) for x in caltable.gaintable]
                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
                    spw=caltable.spw, vis=os.path.basename(caltable.vis),
                    name=','.join(basenames))
            else:
                s += '\t{name} applied to {vis} spw #{spw}\n'.format(
                    name=caltable.gaintable, spw=caltable.spw, 
                    vis=os.path.basename(caltable.vis))
        return s


class Applycal(basetask.StandardTaskTemplate):
    """
    Applycal executes CASA applycal tasks for the current context state,
    applying calibrations registered with the pipeline context to the target
    measurement set.
    
    Applying the results from this task to the context marks the referred
    tables as applied. As a result, they will not be included in future
    on-the-fly calibration arguments.
    """
    Inputs = ApplycalInputs
    
    def prepare(self):
        inputs = self.inputs

        # Get the calibration state for the user's target data selection. This
        # dictionary of CalTo:CalFroms gives us which calibrations should be
        # applied and how.
        merged = inputs.calstate.merged()
        
        jobs = []
        for calto, calfroms in merged.items():
            # arrange a calibration job for the unique data selection
            inputs.spw = calto.spw
            inputs.field = calto.field
            inputs.intent = calto.intent
            # patience, grasshopper..
#            inputs.antenna = calto.antenna

            args = inputs.to_casa_args()

            # set the on-the-fly calibration state for the data selection.  
            calapp = callibrary.CalApplication(calto, calfroms)
            args['gaintable'] = calapp.gaintable
            args['gainfield'] = calapp.gainfield
            args['spwmap']    = calapp.spwmap
            args['interp']    = calapp.interp
            args['calwt']     = calapp.calwt
            
            jobs.append(casa_tasks.applycal(**args))

        # execute the jobs
        for job in jobs:
            self._executor.execute(job)

        applied = [callibrary.CalApplication(calto, calfroms) 
                   for calto, calfroms in merged.items()]

        result = ApplycalResults(applied)

        return result

    def analyse(self, result):
        return result
