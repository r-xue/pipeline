import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag

LOG = infrastructure.get_logger(__name__)


class PolcalflagResults(basetask.Results):
    def __init__(self):
        super(PolcalflagResults, self).__init__()
        self.cafresult = None

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'PolcalflagResults'


class PolcalflagInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis

@task_registry.set_equivalent_casa_task('hifa_polcalflag')
@task_registry.set_casa_commands_comment('Flag polarization calibrator outliers.')
class Polcalflag(basetask.StandardTaskTemplate):
    Inputs = PolcalflagInputs

    def prepare(self):

        inputs = self.inputs

        # Initialize results.
        result = PolcalflagResults()

        # Check for any polarization intents
        eb_intents = inputs.context.observing_run.get_ms(inputs.vis).intents
        if 'POLARIZATION' not in eb_intents and 'POLANGLE' not in eb_intents and 'POLLEAKAGE' not in eb_intents:
            LOG.info('No polarization intents found.')
            return result

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-polcalflag" flagging state')
        flag_backup_name_prepcf = 'before_pcflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_prepcf)
        self._executor.execute(task)

        # Ensure that any flagging applied to the MS by this applycal is
        # reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags
            LOG.info('Applying pre-existing cal tables.')
            acinputs = applycal.IFApplycalInputs(
                context=inputs.context, vis=inputs.vis, intent='POLARIZATION,POLANGLE,POLLEAKAGE', flagsum=False, flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            acresult = self._executor.execute(actask, merge=True)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify polarization calibrator outliers to flag.')

            # This task is called by the framework for each EB in the vis list.

            # Call correctedampflag for the polarization calibrator intent.
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                        context=inputs.context, vis=inputs.vis, intent='POLARIZATION,POLANGLE,POLLEAKAGE')
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

        finally:
            # Restore the "pre-polcalflag" backup of the flagging state.
            LOG.info('Restoring back-up of "pre-polcalflag" flagging state.')
            task = casa_tasks.flagmanager(
                vis=inputs.vis, mode='restore', versionname=flag_backup_name_prepcf)
            self._executor.execute(task)

        # Store correctedampflag result
        result.cafresult = cafresult

        # Get new flag commands
        cafflags = cafresult.flagcmds()

        # If new outliers were identified...
        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(
                context=inputs.context, vis=inputs.vis, table=inputs.vis,
                inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            fsresult = self._executor.execute(fstask)

        return result

    def analyse(self, results):
        return results
