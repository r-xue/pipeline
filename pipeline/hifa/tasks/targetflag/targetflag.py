import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, task_registry
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag

LOG = infrastructure.get_logger(__name__)


class TargetflagResults(basetask.Results):
    def __init__(self):
        super(TargetflagResults, self).__init__()
        self.cafresult = None

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'TargetflagResults'


class TargetflagInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifa_targetflag')
@task_registry.set_casa_commands_comment('Flag target source outliers.')
class Targetflag(basetask.StandardTaskTemplate):
    Inputs = TargetflagInputs

    def prepare(self):

        inputs = self.inputs

        # Initialize results.
        result = TargetflagResults()

        # Check for any polarization intents
        eb_intents = inputs.context.observing_run.get_ms(inputs.vis).intents
        if 'TARGET' not in eb_intents:
            LOG.info('No target intents found.')
            return result

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-targetflag" flagging state')
        flag_backup_name_pretgtf = 'before_tgtflag'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name_pretgtf)
        self._executor.execute(task)

        # Ensure that any flagging applied to the MS by this applycal are
        # reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags to the MS. Should typically include Tsys,
            # bandpass, and spwphaseup tables, as well as WVR if 12-m antennas
            # are present, and antpos if any position corrections were made.
            LOG.info('Applying pre-existing cal tables.')
            acinputs = applycal.IFApplycalInputs(
                context=inputs.context, vis=inputs.vis, intent='TARGET', flagsum=False, flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            acresult = self._executor.execute(actask, merge=True)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify target source outliers to flag.')

            # This task is called by the framework for each EB in the vis list.

            # Call correctedampflag for the target intent. For that intent it
            # will loop over spw and field IDs to inspect the flags individually
            # per mosaic pointing.
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent='TARGET')
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

        finally:
            # Restore the "pre-targetflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-targetflag" flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name_pretgtf)
            self._executor.execute(task)

        # Store correctedampflag result
        result.cafresult = cafresult

        # Get new flag commands
        cafflags = cafresult.flagcmds()

        # If new outliers were identified...
        if cafflags:
            # Re-apply the newly found flags from correctedampflag.
            LOG.info('Re-applying flags from correctedampflag.')
            fsinputs = FlagdataSetter.Inputs(context=inputs.context, vis=inputs.vis, table=inputs.vis, inpfile=[])
            fstask = FlagdataSetter(fsinputs)
            fstask.flags_to_set(cafflags)
            fsresult = self._executor.execute(fstask)

        return result

    def analyse(self, results):
        return results
