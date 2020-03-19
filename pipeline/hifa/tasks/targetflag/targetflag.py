import os

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
        self.cafresults = []

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        return 'TargetflagResults:'


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

        # Initialize correctedampflag result dictionaries
        cafresults = {}

        # Create back-up of current calibration state.
        LOG.info('Creating back-up of calibration state')
        calstate_backup_name = 'before_tgtflag.calstate'
        inputs.context.callibrary.export(calstate_backup_name)

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-targetflag" flagging state')
        flag_backup_name_pretgtf = 'before_tgtflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_pretgtf)
        self._executor.execute(task)

        # Ensure that any pre-applycal and flagging applied to the MS by this
        # applycal are reverted at the end, even in the case of exceptions.
        try:
            # Run applycal to apply pre-existing caltables and propagate their
            # corresponding flags (should typically include Tsys, WVR, antpos).
            LOG.info('Applying pre-existing cal tables.')
            acinputs = applycal.IFApplycalInputs(
                context=inputs.context, vis=inputs.vis,
                intent='TARGET', flagsum=False, flagbackup=False)
            actask = applycal.IFApplycal(acinputs)
            acresult = self._executor.execute(actask, merge=True)

            # Find amplitude outliers and flag data. This needs to be done
            # per source / per field ID / per spw basis.
            LOG.info('Running correctedampflag to identify target source outliers to flag.')
            # This task is called by the framework for each EB in the vis list.
            # Loop here over sources, field names and spws and collect the
            # flags per data selection. The result objects are collected in
            # in a list.

            # MS domain object
            ms_do = inputs.context.observing_run.get_ms(inputs.vis)

            # Target source names (assumes ALMA setup)
            field_names = set([f.name for f in ms_do.fields if 'TARGET' in f.intents])
            # Real science spw IDs
            spw_ids = [s.id for s in ms_do.get_spectral_windows()]

            for field_name in field_names:
                for spw_id in spw_ids:
                    # Do not stop on individual issues for a data selection (?)
                    try:
                        # Call correctedampflag per field name. Inside that
                        # task there is a loop over field IDs to inspect the
                        # flags individually per mosaic pointing.
                        cafinputs = correctedampflag.Correctedampflag.Inputs(
                                    context=inputs.context,
                                    vis=inputs.vis, intent='TARGET',
                                    field=field_name, spw=str(spw_id))
                        caftask = correctedampflag.Correctedampflag(cafinputs)
                        cafresult = self._executor.execute(caftask)
                        # Save result
                        cafresults[(field_name, spw_id)] = cafresult
                    except Exception as e:
                        LOG.warning(f'{e}')

        finally:
            # Restore the calibration state
            LOG.info('Restoring back-up of calibration state.')
            inputs.context.callibrary.import_state(calstate_backup_name)

            # Restore the "pre-targetflag" backup of the flagging state.
            LOG.info('Restoring back-up of "pre-targetflag" flagging state.')
            task = casa_tasks.flagmanager(
                vis=inputs.vis, mode='restore', versionname=flag_backup_name_pretgtf)
            self._executor.execute(task)

        # Store all correctedampflag results
        result.cafresults = cafresults

        # Collect all new flag commands
        cafflags = []
        for field_name in field_names:
            for spw_id in spw_ids:
                cafflags.extend(cafresults[(field_name, spw_id)].flagcmds())

        # If new outliers were identified...
        if cafflags != []:
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
