"""
Created on 01 Jun 2017

@author: Vincent Geers (UKATC)
"""
import functools
import os.path

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common.displays import applycal as applycal_displays
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import correctedampflag
from pipeline.hif.tasks import gaincal
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from .resultobjects import GfluxscaleflagResults

__all__ = [
    'GfluxscaleflagInputs',
    'GfluxscaleflagResults',
    'Gfluxscaleflag'
]

LOG = infrastructure.get_logger(__name__)


class GfluxscaleflagInputs(vdp.StandardInputs):
    """
    GfluxscaleflagInputs defines the inputs for the Gfluxscaleflag pipeline task.
    """
    # Lower sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # catchNegativeOutliers['scalardiff']
    antblnegsig = vdp.VisDependentProperty(default=3.4)

    # Upper sigma threshold for identifying outliers as a result of "bad
    # baselines" and/or "bad antennas" within baselines (across all
    # timestamps); equivalent to:
    # flag_nsigma['scalardiff']
    antblpossig = vdp.VisDependentProperty(default=3.2)

    # Lower sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # relaxationSigma
    antnegsig = vdp.VisDependentProperty(default=4.0)

    # Upper sigma threshold for identifying outliers as a result of bad
    # antennas within individual timestamps; equivalent to:
    # positiveSigmaAntennaBased
    antpossig = vdp.VisDependentProperty(default=4.6)

    @vdp.VisDependentProperty
    def field(self):
        # By default, return the fields corresponding to the input
        # intents.
        fieldids = [field.name
                    for field in self.ms.get_fields(intent=self.intent)]
        return ','.join(fieldids)

    @vdp.VisDependentProperty
    def intent(self):
        # By default, this task will run for AMPLITUDE, PHASE, and CHECK
        # intents.
        intents_to_flag = 'AMPLITUDE,PHASE,CHECK'

        # Check if any of the AMPLITUDE intent fields were also used for
        # BANDPASS, in which case it has already been flagged by
        # hifa_bandpassflag, and this task will just do PHASE and CHECK
        # fields. This assumes that there will only be 1 field for BANDPASS
        # and 1 field for AMPLITUDE (which can be the same), which is valid as
        # of Cycle 5.
        for field in self.ms.get_fields(intent='AMPLITUDE'):
            for fieldintent in field.intents:
                if 'BANDPASS' in fieldintent:
                    intents_to_flag = 'PHASE,CHECK'
        return intents_to_flag

    minsnr = vdp.VisDependentProperty(default=2.0)
    niter = vdp.VisDependentProperty(default=2)
    phaseupsolint = vdp.VisDependentProperty(default='int')
    refant = vdp.VisDependentProperty(default='')

    # Relaxed value to set the threshold scaling factor to under certain
    # conditions; equivalent to:
    # relaxationFactor
    relaxed_factor = vdp.VisDependentProperty(default=2.0)

    solint = vdp.VisDependentProperty(default='inf')

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(
            science_windows_only=True)
        return ','.join([str(spw.id) for spw in science_spws])

    # Threshold for maximum fraction of timestamps that are allowed
    # to contain outliers; equivalent to:
    # checkForAntennaBasedBadIntegrations
    tmantint = vdp.VisDependentProperty(default=0.063)

    # Initial threshold for maximum fraction of "bad baselines" over "all
    # baselines" that an antenna may be a part of; equivalent to:
    # tooManyBaselinesFraction
    tmbl = vdp.VisDependentProperty(default=0.175)

    # Initial threshold for maximum fraction of "outlier timestamps" over
    # "total timestamps" that a baseline may be a part of; equivalent to:
    # tooManyIntegrationsFraction
    tmint = vdp.VisDependentProperty(default=0.085)

    def __init__(self, context, output_dir=None, vis=None, intent=None, field=None, spw=None, solint=None,
                 phaseupsolint=None, minsnr=None, refant=None, antnegsig=None, antpossig=None, tmantint=None,
                 tmint=None, tmbl=None, antblnegsig=None, antblpossig=None, relaxed_factor=None, niter=None):
        super(GfluxscaleflagInputs, self).__init__()

        # pipeline inputs
        self.context = context
        # vis must be set first, as other properties may depend on it
        self.vis = vis
        self.output_dir = output_dir

        # data selection arguments
        self.field = field
        self.intent = intent
        self.spw = spw

        # gaincal parameters
        self.solint = solint
        self.phaseupsolint = phaseupsolint
        self.minsnr = minsnr
        self.refant = refant

        # flagging parameters
        self.antnegsig = antnegsig
        self.antpossig = antpossig
        self.tmantint = tmantint
        self.tmint = tmint
        self.tmbl = tmbl
        self.antblnegsig = antblnegsig
        self.antblpossig = antblpossig
        self.relaxed_factor = relaxed_factor
        self.niter = niter


@task_registry.set_equivalent_casa_task('hifa_gfluxscaleflag')
@task_registry.set_casa_commands_comment(
    'This task calls hif_correctedampflag to evaluate flagging heuristics on the phase calibrator and flux calibrator, '
    'looking for outlier visibility points by statistically examining the scalar difference of corrected amplitudes '
    'minus model amplitudes, and flagging those outliers.'
)
class Gfluxscaleflag(basetask.StandardTaskTemplate):
    Inputs = GfluxscaleflagInputs

    def prepare(self):
        inputs = self.inputs

        # Initialize results.
        result = GfluxscaleflagResults()

        # Store the vis in the result
        result.vis = inputs.vis
        result.plots = dict()

        # create a shortcut to the plotting function that pre-supplies the inputs and context
        plot_fn = functools.partial(create_plots, inputs, inputs.context)

        # Create back-up of flags.
        LOG.info('Creating back-up of "pre-gfluxscaleflag" flagging state')
        flag_backup_name_pregfsf = 'before_gfsflag'
        task = casa_tasks.flagmanager(
            vis=inputs.vis, mode='save', versionname=flag_backup_name_pregfsf)
        self._executor.execute(task)

        # Create phase caltable(s) and merge into the local context.
        self._do_phasecal()

        # Create amplitude caltable and merge it into the local context.
        # CAS-10491: for scan-based (solint='inf') amplitude solves that
        # will be applied to the calibrator, set interp to 'nearest'.
        LOG.info('Compute amplitude gaincal table.')
        amp_interp = 'nearest,linear' if inputs.solint == 'inf' else 'linear,linear'
        self._do_gaincal(intent=inputs.intent, gaintype='T', calmode='a', combine='', solint=inputs.solint,
                         minsnr=inputs.minsnr, refant=inputs.refant, interp=amp_interp, merge=True)

        # Ensure that any flagging applied to the MS by this applycal are
        # reverted at the end, even in the case of exceptions.
        try:
            # Apply the new caltables to the MS.
            LOG.info('Applying phase-up, bandpass, and amplitude cal tables.')
            # Apply the calibrations.
            callib_map = self._do_applycal(merge=False)
            # copy across the vis:callibrary dict to our result. This dict 
            # will be inspected by the renderer to know if/which callibrary
            # files should be copied across to the weblog stage directory
            result.callib_map.update(callib_map)

            # Make "after calibration, before flagging" plots for the weblog
            LOG.info('Creating "after calibration, before flagging" plots')
            result.plots['before'] = plot_fn(inputs.intent, suffix='before')

            # Run correctedampflag to identify outliers for intents specified in
            # intents_for_flagging; let "field" and "spw" be initialized
            # automatically based on intents and context.
            LOG.info('Running correctedampflag to identify outliers to flag.')
            cafinputs = correctedampflag.Correctedampflag.Inputs(
                context=inputs.context, vis=inputs.vis, intent=inputs.intent,
                antnegsig=inputs.antnegsig, antpossig=inputs.antpossig,
                tmantint=inputs.tmantint, tmint=inputs.tmint, tmbl=inputs.tmbl,
                antblnegsig=inputs.antblnegsig, antblpossig=inputs.antblpossig,
                relaxed_factor=inputs.relaxed_factor, niter=inputs.niter)
            caftask = correctedampflag.Correctedampflag(cafinputs)
            cafresult = self._executor.execute(caftask)

            # If flags were found in the bandpass calibrator...
            cafflags = cafresult.flagcmds()
            if cafflags:
                # Create the "after calibration, after flagging" plots for the weblog.
                LOG.info('Creating "after calibration, after flagging" plots')
                result.plots['after'] = plot_fn(inputs.intent, suffix='after')

        finally:
            # Restore the "pre-gfluxscaleflag" backup of the flagging state, to
            # undo any flags that were propagated from caltables to the MS by
            # the applycal call.
            LOG.info('Restoring back-up of "pre-gfluxscaleflag" flagging state.')
            task = casa_tasks.flagmanager(
                vis=inputs.vis, mode='restore', versionname=flag_backup_name_pregfsf)
            self._executor.execute(task)

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

        # Store flagging task result.
        result.cafresult = cafresult

        return result

    def analyse(self, result):
        return result

    def _do_applycal(self, merge):
        inputs = self.inputs

        # SJW - always just one job
        ac_intents = [inputs.intent]

        applycal_tasks = []
        for intent in ac_intents:
            task_inputs = applycal.IFApplycalInputs(inputs.context, vis=inputs.vis, intent=intent, flagsum=False,
                                                    flagbackup=False)
            task = applycal.IFApplycal(task_inputs)
            applycal_tasks.append(task)

        callib_map = {}
        for task in applycal_tasks:
            applycal_result = self._executor.execute(task, merge=merge)
            callib_map.update(applycal_result.callib_map)

        return callib_map

    def _do_gaincal(self, caltable=None, intent=None, gaintype='G',
                    calmode=None, combine=None, solint=None, antenna=None,
                    uvrange='', minsnr=None, refant=None, minblperant=None,
                    spwmap=None, interp=None, append=None, merge=True):
        inputs = self.inputs
        ms = inputs.ms

        # Get the science spws
        request_spws = ms.get_spectral_windows(task_arg=inputs.spw)
        targeted_scans = ms.get_scans(scan_intent=intent, spw=inputs.spw)

        # boil it down to just the valid spws for these fields and request
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}

        for spectral_spec, tuning_spw_ids in utils.get_spectralspec_to_spwid_map(scan_spws).items():
            tuning_spw_str = ','.join([str(i) for i in sorted(tuning_spw_ids)])
            LOG.info('Processing spectral spec {}, spws {}'.format(spectral_spec, tuning_spw_str))

            scans_with_data = ms.get_scans(spw=tuning_spw_str, scan_intent=intent)
            if not scans_with_data:
                LOG.info('No data to process for spectral spec {}. Continuing...'.format(spectral_spec))
                continue

            # of the fields that we are about to process, does any field have
            # multiple intents?
            mixed_intents = False
            fields_in_scans = {fld for scan in scans_with_data for fld in scan.fields}
            singular_intents = frozenset(intent.split(','))
            if len(singular_intents) > 1:
                for field in fields_in_scans:
                    intents_to_scans = {si: ms.get_scans(scan_intent=si, field=field.id, spw=tuning_spw_str)
                                        for si in singular_intents}
                    valid_intents = [k for k, v in intents_to_scans.items() if v]
                    if len(valid_intents) > 1:
                        mixed_intents = True
                        break

            if mixed_intents and solint == 'inf':
                # multiple items, one for each intent. Each intent will result
                # in a separate job.
                # Make sure data for the intent exists (PIPE-367)
                intents_to_scans = {si: [scan for scan in scans_with_data if si in scan.intents]
                                    for si in singular_intents}
                valid_intents = [k for k, v in intents_to_scans.items() if v]
                task_intents = valid_intents

            else:
                # one item, and hence one job, with 'PHASE,BANDPASS,...'
                task_intents = [','.join(singular_intents)]

            for intent in task_intents:
                # Initialize gaincal inputs.
                task_inputs = gaincal.GTypeGaincal.Inputs(
                    inputs.context,
                    vis=inputs.vis,
                    caltable=caltable,
                    intent=intent,
                    spw=tuning_spw_str,
                    solint=solint,
                    gaintype=gaintype,
                    calmode=calmode,
                    minsnr=minsnr,
                    combine=combine,
                    refant=refant,
                    antenna=antenna,
                    uvrange=uvrange,
                    minblperant=minblperant,
                    solnorm=False,
                    append=append)

                # if we need to generate multiple caltables, make the caltable
                # names unique by inserting the intent to prevent them overwriting
                # each other
                if len(task_intents) > 1:
                    root, ext = os.path.splitext(task_inputs.caltable)
                    task_inputs.caltable = '{!s}.{!s}{!s}'.format(root, intent, ext)

                # Modify output table filename to append "prelim".
                if task_inputs.caltable.endswith('.tbl'):
                    task_inputs.caltable = task_inputs.caltable[:-4] + '.prelim.tbl'
                else:
                    task_inputs.caltable += '.prelim'

                # Initialize and execute gaincal task.
                task = gaincal.GTypeGaincal(task_inputs)
                result = self._executor.execute(task)

                # modify the result so that this caltable is only applied to
                # the intent from which the calibration was derived
                calapp_overrides = dict(intent=intent)

                # Adjust the interp if provided.
                if interp:
                    calapp_overrides['interp'] = interp

                # Adjust the spw map if provided.
                if spwmap:
                    calapp_overrides['spwmap'] = spwmap

                # https://open-jira.nrao.edu/browse/PIPE-367?focusedCommentId=141097&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-141097
                #
                # '... then to apply this table you need the same spw map that
                # is printed in the spwphaseup stage'
                if combine == 'spw':
                    calapp_overrides['spwmap'] = ms.combine_spwmap

                # Create modified CalApplication and replace CalApp in result
                # with this new one.
                original_calapp = result.final[0]
                modified_calapp = callibrary.copy_calapplication(original_calapp, **calapp_overrides)
                result.pool[0] = modified_calapp
                result.final[0] = modified_calapp

                # If requested, merge the result...
                if merge:
                    # Merge result to the local context
                    result.accept(inputs.context)

    def _do_phasecal(self):
        inputs = self.inputs

        # Determine the parameters to use for the gaincal to create the
        # phase-only caltable.
        # If a non-empty combine spw mapping is defined
        # then use spw combination with corresponding map and interpolation.
        if inputs.ms.combine_spwmap:
            phase_combine = 'spw'
            phaseup_spwmap = inputs.ms.combine_spwmap
            phase_interp = 'linearPD,linear'
            # Note: at present, phaseupsolint is specified as a fixed
            # value, defined in inputs. In the future, phaseupsolint may
            # need to be set based on exposure times; if so, see discussion
            # in CAS-10158 and logic in hifa.tasks.fluxscale.GcorFluxscale.

        # If no valid combine spw map was defined, then use regular spw mapping
        # using the phase up spw map, without any interpolation.
        else:
            phase_combine = ''
            phaseup_spwmap = inputs.ms.phaseup_spwmap
            phase_interp = None

        # Create phase caltable and merge it into the local context.
        LOG.info('Compute phase gaincal table.')
        self._do_gaincal(
            intent=inputs.intent, gaintype='G', calmode='p',
            combine=phase_combine, solint=inputs.phaseupsolint,
            minsnr=inputs.minsnr, refant=inputs.refant,
            spwmap=phaseup_spwmap, interp=phase_interp,
            merge=True)


def create_plots(inputs, context, intents, suffix=''):
    """
    Return amplitude vs time and amplitude vs UV distance plots for the given
    intents.

    :param inputs: pipeline inputs
    :param context: pipeline context
    :param intents: intents to plot
    :param suffix: optional component to add to the plot filenames
    :return: dict of (x axis type => str, [plots,...])
    """
    # Exit early if weblog generation has been disabled
    if basetask.DISABLE_WEBLOG:
        return [], []

    calto = callibrary.CalTo(vis=inputs.vis, spw=inputs.spw, field=inputs.field)
    output_dir = context.output_dir

    amp_uvdist_plots, amp_time_plots = [], []
    for intent in intents.split(','):
        amp_uvdist_plots.extend(
            AmpVsXChart('uvdist', intent, context, output_dir, calto, suffix=suffix).plot())
        amp_time_plots.extend(
            AmpVsXChart('time', intent, context, output_dir, calto, suffix=suffix).plot())

    return {
        'uvdist': amp_uvdist_plots,
        'time': amp_time_plots
    }


class AmpVsXChart(applycal_displays.PlotmsFieldSpwComposite):
    """
    Plotting class that creates an amplitude vs X plot for each field and spw,
    where X is given as a constructor argument.
    """
    def __init__(self, xaxis, intent, context, output_dir, calto, **overrides):
        plot_args = {
            'ydatacolumn': 'corrected',
            'avgtime': '',
            'avgscan': False,
            'avgbaseline': False,
            'avgchannel': '9000',
            'coloraxis': 'corr',
            'overwrite': True,
            'plotrange': [0, 0, 0, 0]
        }
        plot_args.update(**overrides)

        super(AmpVsXChart, self).__init__(context, output_dir, calto, xaxis=xaxis, yaxis='amp', intent=intent,
                                          **plot_args)
