import collections
import os
from typing import Optional

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hifa.heuristics.phasespwmap import combine_spwmap
from pipeline.hifa.heuristics.phasespwmap import simple_n2wspwmap
from pipeline.hifa.heuristics.phasespwmap import snr_n2wspwmap
from pipeline.hifa.tasks.gaincalsnr import gaincalsnr
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'SpwPhaseupInputs',
    'SpwPhaseup',
    'SpwPhaseupResults'
]

IntentField = collections.namedtuple('IntentField', 'intent field')
SpwMapping = collections.namedtuple('SpwMapping', 'combine spwmap low_combinedsnr_spws')


class SpwPhaseupInputs(gtypegaincal.GTypeGaincalInputs):
    # Spw mapping mode heuristics, options are:
    #  'auto': apply SNR-based heuristics to determine which type of spw
    #          mapping / combination is appropriate.
    #  'combine': force use of combined spw mapping
    #  'simple': force use of simple narrow-to-wide spw mapping.
    #  'default': use the standard mapping, mapping each spw to itself.
    hm_spwmapmode = vdp.VisDependentProperty(default='auto')

    @hm_spwmapmode.convert
    def hm_spwmapmode(self, value):
        allowed = ('auto', 'combine', 'simple', 'default')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    # Fraction of bandwidth to ignore on each edge of a spectral window
    # for SNR assessment.
    bwedgefrac = vdp.VisDependentProperty(default=0.03125)

    # Maximum fraction of data that can be flagged for an antenna before that
    # antenna is considered "flagged" and no longer considered in SNR
    # estimation.
    maxfracflagged = vdp.VisDependentProperty(default=0.90)

    # Maximum narrow bandwidth.
    maxnarrowbw = vdp.VisDependentProperty(default='300MHz')

    # Width of spw must be larger than minfracmaxbw * maximum bandwith for
    # a spw to be a match.
    minfracmaxbw = vdp.VisDependentProperty(default=0.8)

    # Phase SNR threshold to use in spw mapping assessment to identify low
    # SNR spws.
    phasesnr = vdp.VisDependentProperty(default=32.0)

    # Toggle to select whether to restrict spw matching to the same baseband.
    samebb = vdp.VisDependentProperty(default=True)

    # Antenna flagging heuristics parameter
    hm_nantennas = vdp.VisDependentProperty(default='all')

    @hm_nantennas.convert
    def hm_nantennas(self, value):
        allowed = ('all', 'unflagged')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    # Allow user to set custom filename for the phase offsets caltable.
    caltable = vdp.VisDependentProperty(default=None)

    # Intent to use in data selection for computing phase offsets caltable.
    intent = vdp.VisDependentProperty(default='BANDPASS')

    # PIPE-629: parameter to unregister existing phaseup tables before appending to callibrary
    unregister_existing = vdp.VisDependentProperty(default=False)

    def __init__(self, context, vis=None, output_dir=None, caltable=None, intent=None, hm_spwmapmode=None,
                 phasesnr=None, bwedgefrac=None, hm_nantennas=None, maxfracflagged=None,
                 maxnarrowbw=None, minfracmaxbw=None, samebb=None, unregister_existing=None, **parameters):
        super(SpwPhaseupInputs, self).__init__(context, vis=vis, output_dir=output_dir, **parameters)
        self.caltable = caltable
        self.intent = intent
        self.hm_spwmapmode = hm_spwmapmode
        self.phasesnr = phasesnr
        self.bwedgefrac = bwedgefrac
        self.hm_nantennas = hm_nantennas
        self.maxfracflagged = maxfracflagged
        self.maxnarrowbw = maxnarrowbw
        self.minfracmaxbw = minfracmaxbw
        self.samebb = samebb
        self.unregister_existing = unregister_existing


@task_registry.set_equivalent_casa_task('hifa_spwphaseup')
class SpwPhaseup(gtypegaincal.GTypeGaincal):
    Inputs = SpwPhaseupInputs

    def prepare(self, **parameters):
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # PIPE-629: if requested, unregister old spwphaseup calibrations from
        # local copy of context, to stop these from being pre-applied during
        # this stage.
        if inputs.unregister_existing:
            self._unregister_spwphaseup()

        # Derive the optimal spectral window maps.
        spwmaps = self._derive_spwmaps()

        # Compute the spw-to-spw phase offsets ("phaseup") cal table.
        LOG.info('Computing spw phaseup table for {}'.format(inputs.ms.basename))
        phaseupresult = self._do_phaseup()

        # Create the results object.
        result = SpwPhaseupResults(vis=inputs.vis, phaseup_result=phaseupresult, spwmaps=spwmaps,
                                   unregister_existing=inputs.unregister_existing)

        return result

    def analyse(self, result):
        # The caltable portion of the result is treated as if it were any other
        # calibration result. With no best caltable to find, our task is simply
        # to set the one caltable as the best result.

        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.phaseup_result.pool if ca.exists() or self._executor._dry_run]
        result.phaseup_result.final[:] = on_disk

        missing = [ca for ca in result.phaseup_result.pool if ca not in on_disk and not self._executor._dry_run]
        result.phaseup_result.error.clear()
        result.phaseup_result.error.update(missing)

        return result

    def _derive_spwmaps(self):
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # Report which spw mapping heuristics mode is being used.
        LOG.info(f"The spw mapping mode for {inputs.ms.basename} is \"{inputs.hm_spwmapmode}\"")

        # Initialize collection of spectral window maps.
        spwmaps = {}

        # Intents to derive separate spwmaps for:
        spwmap_intents = 'CHECK,PHASE'

        # Do not derive separate spwmsp for fields that also cover any of these
        # calibrator intents:
        exclude_intents = 'AMPLITUDE,BANDPASS,POLARIZATION,POLANGLE,POLLEAKAGE'

        # Identify the combinations of intent and fields for which to derive a
        # separate spwmap.
        intent_field_to_assess = self._get_intent_field(inputs.ms, intents=spwmap_intents,
                                                        exclude_intents=exclude_intents)

        # Run derivation of spwmap for each intent, field combination.
        for intent, field in intent_field_to_assess:
            LOG.info(f'Deriving optimal spw mapping for {inputs.ms.basename}, intent={intent}, field={field}')
            spwmaps[IntentField(intent, field)] = self._derive_spwmap_for_intent_field(intent, field)

        return spwmaps

    def _derive_spwmap_for_intent_field(self, intent, field):
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # Get a list of the science spws.
        scispws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=True)

        # By default, combine is False (i.e., no combining of spws), the
        # spwmap is empty (i.e. no mapping of spws), and there are no spws
        # whose combined phase SNR does not meet the threshold set by the
        # inputs.phasesnr parameter.
        combine = False
        spwmap = []
        low_combinedsnr_spws = []

        # Compute the spw map according to the rules defined by each
        # mapping mode.
        if inputs.hm_spwmapmode == 'auto':

            # Run a task to estimate the gaincal SNR for given intent and field.
            nosnrs, spwids, snrs, goodsnrs = self._do_snrtest(intent, field)

            # No SNR estimates available, default to simple narrow-to-wide spw
            # mapping.
            if nosnrs:
                LOG.warn(f'No SNR estimates for any spws - Forcing simple spw mapping for'
                         f' {inputs.ms.basename}, intent={intent}, field={field}')
                spwmap = simple_n2wspwmap(scispws, inputs.maxnarrowbw, inputs.minfracmaxbw, inputs.samebb)
                LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

            # All spws have good SNR values, no spw mapping required.
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == len(goodsnrs):
                LOG.info(f'High SNR - Default spw mapping used for all spws {inputs.ms.basename}')
                LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

            # No spws have good SNR values, use combined spw mapping, and test
            # which spws have too low combined phase SNR.
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == 0:
                LOG.warn(f'Low SNR for all spws - Forcing combined spw mapping for {inputs.ms.basename}')

                # Report spws for which no SNR estimate was available.
                if None in goodsnrs:
                    LOG.warn('Spws without SNR measurements {}'
                             ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))

                # Create a spw mapping for combining spws.
                spwmap = combine_spwmap(scispws)
                combine = True
                LOG.info(f'Using combined spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

                # Run a test on the combined spws to identify spws whose
                # combined SNR would not meet the threshold set by
                # inputs.phasesnr.
                low_combinedsnr_spws = self._do_combined_snr_test(spwids, snrs, spwmap)

            # If some, but not all, spws have good SNR values, then try to use
            # an SNR-based approach for, but fall back to combined spw mapping
            # if necessary.
            else:
                LOG.warn(f'Some low SNR spws - using highest good SNR window for these in {inputs.ms.basename}')

                # Report spws for which no SNR estimate was available.
                if None in goodsnrs:
                    LOG.warn('Spws without SNR measurements {}'
                             ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))

                # Create an SRN-based spw mapping.
                goodmap, spwmap, snrmap = snr_n2wspwmap(scispws, snrs, goodsnrs)

                # If the SNR-based mapping gave a good match for all spws, then
                # report the final phase up spw map.
                if goodmap:
                    LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

                # Otherwise, replace the spwmap with a combined spw map.
                else:
                    LOG.warn(f'Still unable to match all spws - Forcing combined spw mapping for {inputs.ms.basename}')

                    # Create a spw mapping for combining spws.
                    spwmap = combine_spwmap(scispws)
                    combine = True
                    LOG.info(f'Using combined spw map {spwmap} for'
                             f' {inputs.ms.basename}, intent={intent}, field={field}')

                    # Run a test on the combined spws to identify spws whose
                    # combined SNR would not meet the threshold set by
                    # inputs.phasesnr.
                    low_combinedsnr_spws = self._do_combined_snr_test(spwids, snrs, spwmap)

        elif inputs.hm_spwmapmode == 'combine':
            spwmap = combine_spwmap(scispws)
            combine = True
            low_combinedsnr_spws = scispws
            LOG.info(f'Using combined spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        elif inputs.hm_spwmapmode == 'simple':
            spwmap = simple_n2wspwmap(scispws, inputs.maxnarrowbw, inputs.minfracmaxbw, inputs.samebb)
            LOG.info(f'Using simple spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        else:
            LOG.info(f'Using standard spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        return SpwMapping(combine, spwmap, low_combinedsnr_spws)

    def _do_snrtest(self, intent, field):

        # Simplify inputs.
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'field': field,
            'intent': intent,
            'phasesnr': inputs.phasesnr,
            'bwedgefrac': inputs.bwedgefrac,
            'hm_nantennas': inputs.hm_nantennas,
            'maxfracflagged': inputs.maxfracflagged,
            'spw': inputs.spw
        }
        task_inputs = gaincalsnr.GaincalSnrInputs(inputs.context, **task_args)
        gaincalsnr_task = gaincalsnr.GaincalSnr(task_inputs)
        result = self._executor.execute(gaincalsnr_task)

        nosnr = True
        spwids = []
        snrs = []
        goodsnrs = []
        for i in range(len(result.spwids)):
            if result.snrs[i] is None:
                spwids.append(result.spwids[i])
                snrs.append(None)
                goodsnrs.append(None)
            elif result.snrs[i] < inputs.phasesnr:
                spwids.append(result.spwids[i])
                snrs.append(result.snrs[i])
                goodsnrs.append(False)
                nosnr = False
            else:
                spwids.append(result.spwids[i])
                goodsnrs.append(True)
                snrs.append(result.snrs[i])
                nosnr = False

        return nosnr, spwids, snrs, goodsnrs

    def _do_combined_snr_test(self, spwlist, perspwsnr, spwmap):
        """
        Calculate combined SNRs from the "per-SpW SNR" and return a list of SpW
        IDs that does not meet phase SNR threshold. Grouping of SpWs is
        specified by input parameter spwmap.

        For each grouped SpWs, combined SNR is calculated by:
            combined SNR = numpy.linalg.nrom(list of per SpW SNR in a group)

        Args:
            spwlist: List of spw IDs to calculate combined SNR
            perspwsnr: List of SNRs of each SpW
            spwmap: A spectral window map that specifies which SpW IDs should
                be combined together.

        Returns:
            List of spectral window IDs whose combined phase SNR is below the
            threshold specified by inputs.phasesnr.
        """
        LOG.info("Start combined SpW SNR test")
        LOG.debug('- spwlist to analyze: {}'.format(spwlist))
        LOG.debug('- per SpW SNR: {}'.format(perspwsnr))
        LOG.debug('- spwmap = {}'.format(spwmap))

        # Initialize return list.
        low_snr_spwids = []

        # Filter reference SpW IDs of each group.
        unique_mappedspw = {spwmap[spwid] for spwid in spwlist}
        for mappedspwid in unique_mappedspw:
            snrlist = []
            combined_idx = []

            # only consider SpW IDs in spwlist for combination
            for i in range(len(spwlist)):
                spwid = spwlist[i]
                if spwmap[spwid] == mappedspwid:
                    snr = perspwsnr[i]
                    if snr is None:
                        LOG.error('SNR not calculated for spw={}. Cannot calculate combined SNR'.format(spwid))
                        return False, [], [], []
                    snrlist.append(perspwsnr[i])
                    combined_idx.append(i)

            # calculate combined SNR from per spw SNR
            combined_snr = numpy.linalg.norm(snrlist)
            LOG.info('Reference SpW ID = {} (Combined SpWs = {}) : Combined SNR = {}'
                     ''.format(mappedspwid, str([spwlist[j] for j in combined_idx]), combined_snr))

            # If the combined SNR does not meet the phase SNR threshold, then
            # add these to the list of low combined SNR spws.
            if combined_snr < self.inputs.phasesnr:
                low_snr_spwids.extend([spwlist[i] for i in combined_idx])

        # Log results from SNR test.
        LOG.info('SpW IDs that has low combined SNR (threshold: {}) = {}'.format(self.inputs.phasesnr, low_snr_spwids))

        return low_snr_spwids

    def _do_phaseup(self):
        inputs = self.inputs
        ms = inputs.ms

        # Get the science spws
        request_spws = ms.get_spectral_windows(task_arg=inputs.spw)
        targeted_scans = ms.get_scans(scan_intent=inputs.intent, spw=inputs.spw)

        # boil it down to just the valid spws for these fields and request
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}

        # For first SpectralSpec, create a new caltable.
        append = False

        original_calapps = []
        for spectral_spec, tuning_spw_ids in utils.get_spectralspec_to_spwid_map(scan_spws).items():
            tuning_spw_str = ','.join([str(i) for i in sorted(tuning_spw_ids)])
            LOG.info('Processing spectral spec {}, spws {}'.format(spectral_spec, tuning_spw_str))

            scans_with_data = ms.get_scans(spw=tuning_spw_str, scan_intent=inputs.intent)
            if not scans_with_data:
                LOG.info('No data to process for spectral spec {}. Continuing...'.format(spectral_spec))
                continue

            task_args = {
                'output_dir': inputs.output_dir,
                'vis': inputs.vis,
                'caltable': inputs.caltable,
                'field': inputs.field,
                'intent': inputs.intent,
                'spw': tuning_spw_str,
                'solint': 'inf',
                'gaintype': 'G',
                'calmode': 'p',
                'minsnr': inputs.minsnr,
                'combine': inputs.combine,
                'refant': inputs.refant,
                'minblperant': inputs.minblperant,
                'append': append
            }
            task_inputs = gtypegaincal.GTypeGaincalInputs(inputs.context, **task_args)
            phaseup_task = gtypegaincal.GTypeGaincal(task_inputs)
            tuning_result = self._executor.execute(phaseup_task)
            original_calapps.extend(tuning_result.pool)
            # For subsequent SpectralSpecs, append calibration to existing
            # caltable.
            append = True

        # Created an updated version of each CalApplication with an override to
        # set calwt to False. Replace any existing CalApplications in latest
        # tuning result with complete list of all updated CalApplications, and
        # return this as the final result.
        processed_calapps = [callibrary.copy_calapplication(c, calwt=False) for c in original_calapps]
        tuning_result.pool = processed_calapps
        tuning_result.final = processed_calapps

        return tuning_result

    @staticmethod
    def _get_intent_field(ms, intents, exclude_intents=None):
        # If provided, convert "intents to exclude" into set of strings.
        if exclude_intents is not None:
            exclude_intents = set(exclude_intents.split(','))

        intent_field = []
        for intent in intents.split(','):
            for field in ms.get_fields(intent=intent):
                # Check whether found field also covers any of the intents to
                # skip.
                excluded_intents_found = field.intents.intersection(exclude_intents)
                if not excluded_intents_found:
                    intent_field.append((intent, field.name))
                else:
                    # Log a message to explain why no spwmap will be derived
                    # for this particular combination of field and intent.
                    excluded_intents_str = ", ".join(sorted(excluded_intents_found))
                    LOG.info(f'{ms.basename}: will not derive spwmap for field {field.name} (#{field.id}) and intent'
                             f' {intent} because this field also covers calibrator intent(s) {excluded_intents_str}')

        return intent_field

    def _unregister_spwphaseup(self):
        inputs = self.inputs
        ms = inputs.ms

        # predicate function that triggers when the spwphaseup caltable is detected
        def spwphaseup_matcher(_: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            return 'hifa_spwphaseup' in calfrom.gaintable

        LOG.info('Temporarily unregistering previous spwphaseup tables while task executes')
        inputs.context.callibrary.unregister_calibrations(spwphaseup_matcher)

        # Reset the spwmaps registered in the MS. This will be restored if the
        # result is not accepted.
        LOG.info('Temporarily resetting spwmaps for %s', ms.basename)
        ms.spwmaps = {}


class SpwPhaseupResults(basetask.Results):
    def __init__(self, vis=None, phaseup_result=None, spwmaps=None, unregister_existing: Optional[bool] = False):
        """
        Initialise the phaseup spw mapping results object.
        """
        super(SpwPhaseupResults, self).__init__()

        if spwmaps is None:
            spwmaps = {}

        self.vis = vis
        self.phaseup_result = phaseup_result
        self.spwmaps = spwmaps
        self.unregister_existing = unregister_existing

    def merge_with_context(self, context):
        if self.vis is None:
            LOG.error(' No results to merge ')
            return

        if not self.phaseup_result.final:
            LOG.error(' No results to merge ')
            return

        # PIPE-629: if requested, unregister previous spwphaseup caltables from
        # the context before merging in the newly derived caltable.
        if self.unregister_existing:
            # Identify the MS to process
            vis: str = os.path.basename(self.inputs['vis'])

            # predicate function that triggers when the spwphaseup caltable is
            # detected for this MS
            def spwphaseup_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
                calto_vis = {os.path.basename(v) for v in calto.vis}
                do_delete = 'hifa_spwphaseup' in calfrom.gaintable and vis in calto_vis
                if do_delete:
                    LOG.info(f'Unregistering previous spwphaseup tables for {vis}')
                return do_delete

            context.callibrary.unregister_calibrations(spwphaseup_matcher)

        # Merge the spw phaseup offset table
        self.phaseup_result.merge_with_context(context)

        # Merge the spectral window mappings and the list of spws whose
        # combined SNR does not meet the threshold.
        ms = context.observing_run.get_ms(name=self.vis)
        if ms:
            ms.spwmaps = self.spwmaps

    def __repr__(self):
        if self.vis is None or not self.phaseup_result:
            return ('SpwPhaseupResults:\n'
                    '\tNo spw phaseup table computed')
        else:
            s = f'SpwPhaseupResults:\nvis={self.vis}\n'
            for (intent, field), spwmapping in self.spwmaps.items():
                s += f'\t{intent}, {field}:\n'
                s += f'\t\tCombine = {spwmapping.combine}\n'
                s += f'\t\tSpwmap = {spwmapping.spwmap}\n'
                if spwmapping.combine:
                    s += f'\t\tLow combined phase SNR spws = {spwmapping.low_combinedsnr_spws}'
            return s
