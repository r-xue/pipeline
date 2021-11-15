import os
from typing import List, Optional, Tuple

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


class SpwPhaseupInputs(gtypegaincal.GTypeGaincalInputs):
    intent = vdp.VisDependentProperty(default='BANDPASS')

    # Spw mapping mode heuristics, options are 'auto', 'combine', 'simple', and 'default'
    hm_spwmapmode = vdp.VisDependentProperty(default='auto')

    @hm_spwmapmode.convert
    def hm_spwmapmode(self, value):
        allowed = ('auto', 'combine', 'simple', 'default')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    phasesnr = vdp.VisDependentProperty(default=32.0)
    bwedgefrac = vdp.VisDependentProperty(default=0.03125)

    # Antenna flagging heuristics parameter
    hm_nantennas = vdp.VisDependentProperty(default='all')

    @hm_nantennas.convert
    def hm_nantennas(self, value):
        allowed = ('all', 'unflagged')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    maxfracflagged = vdp.VisDependentProperty(default=0.90)
    maxnarrowbw = vdp.VisDependentProperty(default='300MHz')
    minfracmaxbw = vdp.VisDependentProperty(default=0.8)
    samebb = vdp.VisDependentProperty(default=True)
    caltable = vdp.VisDependentProperty(default=None)
    # PIPE-629: new parameter to unregister existing phaseup tables before appending to callibrary
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

        if inputs.unregister_existing:
            # Unregister old spwphaseup calibrations to stop them from being preapplied

            # predicate function that triggers when the spwphaseup caltable is detected
            def spwphaseup_matcher(_: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
                return 'hifa_spwphaseup' in calfrom.gaintable

            LOG.info('Temporarily unregistering previous spwphaseup tables while task executes')
            inputs.context.callibrary.unregister_calibrations(spwphaseup_matcher)

            # Reset the spwmaps. The will be restored if the result is not accepted
            ms = inputs.ms
            LOG.info('Temporarily resetting spwmaps for %s', ms.basename)
            ms.phaseup_spwmap = []
            ms.combine_spwmap = []
            ms.low_combined_phasesnr_spws = []

        # Get a list of all the spws and a list of the science spws
        allspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=False)
        scispws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=True)

        # Compute the spw map according to the rules defined by each
        # mapping mode. The default map is [] which stands for the
        # default one to one spw mapping.
        LOG.info("The spw mapping mode for {} is {}".format(inputs.ms.basename, inputs.hm_spwmapmode))

        snrs = []
        spwids = []
        low_combined_phasesnr_spws = []
        if inputs.hm_spwmapmode == 'auto':

            nosnrs, spwids, snrs, goodsnrs = self._do_snrtest()

            # No SNR estimates available, default to simple spw mapping
            if nosnrs:
                LOG.warning('    No SNR estimates for any spws - Forcing simple spw mapping for {}'
                            ''.format(inputs.ms.basename))
                combinespwmap = []
                phaseupspwmap = simple_n2wspwmap(allspws, scispws, inputs.maxnarrowbw, inputs.minfracmaxbw,
                                                 inputs.samebb)
                LOG.info('    Using spw map {} for {}'.format(phaseupspwmap, inputs.ms.basename))

            # All spws have good SNR values, no spw mapping required
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == len(goodsnrs):
                LOG.info('    High SNR - Default spw mapping used for all spws {}'.format(inputs.ms.basename))
                combinespwmap = []
                phaseupspwmap = []
                LOG.info('    Using spw map {} for {}'.format(phaseupspwmap, inputs.ms.basename))

            # No spws have good SNR values use combined spw mapping
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == 0:
                LOG.warning('    Low SNR for all spws - Forcing combined spw mapping for {}'.format(inputs.ms.basename))
                if None in goodsnrs:
                    LOG.warning('    Spws without SNR measurements {}'
                                ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))
                combinespwmap = combine_spwmap(scispws)
                phaseupspwmap = []
                low_combined_phasesnr_spws = self._do_combined_snr_test(spwids, snrs, combinespwmap)
                LOG.info('    Using combined spw map {} for {}'.format(combinespwmap, inputs.ms.basename))

            else:
                LOG.warning('    Some low SNR spws - using highest good SNR window for these in {}'
                            ''.format(inputs.ms.basename))
                if None in goodsnrs:
                    LOG.warning('    Spws without SNR measurements {}'
                                ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))
                goodmap, phaseupspwmap, snrmap = snr_n2wspwmap(allspws, scispws, snrs, goodsnrs)
                if not goodmap:
                    LOG.warning('    Still unable to match all spws - Forcing combined spw mapping for {}'
                                ''.format(inputs.ms.basename))
                    phaseupspemap = []
                    combinespwmap = combine_spwmap(scispws)
                    low_combined_phasesnr_spws = self._do_combined_snr_test(spwids, snrs, combinespwmap)
                    LOG.info('    Using spw map {} for {}'.format(combinespwmap, inputs.ms.basename))
                else:
                    combinespwmap = []
                    LOG.info('    Using spw map {} for {}'.format(phaseupspwmap, inputs.ms.basename))

        elif inputs.hm_spwmapmode == 'combine':
            combinespwmap = combine_spwmap(scispws)
            low_combined_phasesnr_spws = scispws
            phaseupspwmap = []
            LOG.info('    Using combined spw map {} for {}'.format(combinespwmap, inputs.ms.basename))

        elif inputs.hm_spwmapmode == 'simple':
            combinespwmap = []
            phaseupspwmap = simple_n2wspwmap(allspws, scispws, inputs.maxnarrowbw, inputs.minfracmaxbw, inputs.samebb)
            LOG.info('    Using simple spw map {} for {}'.format(phaseupspwmap, inputs.ms.basename))

        else:
            phaseupspwmap = []
            combinespwmap = []
            LOG.info('    Using standard spw map {} for {}'.format(phaseupspwmap, inputs.ms.basename))

        # Compute the phaseup table and set calwt to False
        LOG.info('Computing spw phaseup table for {} is {}'.format(inputs.ms.basename, inputs.hm_spwmapmode))
        phaseupresult = self._do_phaseup()

        # Collect SNR info.
        snr_info = self._get_snr_info(spwids, snrs)

        # Create the results object.
        result = SpwPhaseupResults(vis=inputs.vis, phaseup_result=phaseupresult, combine_spwmap=combinespwmap,
                                   phaseup_spwmap=phaseupspwmap, low_combined_phasesnr_spws=low_combined_phasesnr_spws,
                                   unregister_existing=inputs.unregister_existing, snr_info=snr_info)

        return result

    def analyse(self, result):

        # The caltable portion of the result is treated as if it
        # were any other calibration result.

        # With no best caltable to find, our task is simply to set the one
        # caltable as the best result

        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.phaseup_result.pool if ca.exists() or self._executor._dry_run]
        result.phaseup_result.final[:] = on_disk

        missing = [ca for ca in result.phaseup_result.pool if ca not in on_disk and not self._executor._dry_run]
        result.phaseup_result.error.clear()
        result.phaseup_result.error.update(missing)

        return result

    def _do_snrtest(self):

        # Simplify inputs.
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'intent': 'PHASE',
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
        Calculate combined SNRs from per SpW SNR and
        return a list of SpW IDs that does not meet phasesnr threshold.
        Grouping of SpWs is specified by an input parameter, spwmap.
        For each grouped SpWs, combined SNR is calculated by
            combined SNR = numpy.linalg.nrom(list of per SpW SNR in a group)

        Prameters:
            spwlist : A list of spw IDs to calculate combined SNR
            perspwsnr : A list of SNRs of each SpW
            spwmap : A spectral window map that specifies which SpW IDs
                    should be combined together.
        """
        LOG.info("Start combined SpW SNR test")
        LOG.debug('- spwlist to analyze: {}'.format(spwlist))
        LOG.debug('- per SpW SNR: {}'.format(perspwsnr))
        LOG.debug('- spwmap = {}'.format(spwmap))
        nosnr = True
        combined_spwids = []
        combined_snrs = []
        combined_goodsnrs = [False for _ in spwlist]
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
                        LOG.error('SNR not calculated for spw={}. Cannnot calculate combined SNR'.format(spwid))
                        return False, [], [], []
                    snrlist.append(perspwsnr[i])
                    combined_idx.append(i)
            # calculate combined SNR from per spw SNR
            combined_snr = numpy.linalg.norm(snrlist)
            LOG.info('Reference SpW ID = {} (Combined SpWs = {}) : Combined SNR = {}'.format(mappedspwid, str(
                [spwlist[j] for j in combined_idx]), combined_snr))

            if combined_snr < self.inputs.phasesnr:
                low_snr_spwids.extend([spwlist[i] for i in combined_idx])
            else:
                nosnr = False
                for spwid in combined_idx:
                    combined_goodsnrs[i] = True
            combined_spwids.append(mappedspwid)
            combined_snrs.append(combined_snr)
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
            append = True

        processed_calapps = [callibrary.copy_calapplication(c, calwt=False) for c in original_calapps]
        tuning_result.pool = processed_calapps
        tuning_result.final = processed_calapps

        return tuning_result

    def _get_snr_info(self, spwids: List[int], snrs: List[float]) -> List[Tuple[int, float]]:
        """
        Helper method that takes phase SNR info from the SNR test, and returns
        return phase SNR info for all SpWs specified in inputs.spw.

        Args:
            spwids: list of SpW IDs for which phase SNRs were determined.
            snrs: list of phase SNRs.

        Returns:
            List of tuples, specifying SpW ID and corresponding phase SNR.
        """
        spw_snr = {str(k): v for k, v in zip(spwids, snrs)}
        snr_info = []
        # Create entry for each SpW specified by inputs.
        for spwid in self.inputs.spw.split(','):
            # If no SNR info was available, set to None, otherwise use the
            # derived value.
            snr = None
            if spwid in spw_snr:
                snr = spw_snr[spwid]
            snr_info.append((spwid, snr))
        return snr_info


class SpwPhaseupResults(basetask.Results):
    def __init__(self, vis=None, phaseup_result=None, combine_spwmap=None, phaseup_spwmap=None,
                 low_combined_phasesnr_spws=None, unregister_existing: Optional[bool] = False, snr_info=None):
        """
        Initialise the phaseup spw mapping results object.
        """
        super(SpwPhaseupResults, self).__init__()
        if combine_spwmap is None:
            combine_spwmap = []
        if phaseup_spwmap is None:
            phaseup_spwmap = []
        if low_combined_phasesnr_spws is None:
            low_combined_phasesnr_spws = []

        self.vis = vis
        self.phaseup_result = phaseup_result
        self.combine_spwmap = combine_spwmap
        self.phaseup_spwmap = phaseup_spwmap
        self.low_combined_phasesnr_spws = low_combined_phasesnr_spws
        self.unregister_existing = unregister_existing
        self.snr_info = snr_info

    def merge_with_context(self, context):
        if self.vis is None:
            LOG.error(' No results to merge ')
            return

        if not self.phaseup_result.final:
            LOG.error(' No results to merge ')
            return

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

        # Merge the phaseup spwmap
        ms = context.observing_run.get_ms(name=self.vis)
        if ms:
            ms.phaseup_spwmap = self.phaseup_spwmap
            ms.combine_spwmap = self.combine_spwmap
            ms.low_combined_phasesnr_spws = self.low_combined_phasesnr_spws

    def __repr__(self):
        if self.vis is None or not self.phaseup_result:
            return ('SpwPhaseupResults:\n'
                    '\tNo spw phaseup table computed')
        else:
            spwmap = 'SpwPhaseupResults:\nCombine spwmap = {}\nNarrow to wide spwmap = {}\n' \
                     ''.format(self.combine_spwmap, self.phaseup_spwmap)
            return spwmap
