
import glob
import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.pipelineqa as pipelineqa
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from . import cleanbase
from .automaskthresholdsequence import AutoMaskThresholdSequence
from .imagecentrethresholdsequence import ImageCentreThresholdSequence
from .manualmaskthresholdsequence import ManualMaskThresholdSequence
from .nomaskthresholdsequence import NoMaskThresholdSequence
from .resultobjects import TcleanResult

LOG = infrastructure.get_logger(__name__)


class TcleanInputs(cleanbase.CleanBaseInputs):
    # simple properties ------------------------------------------------------------------------------------------------

    calcsb = vdp.VisDependentProperty(default=False)
    cleancontranges = vdp.VisDependentProperty(default=False)
    hm_cleaning = vdp.VisDependentProperty(default='rms')
    masklimit = vdp.VisDependentProperty(default=4.0)
    mosweight = vdp.VisDependentProperty(default=None)
    rms_nsigma = vdp.VisDependentProperty(default=None)
    reffreq = vdp.VisDependentProperty(default=None)
    restfreq = vdp.VisDependentProperty(default=None)
    tlimit = vdp.VisDependentProperty(default=2.0)
    usepointing = vdp.VisDependentProperty(default=None)
    weighting = vdp.VisDependentProperty(default='briggs')

    # override CleanBaseInputs default value of 'auto'
    hm_masking = vdp.VisDependentProperty(default='centralregion')

    # properties requiring some logic ----------------------------------------------------------------------------------

    @vdp.VisDependentProperty
    def image_heuristics(self):
        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()
        return image_heuristics_factory.getHeuristics(
            vislist=self.vis,
            spw=self.spw,
            observing_run=self.context.observing_run,
            imagename_prefix=self.context.project_structure.ousstatus_entity_id,
            proj_params=self.context.project_performance_parameters,
            contfile=self.context.contfile,
            linesfile=self.context.linesfile,
            imaging_params=self.context.imaging_parameters,
            # TODO: imaging_mode should not be fixed
            imaging_mode='ALMA'
        )

    imagename = vdp.VisDependentProperty(default='')
    @imagename.convert
    def imagename(self, value):
        return value.replace('STAGENUMBER', str(self.context.stage))

    specmode = vdp.VisDependentProperty(default=None)
    @specmode.convert
    def specmode(self, value):
        if value == 'repBW':
            self.orig_specmode = 'repBW'
            return 'cube'
        self.orig_specmode = value
        return value

    @vdp.VisDependentProperty
    def spwsel_lsrk(self):
        # mutable object, so should not use VisDependentProperty(default={})
        return {}

    @vdp.VisDependentProperty
    def spwsel_topo(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @vdp.VisDependentProperty(null_input=[None, -999, -999.0])
    def robust(self):
        # Fallback value if undefined in the imaging target and in the
        # imaging parameters. TODO: Use heuristic
        return 0.5

    @vdp.VisDependentProperty
    def uvtaper(self):
        # Fallback value if undefined in the imaging target and in the
        # imaging parameters. TODO: Use heuristic
        return []

    # class methods ----------------------------------------------------------------------------------------------------

    def __init__(self, context, output_dir=None, vis=None, imagename=None, intent=None, field=None, spw=None,
                 spwsel_lsrk=None, spwsel_topo=None, uvrange=None, specmode=None, gridder=None, deconvolver=None,
                 nterms=None, outframe=None, imsize=None, cell=None, phasecenter=None, stokes=None, nchan=None,
                 start=None, width=None, nbin=None,
                 restoringbeam=None, hm_masking=None, hm_sidelobethreshold=None, hm_noisethreshold=None,
                 hm_lownoisethreshold=None, hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None,
                 hm_dogrowprune=None, hm_minpercentchange=None, hm_fastnoise=None, hm_nsigma=None, hm_cleaning=None,
                 iter=None, mask=None, niter=None, threshold=None, tlimit=None, masklimit=None,
                 calcsb=None, cleancontranges=None, parallel=None,
                 # Extra parameters not in the CLI task interface
                 weighting=None, robust=None, uvtaper=None, scales=None, rms_nsigma=None, cycleniter=None, cyclefactor=None,
                 sensitivity=None, reffreq=None, restfreq=None, conjbeams=None, is_per_eb=None, antenna=None,
                 usepointing=None, mosweight=None,
                 # End of extra parameters
                 heuristics=None):
        super(TcleanInputs, self).__init__(context, output_dir=output_dir, vis=vis,
                                           imagename=imagename, antenna=antenna,
                                           intent=intent, field=field, spw=spw, uvrange=uvrange, specmode=specmode,
                                           gridder=gridder, deconvolver=deconvolver, uvtaper=uvtaper, nterms=nterms,
                                           cycleniter=cycleniter, cyclefactor=cyclefactor, scales=scales,
                                           outframe=outframe, imsize=imsize, cell=cell, phasecenter=phasecenter,
                                           nchan=nchan, start=start, width=width, stokes=stokes, weighting=weighting,
                                           robust=robust, restoringbeam=restoringbeam,
                                           iter=iter, mask=mask, hm_masking=hm_masking,
                                           hm_sidelobethreshold=hm_sidelobethreshold,
                                           hm_noisethreshold=hm_noisethreshold,
                                           hm_lownoisethreshold=hm_lownoisethreshold,
                                           hm_negativethreshold=hm_negativethreshold, hm_minbeamfrac=hm_minbeamfrac,
                                           hm_growiterations=hm_growiterations, hm_dogrowprune=hm_dogrowprune,
                                           hm_minpercentchange=hm_minpercentchange, hm_fastnoise=hm_fastnoise, 
                                           hm_nsigma=hm_nsigma, niter=niter, threshold=threshold,
                                           sensitivity=sensitivity, conjbeams=conjbeams, is_per_eb=is_per_eb,
                                           usepointing=usepointing, mosweight=mosweight,
                                           parallel=parallel, heuristics=heuristics)

        self.calcsb = calcsb
        self.cleancontranges = cleancontranges
        self.hm_cleaning = hm_cleaning
        self.image_heuristics = heuristics
        self.masklimit = masklimit
        self.nbin = nbin
        self.rms_nsigma = rms_nsigma
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.spwsel_lsrk = spwsel_lsrk
        self.spwsel_topo = spwsel_topo
        self.tlimit = tlimit

        # For MOM0/8_FC and cube RMS we need the LSRK frequency ranges in
        # various places
        self.cont_freq_ranges = ''

        self.is_per_eb = is_per_eb
        self.antenna = antenna
        self.usepointing = usepointing
        self.mosweight = mosweight


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
api.ImagingMeasurementSetsPreferred.register(TcleanInputs)


@task_registry.set_equivalent_casa_task('hif_tclean')
@task_registry.set_casa_commands_comment('A single target source is cleaned.')
class Tclean(cleanbase.CleanBase):
    Inputs = TcleanInputs

    is_multi_vis_task = True

    def rm_stage_files(self, imagename, stokes=''):
        filenames = glob.glob('%s*.%s.iter*' % (imagename, stokes))
        for filename in filenames:
            try:
                if os.path.isfile(filename):
                    os.remove(filename)
                elif os.path.isdir(filename):
                    rmtree_job = casa_tasks.rmtree(filename, ignore_errors=False)
                    self._executor.execute(rmtree_job)
                else:
                    raise Exception('Cannot remove %s' % filename)
            except Exception as e:
                LOG.warning('Exception while deleting %s: %s' % (filename, e))

    def copy_products(self, old_pname, new_pname, ignore=None):
        imlist = glob.glob('%s.*' % (old_pname))
        imlist = [xx for xx in imlist if ignore is None or ignore not in xx]
        for image_name in imlist:
            newname = image_name.replace(old_pname, new_pname)
            if image_name == old_pname + '.workdirectory':
                mkcmd = 'mkdir '+ newname
                os.system(mkcmd)
                self.copy_products(os.path.join(image_name, old_pname), os.path.join(newname, new_pname))
            else:
                if 'summaryplot.png' in image_name:
                    LOG.info('Copying {} to {}'.format(image_name, newname))
                    job = casa_tasks.copyfile(image_name, newname)
                else:
                    LOG.info('Copying {} to {}'.format(image_name, newname))
                    job = casa_tasks.copytree(image_name, newname)
                self._executor.execute(job)

    def prepare(self):
        inputs = self.inputs
        context = self.inputs.context

        LOG.info('\nCleaning for intent "%s", field %s, spw %s\n',
                 inputs.intent, inputs.field, inputs.spw)

        per_spw_cont_sensitivities_all_chan = context.per_spw_cont_sensitivities_all_chan
        qaTool = casatools.quanta

        # if 'start' or 'width' are defined in velocity units, track these
        #  for conversion to frequency and back before and after tclean call. Added
        #  to support SRDP ALMA optimized imaging.
        self.start_as_velocity = None
        self.width_as_velocity = None
        self.start_as_frequency = None
        self.width_as_frequency = None

        # delete any old files with this naming root. One of more
        # of these (don't know which) will interfere with this run.
        if inputs.stokes:
            LOG.info('deleting {}*.{}.iter*'.format(inputs.imagename, inputs.stokes))
            self.rm_stage_files(inputs.imagename, inputs.stokes)
        else:
            LOG.info('deleting {}*.iter*'.format(inputs.imagename))
            self.rm_stage_files(inputs.imagename)

        # Set initial masking limits
        self.pblimit_image = 0.2
        self.pblimit_cleanmask = 0.3
        inputs.pblimit = self.pblimit_image

        # Get the image parameter heuristics
        self.image_heuristics = inputs.image_heuristics

        # Remove MSs that do not contain data for the given field(s)
        scanidlist, visindexlist = self.image_heuristics.get_scanidlist(inputs.vis, inputs.field, inputs.intent)
        inputs.vis = [inputs.vis[i] for i in visindexlist]

        # Generate the image name if one is not supplied.
        if inputs.imagename in (None, ''):
            inputs.imagename = self.image_heuristics.imagename(intent=inputs.intent,
                                                               field=inputs.field,
                                                               spwspec=inputs.spw,
                                                               specmode=inputs.specmode)

        # Determine the default gridder
        if inputs.gridder in (None, ''):
            inputs.gridder = self.image_heuristics.gridder(inputs.intent, inputs.field)

        # Determine deconvolver
        if inputs.deconvolver in (None, ''):
            inputs.deconvolver = self.image_heuristics.deconvolver(inputs.specmode, inputs.spw)

        # Determine nterms
        if (inputs.nterms in ('', None)) and (inputs.deconvolver == 'mtmfs'):
            inputs.nterms = self.image_heuristics.nterms()

        # Determine antennas to be used
        if inputs.antenna in (None, [], ''):
            antenna_ids = self.image_heuristics.antenna_ids(inputs.intent)
            inputs.antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), ''))) for v in inputs.vis]

        # Determine the phase center
        if inputs.phasecenter in ('', None):
            if inputs.intent == 'TARGET' and inputs.gridder == 'mosaic':
                field_id = self.image_heuristics.field('TARGET', inputs.field, exclude_intent='ATMOSPHERE')
            else:
                field_id = self.image_heuristics.field(inputs.intent, inputs.field)
            inputs.phasecenter = self.image_heuristics.phasecenter(field_id)

        # If imsize not set then use heuristic code to calculate the
        # centers for each field  / spw
        imsize = inputs.imsize
        cell = inputs.cell
        if imsize in (None, [], '') or cell in (None, [], ''):

            # The heuristics cell size  is always the same for x and y as
            # the value derives from a single value returned by imager.advise
            synthesized_beam, known_synthesized_beams = \
                self.image_heuristics.synthesized_beam(field_intent_list=[(inputs.field, inputs.intent)],
                                                       spwspec=inputs.spw,
                                                       robust=inputs.robust,
                                                       uvtaper=inputs.uvtaper)
            cell = self.image_heuristics.cell(beam=synthesized_beam)

            if inputs.cell in (None, [], ''):
                inputs.cell = cell
                LOG.info('Heuristic cell: %s' % cell)

            if inputs.intent == 'TARGET' and inputs.gridder == 'mosaic':
                field_ids = self.image_heuristics.field('TARGET', inputs.field, exclude_intent='ATMOSPHERE')
            else:
                field_ids = self.image_heuristics.field(inputs.intent, inputs.field)
            largest_primary_beam = self.image_heuristics.largest_primary_beam_size(spwspec=inputs.spw,
                                                                                   intent=inputs.intent)
            imsize = self.image_heuristics.imsize(fields=field_ids,
                                                  cell=inputs.cell,
                                                  primary_beam=largest_primary_beam)

            if inputs.imsize in (None, [], ''):
                inputs.imsize = imsize
                LOG.info('Heuristic imsize: %s', imsize)

        if inputs.specmode == 'cube':
            # To avoid noisy edge channels, use only the frequency
            # intersection and skip one channel on either end.
            if self.image_heuristics.is_eph_obj(inputs.field):
                frame = 'TOPO'
            else:
                frame = 'LSRK'
            if0, if1, channel_width = self.image_heuristics.freq_intersection(inputs.vis, inputs.field,
                                                                              inputs.spw, frame)

            if (if0 == -1) or (if1 == -1):
                LOG.error('No frequency intersect among selected MSs for Field %s SPW %s' % (inputs.field, inputs.spw))
                error_result = TcleanResult(vis=inputs.vis,
                                            sourcename=inputs.field,
                                            intent=inputs.intent,
                                            spw=inputs.spw,
                                            specmode=inputs.specmode)
                error_result.error = '%s/%s/spw%s clean error: No frequency intersect among selected MSs' % (inputs.field, inputs.intent, inputs.spw)
                return error_result

            # Check for manually supplied values
            if0_auto = if0
            if1_auto = if1
            channel_width_auto = channel_width

            if inputs.start != '':

                # if specified in velocity units, convert to frequency
                #    then back again before the tclean call
                if 'm/' in inputs.start:
                    self.start_as_velocity = qaTool.quantity(inputs.start)
                    inputs.start = self._to_frequency(inputs.start, inputs.restfreq)
                    self.start_as_frequency = inputs.start

                if0 = qaTool.convert(inputs.start, 'Hz')['value']
                if if0 < if0_auto:
                    LOG.error('Supplied start frequency %s < f_low (%s)for Field %s SPW %s' % (if0, if0_auto, inputs.field,
                                                                                           inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode)
                    error_result.error = '%s/%s/spw%s clean error: f_start < f_low_native' % (inputs.field,
                                                                                              inputs.intent, inputs.spw)
                    return error_result
                LOG.info('Using supplied start frequency %s' % inputs.start)

            if (inputs.width != '') and (inputs.nbin not in (None, -1)):
                LOG.error('Field %s SPW %s: width and nbin are mutually exclusive' % (inputs.field, inputs.spw))
                error_result = TcleanResult(vis=inputs.vis,
                                            sourcename=inputs.field,
                                            intent=inputs.intent,
                                            spw=inputs.spw,
                                            specmode=inputs.specmode)
                error_result.error = '%s/%s/spw%s clean error: width and nbin are mutually exclusive' % (inputs.field,
                                                                                                         inputs.intent,
                                                                                                         inputs.spw)
                return error_result

            if inputs.width != '':
                # if specified in velocity units, convert to frequency
                #    then back again before the tclean call
                if 'm/' in inputs.width:
                    self.width_as_velocity = qaTool.quantity(inputs.width)
                    start_plus_width = qaTool.add(self.start_as_velocity, inputs.width)
                    start_plus_width_freq = self._to_frequency(start_plus_width, inputs.restfreq)
                    inputs.width = qaTool.sub(start_plus_width_freq, inputs.start)
                    self.width_as_frequency = inputs.width

                channel_width_manual = qaTool.convert(inputs.width, 'Hz')['value']
                if abs(channel_width_manual) < channel_width_auto:
                    LOG.error('User supplied channel width (%s) smaller than native '
                              'value of %s GHz for Field %s SPW %s' % (channel_width_manual/1e9, channel_width_auto/1e9, inputs.field, inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode)
                    error_result.error = '%s/%s/spw%s clean error: user channel width too small' % (inputs.field,
                                                                                                    inputs.intent,
                                                                                                    inputs.spw)
                    return error_result

                LOG.info('Using supplied width %s' % inputs.width)
                channel_width = channel_width_manual
                if abs(channel_width) > channel_width_auto:
                    inputs.nbin = int(round(abs(channel_width) / channel_width_auto) + 0.5)
            elif inputs.nbin not in (None, -1):
                LOG.info('Applying binning factor %d' % inputs.nbin)
                channel_width *= inputs.nbin

            if inputs.nchan not in (None, -1):
                if1 = if0 + channel_width * inputs.nchan
                if if1 > if1_auto:
                    LOG.error('Calculated stop frequency %s GHz > f_high_native for Field %s SPW %s' % (if1,
                                                                                                        inputs.field,
                                                                                                        inputs.spw))
                    error_result = TcleanResult(vis=inputs.vis,
                                                sourcename=inputs.field,
                                                intent=inputs.intent,
                                                spw=inputs.spw,
                                                specmode=inputs.specmode)
                    error_result.error = '%s/%s/spw%s clean error: f_stop > f_high' % (inputs.field,
                                                                                       inputs.intent, inputs.spw)
                    return error_result
                LOG.info('Using supplied nchan %d' % inputs.nchan)

            # tclean interprets the start frequency as the center of the
            # first channel. We have, however, an edge to edge range.
            # Thus shift by 0.5 channels if no start is supplied.
            if inputs.start == '':
                inputs.start = '%sGHz' % ((if0 + 1.5 * channel_width) / 1e9)

            # Always adjust width to apply possible binning
            if self.image_heuristics.is_eph_obj(inputs.field):
                # For ephemeris objects the frequency overlap is done in TOPO since
                # advisechansel does not support the SOURCE frame. The resulting
                # widths are too narrow for objects that are moving away from earth.
                # To compensate this, the width is increased slightly (CAS-11800).
                inputs.width = '%sMHz' % (1.0002 * channel_width / 1e6)
            else:
                inputs.width = '%sMHz' % (channel_width / 1e6)

            # Skip edge channels if no nchan is supplied
            if inputs.nchan in (None, -1):
                inputs.nchan = int(round((if1 - if0) / channel_width - 2))

        # Make sure there are LSRK selections if cont.dat/lines.dat exist.
        # For ALMA this is already done at the hif_makeimlist step. For VLASS
        # this does not (yet) happen in hif_editimlist.
        if inputs.spwsel_lsrk == {}:
            for spwid in inputs.spw.split(','):
                spwsel_spwid = self.image_heuristics.cont_ranges_spwsel().get(utils.dequote(inputs.field),
                                                                              {}).get(spwid, 'NONE')
                if inputs.intent == 'TARGET':
                    if (spwsel_spwid == 'NONE') and self.image_heuristics.warn_missing_cont_ranges():
                        LOG.warn('No continuum frequency range information detected for %s, spw %s.' % (inputs.field,
                                                                                                        spwid))

                if spwsel_spwid in ('ALL', '', 'NONE'):
                    spwsel_spwid_refer = 'LSRK'
                else:
                    spwsel_spwid_freqs, spwsel_spwid_refer = spwsel_spwid.split()

                if spwsel_spwid_refer != 'LSRK':
                    LOG.warn('Frequency selection is specified in %s but must be in LSRK' % spwsel_spwid_refer)

                inputs.spwsel_lsrk['spw%s' % spwid] = spwsel_spwid

        # Get TOPO frequency ranges for all MSs
        (spw_topo_freq_param, spw_topo_chan_param, spw_topo_freq_param_dict, spw_topo_chan_param_dict,
         total_topo_bw, aggregate_topo_bw, aggregate_lsrk_bw) = self.image_heuristics.calc_topo_ranges(inputs)

        # Save continuum frequency ranges for later.
        if (inputs.specmode == 'cube') and (inputs.spwsel_lsrk.get('spw%s' % inputs.spw, None) not in (None,
                                                                                                       'NONE', '')):
            self.cont_freq_ranges = inputs.spwsel_lsrk['spw%s' % inputs.spw].split()[0]
        else:
            self.cont_freq_ranges = ''

        # Get sensitivity
        if inputs.sensitivity is not None:
            # Override with manually set value
            sensitivity = qaTool.convert(inputs.sensitivity, 'Jy')['value']
            eff_ch_bw = 1.0
            sens_bw = 1.0
        else:
            # Get a noise estimate from the CASA sensitivity calculator
            (sensitivity,
             eff_ch_bw,
             sens_bw,
             per_spw_cont_sensitivities_all_chan) = \
                self.image_heuristics.calc_sensitivities(inputs.vis, inputs.field, inputs.intent, inputs.spw,
                                                         inputs.nbin, spw_topo_chan_param_dict, inputs.specmode,
                                                         inputs.gridder, inputs.cell, inputs.imsize, inputs.weighting,
                                                         inputs.robust, inputs.uvtaper,
                                                         known_sensitivities=per_spw_cont_sensitivities_all_chan,
                                                         force_calc=inputs.calcsb)

        if sensitivity is None:
            LOG.error('Could not calculate the sensitivity for Field %s Intent %s SPW %s' % (inputs.field,
                                                                                             inputs.intent, inputs.spw))
            error_result = TcleanResult(vis=inputs.vis,
                                        sourcename=inputs.field,
                                        intent=inputs.intent,
                                        spw=inputs.spw,
                                        specmode=inputs.specmode)
            error_result.error = '%s/%s/spw%s clean error: no sensitivity' % (inputs.field, inputs.intent, inputs.spw)
            return error_result

        # Choose TOPO frequency selections
        if inputs.specmode != 'cube':
            inputs.spwsel_topo = spw_topo_freq_param
        else:
            inputs.spwsel_topo = ['%s' % inputs.spw] * len(inputs.vis)

        # Determine threshold
        if inputs.hm_cleaning == 'manual':
            threshold = inputs.threshold
        elif inputs.hm_cleaning == 'sensitivity':
            raise Exception('sensitivity threshold not yet implemented')
        elif inputs.hm_cleaning == 'rms':
            if inputs.threshold not in (None, '', 0.0):
                threshold = inputs.threshold
            else:
                threshold = '%.3gJy' % (inputs.tlimit * sensitivity)
        else:
            raise Exception('hm_cleaning mode {} not recognized. '
                            'Threshold not set.'.format(inputs.hm_cleaning))

        multiterm = inputs.nterms if inputs.deconvolver == 'mtmfs' else None

        # Choose sequence manager
        # Central mask based on PB
        if inputs.hm_masking == 'centralregion':
            sequence_manager = ImageCentreThresholdSequence(multiterm=multiterm,
                                                            gridder=inputs.gridder, threshold=threshold,
                                                            sensitivity=sensitivity, niter=inputs.niter)
        # Auto-boxing
        elif inputs.hm_masking == 'auto':
            sequence_manager = AutoMaskThresholdSequence(multiterm=multiterm,
                                                         gridder=inputs.gridder, threshold=threshold,
                                                         sensitivity=sensitivity, niter=inputs.niter)
        # Manually supplied mask
        elif inputs.hm_masking == 'manual':
            sequence_manager = ManualMaskThresholdSequence(multiterm=multiterm, mask=inputs.mask,
                                                           gridder=inputs.gridder, threshold=threshold,
                                                           sensitivity=sensitivity, niter=inputs.niter)
        # No mask
        elif inputs.hm_masking == 'none':
            sequence_manager = NoMaskThresholdSequence(multiterm=multiterm,
                                                       gridder=inputs.gridder, threshold=threshold,
                                                       sensitivity=sensitivity, niter=inputs.niter)
        else:
            raise Exception('hm_masking mode {} not recognized. '
                            'Sequence manager not set.'.format(inputs.hm_masking))

        result = self._do_iterative_imaging(sequence_manager=sequence_manager)

        # The updated sensitivity dictionary needs to be transported via the
        # result object so that one can update the context later on in the
        # merge_with_context method (direct updates of the context do not
        # work since we are working on copies and since the HPC case will
        # have multiple instances which would overwrite each others results).
        # This result object is, however, only created in cleanbase.py while
        # we have the dictionary already here in tclean.py. Thus one has to
        # set this property only after getting the final result object.
        result.per_spw_cont_sensitivities_all_chan = per_spw_cont_sensitivities_all_chan

        # Record aggregate LSRK bandwidth and mosaic field sensitivities for weblog
        # TODO: Record total bandwidth as opposed to range
        #       Save channel selection in result for weblog.
        result.set_aggregate_bw(aggregate_lsrk_bw)
        result.set_eff_ch_bw(eff_ch_bw)

        return result

    def analyse(self, result):
        # Perform QA here if this is a sub-task
        context = self.inputs.context
        pipelineqa.qa_registry.do_qa(context, result)

        return result

    def _do_iterative_imaging(self, sequence_manager):

        inputs = self.inputs

        # Compute the dirty image
        LOG.info('Compute the dirty image')
        iteration = 0
        result = self._do_clean(iternum=iteration, cleanmask='', niter=0, threshold='0.0mJy',
                                sensitivity=sequence_manager.sensitivity, result=None)

        # Determine masking limits depending on PB
        extension = '.tt0' if result.multiterm else ''
        self.pblimit_image, self.pblimit_cleanmask = self.image_heuristics.pblimits(result.flux+extension)

        # Give the result to the sequence_manager for analysis
        (residual_cleanmask_rms,  # printed
         residual_non_cleanmask_rms,  # printed
         residual_min,  # printed
         residual_max,  # USED
         nonpbcor_image_non_cleanmask_rms_min,  # added to result, later used in Weblog under name 'image_rms_min'
         nonpbcor_image_non_cleanmask_rms_max,  # added to result, later used in Weblog under name 'image_rms_max'
         nonpbcor_image_non_cleanmask_rms,   # printed added to result, later used in Weblog under name 'image_rms'
         pbcor_image_min,  # added to result, later used in Weblog under name 'image_min'
         pbcor_image_max,  # added to result, later used in Weblog under name 'image_max'
         # USED
         residual_robust_rms,
         nonpbcor_image_robust_rms_and_spectra) = \
            sequence_manager.iteration_result(model=result.model,
                                              restored=result.image, residual=result.residual,
                                              flux=result.flux, cleanmask=None,
                                              pblimit_image=self.pblimit_image,
                                              pblimit_cleanmask=self.pblimit_cleanmask,
                                              cont_freq_ranges=self.cont_freq_ranges)

        LOG.info('Dirty image stats')
        LOG.info('    Residual rms: %s', residual_non_cleanmask_rms)
        LOG.info('    Residual max: %s', residual_max)
        LOG.info('    Residual min: %s', residual_min)
        LOG.info('    Residual scaled MAD: %s', residual_robust_rms)

        # Adjust threshold based on the dirty image statistics
        dirty_dynamic_range = residual_max / sequence_manager.sensitivity
        new_threshold, DR_correction_factor, maxEDR_used = \
            self.image_heuristics.dr_correction(sequence_manager.threshold, dirty_dynamic_range, residual_max,
                                                inputs.intent, inputs.tlimit)
        sequence_manager.threshold = new_threshold
        sequence_manager.dr_corrected_sensitivity = sequence_manager.sensitivity * DR_correction_factor

        # Adjust niter based on the dirty image statistics
        new_niter = self.image_heuristics.niter_correction(sequence_manager.niter, inputs.cell, inputs.imsize,
                                                           residual_max, new_threshold)
        sequence_manager.niter = new_niter

        keep_iterating = True
        iteration = 1
        do_not_copy_mask = False
        while keep_iterating:
            # Create the name of the next clean mask from the root of the 
            # previous residual image.
            rootname, ext = os.path.splitext(result.residual)
            rootname, ext = os.path.splitext(rootname)

            # Delete any old files with this naming root
            filenames = glob.glob('%s.iter%s*' % (rootname, iteration))
            for filename in filenames:
                try:
                    rmtree_job = casa_tasks.rmtree(filename)
                    self._executor.execute(rmtree_job)
                except Exception as e:
                    LOG.warning('Exception while deleting %s: %s' % (filename, e))

            if inputs.hm_masking == 'auto':
                new_cleanmask = '%s.iter%s.mask' % (rootname, iteration)
            elif inputs.hm_masking == 'manual':
                new_cleanmask = inputs.mask
            elif inputs.hm_masking == 'none':
                new_cleanmask = ''
            else:
                new_cleanmask = '%s.iter%s.cleanmask' % (rootname, iteration)

            rms_threshold = self.image_heuristics.rms_threshold(residual_robust_rms, inputs.rms_nsigma)
            threshold = self.image_heuristics.threshold(iteration, sequence_manager.threshold, rms_threshold,
                                                        inputs.rms_nsigma, inputs.hm_masking)
            nsigma = self.image_heuristics.nsigma(iteration, inputs.hm_masking, inputs.hm_nsigma)
            savemodel = self.image_heuristics.savemodel(iteration)

            # perform an iteration.
            if (inputs.specmode == 'cube') and (not inputs.cleancontranges):
                seq_result = sequence_manager.iteration(new_cleanmask, self.pblimit_image,
                                                        self.pblimit_cleanmask, inputs.spw, inputs.spwsel_lsrk,
                                                        iteration=iteration)
            else:
                seq_result = sequence_manager.iteration(new_cleanmask, self.pblimit_image,
                                                        self.pblimit_cleanmask, iteration=iteration)

            # Use previous iterations's products as starting point
            old_pname = '%s.iter%s' % (rootname, iteration-1)
            new_pname = '%s.iter%s' % (rootname, iteration)
            self.copy_products(os.path.basename(old_pname), os.path.basename(new_pname),
                               ignore='mask' if do_not_copy_mask else None)

            LOG.info('Iteration %s: Clean control parameters' % iteration)
            LOG.info('    Mask %s', new_cleanmask)
            LOG.info('    Threshold %s', threshold)
            LOG.info('    Niter %s', seq_result.niter)

            result = self._do_clean(iternum=iteration, cleanmask=new_cleanmask, niter=seq_result.niter, nsigma=nsigma,
                                    threshold=threshold, sensitivity=sequence_manager.sensitivity, savemodel=savemodel,
                                    result=result)

            # Give the result to the clean 'sequencer'
            (residual_cleanmask_rms,
             residual_non_cleanmask_rms,
             residual_min,
             residual_max,
             nonpbcor_image_non_cleanmask_rms_min,
             nonpbcor_image_non_cleanmask_rms_max,
             nonpbcor_image_non_cleanmask_rms,
             pbcor_image_min,
             pbcor_image_max,
             residual_robust_rms,
             nonpbcor_image_robust_rms_and_spectra) = \
                sequence_manager.iteration_result(model=result.model,
                                                  restored=result.image, residual=result.residual,
                                                  flux=result.flux, cleanmask=new_cleanmask,
                                                  pblimit_image=self.pblimit_image,
                                                  pblimit_cleanmask=self.pblimit_cleanmask,
                                                  cont_freq_ranges=self.cont_freq_ranges)

            keep_iterating, hm_masking = self.image_heuristics.keep_iterating(iteration, inputs.hm_masking,
                                                                              result.tclean_stopcode,
                                                                              dirty_dynamic_range, residual_max,
                                                                              residual_robust_rms,
                                                                              inputs.field, inputs.intent, inputs.spw,
                                                                              inputs.specmode)
            do_not_copy_mask = hm_masking != inputs.hm_masking
            inputs.hm_masking = hm_masking

            # Keep image cleanmask area min and max and non-cleanmask area RMS for weblog and QA
            result.set_image_min(pbcor_image_min)
            result.set_image_max(pbcor_image_max)
            result.set_image_rms(nonpbcor_image_non_cleanmask_rms)
            result.set_image_rms_min(nonpbcor_image_non_cleanmask_rms_min)
            result.set_image_rms_max(nonpbcor_image_non_cleanmask_rms_max)
            result.set_image_robust_rms_and_spectra(nonpbcor_image_robust_rms_and_spectra)

            # Keep dirty DR, correction factor and information about maxEDR heuristic for weblog
            result.set_dirty_dynamic_range(dirty_dynamic_range)
            result.set_DR_correction_factor(DR_correction_factor)
            result.set_maxEDR_used(maxEDR_used)
            result.set_dr_corrected_sensitivity(sequence_manager.dr_corrected_sensitivity)

            LOG.info('Clean image iter %s stats' % iteration)
            LOG.info('    Clean image annulus area rms: %s', nonpbcor_image_non_cleanmask_rms)
            LOG.info('    Clean image min: %s', pbcor_image_min)
            LOG.info('    Clean image max: %s', pbcor_image_max)
            LOG.info('    Residual annulus area rms: %s', residual_non_cleanmask_rms)
            LOG.info('    Residual cleanmask area rms: %s', residual_cleanmask_rms)
            LOG.info('    Residual max: %s', residual_max)
            LOG.info('    Residual min: %s', residual_min)

            # Keep tclean summary plot
            try:
                move_job = casa_tasks.move('summaryplot_1.png', '%s.iter%s.summaryplot.png' % (rootname, iteration))
                self._executor.execute(move_job)
            except (IOError, OSError):
                LOG.info('Could not save tclean summary plot.')

            # Up the iteration counter
            iteration += 1

        # If specmode is "cube", create from the non-pbcorrected cube
        # after continuum subtraction an image of the moment 0 / 8 integrated
        # intensity for the line-free channels.
        if inputs.specmode == 'cube':
            self._calc_mom0_8_fc(result)

        return result

    def _do_clean(self, iternum, cleanmask, niter, threshold, sensitivity, result, nsigma=None, savemodel=None):
        """
        Do basic cleaning.
        """
        inputs = self.inputs

        parallel = mpihelpers.parse_mpi_input_parameter(inputs.parallel)

        # if 'start' or 'width' are defined in velocity units, convert from frequency back
        # to velocity before tclean call. Added to support SRDP ALMA optimized imaging.
        if self.start_as_velocity:
            inputs.start = casatools.quanta.tos(self.start_as_velocity)
        if self.width_as_velocity:
            inputs.width = casatools.quanta.tos(self.width_as_velocity)

        clean_inputs = cleanbase.CleanBase.Inputs(inputs.context,
                                                  output_dir=inputs.output_dir,
                                                  vis=inputs.vis,
                                                  is_per_eb=inputs.is_per_eb,
                                                  imagename=inputs.imagename,
                                                  antenna=inputs.antenna,
                                                  intent=inputs.intent,
                                                  field=inputs.field,
                                                  spw=inputs.spw,
                                                  spwsel=inputs.spwsel_topo,
                                                  reffreq=inputs.reffreq,
                                                  restfreq=inputs.restfreq,
                                                  conjbeams=inputs.conjbeams,
                                                  uvrange=inputs.uvrange,
                                                  orig_specmode=inputs.orig_specmode,
                                                  specmode=inputs.specmode,
                                                  gridder=inputs.gridder,
                                                  deconvolver=inputs.deconvolver,
                                                  nterms=inputs.nterms,
                                                  cycleniter=inputs.cycleniter,
                                                  cyclefactor=inputs.cyclefactor,
                                                  scales=inputs.scales,
                                                  outframe=inputs.outframe,
                                                  imsize=inputs.imsize,
                                                  cell=inputs.cell,
                                                  phasecenter=inputs.phasecenter,
                                                  stokes=inputs.stokes,
                                                  nchan=inputs.nchan,
                                                  start=inputs.start,
                                                  width=inputs.width,
                                                  weighting=inputs.weighting,
                                                  robust=inputs.robust,
                                                  uvtaper=inputs.uvtaper,
                                                  restoringbeam=inputs.restoringbeam,
                                                  iter=iternum,
                                                  mask=cleanmask,
                                                  savemodel=savemodel,
                                                  hm_masking=inputs.hm_masking,
                                                  hm_sidelobethreshold=inputs.hm_sidelobethreshold,
                                                  hm_noisethreshold=inputs.hm_noisethreshold,
                                                  hm_lownoisethreshold=inputs.hm_lownoisethreshold,
                                                  hm_negativethreshold=inputs.hm_negativethreshold,
                                                  hm_minbeamfrac=inputs.hm_minbeamfrac,
                                                  hm_growiterations=inputs.hm_growiterations,
                                                  hm_dogrowprune=inputs.hm_dogrowprune,
                                                  hm_minpercentchange=inputs.hm_minpercentchange,
                                                  hm_fastnoise=inputs.hm_fastnoise,
                                                  niter=niter,
                                                  hm_nsigma=nsigma,
                                                  threshold=threshold,
                                                  sensitivity=sensitivity,
                                                  pblimit=inputs.pblimit,
                                                  result=result,
                                                  parallel=parallel,
                                                  heuristics=inputs.image_heuristics)
        clean_task = cleanbase.CleanBase(clean_inputs)

        clean_result = self._executor.execute(clean_task)

        # if 'start' or 'width' were defined in velocity units, convert from velocity back
        # to frequency after tclean call. Added to support SRDP ALMA optimized imaging.
        if self.start_as_velocity:
            inputs.start = self.start_as_frequency
        if self.width_as_velocity:
            inputs.width = self.width_as_frequency

        return clean_result


    # Remove pointing table.
    def _empty_pointing_table(self):
        # Concerned that simply renaming things directly 
        # will corrupt the table cache, so do things using only the
        # table tool.
        for vis in self.inputs.vis:
            with casatools.TableReader('%s/POINTING' % vis,
                                       nomodify=False) as table:
                # make a copy of the table
                LOG.debug('Making copy of POINTING table')
                copy = table.copy('%s/POINTING_COPY' % vis, valuecopy=True)
                LOG.debug('Removing all POINTING table rows')
                table.removerows(range(table.nrows()))
                copy.done()

    # Restore pointing table
    def _restore_pointing_table(self):
        for vis in self.inputs.vis:
            # restore the copy of the POINTING table
            with casatools.TableReader('%s/POINTING_COPY' % vis,
                                       nomodify=False) as table:
                LOG.debug('Copying back into POINTING table')
                original = table.copy('%s/POINTING' % vis, valuecopy=True)
                original.done()

    # Calculate a "mom0_fc" and "mom8_fc" image: this is a moment 0 and 8
    # integration over the line-free channels of the non-primary-beam
    # corrected image-cube, after continuum subtraction; where the "line-free"
    # channels are taken from those identified as continuum channels. 
    # This is a diagnostic plot representing the residual emission 
    # in the line-free (aka continuum) channels. If the continuum subtraction
    # worked well, then this image should just contain noise.
    def _calc_mom0_8_fc(self, result):
 
        context = self.inputs.context

        # Find max iteration that was performed.
        maxiter = max(result.iterations.keys())

        # Get filename of image from result, and modify to select the  
        # non-PB-corrected image.
        imagename = result.iterations[maxiter]['image'].replace('.pbcor', '')

        # Set output filename for MOM0_FC image.
        mom0_name = '%s.mom0_fc' % imagename

        # Set output filename for MOM8_FC image.
        mom8_name = '%s.mom8_fc' % imagename

        # Convert frequency ranges to channel ranges.
        cont_chan_ranges = utils.freq_selection_to_channels(imagename, self.cont_freq_ranges)

        # Only continue if there were continuum channel ranges for this spw.
        if cont_chan_ranges[0] != 'NONE':

            # Create a channel ranges string.
            cont_chan_ranges_str = ";".join(["%s~%s" % (ch0, ch1) for ch0, ch1 in cont_chan_ranges])

            # Execute job to create the MOM0_FC image.
            job = casa_tasks.immoments(imagename=imagename, moments=[0], outfile=mom0_name, chans=cont_chan_ranges_str)
            job.execute(dry_run=False)
            assert os.path.exists(mom0_name)

            # Update the metadata in the MOM0_FC image.
            cleanbase.set_miscinfo(name=mom0_name, spw=self.inputs.spw,
                                   field=self.inputs.field, iter=maxiter, type='mom0_fc',
                                   intent=self.inputs.intent, specmode=self.inputs.specmode,
                                   observing_run=context.observing_run)

            # Update the result.
            result.set_mom0_fc(maxiter, mom0_name)

            # Execute job to create the MOM8_FC image.
            job = casa_tasks.immoments(imagename=imagename, moments=[8], outfile=mom8_name, chans=cont_chan_ranges_str)
            job.execute(dry_run=False)
            assert os.path.exists(mom8_name)

            # Update the metadata in the MOM8_FC image.
            cleanbase.set_miscinfo(name=mom8_name, spw=self.inputs.spw,
                                   field=self.inputs.field, iter=maxiter, type='mom8_fc',
                                   intent=self.inputs.intent, specmode=self.inputs.specmode,
                                   observing_run=context.observing_run)

            # Update the result.
            result.set_mom8_fc(maxiter, mom8_name)
        else:
            LOG.warning('Cannot create MOM0_FC / MOM8_FC images for intent "%s", '
                        'field %s, spw %s, no continuum ranges found.' %
                        (self.inputs.intent, self.inputs.field, self.inputs.spw))

    def _to_frequency(self, velocity, restfreq):
        # f = f_rest / (v/c + 1)
        # https://www.iram.fr/IRAMFR/ARN/may95/node4.htm
        qa = casatools.quanta
        light_speed = qa.convert(qa.constants('c'), 'km/s')['value']
        velocity = qa.convert(qa.quantity(velocity), 'km/s')['value']
        val = qa.getvalue(restfreq)[0] / ((velocity / light_speed) + 1)
        unit = qa.getunit(restfreq)
        frequency = qa.tos(qa.quantity(val, unit))
        return frequency

    def _to_velocity(self, frequency, restfreq, velo):
        # v = c * (f - f_rest) / f
        # https://www.iram.fr/IRAMFR/ARN/may95/node4.htm
        qa = casatools.quanta
        light_speed = qa.convert(qa.constants('c'), 'km/s')['value']
        restfreq = qa.getvalue(qa.convert(restfreq, 'MHz'))[0]
        freq = qa.getvalue(qa.convert(frequency, 'MHz'))[0]
        val = light_speed * ((freq - restfreq) / freq)
        unit = qa.getunit(velo)
        velocity = qa.tos(qa.quantity(val, unit))
        return velocity
