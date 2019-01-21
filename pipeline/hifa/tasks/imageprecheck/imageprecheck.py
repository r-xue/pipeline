from __future__ import absolute_import

import copy

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.imageparamsfilehandler as imageparamsfilehandler
import pipeline.infrastructure.vdp as vdp
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.hifa.heuristics import imageprecheck
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class ImagePreCheckResults(basetask.Results):
    def __init__(self, real_repr_target=False, repr_target='', repr_source='', repr_spw=None,
                 reprBW_mode=None, reprBW_nbin=None,
                 minAcceptableAngResolution='0.0arcsec', maxAcceptableAngResolution='0.0arcsec',
                 sensitivityGoal='0mJy', hm_robust=0.5, hm_uvtaper=[],
                 sensitivities=None, sensitivity_bandwidth=None, score=None, single_continuum=False,
                 per_spw_cont_sensitivities_all_chan=None, synthesized_beams=None):
        super(ImagePreCheckResults, self).__init__()

        if sensitivities is None:
            sensitivities = []

        self.real_repr_target = real_repr_target
        self.repr_target = repr_target
        self.repr_source = repr_source
        self.repr_spw = repr_spw
        self.reprBW_mode = reprBW_mode
        self.reprBW_nbin = reprBW_nbin
        self.minAcceptableAngResolution = minAcceptableAngResolution
        self.maxAcceptableAngResolution = maxAcceptableAngResolution
        self.sensitivityGoal = sensitivityGoal
        self.hm_robust = hm_robust
        self.hm_uvtaper = hm_uvtaper
        self.sensitivities = sensitivities
        self.sensitivities_for_aqua = []
        self.sensitivity_bandwidth = sensitivity_bandwidth
        self.score = score
        self.single_continuum = single_continuum
        self.per_spw_cont_sensitivities_all_chan = per_spw_cont_sensitivities_all_chan
        self.synthesized_beams = synthesized_beams

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """

        # Store imaging parameters in context

        # Calculated sensitivities for later stages
        if self.per_spw_cont_sensitivities_all_chan is not None:
            if 'recalc' in self.per_spw_cont_sensitivities_all_chan:
                context.per_spw_cont_sensitivities_all_chan = copy.deepcopy(self.per_spw_cont_sensitivities_all_chan)
                del context.per_spw_cont_sensitivities_all_chan['recalc']
            else:
                utils.update_sens_dict(context.per_spw_cont_sensitivities_all_chan, self.per_spw_cont_sensitivities_all_chan)

        # Calculated beams for later stages
        if self.synthesized_beams is not None:
            if 'recalc' in self.synthesized_beams:
                context.synthesized_beams = copy.deepcopy(self.synthesized_beams)
                del context.synthesized_beams['recalc']
            else:
                utils.update_beams_dict(context.synthesized_beams, self.synthesized_beams)

        # Calculated robust and uvtaper values for later stages
        #
        # Note: For Cycle 6 the robust heuristic is used in subsequent stages.
        #       The uvtaper heuristic is not yet to be used.
        context.imaging_parameters['robust'] = self.hm_robust
        #context.imaging_parameters['uvtaper'] = self.hm_uvtaper

        # It was decided not use a file based transport for the time being (03/2018)
        # Write imageparams.dat file
        #imageparams_filehandler = imageparamsfilehandler.ImageParamsFileHandler()
        #imageparams_filehandler.write(self.hm_robust, self.hm_uvtaper)

        # Add sensitivities to be reported to AQUA
        self.sensitivities_for_aqua.extend([s for s in self.sensitivities if s['robust']==self.hm_robust and s['uvtaper']==self.hm_uvtaper])

    def __repr__(self):
        return 'ImagePreCheckResults:\n\t{0}'.format(
            '\n\t'.join(['robust=%.2f' % (self.hm_robust), 'uvtaper=%s' % (self.hm_uvtaper)]))


class ImagePreCheckInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None, calcsb=None):
        self.context = context
        self.vis = vis
        self.calcsb = calcsb


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
api.ImagingMeasurementSetsPreferred.register(ImagePreCheckInputs)


@task_registry.set_equivalent_casa_task('hifa_imageprecheck')
class ImagePreCheck(basetask.StandardTaskTemplate):
    Inputs = ImagePreCheckInputs

    is_multi_vis_task = True

    def prepare(self):

        inputs = self.inputs
        context = self.inputs.context

        cqa = casatools.quanta

        calcsb = inputs.calcsb

        known_per_spw_cont_sensitivities_all_chan = context.per_spw_cont_sensitivities_all_chan
        known_synthesized_beams = context.synthesized_beams

        imageprecheck_heuristics = imageprecheck.ImagePreCheckHeuristics(inputs)

        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()
        image_heuristics = image_heuristics_factory.getHeuristics(
            vislist=inputs.vis,
            spw='',
            observing_run=context.observing_run,
            imagename_prefix=context.project_structure.ousstatus_entity_id,
            proj_params=context.project_performance_parameters,
            contfile=context.contfile,
            linesfile=context.linesfile,
            imaging_params=context.imaging_parameters,
            imaging_mode='ALMA'
        )

        repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, sensitivityGoal = image_heuristics.representative_target()

        repr_field = list(image_heuristics.field_intent_list('TARGET', repr_source))[0][0]

        repr_ms = self.inputs.ms[0]
        real_repr_spw = context.observing_run.virtual2real_spw_id(int(repr_spw), repr_ms)
        real_repr_spw_obj = repr_ms.get_spectral_window(real_repr_spw)
        single_continuum = any(['Single_Continuum' in t for t in real_repr_spw_obj.transitions])

        # Get the array
        diameter = min([a.diameter for a in repr_ms.antennas])
        if diameter == 7.0:
            array = '7m'
        else:
            array = '12m'

        # Approximate reprBW with nbin
        if reprBW_mode in ['nbin', 'repr_spw']:
            physicalBW_of_1chan = float(real_repr_spw_obj.channels[0].getWidth().convert_to(measures.FrequencyUnits.HERTZ).value)
            nbin = int(cqa.getvalue(cqa.convert(repr_target[2], 'Hz'))/physicalBW_of_1chan + 0.5)
            cont_sens_bw_modes = ['aggBW']
            scale_aggBW_to_repBW = False
        elif reprBW_mode == 'multi_spw':
            nbin = -1
            cont_sens_bw_modes = ['repBW', 'aggBW']
            scale_aggBW_to_repBW = True
        else:
            nbin = -1
            cont_sens_bw_modes = ['repBW', 'aggBW']
            scale_aggBW_to_repBW = False

        primary_beam_size = image_heuristics.largest_primary_beam_size(spwspec=str(repr_spw), intent='TARGET')
        gridder = image_heuristics.gridder('TARGET', repr_field)
        if gridder == 'mosaic':
            # Only pass phase center for mosaics to avoid filtering single fields with
            # TARGET and ATMOSPHERE intents (CAS-10146).
            field_ids = image_heuristics.field('TARGET', repr_field, exclude_intent='ATMOSPHERE')
            phasecenter = image_heuristics.phasecenter(field_ids)
        else:
            field_ids = image_heuristics.field('TARGET', repr_field)
            phasecenter = None
        cont_spwids = [s for s in context.observing_run.virtual_science_spw_ids.keys()]
        cont_spwids.sort()
        cont_spw = ','.join(map(str, cont_spwids))
        num_cont_spw = len(cont_spwids)

        beams = {}
        cells = {}
        imsizes = {}
        sensitivities = []
        sensitivity_bandwidth = None
        # Get default heuristics uvtaper value
        default_uvtaper = image_heuristics.uvtaper()
        for robust in [-0.5, 0.0, 0.5, 1.0, 2.0]:
            # Calculate nbin / reprBW sensitivity if necessary
            if reprBW_mode in ['nbin', 'repr_spw']:
                beams[(robust, str(default_uvtaper), 'repBW')], known_synthesized_beams = image_heuristics.synthesized_beam([(repr_field, 'TARGET')], str(repr_spw), robust=robust, uvtaper=default_uvtaper, known_beams=known_synthesized_beams, force_calc=calcsb)
                cells[(robust, str(default_uvtaper), 'repBW')] = image_heuristics.cell(beams[(robust, str(default_uvtaper), 'repBW')])
                imsizes[(robust, str(default_uvtaper), 'repBW')] = image_heuristics.imsize(field_ids, cells[(robust, str(default_uvtaper), 'repBW')], primary_beam_size, centreonly=False)

                try:
                    sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                        image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', str(repr_spw), nbin, {}, 'cube', gridder, cells[(robust, str(default_uvtaper), 'repBW')], imsizes[(robust, str(default_uvtaper), 'repBW')], 'briggs', robust, default_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                    calcsb = False
                    sensitivities.append(Sensitivity(
                        array=array,
                        field=repr_field,
                        spw=str(repr_spw),
                        bandwidth=cqa.quantity(sens_bw, 'Hz'),
                        bwmode='repBW',
                        beam=beams[(robust, str(default_uvtaper), 'repBW')],
                        cell=[cqa.convert(cells[(robust, str(default_uvtaper), 'repBW')][0], 'arcsec'),
                              cqa.convert(cells[(robust, str(default_uvtaper), 'repBW')][0], 'arcsec')],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(sensitivity, 'Jy/beam')))
                except:
                    sensitivities.append(Sensitivity(
                        array=array,
                        field=repr_field,
                        spw=str(repr_spw),
                        bandwidth=cqa.quantity(0.0, 'Hz'),
                        bwmode='repBW',
                        beam=beams[(robust, str(default_uvtaper), 'repBW')],
                        cell=['0.0 arcsec', '0.0 arcsec'],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(0.0, 'Jy/beam')))
                    sens_bw = 0.0

                sensitivity_bandwidth = cqa.quantity(sens_bw, 'Hz')

            beams[(robust, str(default_uvtaper), 'aggBW')], known_synthesized_beams = image_heuristics.synthesized_beam([(repr_field, 'TARGET')], cont_spw, robust=robust, uvtaper=default_uvtaper, known_beams=known_synthesized_beams, force_calc=calcsb)
            cells[(robust, str(default_uvtaper), 'aggBW')] = image_heuristics.cell(beams[(robust, str(default_uvtaper), 'aggBW')])
            imsizes[(robust, str(default_uvtaper), 'aggBW')] = image_heuristics.imsize(field_ids, cells[(robust, str(default_uvtaper), 'aggBW')], primary_beam_size, centreonly=False)

            # Calculate full cont sensitivity (no frequency ranges excluded)
            try:
                sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                    image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', cont_spw, -1, {}, 'cont', gridder, cells[(robust, str(default_uvtaper), 'aggBW')], imsizes[(robust, str(default_uvtaper), 'aggBW')], 'briggs', robust, default_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                calcsb = False
                for cont_sens_bw_mode in cont_sens_bw_modes:
                    if scale_aggBW_to_repBW and cont_sens_bw_mode == 'repBW':
                        # Handle scaling to repSPW_BW < repBW <= 0.9 * aggBW case
                        _bandwidth = repr_target[2]
                        _sensitivity = cqa.mul(cqa.quantity(sensitivity, 'Jy/beam'), cqa.sqrt(cqa.div(cqa.quantity(sens_bw, 'Hz'), repr_target[2])))
                    else:
                        _bandwidth = cqa.quantity(min(sens_bw, num_cont_spw * 1.875e9), 'Hz')
                        _sensitivity = cqa.quantity(sensitivity, 'Jy/beam')

                    sensitivities.append(Sensitivity(
                        array=array,
                        field=repr_field,
                        spw=cont_spw,
                        bandwidth=_bandwidth,
                        bwmode=cont_sens_bw_mode,
                        beam=beams[(robust, str(default_uvtaper), 'aggBW')],
                        cell=[cqa.convert(cells[(robust, str(default_uvtaper), 'aggBW')][0], 'arcsec'),
                              cqa.convert(cells[(robust, str(default_uvtaper), 'aggBW')][0], 'arcsec')],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=_sensitivity))
            except Exception as e:
                for _ in cont_sens_bw_modes:
                    sensitivities.append(Sensitivity(
                        array=array,
                        field=repr_field,
                        spw=cont_spw,
                        bandwidth=cqa.quantity(0.0, 'Hz'),
                        bwmode='aggBW',
                        beam=beams[(robust, str(default_uvtaper), 'aggBW')],
                        cell=['0.0 arcsec', '0.0 arcsec'],
                        robust=robust,
                        uvtaper=default_uvtaper,
                        sensitivity=cqa.quantity(0.0, 'Jy/beam')))
                sens_bw = 0.0

            if sensitivity_bandwidth is None:
                sensitivity_bandwidth = cqa.quantity(_bandwidth, 'Hz')

        # Apply robust heuristic based on beam sizes for robust=(-0.5, 0.0, 0.5, 1.0, 2.0)
        if reprBW_mode in ['nbin', 'repr_spw']:
            hm_robust, hm_robust_score = imageprecheck_heuristics.compare_beams( \
                beams[(-0.5, str(default_uvtaper), 'repBW')], \
                beams[(0.0, str(default_uvtaper), 'repBW')], \
                beams[(0.5, str(default_uvtaper), 'repBW')], \
                beams[(1.0, str(default_uvtaper), 'repBW')], \
                beams[(2.0, str(default_uvtaper), 'repBW')], \
                minAcceptableAngResolution, \
                maxAcceptableAngResolution, \
                gridder == 'mosaic')
        else:
            hm_robust, hm_robust_score = imageprecheck_heuristics.compare_beams( \
                beams[(-0.5, str(default_uvtaper), 'aggBW')], \
                beams[(0.0, str(default_uvtaper), 'aggBW')], \
                beams[(0.5, str(default_uvtaper), 'aggBW')], \
                beams[(1.0, str(default_uvtaper), 'aggBW')], \
                beams[(2.0, str(default_uvtaper), 'aggBW')], \
                minAcceptableAngResolution, \
                maxAcceptableAngResolution, \
                gridder == 'mosaic')

        if real_repr_target:
            # Determine heuristic UV taper value
            #
            # For ALMA Cycle 6 the additional beam, cell and sensitivity values for a different
            # uvtaper are not to be calculated, shown or used.
            if False and hm_robust == 2.0:
                if reprBW_mode in ['nbin', 'repr_spw']:
                    hm_uvtaper = image_heuristics.uvtaper(beam_natural=beams[(2.0, str(default_uvtaper), 'repBW')], protect_long=None)
                else:
                    hm_uvtaper = image_heuristics.uvtaper(beam_natural=beams[(2.0, str(default_uvtaper), 'aggBW')], protect_long=None)
                if hm_uvtaper != []:
                    # Add sensitivity entries with actual tapering
                    beams[(hm_robust, str(hm_uvtaper), 'repBW')], known_synthesized_beams = image_heuristics.synthesized_beam([(repr_field, 'TARGET')], str(repr_spw), robust=hm_robust, uvtaper=hm_uvtaper, known_beams=known_synthesized_beams, force_calc=calcsb)
                    cells[(hm_robust, str(hm_uvtaper), 'repBW')] = image_heuristics.cell(beams[(hm_robust, str(hm_uvtaper), 'repBW')])
                    imsizes[(hm_robust, str(hm_uvtaper), 'repBW')] = image_heuristics.imsize(field_ids, cells[(hm_robust, str(hm_uvtaper), 'repBW')], primary_beam_size, centreonly=False)
                    if reprBW_mode in ['nbin', 'repr_spw']:
                        try:
                            sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                                image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', str(repr_spw), nbin, {}, 'cube', gridder, cells[(hm_robust, str(hm_uvtaper), 'repBW')], imsizes[(hm_robust, str(hm_uvtaper), 'repBW')], 'briggs', hm_robust, hm_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                            sensitivities.append(Sensitivity(
                                array=array,
                                field=repr_field,
                                spw=str(repr_spw),
                                bandwidth=cqa.quantity(sens_bw, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'repBW')],
                                cell=[cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'repBW')][0], 'arcsec'),
                                      cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'repBW')][0], 'arcsec')],
                                robust=hm_robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(sensitivity, 'Jy/beam')))
                        except Exception as e:
                            sensitivities.append(Sensitivity(
                                array=array,
                                field=repr_field,
                                spw=str(repr_spw),
                                bandwidth=cqa.quantity(0.0, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'repBW')],
                                cell=['0.0 arcsec', '0.0 arcsec'],
                                robust=robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(0.0, 'Jy/beam')))

                    beams[(hm_robust, str(hm_uvtaper), 'aggBW')], known_synthesized_beams = image_heuristics.synthesized_beam([(repr_field, 'TARGET')], cont_spw, robust=hm_robust, uvtaper=hm_uvtaper, known_beams=known_synthesized_beams, force_calc=calcsb)
                    cells[(hm_robust, str(hm_uvtaper), 'aggBW')] = image_heuristics.cell(beams[(hm_robust, str(hm_uvtaper), 'aggBW')])
                    imsizes[(hm_robust, str(hm_uvtaper), 'aggBW')] = image_heuristics.imsize(field_ids, cells[(hm_robust, str(hm_uvtaper), 'aggBW')], primary_beam_size, centreonly=False)
                    try:
                        sensitivity, eff_ch_bw, sens_bw, known_per_spw_cont_sensitivities_all_chan = \
                            image_heuristics.calc_sensitivities(inputs.vis, repr_field, 'TARGET', cont_spw, -1, {}, 'cont', gridder, cells[(hm_robust, str(hm_uvtaper), 'aggBW')], imsizes[(hm_robust, str(hm_uvtaper), 'aggBW')], 'briggs', hm_robust, hm_uvtaper, True, known_per_spw_cont_sensitivities_all_chan, calcsb)
                        if scale_aggBW_to_repBW and cont_sens_bw_mode == 'repBW':
                            # Handle scaling to repSPW_BW < repBW <= 0.9 * aggBW case
                            _bandwidth = repr_target[2]
                            _sensitivity = cqa.mul(cqa.quantity(sensitivity, 'Jy/beam'), cqa.sqrt(cqa.div(cqa.quantity(sens_bw, 'Hz'), repr_target[2])))
                        else:
                            _bandwidth = cqa.quantity(min(sens_bw, num_cont_spw * 1.875e9), 'Hz')
                            _sensitivity = cqa.quantity(sensitivity, 'Jy/beam')

                        for cont_sens_bw_mode in cont_sens_bw_modes:
                            sensitivities.append(Sensitivity(
                                array=array,
                                field=repr_field,
                                spw=str(repr_spw),
                                bandwidth=_bandwidth,
                                bwmode=cont_sens_bw_mode,
                                beam=beams[(hm_robust, str(hm_uvtaper), 'aggBW')],
                                cell=[cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'aggBW')][0], 'arcsec'),
                                      cqa.convert(cells[(hm_robust, str(hm_uvtaper), 'aggBW')][0], 'arcsec')],
                                robust=hm_robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=_sensitivity))
                    except:
                        for _ in cont_sens_bw_modes:
                            sensitivities.append(Sensitivity(
                                array=array,
                                field=repr_field,
                                spw=str(repr_spw),
                                bandwidth=cqa.quantity(0.0, 'Hz'),
                                bwmode='repBW',
                                beam=beams[(hm_robust, str(hm_uvtaper), 'aggBW')],
                                cell=['0.0 arcsec', '0.0 arcsec'],
                                robust=robust,
                                uvtaper=hm_uvtaper,
                                sensitivity=cqa.quantity(0.0, 'Jy/beam')))
            else:
                hm_uvtaper = default_uvtaper
        else:
            hm_robust = 0.5
            hm_uvtaper = default_uvtaper
            minAcceptableAngResolution = cqa.quantity(0.0, 'arcsec')
            maxAcceptableAngResolution = cqa.quantity(0.0, 'arcsec')

        hm_uvtaper = default_uvtaper

        return ImagePreCheckResults(
            real_repr_target,
            repr_target,
            repr_source,
            repr_spw,
            reprBW_mode,
            nbin,
            minAcceptableAngResolution=minAcceptableAngResolution,
            maxAcceptableAngResolution=maxAcceptableAngResolution,
            sensitivityGoal=sensitivityGoal,
            hm_robust=hm_robust,
            hm_uvtaper=hm_uvtaper,
            sensitivities=sensitivities,
            sensitivity_bandwidth=sensitivity_bandwidth,
            score=hm_robust_score,
            single_continuum=single_continuum,
            per_spw_cont_sensitivities_all_chan=known_per_spw_cont_sensitivities_all_chan,
            synthesized_beams=known_synthesized_beams
        )

    def analyse(self, results):
        return results
