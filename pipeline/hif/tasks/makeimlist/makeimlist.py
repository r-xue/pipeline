from __future__ import absolute_import

import copy
import os

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import task_registry
from .cleantarget import CleanTarget
from .resultobjects import MakeImListResult

LOG = infrastructure.get_logger(__name__)


class MakeImListInputs(vdp.StandardInputs):
    # simple properties with no logic ----------------------------------------------------------------------------------
    calmaxpix = vdp.VisDependentProperty(default=300)
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='TARGET')
    nchan = vdp.VisDependentProperty(default=-1)
    outframe = vdp.VisDependentProperty(default='LSRK')
    phasecenter = vdp.VisDependentProperty(default='')
    start = vdp.VisDependentProperty(default='')
    uvrange = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default='')
    clearlist = vdp.VisDependentProperty(default=True)
    pbe_eb = vdp.VisDependentProperty(default=False)
    calcsb = vdp.VisDependentProperty(default=False)
    parallel = vdp.VisDependentProperty(default='automatic')

    # properties requiring some processing or MS-dependent logic -------------------------------------------------------

    contfile = vdp.VisDependentProperty(default='cont.dat')

    @contfile.postprocess
    def contfile(self, unprocessed):
        return os.path.join(self.context.output_dir, unprocessed)

    @vdp.VisDependentProperty
    def field(self):
        if 'TARGET' in self.intent and 'field' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['field']
        return ''

    @vdp.VisDependentProperty
    def hm_cell(self):
        if 'TARGET' in self.intent and 'hm_cell' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['hm_cell']
        return []

    @hm_cell.convert
    def hm_cell(self, val):
        if not isinstance(val, str) and not isinstance(val, list):
            raise ValueError('Malformatted value for hm_cell: {!r}'.format(val))

        if isinstance(val, str):
            val = [val]

        for item in val:
            if isinstance(item, str):
                if 'ppb' in item:
                    return item

        return val

    @vdp.VisDependentProperty
    def hm_imsize(self):
        if 'TARGET' in self.intent and 'hm_imsize' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['hm_imsize']
        return []

    @hm_imsize.convert
    def hm_imsize(self, val):
        if not isinstance(val, int) and not isinstance(val, str) and not isinstance(val, list):
            raise ValueError('Malformatted value for hm_imsize: {!r}'.format(val))

        if isinstance(val, int):
            return [val, val]

        if isinstance(val, str):
            val = [val]

        for item in val:
            if isinstance(item, str):
                if 'pb' in item:
                    return item

        return val

    linesfile = vdp.VisDependentProperty(default='lines.dat')

    @linesfile.postprocess
    def linesfile(self, unprocessed):
        return os.path.join(self.context.output_dir, unprocessed)

    @vdp.VisDependentProperty
    def nbins(self):
        if 'TARGET' in self.intent and 'nbins' in self.context.size_mitigation_parameters:
            return self.context.size_mitigation_parameters['nbins']
        return ''

    @vdp.VisDependentProperty
    def spw(self):
        if 'TARGET' in self.intent and 'spw' in self.context.size_mitigation_parameters and self.specmode=='cube':
            return self.context.size_mitigation_parameters['spw']
        return ''

    @spw.convert
    def spw(self, val):
        # Use str() method to catch single spwid case via PPR which maps to int.
        return str(val)

    @vdp.VisDependentProperty
    def specmode(self):
        if 'TARGET' in self.intent:
            return 'cube'
        return 'mfs'

    def __init__(self, context, output_dir=None, vis=None, imagename=None, intent=None, field=None, spw=None,
                 contfile=None, linesfile=None, uvrange=None, specmode=None, outframe=None, hm_imsize=None,
                 hm_cell=None, calmaxpix=None, phasecenter=None, nchan=None, start=None, width=None, nbins=None,
                 robust=None, uvtaper=None, clearlist=None, per_eb=None, calcsb=None, parallel=None, known_synthesized_beams=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.imagename = imagename
        self.intent = intent
        self.field = field
        self.spw = spw
        self.contfile = contfile
        self.linesfile = linesfile
        self.uvrange = uvrange
        self.specmode = specmode
        self.outframe = outframe
        self.hm_imsize = hm_imsize
        self.hm_cell = hm_cell
        self.calmaxpix = calmaxpix
        self.phasecenter = phasecenter
        self.nchan = nchan
        self.start = start
        self.width = width
        self.nbins = nbins
        self.robust = robust
        self.uvtaper = uvtaper
        self.clearlist = clearlist
        self.per_eb = per_eb
        self.calcsb = calcsb
        self.parallel = parallel
        self.known_synthesized_beams = known_synthesized_beams


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
api.ImagingMeasurementSetsPreferred.register(MakeImListInputs)


@task_registry.set_equivalent_casa_task('hif_makeimlist')
@task_registry.set_casa_commands_comment('A list of target sources to be imaged is constructed.')
class MakeImList(basetask.StandardTaskTemplate):
    Inputs = MakeImListInputs

    is_multi_vis_task = True

    def prepare(self):
        # this python class will produce a list of images to be calculated.
        inputs = self.inputs

        calcsb = inputs.calcsb
        parallel = inputs.parallel
        if inputs.known_synthesized_beams is not None:
            known_synthesized_beams = inputs.known_synthesized_beams
        else:
            known_synthesized_beams = inputs.context.synthesized_beams

        qaTool = casatools.quanta

        result = MakeImListResult()
        result.clearlist = inputs.clearlist

        # describe the function of this task by interpreting the inputs
        # parameters to give an execution context
        long_descriptions = [_DESCRIPTIONS.get((intent.strip(), inputs.specmode), inputs.specmode) for intent in inputs.intent.split(',')]
        result.metadata['long description'] = 'Set-up parameters for %s imaging' % ' & '.join(set(long_descriptions))

        sidebar_suffixes = {_SIDEBAR_SUFFIX.get((intent.strip(), inputs.specmode), inputs.specmode) for intent in inputs.intent.split(',')}
        result.metadata['sidebar suffix'] = '/'.join(sidebar_suffixes)

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result.mitigation_error = True
                result.set_info({'msg': 'Size mitigation had failed. No imaging targets were created.',
                                 'intent': inputs.intent,
                                 'specmode': inputs.specmode})
                result.contfile = None
                result.linesfile = None
                return result

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()

        # representative target case
        if inputs.specmode == 'repBW':
            repr_target_mode = True
            image_repr_target = False

            # Initial heuristics instance without spw information.
            self.heuristics = image_heuristics_factory.getHeuristics(
                vislist=inputs.vis,
                spw='',
                observing_run=inputs.context.observing_run,
                imagename_prefix=inputs.context.project_structure.ousstatus_entity_id,
                proj_params=inputs.context.project_performance_parameters,
                contfile=inputs.contfile,
                linesfile=inputs.linesfile,
                imaging_params=inputs.context.imaging_parameters,
                imaging_mode=inputs.context.project_summary.telescope
            )

            repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, maxAllowedBeamAxialRatio, sensitivityGoal = self.heuristics.representative_target()
            # The PI cube shall only be created for real representative targets
            if not real_repr_target:
                LOG.info('No representative target found. No PI cube will be made.')
                result.set_info({'msg': 'No representative target found. No PI cube will be made.',
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            # The PI cube shall only be created for cube mode
            elif reprBW_mode in ['multi_spw', 'all_spw']:
                LOG.info("Representative target bandwidth specifies aggregate continuum. No PI cube will be made since"
                         " specmode='cont' already covers this case.")
                result.set_info({'msg': "Representative target bandwidth specifies aggregate continuum. No PI cube will"
                                        " be made since specmode='cont' already covers this case.",
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            elif reprBW_mode == 'repr_spw':
                LOG.info("Representative target bandwidth specifies per spw continuum. No PI cube will be made since"
                         " specmode='mfs' already covers this case.")
                result.set_info({'msg': "Representative target bandwidth specifies per spw continuum. No PI cube will"
                                        " be made since specmode='mfs' already covers this case.",
                                 'intent': 'TARGET',
                                 'specmode': 'repBW'})
                result.contfile = None
                result.linesfile = None
                return result
            else:
                repr_spw_nbin = 1
                if inputs.context.size_mitigation_parameters.get('nbins', '') != '':
                    nbin_items = inputs.nbins.split(',')
                    for nbin_item in nbin_items:
                        key, value = nbin_item.split(':')
                        if key == str(repr_spw):
                            repr_spw_nbin = int(value)

                # The PI cube shall only be created if the PI bandwidth is greater
                # than 4 times the nbin averaged bandwidth used in the default cube
                ref_ms = inputs.context.observing_run.get_ms(inputs.vis[0])
                real_repr_spw = inputs.context.observing_run.virtual2real_spw_id(repr_spw, ref_ms)
                physicalBW_of_1chan_Hz = float(ref_ms.get_spectral_window(real_repr_spw).channels[0].getWidth().convert_to(measures.FrequencyUnits.HERTZ).value)
                repr_spw_nbin_bw_Hz = repr_spw_nbin * physicalBW_of_1chan_Hz
                reprBW_Hz = qaTool.getvalue(qaTool.convert(repr_target[2], 'Hz'))

                if reprBW_Hz > 4.0 * repr_spw_nbin_bw_Hz:
                    repr_spw_nbin = int(reprBW_Hz / physicalBW_of_1chan_Hz + 0.5)
                    inputs.nbins = '%d:%d' % (repr_spw, repr_spw_nbin)
                    LOG.info('Making PI cube at %.3g MHz channel width.' % (physicalBW_of_1chan_Hz * repr_spw_nbin / 1e6))
                    image_repr_target = True
                    inputs.field = repr_source
                    inputs.spw = str(repr_spw)
                else:
                    LOG.info('Representative target bandwidth is less or equal than 4 times the nbin averaged default'
                             ' cube channel width. No PI cube will be made since the default cube already covers this'
                             ' case.')
                    result.set_info({'msg': 'Representative target bandwidth is less or equal than 4 times the nbin'
                                            ' averaged default cube channel width. No PI cube will be made since the'
                                            ' default cube already covers this case.',
                                     'intent': 'TARGET',
                                     'specmode': 'repBW'})
                    result.contfile = None
                    result.linesfile = None
                    return result
        else:
            repr_target_mode = False
            image_repr_target = False

        if (not repr_target_mode) or (repr_target_mode and image_repr_target):
            # read the spw, if none then set default 
            spw = inputs.spw

            if spw == '':
                spwids = sorted(inputs.context.observing_run.virtual_science_spw_ids.keys(), key=int)
            else:
                spwids = spw.split(',')
            spw = ','.join("'%s'" % (spwid) for spwid in spwids)
            spw = '[%s]' % spw

            spwlist = spw.replace('[', '').replace(']', '')
            spwlist = spwlist[1:-1].split("','")
        else:
            spw = '[]'
            spwlist = []

        if inputs.per_eb:
            vislists = [[vis] for vis in inputs.vis]
        else:
            vislists = [inputs.vis]

        # VLA only
        if inputs.context.project_summary.telescope in ('VLA', 'JVLA', 'EVLA') and inputs.specmode == 'cont':
            ms = inputs.context.observing_run.get_ms(inputs.vis[0])
            band = ms.get_vla_spw2band()
            band_spws = {}
            for k, v in band.iteritems():
                if str(k) in spwlist:
                    band_spws.setdefault(v, []).append(k)
        else:
            band_spws = {None: 0}

        # Need to record if there are targets for a vislist
        have_targets = {}

        max_num_targets = 0

        for band in band_spws:
            if band != None:
                spw = band_spws[band].__repr__()
                spwlist = band_spws[band]
            for vislist in vislists:
                if inputs.per_eb:
                    imagename_prefix = os.path.basename(vislist[0]).strip('.ms')
                else:
                    imagename_prefix = inputs.context.project_structure.ousstatus_entity_id

                self.heuristics = image_heuristics_factory.getHeuristics(
                    vislist=vislist,
                    spw=spw,
                    observing_run=inputs.context.observing_run,
                    imagename_prefix=imagename_prefix,
                    proj_params=inputs.context.project_performance_parameters,
                    contfile=inputs.contfile,
                    linesfile=inputs.linesfile,
                    imaging_params=inputs.context.imaging_parameters,
                    imaging_mode=inputs.context.project_summary.telescope
                )
                if inputs.specmode == 'cont':
                    # Make sure the spw list is sorted numerically
                    spwlist = [','.join(map(str, sorted(map(int, spwlist))))]

                # get list of field_ids/intents to be cleaned
                if (not repr_target_mode) or (repr_target_mode and image_repr_target):
                    field_intent_list = self.heuristics.field_intent_list(
                      intent=inputs.intent, field=inputs.field)
                else:
                    continue

                # Parse hm_cell to get optional pixperbeam setting
                cell = inputs.hm_cell
                if isinstance(cell, str):
                    pixperbeam = float(cell.split('ppb')[0])
                    cell = []
                else:
                    pixperbeam = 5.0

                # Expand cont spws
                if inputs.specmode == 'cont':
                    spwids = spwlist[0].split(',')
                else:
                    spwids = spwlist

                # Generate list of possible vis/field/spw combinations
                vislist_field_spw_combinations = {}
                for field_intent in field_intent_list:
                    vislist_for_field = []
                    spwids_for_field = set()
                    for vis in vislist:
                        ms_domain_obj = inputs.context.observing_run.get_ms(vis)
                        # Get the real spw IDs for this MS
                        ms_science_spwids = [s.id for s in ms_domain_obj.get_spectral_windows()]
                        if field_intent[0] in [f.name for f in ms_domain_obj.fields]:
                            try:
                                field_domain_obj = ms_domain_obj.get_fields(field_intent[0])[0]
                                # Get all science spw IDs for this field and record the ones that are present in this MS
                                field_science_spwids = [spw_domain_obj.id for spw_domain_obj in field_domain_obj.valid_spws if spw_domain_obj.id in ms_science_spwids]
                                # Record the virtual spwids
                                spwids_per_vis_and_field = [inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj) for spwid in field_science_spwids if inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj) in map(int, spwids)]
                            except:
                                spwids_per_vis_and_field = []
                        else:
                            spwids_per_vis_and_field = []
                        if spwids_per_vis_and_field != []:
                            vislist_for_field.append(vis)
                            spwids_for_field.update(spwids_per_vis_and_field)
                    vislist_field_spw_combinations[field_intent[0]] = {'vislist': None, 'spwids': None}
                    if vislist_for_field != []:
                        vislist_field_spw_combinations[field_intent[0]]['vislist'] = vislist_for_field
                        vislist_field_spw_combinations[field_intent[0]]['spwids'] = sorted(list(spwids_for_field), key=int)

                        # Add number of expected clean targets
                        if inputs.specmode == 'cont':
                            max_num_targets += 1
                        else:
                            max_num_targets += len(spwids_for_field)

                # Remove bad spws
                if field_intent_list != set([]):
                    valid_data = {}
                    # Check only possible field/spw combinations to speed up
                    filtered_spwlist = []
                    for field_intent in field_intent_list:
                        if vislist_field_spw_combinations.get(field_intent[0], None) is not None:
                            spwids_list = vislist_field_spw_combinations.get(field_intent[0], None).get('spwids', None)
                            if spwids_list is not None:
                                for spw in map(str, spwids_list):
                                    valid_data[str(spw)] = self.heuristics.has_data(field_intent_list=[field_intent], spwspec=spw)
                                    if valid_data[str(spw)][field_intent]:
                                        filtered_spwlist.append(spw)
                    filtered_spwlist = sorted(list(set(filtered_spwlist)), key=int)
                else:
                    continue

                # Collapse cont spws
                if inputs.specmode == 'cont':
                    spwlist = [','.join(filtered_spwlist)]
                else:
                    spwlist = filtered_spwlist

                # Need all spw keys (individual and cont) to distribute the
                # cell and imsize heuristic results which work on the
                # highest/lowest frequency spw only.
                # The deep copy is necessary to avoid modifying filtered_spwlist
                all_spw_keys = copy.deepcopy(filtered_spwlist)
                all_spw_keys.append(','.join(filtered_spwlist))
                # Add actual cont spw combinations to be able to properly populate the lookup tables later on
                all_spw_keys.extend([','.join(map(str, vislist_field_spw_combinations[field_intent[0]]['spwids'])) for field_intent in field_intent_list if vislist_field_spw_combinations[field_intent[0]]['spwids'] is not None])

                # Select only the lowest / highest frequency spw to get the smallest (for cell size)
                # and largest beam (for imsize)
                ref_ms = inputs.context.observing_run.get_ms(vislist[0])
                min_freq = 1e15
                max_freq = 0.0
                min_freq_spwid = -1
                max_freq_spwid = -1
                for spwid in filtered_spwlist:
                    real_spwid = inputs.context.observing_run.virtual2real_spw_id(spwid, ref_ms)
                    spwid_centre_freq = ref_ms.get_spectral_window(real_spwid).centre_frequency.to_units(measures.FrequencyUnits.HERTZ)
                    if spwid_centre_freq < min_freq:
                        min_freq = spwid_centre_freq
                        min_freq_spwid = spwid
                    if spwid_centre_freq > max_freq:
                        max_freq = spwid_centre_freq
                        max_freq_spwid = spwid
                min_freq_spwlist = [str(min_freq_spwid)]
                max_freq_spwlist = [str(max_freq_spwid)]

                # Get robust and uvtaper values
                if inputs.robust not in (None, -999.0):
                    robust = inputs.robust
                elif 'robust' in inputs.context.imaging_parameters:
                    robust = inputs.context.imaging_parameters['robust']
                else:
                    robust = self.heuristics.robust()

                if inputs.uvtaper not in (None, []):
                    uvtaper = inputs.uvtaper
                elif 'uvtaper' in inputs.context.imaging_parameters:
                    uvtaper = inputs.context.imaging_parameters['uvtaper']
                else:
                    uvtaper = self.heuristics.uvtaper()

                # cell is a list of form [cellx, celly]. If the list has form [cell]
                # then that means the cell is the same size in x and y. If cell is
                # empty then fill it with a heuristic result
                cells = {}
                if cell == []:
                    synthesized_beams = {}
                    min_cell = ['3600arcsec']
                    for spwspec in max_freq_spwlist:
                        # Use only fields that were observed in spwspec
                        actual_field_intent_list = []
                        for field_intent in field_intent_list:
                            if vislist_field_spw_combinations.get(field_intent[0], None) is not None:
                                if vislist_field_spw_combinations[field_intent[0]].get('spwids', None) is not None:
                                    if spwspec in map(str, vislist_field_spw_combinations[field_intent[0]]['spwids']):
                                        actual_field_intent_list.append(field_intent)
                        synthesized_beams[spwspec], known_synthesized_beams = self.heuristics.synthesized_beam(field_intent_list=actual_field_intent_list, spwspec=spwspec, robust=robust, uvtaper=uvtaper, pixperbeam=pixperbeam, known_beams=known_synthesized_beams, force_calc=calcsb, parallel=parallel, shift=True)
                        if synthesized_beams[spwspec] == 'invalid':
                            LOG.error('Beam for virtual spw %s and robust value of %.1f is invalid. Cannot continue.'
                                      '' % (spwspec, robust))
                            result.error = True
                            result.error_msg = 'Invalid beam'
                            return result
                        # Avoid recalculating every time since the dictionary will be cleared with the first recalculation request.
                        calcsb = False
                        # the heuristic cell is always the same for x and y as
                        # the value derives from the single value returned by
                        # imager.advise
                        cells[spwspec] = self.heuristics.cell(beam=synthesized_beams[spwspec], pixperbeam=pixperbeam)
                        if ('invalid' not in cells[spwspec]):
                            min_cell = cells[spwspec] if (qaTool.convert(cells[spwspec][0], 'arcsec')['value'] < qaTool.convert(min_cell[0], 'arcsec')['value']) else min_cell
                    # Rounding to two significant figures
                    min_cell = ['%.2g%s' % (qaTool.getvalue(min_cell[0]), qaTool.getunit(min_cell[0]))]
                    # Use same cell size for all spws (in a band (TODO))
                    # Need to populate all spw keys because the imsize heuristic picks
                    # up the lowest frequency spw.
                    for spwspec in all_spw_keys:
                        cells[spwspec] = min_cell
                else:
                    for spwspec in all_spw_keys:
                        cells[spwspec] = cell

                # if phase center not set then use heuristic code to calculate the
                # centers for each field
                phasecenter = inputs.phasecenter
                phasecenters = {}
                if phasecenter == '':
                    for field_intent in field_intent_list:
                        try:
                            gridder = self.heuristics.gridder(field_intent[1], field_intent[0])
                            field_ids = self.heuristics.field(field_intent[1], field_intent[0])
                            phasecenters[field_intent[0]] = self.heuristics.phasecenter(field_ids)
                        except Exception as e:
                            # problem defining center
                            LOG.warn(e)
                            pass
                else:
                    for field_intent in field_intent_list:
                        phasecenters[field_intent[0]] = phasecenter

                # if imsize not set then use heuristic code to calculate the
                # centers for each field/spwspec
                imsize = inputs.hm_imsize
                if isinstance(imsize, str):
                    sfpblimit = float(imsize.split('pb')[0])
                    imsize = []
                else:
                    sfpblimit = 0.2
                imsizes = {}
                if imsize == []:
                    # get primary beams
                    largest_primary_beams = {}
                    for spwspec in min_freq_spwlist:
                        if list(field_intent_list) != []:
                            largest_primary_beams[spwspec] = self.heuristics.largest_primary_beam_size(spwspec=spwspec, intent=list(field_intent_list)[0][1])
                        else:
                            largest_primary_beams[spwspec] = self.heuristics.largest_primary_beam_size(spwspec=spwspec, intent='TARGET')

                    for field_intent in field_intent_list:
                        max_x_size = 1
                        max_y_size = 1
                        for spwspec in min_freq_spwlist:

                            try:
                                gridder = self.heuristics.gridder(field_intent[1], field_intent[0])
                                field_ids = self.heuristics.field(field_intent[1], field_intent[0])
                                himsize = self.heuristics.imsize(
                                    fields=field_ids, cell=cells[spwspec], primary_beam=largest_primary_beams[spwspec],
                                    sfpblimit=sfpblimit, centreonly=False)
                                if field_intent[1] in ['PHASE', 'BANDPASS', 'AMPLITUDE', 'FLUX', 'CHECK']:
                                    himsize = [min(npix, inputs.calmaxpix) for npix in himsize]
                                imsizes[(field_intent[0], spwspec)] = himsize
                                if imsizes[(field_intent[0], spwspec)][0] > max_x_size:
                                    max_x_size = imsizes[(field_intent[0], spwspec)][0]
                                if imsizes[(field_intent[0], spwspec)][1] > max_y_size:
                                    max_y_size = imsizes[(field_intent[0], spwspec)][1]
                            except Exception as e:
                                # problem defining imsize
                                LOG.warn(e)
                                pass

                        if max_x_size == 1 or max_y_size == 1:
                            LOG.error('imsize of [{:d}, {:d}] for field {!s} intent {!s} spw {!s} is degenerate.'.format(max_x_size, max_y_size), field_intent[0], field_intent[1], min_freq_spwlist)
                        else:
                            # Use same size for all spws (in a band (TODO))
                            # Need to populate all spw keys because the imsize for the cont
                            # target is taken from this dictionary.
                            for spwspec in all_spw_keys:
                                imsizes[(field_intent[0], spwspec)] = [max_x_size, max_y_size]

                else:
                    for field_intent in field_intent_list:
                        for spwspec in all_spw_keys:
                            imsizes[(field_intent[0], spwspec)] = imsize

                # if nchan is not set then use heuristic code to calculate it
                # for each field/spwspec. The channel width needs to be calculated
                # at the same time.
                specmode = inputs.specmode
                nchan = inputs.nchan
                nchans = {}
                width = inputs.width
                widths = {}
                if specmode not in ('mfs', 'cont') and width == 'pilotimage':
                    for field_intent in field_intent_list:
                        for spwspec in spwlist:
                            try:
                                nchans[(field_intent[0], spwspec)], widths[(field_intent[0], spwspec)] = \
                                  self.heuristics.nchan_and_width(field_intent=field_intent[1], spwspec=spwspec)
                            except Exception as e:
                                # problem defining nchan and width
                                LOG.warn(e)
                                pass

                else:
                    for field_intent in field_intent_list:
                        for spwspec in all_spw_keys:
                            nchans[(field_intent[0], spwspec)] = nchan
                            widths[(field_intent[0], spwspec)] = width

                usepointing = self.heuristics.usepointing()

                # now construct the list of imaging command parameter lists that must
                # be run to obtain the required images

                # Remember if there are targets for this vislist
                have_targets[','.join(vislist)] = len(field_intent_list) > 0

                for field_intent in field_intent_list:
                    mosweight = self.heuristics.mosweight(field_intent[1], field_intent[0])
                    for spwspec in spwlist:
                        # The field/intent and spwspec loops still cover the full parameter
                        # space. Here we filter the actual combinations.
                        valid_field_spwspec_combination = False
                        for spwid in spwspec.split(','):
                            if vislist_field_spw_combinations[field_intent[0]].get('spwids', None) is not None:
                                if int(spwid) in vislist_field_spw_combinations[field_intent[0]]['spwids']:
                                    valid_field_spwspec_combination = True
                        if not valid_field_spwspec_combination:
                            continue

                        # Save the specific vislist in a copy of the heuristics object tailored to the
                        # current imaging target
                        target_heuristics = copy.deepcopy(self.heuristics)
                        target_heuristics.vislist = vislist_field_spw_combinations[field_intent[0]]['vislist']

                        # For 'cont' mode we still need to restrict the virtual spw ID list to just
                        # the ones that were actually observed for this field.
                        if inputs.specmode == 'cont':
                            adjusted_spwspec = ','.join(map(str, vislist_field_spw_combinations[field_intent[0]]['spwids']))
                        else:
                            adjusted_spwspec = spwspec

                        spwspec_ok = True
                        actual_spwspec_list = []
                        spwsel = {}
                        all_continuum = True
                        cont_ranges_spwsel_dict = {}
                        all_continuum_spwsel_dict = {}
                        spwsel_spwid_dict = {}

                        for spwid in adjusted_spwspec.split(','):
                            cont_ranges_spwsel_dict[spwid], all_continuum_spwsel_dict[spwid] = target_heuristics.cont_ranges_spwsel()
                            spwsel_spwid_dict[spwid] = cont_ranges_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, 'NONE')

                        no_cont_ranges = False
                        if field_intent[1] == 'TARGET' and specmode == 'cont' and all([v == 'NONE' for v in spwsel_spwid_dict.itervalues()]):
                            LOG.warn('No valid continuum ranges were found for any spw. Creating an aggregate continuum'
                                     ' image from the full bandwidth from all spws, but this should be used with'
                                     ' caution.')
                            no_cont_ranges = True

                        for spwid in adjusted_spwspec.split(','):
                            spwsel_spwid = spwsel_spwid_dict[spwid]
                            if field_intent[1] == 'TARGET' and not no_cont_ranges:
                                if spwsel_spwid == 'NONE':
                                    if specmode == 'cont':
                                        LOG.warn('Spw {!s} will not be used in creating the aggregate continuum image'
                                                 ' of {!s} because no continuum range was found.'
                                                 ''.format(spwid, field_intent[0]))
                                    else:
                                        LOG.warn('Spw {!s} will not be used for {!s} because no continuum range was'
                                                 ' found.'.format(spwid, field_intent[0]))
                                        spwspec_ok = False
                                    continue
                                #elif (spwsel_spwid == ''):
                                #    LOG.warn('Empty continuum frequency range for %s, spw %s. Run hif_findcont ?' % (field_intent[0], spwid))

                            all_continuum = all_continuum and all_continuum_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, False)

                            if spwsel_spwid in ('ALL', '', 'NONE'):
                                spwsel_spwid_freqs = ''
                                spwsel_spwid_refer = 'LSRK'
                            else:
                                spwsel_spwid_freqs, spwsel_spwid_refer = spwsel_spwid.split()

                            if spwsel_spwid_refer != 'LSRK':
                                LOG.warn('Frequency selection is specified in %s but must be in LSRK'
                                         '' % spwsel_spwid_refer)
                                # TODO: skip this field and/or spw ?

                            actual_spwspec_list.append(spwid)
                            spwsel['spw%s' % (spwid)] = spwsel_spwid

                        actual_spwspec = ','.join(actual_spwspec_list)

                        num_all_spws = len(adjusted_spwspec.split(','))
                        num_good_spws = 0 if no_cont_ranges else len(actual_spwspec_list)

                        # construct imagename
                        if inputs.imagename == '':
                            imagename = target_heuristics.imagename(output_dir=inputs.output_dir, intent=field_intent[1],
                                                                    field=field_intent[0], spwspec=actual_spwspec,
                                                                    specmode=specmode, band=band)
                        else:
                            imagename = inputs.imagename

                        if inputs.nbins != '' and inputs.specmode != 'cont':
                            nbin_items = inputs.nbins.split(',')
                            nbins_dict = {}
                            for nbin_item in nbin_items:
                                key, value = nbin_item.split(':')
                                nbins_dict[key] = int(value)
                            try:
                                if '*' in nbins_dict.keys():
                                    nbin = nbins_dict['*']
                                else:
                                    nbin = nbins_dict[adjusted_spwspec]
                            except:
                                LOG.warn('Could not determine binning factor for spw %s. Using default channel width.'
                                         '' % adjusted_spwspec)
                                nbin = -1
                        else:
                            nbin = -1

                        if spwspec_ok and (field_intent[0], adjusted_spwspec) in imsizes and ('invalid' not in cells[adjusted_spwspec]):
                            LOG.debug(
                              'field:%s intent:%s spw:%s cell:%s imsize:%s phasecenter:%s' %
                              (field_intent[0], field_intent[1], adjusted_spwspec,
                               cells[adjusted_spwspec], imsizes[(field_intent[0], adjusted_spwspec)],
                               phasecenters[field_intent[0]]))

                            # Remove MSs that do not contain data for the given field/intent combination
                            scanidlist, visindexlist = target_heuristics.get_scanidlist(vislist_field_spw_combinations[field_intent[0]]['vislist'],
                                                                                        field_intent[0], field_intent[1])
                            filtered_vislist = [vislist_field_spw_combinations[field_intent[0]]['vislist'][i] for i in visindexlist]

                            # Save the filtered vislist
                            target_heuristics.vislist = filtered_vislist

                            # Get list of antenna IDs
                            antenna_ids = target_heuristics.antenna_ids(inputs.intent)
                            antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), '')))
                                       for v in filtered_vislist]

                            any_non_imaging_ms = any([not inputs.context.observing_run.get_ms(vis).is_imaging_ms
                                                      for vis in filtered_vislist])

                            target = CleanTarget(
                                antenna=antenna,
                                field=field_intent[0],
                                intent=field_intent[1],
                                spw=actual_spwspec,
                                spwsel_lsrk=spwsel,
                                spwsel_all_cont=all_continuum,
                                num_all_spws=num_all_spws,
                                num_good_spws=num_good_spws,
                                cell=cells[adjusted_spwspec],
                                imsize=imsizes[(field_intent[0], adjusted_spwspec)],
                                phasecenter=phasecenters[field_intent[0]],
                                specmode=inputs.specmode,
                                gridder=target_heuristics.gridder(field_intent[1], field_intent[0]),
                                imagename=imagename,
                                start=inputs.start,
                                width=widths[(field_intent[0], adjusted_spwspec)],
                                nbin=nbin,
                                nchan=nchans[(field_intent[0], adjusted_spwspec)],
                                robust=robust,
                                uvrange=inputs.uvrange,
                                uvtaper=uvtaper,
                                stokes='I',
                                heuristics=target_heuristics,
                                # TODO: should one always use the filtered vis list to cope with sparse field/spw setups ?
                                vis=filtered_vislist if inputs.per_eb or any_non_imaging_ms else None,
                                is_per_eb=inputs.per_eb if inputs.per_eb else None,
                                usepointing=usepointing,
                                mosweight=mosweight
                            )

                            result.add_target(target)

        if inputs.intent == 'CHECK':
            if not any(have_targets.values()):
                info_msg = 'No check source found.'
                LOG.info(info_msg)
                result.set_info({'msg': info_msg, 'intent': 'CHECK', 'specmode': inputs.specmode})
            elif inputs.per_eb and (not all(have_targets.values())):
                info_msg = 'No check source data found in EBs %s.' % (','.join([os.path.basename(k)
                                                                                for k, v in have_targets.iteritems()
                                                                                if not v]))
                LOG.info(info_msg)
                result.set_info({'msg': info_msg, 'intent': 'CHECK', 'specmode': inputs.specmode})

        # Record total number of expected clean targets
        result.set_max_num_targets(max_num_targets)

        # Pass contfile and linefile names to context (via resultobjects)
        # for hif_findcont and hif_makeimages
        result.contfile = inputs.contfile
        result.linesfile = inputs.linesfile

        result.synthesized_beams = known_synthesized_beams

        return result

    def analyse(self, result):
        return result


# maps intent and specmode Inputs parameters to textual description of execution context.
_DESCRIPTIONS = {
    ('PHASE', 'mfs'): 'phase calibrator',
    ('BANDPASS', 'mfs'): 'bandpass calibrator',
    ('AMPLITUDE', 'mfs'): 'flux calibrator',
    ('CHECK', 'mfs'): 'check source',
    ('TARGET', 'mfs'): 'target per-spw continuum',
    ('TARGET', 'cont'): 'target aggregate continuum',
    ('TARGET', 'cube'): 'target cube',
    ('TARGET', 'repBW'): 'representative bandwidth target cube',
}

_SIDEBAR_SUFFIX = {
    ('PHASE', 'mfs'): 'cals',
    ('BANDPASS', 'mfs'): 'cals',
    ('AMPLITUDE', 'mfs'): 'cals',
    ('CHECK', 'mfs'): 'checksrc',
    ('TARGET', 'mfs'): 'mfs',
    ('TARGET', 'cont'): 'cont',
    ('TARGET', 'cube'): 'cube',
    ('TARGET', 'repBW'): 'cube_repBW',
}
