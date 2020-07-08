import collections
import os

import math
import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.imageheader as imageheader
import pipeline.infrastructure.utils as utils
from pipeline.extern import sensitivity_improvement
from pipeline.h.heuristics import fieldnames
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.domain import DataTable
from . import gridding
from . import sdcombine
from . import weighting
from . import worker
from . import resultobjects
from . import detectcontamination
from .. import common
from ..baseline import baseline
from ..common import compress
from ..common import rasterutil
from ..common import utils as sdutils

LOG = infrastructure.get_logger(__name__)

SensitivityInfo = collections.namedtuple('SensitivityInfo', 'sensitivity representative frequency_range')
# RasterInfo: width=map extent along scan, height=map extent perpendicular to scan
#             angle=scan direction w.r.t. horizontal coordinate, row_separation=separation between raster rows.
RasterInfo = collections.namedtuple('RasterInfo', 'width height scan_angle row_separation row_duration') 


class SDImagingInputs(vdp.StandardInputs):
    """
    Inputs for imaging
    """
    infiles = vdp.VisDependentProperty(default='', null_input=['', None, [], ['']])
    spw = vdp.VisDependentProperty(default='')
    pol = vdp.VisDependentProperty(default='')
    field = vdp.VisDependentProperty(default='')
    mode = vdp.VisDependentProperty(default='line')

    @field.postprocess
    def field(self, unprocessed):
        #LOG.info('field.postprocess: unprocessed = "{0}"'.format(unprocessed))
        if unprocessed is not None and unprocessed != '':
            return unprocessed

        # filters field with intents in self.intent
        p = fieldnames.IntentFieldnames()
        fields = set()
        vislist = [self.vis] if isinstance(self.vis, str) else self.vis
        for vis in vislist:
            # This assumes the same fields in all MSes
            msobj = self.context.observing_run.get_ms(vis)
            # this will give something like '0542+3243,0343+242'
            intent_fields = p(msobj, self.intent)
            fields.update(utils.safe_split(intent_fields))

        #LOG.info('field.postprocess: fields = "{0}"'.format(fields))

        return ','.join(fields)

    @spw.postprocess
    def spw(self, unprocessed):
        if unprocessed is not None and unprocessed != '':
            return unprocessed

        # filters science spws by default (assumes the same spw setting for all MSes)
        vis = self.vis if isinstance(self.vis, str) else self.vis[0]
        msobj = self.context.observing_run.get_ms(vis)
        science_spws = msobj.get_spectral_windows(unprocessed, with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @property
    def antenna(self):
        return ''

    @property
    def intent(self):
        return 'TARGET'

    # Synchronization between infiles and vis is still necessary
    @vdp.VisDependentProperty
    def vis(self):
        return self.infiles

    @property
    def is_ampcal(self):
        return self.mode.upper() == 'AMPCAL'

    def __init__(self, context, mode=None, restfreq=None, infiles=None, field=None, spw=None, org_direction=None):
        super(SDImagingInputs, self).__init__()

        self.context = context
        self.restfreq = restfreq
        if self.restfreq is None:
            self.restfreq = ''
        self.mode = mode
        self.infiles = infiles
        self.field = field
        self.mode = mode
        self.spw = spw
        self.org_direction = org_direction


@task_registry.set_equivalent_casa_task('hsd_imaging')
@task_registry.set_casa_commands_comment('Perform single dish imaging.')
class SDImaging(basetask.StandardTaskTemplate):
    Inputs = SDImagingInputs
    # stokes to image and requred POLs for it
    stokes = 'I'
    # for linear feed in ALMA. this affects pols passed to gridding module
    required_pols = ['XX', 'YY']

    is_multi_vis_task = True

    @classmethod
    def _finalize_worker_result(cls, context, result,
                                sourcename, spwlist, antenna,  # specmode='cube', sourcetype='TARGET',
                                imagemode, stokes, validsp, rms, edge,
                                reduction_group_id, file_index,
                                assoc_antennas, assoc_fields, assoc_spws, # , assoc_pols=pols,
                                sensitivity_info=None, theoretical_rms=None):
        # override attributes for image item
        # the following two attributes are currently hard-coded
        specmode = 'cube'
        sorucetype = 'TARGET'
        _locals = locals()
        image_keys = ('sourcename', 'spwlist', 'antenna', 'ant_name', 'specmode', 'sourcetype')
        for x in image_keys:
            if x in _locals:
                setattr(result.outcome['image'], x, _locals[x])

        # fill outcomes
        outcome_keys = ('imagemode', 'stokes', 'validsp', 'rms', 'edge', 'reduction_group_id',
                        'file_index', 'assoc_antennas', 'assoc_fields', 'assoc_spws', 'assoc_spws')
        for x in outcome_keys:
            if x in _locals:
                result.outcome[x] = _locals[x]

        # attach sensitivity_info if available
        if sensitivity_info is not None:
            result.sensitivity_info = sensitivity_info
        # attach theoretical RMS if available
        if theoretical_rms is not None:
            result.theoretical_rms = theoretical_rms

        # set some information to image header
        image_item = result.outcome['image']
        imagename = image_item.imagename
        for name in (imagename, imagename+'.weight'):
            imageheader.set_miscinfo(name=name,
                                     spw=','.join(map(str, spwlist)),
                                     field=image_item.sourcename,
                                     nfield=1,
                                     type='singledish',
                                     iter=1,  # nominal
                                     intent='TARGET',
                                     specmode='cube',
                                     is_per_eb=False,
                                     context=context)

        # finally replace task attribute with the top-level one
        result.task = cls

    def prepare(self):
        inputs = self.inputs
        context = inputs.context
        reduction_group = context.observing_run.ms_reduction_group
        infiles = inputs.infiles
        restfreq_list = inputs.restfreq
        # list of ms to process
        ms_list = inputs.ms
        ms_names = [msobj.name for msobj in ms_list]
        in_spw = inputs.spw
        # in_field is comma-separated list of target field names that are
        # extracted from all input MSs
        in_field = inputs.field
#         antennalist = inputs.antennalist
        imagemode = inputs.mode.upper()
        cqa = casatools.quanta

        # check if data is NRO
        is_nro = sdutils.is_nro(context)

        # task returns ResultsList
        results = resultobjects.SDImagingResults()
        # search results and retrieve edge parameter from the most
        # recent SDBaselineResults if it exists
        getresult = lambda r: r.read() if hasattr(r, 'read') else r
        registered_results = [getresult(r) for r in context.results]
        baseline_stage = -1
        for stage in range(len(registered_results) - 1, -1, -1):
            if isinstance(registered_results[stage], baseline.SDBaselineResults):
                baseline_stage = stage
        if baseline_stage > 0:
            edge = list(registered_results[baseline_stage].outcome['edge'])
            LOG.info('Retrieved edge information from SDBaselineResults: {}'.format(edge))
        else:
            LOG.info('No SDBaselineResults available. Set edge as [0,0]')
            edge = [0, 0]

        dt_dict = dict((ms.basename, DataTable(os.path.join(context.observing_run.ms_datatable_name, ms.basename)))
                       for ms in ms_list)

        # loop over reduction group (spw and source combination)
        for group_id, group_desc in reduction_group.items():
            LOG.debug('Processing Reduction Group {}'.format(group_id))
            LOG.debug('Group Summary:')
            for m in group_desc:
                LOG.debug('\t{}: Antenna {:d} ({}) Spw {:d} Field {:d} ({})'.format(os.path.basename(m.ms.work_data),
                                                                                    m.antenna_id, m.antenna_name,
                                                                                    m.spw_id,
                                                                                    m.field_id, m.field_name))
            # Which group in group_desc list should be processed

            # fix for CAS-9747
            # There may be the case that observation didn't complete so that some of
            # target fields are missing in MS. In this case, directly pass in_field
            # to get_valid_ms_members causes trouble. As a workaround, ad hoc pre-selection
            # of field name is applied here.
            # 2017/02/23 TN
            field_sel = ''
            if len(in_field) == 0:
                # fine, just go ahead
                field_sel = in_field
            elif group_desc.field_name in [x.strip('"') for x in in_field.split(',')]:
                # pre-selection of the field name
                field_sel = group_desc.field_name
            else:
                # no field name is included in in_field, skip
                LOG.info('Skip reduction group {:d}'.format(group_id))
                continue

            member_list = list(common.get_valid_ms_members(group_desc, ms_names, inputs.antenna, field_sel, in_spw))
            LOG.trace('group {}: member_list={}'.format(group_id, member_list))

            # skip this group if valid member list is empty
            if len(member_list) == 0:
                LOG.info('Skip reduction group {:d}'.format(group_id))
                continue

            member_list.sort()  # list of group_desc IDs to image
            antenna_list = [group_desc[i].antenna_id for i in member_list]
            spwid_list = [group_desc[i].spw_id for i in member_list]
            ms_list = [group_desc[i].ms for i in member_list]
            fieldid_list = [group_desc[i].field_id for i in member_list]
            temp_dd_list = [ms_list[i].get_data_description(spw=spwid_list[i])
                            for i in range(len(member_list))]
            channelmap_range_list = [group_desc[i].channelmap_range for i in member_list]
            # this becomes list of list [[poltypes for ms0], [poltypes for ms1], ...]
#             polids_list = [[ddobj.get_polarization_id(corr) for corr in ddobj.corr_axis \
#                             if corr in self.required_pols ] for ddobj in temp_dd_list]
            pols_list = [[corr for corr in ddobj.corr_axis \
                          if corr in self.required_pols ] for ddobj in temp_dd_list]
            del temp_dd_list

            # NRO specific
            correlations = None
            if is_nro:
                _correlations = []
                for c in pols_list:
                    if c not in _correlations:
                        _correlations.append(c)
                assert len(_correlations) == 1
                correlations = ''.join(_correlations[0])

            LOG.debug('Members to be processed:')
            for i in range(len(member_list)):
                LOG.debug('\t{}: Antenna {} Spw {} Field {}'.format(os.path.basename(ms_list[i].work_data),
                                                                    antenna_list[i],
                                                                    spwid_list[i],
                                                                    fieldid_list[i]))

            # image is created per antenna (science) or per asdm and antenna (ampcal)
            image_group = {}
            for (msobj, ant, spwid, fieldid, pollist, chanmap) in zip(ms_list, antenna_list,
                                                                      spwid_list, fieldid_list,
                                                                      pols_list, channelmap_range_list):
                field_name = msobj.fields[fieldid].name
                identifier = field_name
                antenna = msobj.antennas[ant].name
                identifier += ('.'+antenna)
                # create image per asdm and antenna for ampcal
                if inputs.is_ampcal:
                    asdm_name = common.asdm_name_from_ms(msobj)
                    identifier += ('.'+asdm_name)
                if identifier in image_group:
                    image_group[identifier].append([msobj, ant, spwid, fieldid, pollist, chanmap])
                else:
                    image_group[identifier] = [[msobj, ant, spwid, fieldid, pollist, chanmap]]
            LOG.debug('image_group={}'.format(image_group))

            # loop over antennas
            combined_infiles = []
            combined_antids = []
            combined_fieldids = []
            combined_spws = []
            combined_v_spws = []
            tocombine_images = []
            tocombine_org_directions = []
            combined_pols = []
            combined_rms_exclude = []

            # for combined images for NRO
            tocombine_images_nro = []
            tocombine_org_directions_nro = []

            coord_set = False
            for name, _members in image_group.items():
                msobjs = [x[0] for x in _members]
                antids = [x[1] for x in _members]
                spwids = [x[2] for x in _members]
                fieldids = [x[3] for x in _members]
                polslist = [x[4] for x in _members]
                chanmap_range_list = [x[5] for x in _members]
                LOG.info("Processing image group: {}".format(name))
                for idx in range(len(msobjs)):
                    LOG.info("\t{}: Antenna {:d} ({}) Spw {} Field {:d} ({})"
                             "".format(msobjs[idx].basename, antids[idx], msobjs[idx].antennas[antids[idx]].name,
                                       spwids[idx], fieldids[idx], msobjs[idx].fields[fieldids[idx]].name))

                # reference data is first MS
                ref_ms = msobjs[0]
                ant_name = ref_ms.antennas[antids[0]].name
                # for ampcal
                asdm = None
                if inputs.is_ampcal:
                    asdm = common.asdm_name_from_ms(ref_ms)

                # source name
                source_name = group_desc.field_name.replace(' ', '_')

                # filenames for gridding
                infiles = [ms.work_data for ms in msobjs]

                LOG.debug('infiles={}'.format(infiles))

                # image name
                # image name should be based on virtual spw id
                v_spwids = [context.observing_run.real2virtual_spw_id(s, m) for s, m in zip(spwids, msobjs)]
                v_spwids_unique = numpy.unique(v_spwids)
                assert len(v_spwids_unique) == 1
                imagename = self.get_imagename(source_name, v_spwids_unique, ant_name, asdm)
                LOG.info("Output image name: {}".format(imagename))
                imagename_nro = None
                if is_nro:
                    imagename_nro = self.get_imagename(source_name, v_spwids_unique, ant_name, asdm, stokes=correlations)
                    LOG.info("Output image name for NRO: {}".format(imagename_nro))

                # pick restfreq from restfreq_list
                if isinstance(restfreq_list, list):
                    v_spwid = context.observing_run.real2virtual_spw_id(spwids[0], msobjs[0])
                    v_idx = in_spw.split(',').index(str(v_spwid))
                    if len(restfreq_list) > v_idx:
                        restfreq = restfreq_list[v_idx]
                        if restfreq is None:
                            restfreq = ''
                        LOG.info( "Picked restfreq = '{}' from {}".format(restfreq, restfreq_list) )
                    else:
                        restfreq = ''
                        LOG.warning( "No restfreq for spw {} in {}. Applying default value.".format(v_spwid, restfreq_list) )
                else:
                    restfreq = restfreq_list
                    LOG.info("Processing with restfreq = {}".format(restfreq ))

                # Step 1.
                # Initialize weight column based on baseline RMS.
                original_ms = [msobj.name for msobj in msobjs]
                work_ms = [msobj.work_data for msobj in msobjs]
                weighting_inputs = vdp.InputsContainer(weighting.WeightMS, context,
                                                       infiles=original_ms, outfiles=work_ms,
                                                       antenna=antids, spwid=spwids, fieldid=fieldids)
                weighting_task = weighting.WeightMS(weighting_inputs)
                job = common.ParameterContainerJob(weighting_task, datatable_dict=dt_dict)
                weighting_result = self._executor.execute(job, merge=False)
                del weighting_result # Not used

                # Step 2.
                # Imaging
                # Image per antenna, source
                LOG.info('Imaging Source {}, Ant {} Spw {:d}'.format(source_name, ant_name, spwids[0]))
                # map coordinate (use identical map coordinate per spw)
                if not coord_set:
                    # PIPE-313: evaluate map extent using pointing data from all the antenna in the data
                    dummyids = [None for _ in antids]
                    image_coord = worker.ALMAImageCoordinateUtil(context, infiles, dummyids, spwids, fieldids)
                    if not image_coord:  # No valid data is found
                        continue
                    coord_set = True
                    (phasecenter, cellx, celly, nx, ny, org_direction) = image_coord

                # register data for combining
                combined_infiles.extend(infiles)
                combined_antids.extend(antids)
                combined_fieldids.extend(fieldids)
                combined_spws.extend(spwids)
                combined_v_spws.extend(v_spwids)
                combined_pols.extend(polslist)

                stokes_list = [self.stokes]
                imagename_list = [imagename]
                if is_nro:
                    stokes_list.append(correlations)
                    imagename_list.append(imagename_nro)

                imager_results = []
                for _stokes, _imagename in zip(stokes_list, imagename_list):
                    imager_inputs = worker.SDImagingWorker.Inputs(context, infiles,
                                                                  outfile=_imagename,
                                                                  mode=imagemode,
                                                                  antids=antids,
                                                                  spwids=spwids,
                                                                  fieldids=fieldids,
                                                                  restfreq=restfreq,
                                                                  stokes=_stokes,
                                                                  edge=edge,
                                                                  phasecenter=phasecenter,
                                                                  cellx=cellx,
                                                                  celly=celly,
                                                                  nx=nx, ny=ny,
                                                                  org_direction=org_direction)
                    imager_task = worker.SDImagingWorker(imager_inputs)
                    _imager_result = self._executor.execute(imager_task)
                    imager_results.append(_imager_result)
                # per-antenna image (usually Stokes I)
                imager_result = imager_results[0]
                # per-antenna correlation image (XXYY/RRLL)
                imager_result_nro = imager_results[1] if is_nro else None

                if imager_result.outcome is not None:
                    # Imaging was successful, proceed following steps

                    # add image list to combine
                    if os.path.exists(imagename) and os.path.exists(imagename+'.weight'):
                        tocombine_images.append(imagename)
                        tocombine_org_directions.append(org_direction)
                    # Additional Step.
                    # Make grid_table and put rms and valid spectral number array
                    # to the outcome.
                    # The rms and number of valid spectra is used to create RMS maps.
                    LOG.info('Additional Step. Make grid_table')
                    imagename = imager_result.outcome['image'].imagename
                    with casatools.ImageReader(imagename) as ia:
                        cs = ia.coordsys()
                        dircoords = [i for i in range(cs.naxes())
                                     if cs.axiscoordinatetypes()[i] == 'Direction']
                        cs.done()
                        nx = ia.shape()[dircoords[0]]
                        ny = ia.shape()[dircoords[1]]

                    observing_pattern = msobjs[0].observing_pattern[antids[0]][spwids[0]][fieldids[0]]
                    grid_task_class = gridding.gridding_factory(observing_pattern)
                    validsps = []
                    rmss = []
                    grid_input_dict = {}
                    for (msobj, antid, spwid, fieldid, poltypes, _dummy) in _members:
                        msname = msobj.name # Use parent ms
                        for p in poltypes:
                            if p not in grid_input_dict:
                                grid_input_dict[p] = [[msname], [antid], [fieldid], [spwid]]
                            else:
                                grid_input_dict[p][0].append(msname)
                                grid_input_dict[p][1].append(antid)
                                grid_input_dict[p][2].append(fieldid)
                                grid_input_dict[p][3].append(spwid)

                    # Generate grid table for each POL in image (per ANT,
                    # FIELD, and SPW, over all MSes)
                    for pol, member in grid_input_dict.items():
                        _mses = member[0]
                        _antids = member[1]
                        _fieldids = member[2]
                        _spwids = member[3]
                        _pols = [pol for i in range(len(_mses))]
                        gridding_inputs = grid_task_class.Inputs(context, infiles=_mses,
                                                                 antennaids=_antids,
                                                                 fieldids=_fieldids,
                                                                 spwids=_spwids,
                                                                 poltypes=_pols,
                                                                 nx=nx, ny=ny)
                        gridding_task = grid_task_class(gridding_inputs)
                        job = common.ParameterContainerJob(gridding_task, datatable_dict=dt_dict)
                        gridding_result = self._executor.execute(job, merge=False)

                        # Extract RMS and number of spectra from grid_tables
                        if isinstance(gridding_result.outcome, compress.CompressedObj):
                            grid_table = gridding_result.outcome.decompress()
                        else:
                            grid_table = gridding_result.outcome
                        validsps.append([r[6] for r in grid_table])
                        rmss.append([r[8] for r in grid_table])
                        del grid_table

                    # define RMS ranges in image
                    LOG.info("Calculate spectral line and deviation mask frequency ranges in image.")
                    with casatools.ImageReader(imagename) as ia:
                        cs = ia.coordsys()
                        frequency_frame = cs.getconversiontype('spectral')
                        cs.done()
                        rms_exclude_freq = self._get_rms_exclude_freq_range_image(
                            frequency_frame, chanmap_range_list, edge, msobjs, antids, spwids, fieldids)
                        LOG.info("The spectral line and deviation mask frequency ranges = {}".format(str(rms_exclude_freq)))
                    combined_rms_exclude.extend(rms_exclude_freq)

                    file_index = [common.get_parent_ms_idx(context, name) for name in infiles]
                    self._finalize_worker_result(context, imager_result,
                                                 sourcename=source_name, spwlist=v_spwids, antenna=ant_name, #specmode='cube', sourcetype='TARGET',
                                                 imagemode=imagemode, stokes=self.stokes, validsp=validsps, rms=rmss, edge=edge,
                                                 reduction_group_id=group_id, file_index=file_index,
                                                 assoc_antennas=antids, assoc_fields=fieldids, assoc_spws=v_spwids) #, assoc_pols=pols)

                    if inputs.is_ampcal:
                        if len(infiles)==1 and (asdm not in ['', None]): imager_result.outcome['vis'] = asdm
#                         # to register exported_ms to each scantable instance
#                         outcome['export_results'] = export_results

                    # NRO doesn't need per-antenna Stokes I images
                    if not is_nro:
                        results.append(imager_result)

                if imager_result_nro is not None and imager_result_nro.outcome is not None:
                    # Imaging was successful, proceed following steps

                    # add image list to combine
                    if os.path.exists(imagename_nro) and os.path.exists(imagename_nro+'.weight'):
                        tocombine_images_nro.append(imagename_nro)
                        tocombine_org_directions_nro.append(org_direction)

                    file_index = [common.get_parent_ms_idx(context, name) for name in infiles]
                    self._finalize_worker_result(context, imager_result_nro,
                                                 sourcename=source_name, spwlist=v_spwids, antenna=ant_name, #specmode='cube', sourcetype='TARGET',
                                                 imagemode=imagemode, stokes=stokes_list[1], validsp=validsps, rms=rmss, edge=edge,
                                                 reduction_group_id=group_id, file_index=file_index,
                                                 assoc_antennas=antids, assoc_fields=fieldids, assoc_spws=v_spwids) #, assoc_pols=pols)

                    results.append(imager_result_nro)

            if inputs.is_ampcal:
                LOG.info("Skipping combined image for the amplitude calibrator.")
                continue

            # Make combined image
            if len(tocombine_images) == 0:
                LOG.warn("No valid image to combine for Source {}, Spw {:d}".format(source_name, spwids[0]))
                continue
            # reference MS
            ref_ms = context.observing_run.get_ms(name=sdutils.get_parent_ms_name(context, combined_infiles[0]))

            # image name
            # image name should be based on virtual spw id
            combined_v_spws_unique = numpy.unique(combined_v_spws)
            assert len(combined_v_spws_unique) == 1
            imagename = self.get_imagename(source_name, combined_v_spws_unique)

            # Step 3.
            # Imaging of all antennas
            LOG.info('Combine images of Source {} Spw {:d}'.format(source_name, combined_spws[0]))
            if False:
                imager_inputs = worker.SDImagingWorker.Inputs(context, combined_infiles,
                                                              outfile=imagename, mode=imagemode,
                                                              antids=combined_antids,
                                                              spwids=combined_spws,
                                                              fieldids=combined_fieldids,
                                                              stokes=self.stokes,
                                                              edge=edge,
                                                              phasecenter=phasecenter,
                                                              cellx=cellx, celly=celly,
                                                              nx=nx, ny=ny)
                imager_task = worker.SDImagingWorker(imager_inputs)
                imager_result = self._executor.execute(imager_task)
            else:
                combine_inputs = sdcombine.SDImageCombineInputs(context, inimages=tocombine_images,
                                                                outfile=imagename,
                                                                org_directions=tocombine_org_directions)
                combine_task = sdcombine.SDImageCombine(combine_inputs)
                imager_result = self._executor.execute(combine_task)

            if imager_result.outcome is not None:
                # Imaging was successful, proceed following steps

                # Additional Step.
                # Make grid_table and put rms and valid spectral number array
                # to the outcome
                # The rms and number of valid spectra is used to create RMS maps
                LOG.info('Additional Step. Make grid_table')
                imagename = imager_result.outcome['image'].imagename
                org_direction = imager_result.outcome['image'].org_direction
                with casatools.ImageReader(imagename) as ia:
                    cs = ia.coordsys()
                    dircoords = [i for i in range(cs.naxes())
                                 if cs.axiscoordinatetypes()[i] == 'Direction']
                    cs.done()
                    nx = ia.shape()[dircoords[0]]
                    ny = ia.shape()[dircoords[1]]
                observing_pattern =  ref_ms.observing_pattern[combined_antids[0]][combined_spws[0]][combined_fieldids[0]]
                grid_task_class = gridding.gridding_factory(observing_pattern)
                validsps = []
                rmss = []
                grid_input_dict = {}
                for (msname, antid, spwid, fieldid, poltypes) in zip(combined_infiles, combined_antids, combined_spws,
                                                                     combined_fieldids, combined_pols):
                    # msobj = context.observing_run.get_ms(name=common.get_parent_ms_name(context,msname)) # Use parent ms
                    # ddobj = msobj.get_data_description(spw=spwid)
                    for p in poltypes:
                        if p not in grid_input_dict:
                            grid_input_dict[p] = [[msname], [antid], [fieldid], [spwid]]
                        else:
                            grid_input_dict[p][0].append(msname)
                            grid_input_dict[p][1].append(antid)
                            grid_input_dict[p][2].append(fieldid)
                            grid_input_dict[p][3].append(spwid)

                for pol, member in grid_input_dict.items():
                    _mses = member[0]
                    _antids = member[1]
                    _fieldids = member[2]
                    _spwids = member[3]
                    _pols = [pol for i in range(len(_mses))]
                    gridding_inputs = grid_task_class.Inputs(context, infiles=_mses,
                                                             antennaids=_antids,
                                                             fieldids=_fieldids,
                                                             spwids=_spwids,
                                                             poltypes=_pols,
                                                             nx=nx, ny=ny)
                    gridding_task = grid_task_class(gridding_inputs)
                    job = common.ParameterContainerJob(gridding_task, datatable_dict=dt_dict)
                    gridding_result = self._executor.execute(job, merge=False)
                    # Extract RMS and number of spectra from grid_tables
                    if isinstance(gridding_result.outcome, compress.CompressedObj):
                        grid_table = gridding_result.outcome.decompress()
                    else:
                        grid_table = gridding_result.outcome
                    validsps.append([r[6] for r in grid_table])
                    rmss.append([r[8] for r in grid_table])
                    del grid_table

                # calculate RMS of line free frequencies in a combined image
                LOG.info('Calculate sensitivity of combined image')
                rep_bw = ref_ms.representative_target[2]
                rep_spwid = ref_ms.get_representative_source_spw()[1]
                is_representative_spw = (rep_spwid==combined_spws[0] and rep_bw is not None)
                with casatools.ImageReader(imagename) as ia:
                    cs = ia.coordsys()
                    faxis = cs.findaxisbyname('spectral')
                    num_chan = ia.shape()[faxis]
                    chan_width = cs.increment()['numeric'][faxis]
                    brightnessunit = ia.brightnessunit()
                    beam = ia.restoringbeam()
                ref_world = cs.referencevalue()['numeric']
                qcell = list(cs.increment(format='q', type='direction')['quantity'].values())  # cs.increment(format='s', type='direction')['string']
#                 rms_exclude_freq = self._merge_ranges(combined_rms_exclude)
                LOG.info("Aggregated spectral line frequency ranges of combined image = {}".format(str(combined_rms_exclude)))
                combined_rms_exclude_chan = [] # should be list for sort
                for i in range(len(combined_rms_exclude)):
                    segment = combined_rms_exclude[i]
                    ref_world[faxis] = segment[0]
                    start_chan = cs.topixel(ref_world)['numeric'][faxis]
                    ref_world[faxis] = segment[1]
                    end_chan = cs.topixel(ref_world)['numeric'][faxis]
                    # handling of LSB
                    min_chan = min(start_chan, end_chan)
                    max_chan = max(start_chan, end_chan)
                    #LOG.info("#####Freq to Chan: [{:f}, {:f}] -> [{:f}, {:f}]".format(segment[0], segment[1], min_chan, max_chan))
                    if max_chan < -0.5 or min_chan > num_chan - 0.5: #out of range
                        #LOG.info("#####Omitting channel range [{:f}, {:f}]".format(min_chan, max_chan))
                        continue
                    combined_rms_exclude_chan.append([max(int(min_chan), 0),
                                           min(int(max_chan), num_chan-1)])
                combined_rms_exclude_chan.sort()
                rms_exclude_chan = self._merge_ranges(combined_rms_exclude_chan)
                LOG.info("Merged spectral line channel ranges of combined image = {}".format(str(rms_exclude_chan)))
                include_channel_range = []
                if len(rms_exclude_chan) == 0:
                    include_channel_range = [edge[0], num_chan-1-edge[1]]
                else:
                    if rms_exclude_chan[0][0] > edge[0]:
                        include_channel_range.extend([edge[0], rms_exclude_chan[0][0]-1])
                    for j in range(len(rms_exclude_chan)-1):
                        start_include = rms_exclude_chan[j][1]+1
                        end_include = rms_exclude_chan[j+1][0]-1
                        if start_include <= end_include:
                            include_channel_range.extend([start_include, end_include])
                    if rms_exclude_chan[-1][1] + 1 < num_chan-1-edge[1]:
                        include_channel_range.extend([rms_exclude_chan[-1][1] + 1, num_chan-1-edge[1]])
                LOG.info("Line free channel ranges of image to calculate RMS = {}".format(str(include_channel_range)))

                stat_chans = str(';').join([ '{:d}~{:d}'.format(include_channel_range[iseg], include_channel_range[iseg+1]) for iseg in range(0, len(include_channel_range), 2) ])
                # Image statistics
                imstat_job = casa_tasks.imstat(imagename=imagename, chans=stat_chans)
                statval = self._executor.execute(imstat_job)
                image_rms = statval['rms'][0]
                LOG.info("Statistics of line free channels ({}): RMS = {:f} {}, Stddev = {:f} {}, Mean = {:f} {}".format(stat_chans, statval['rms'][0], brightnessunit, statval['sigma'][0], brightnessunit, statval['mean'][0], brightnessunit))
                # Theoretical RMS
                LOG.info('Calculating theoretical RMS of image, {}'.format(imagename))
                theoretical_rms = self.calculate_theoretical_image_rms(combined_infiles, combined_antids,
                                                                       combined_fieldids, combined_spws,
                                                                       combined_pols, qcell, chan_width, brightnessunit,
                                                                       dt_dict)

                # estimate
                if is_representative_spw:
                    # skip estimate if data is Cycle 2 and earlier + th effective BW is nominal (= chan_width)
                    spwobj = ref_ms.get_spectral_window(rep_spwid)
                    if cqa.time(ref_ms.start_time['m0'], 0, ['ymd', 'no_time'])[0] < '2015/10/01' and \
                            spwobj.channels.chan_effbws[0] == numpy.abs(spwobj.channels.chan_widths[0]):
                        is_representative_spw = False
                        LOG.warn("Cycle 2 and earlier project with nominal effective band width. Reporting RMS at native resolution.")
                    else:
                        if not cqa.isquantity(rep_bw): # assume Hz
                            rep_bw = cqa.quantity(rep_bw, 'Hz')
                        LOG.info("Estimate RMS in representative bandwidth: {:f}kHz (native: {:f}kHz)".format(cqa.getvalue(cqa.convert(cqa.quantity(rep_bw), 'kHz'))[0], chan_width*1.e-3))
                        factor = sensitivity_improvement.sensitivityImprovement(ref_ms.name, rep_spwid, cqa.tos(rep_bw))
                        if factor is None:
                            LOG.warn('No image RMS improvement because representative bandwidth is narrower than native width')
                            factor = 1.0
                        LOG.info("Image RMS improvement of factor {:f} estimated. {:f} => {:f} {}".format(factor, image_rms, image_rms/factor, brightnessunit))
                        image_rms = image_rms/factor
                        chan_width = numpy.abs(cqa.getvalue(cqa.convert(cqa.quantity(rep_bw), 'Hz'))[0])
                        theoretical_rms['value'] = theoretical_rms['value']/factor
                elif rep_bw is None:
                    LOG.warn("Representative bandwidth is not available. Skipping estimate of sensitivity in representative band width.")
                elif rep_spwid is None:
                    LOG.warn("Representative SPW is not available. Skipping estimate of sensitivity in representative band width.")

                # calculate channel and frequency ranges of line free channels
                ref_pixel = cs.referencepixel()['numeric']
                freqs = []
                for ichan in include_channel_range:
                    ref_pixel[faxis] = ichan
                    freqs.append(cs.toworld(ref_pixel)['numeric'][faxis])
                cs.done()
                if len(freqs) > 1 and freqs[0] > freqs[1]:  # LSB
                    freqs.reverse()
                stat_freqs = str(', ').join(['{:f}~{:f}GHz'.format(freqs[iseg]*1.e-9, freqs[iseg+1]*1.e-9)
                                             for iseg in range(0, len(freqs), 2)])

                file_index = [common.get_parent_ms_idx(context, name) for name in combined_infiles]
                sensitivity = Sensitivity(array='TP',
                                          field=source_name,
                                          spw=str(combined_spws[0]),
                                          bandwidth=cqa.quantity(chan_width, 'Hz'),
                                          bwmode='repBW',
                                          beam=beam, cell=qcell,
                                          sensitivity=cqa.quantity(image_rms, brightnessunit))
                theoretical_noise = Sensitivity(array='TP',
                                          field=source_name,
                                          spw=str(combined_spws[0]),
                                          bandwidth=cqa.quantity(chan_width, 'Hz'),
                                          bwmode='repBW',
                                          beam=beam, cell=qcell,
                                          sensitivity=theoretical_rms)
                sensitivity_info = SensitivityInfo(sensitivity, is_representative_spw, stat_freqs)
                self._finalize_worker_result(context, imager_result,
                                             sourcename=source_name, spwlist=combined_v_spws, antenna='COMBINED',  #specmode='cube', sourcetype='TARGET',
                                             imagemode=imagemode, stokes=self.stokes, validsp=validsps, rms=rmss, edge=edge,
                                             reduction_group_id=group_id, file_index=file_index,
                                             assoc_antennas=combined_antids, assoc_fields=combined_fieldids, assoc_spws=combined_v_spws,  #, assoc_pols=pols,
                                             sensitivity_info=sensitivity_info, theoretical_rms=theoretical_noise)

                # PIPE-251: detect contamination
                detectcontamination.detect_contamination(context, imager_result.outcome['image'])

                results.append(imager_result)

            # NRO specific: generate combined image for each correlation
            if is_nro:
                if len(tocombine_images_nro) == 0:
                    LOG.warn("No valid image to combine for Source {}, Spw {:d}".format(source_name, spwids[0]))
                    continue

                # image name
                # image name should be based on virtual spw id
                imagename = self.get_imagename(source_name, combined_v_spws_unique, stokes=correlations)

                # Step 3.
                # Imaging of all antennas
                LOG.info('Combine images of Source {} Spw {:d}'.format(source_name, combined_spws[0]))
                combine_inputs = sdcombine.SDImageCombineInputs(context, inimages=tocombine_images_nro,
                                                                outfile=imagename,
                                                                org_directions=tocombine_org_directions_nro)
                combine_task = sdcombine.SDImageCombine(combine_inputs)
                imager_result = self._executor.execute(combine_task)

                if imager_result.outcome is not None:
                # Imaging was successful, proceed following steps

                    file_index = [common.get_parent_ms_idx(context, name) for name in combined_infiles]
                    self._finalize_worker_result(context, imager_result,
                                                 sourcename=source_name, spwlist=combined_v_spws, antenna='COMBINED',  #specmode='cube', sourcetype='TARGET',
                                                 imagemode=imagemode, stokes=stokes_list[1], validsp=validsps, rms=rmss, edge=edge,
                                                 reduction_group_id=group_id, file_index=file_index,
                                                 assoc_antennas=combined_antids, assoc_fields=combined_fieldids, assoc_spws=combined_v_spws)  #, assoc_pols=pols)

                    results.append(imager_result)

        return results

    def analyse(self, result):
        return result

    def _get_rms_exclude_freq_range_image(self, to_frame, chanmap_ranges, edge,
                                          msobj_list, antid_list, spwid_list, fieldid_list):
        """
        A utility method to obtain combined list of frequency ranges of
        deviation mask, channel map ranges, and edges.

        Arguments
            to_frame    : the frequency frame of output
            chanmap_ranges    : a list of channel ranges to incorporate, e.g., [[min0,max0], [min1,max1], ...]
            edge    : the number of channels in the left and right edges to incorporate, e.g., [0,0]
            msobj_list, antid_list, spwid_list, fieldid_list    : a list of ms instances, antenna, spw
                                            and field IDs from which devition masks should be obtained.

        Returns:
            a list of combined frequency ranges in output frequency frame (to_frame),
            e.g., [ [minfreq0,maxfreq0], [minfreq1,maxfreq1], ...]
        """
        image_rms_freq_range = []
        channelmap_range = []
        #LOG.info("#####Raw chanmap_range={}".format(str(chanmap_ranges)))
        for chanmap_range in chanmap_ranges:
            for map_range in chanmap_range:
                if map_range[2]:
                    min_chan = int(map_range[0]-map_range[1]*0.5)
                    max_chan = int(numpy.ceil(map_range[0]+map_range[1]*0.5))
                    channelmap_range.append([min_chan, max_chan])
        LOG.debug("#####CHANNEL MAP RANGE = {}".format(str(channelmap_range)))
        for i in range(len(msobj_list)):
            # define channel ranges of lines and deviation mask for each MS
            msobj = msobj_list[i]
            fieldid = fieldid_list[i]
            antid = antid_list[i]
            spwid = spwid_list[i]
            spwobj = msobj.get_spectral_window(spwid)
            deviation_mask = getattr(msobj, 'deviation_mask', {})
            exclude_range = deviation_mask.get((fieldid, antid, spwid), [])
            LOG.debug("#####{} : DEVIATION MASK = {}".format(msobj.basename, str(exclude_range)))
            if len(exclude_range)==1 and exclude_range[0] == [0, spwobj.num_channels-1]:
                # deviation mask is full channel range when all data are flagged
                LOG.warn("Ignoring DEVIATION MASK of {} (SPW {:d}, FIELD {:d}, ANT {:d}). Possibly all data flagged".format(msobj.basename, spwid, fieldid, antid))
                exclude_range = []
            if edge[0] > 0: exclude_range.append([0, edge[0]-1])
            if edge[1] > 0: exclude_range.append([spwobj.num_channels-edge[1], spwobj.num_channels-1])
            if len(channelmap_range) >0:
                exclude_range.extend(channelmap_range)
            # check the validity of channel number and fix it when out of range
            min_chan = 0
            max_chan = spwobj.num_channels - 1
            exclude_channel_range = [[max(min_chan, x[0]), min(max_chan, x[1])]
                                     for x in self._merge_ranges(exclude_range)]
            LOG.info("{} : channel map and deviation mask channel ranges in MS frame = {}".format(msobj.basename, str(exclude_channel_range)))
            # define frequency ranges of RMS
            exclude_freq_range = numpy.zeros(2*len(exclude_channel_range))
            for jseg in range(len(exclude_channel_range)):
                (lfreq, rfreq) = (spwobj.channels.chan_freqs[jchan] for jchan in exclude_channel_range[jseg])
                # handling of LSB
                exclude_freq_range[2*jseg:2*jseg+2] = [min(lfreq, rfreq), max(lfreq, rfreq)]
            LOG.debug("#####CHANNEL MAP AND DEVIATION MASK FREQ RANGE = {}".format(str(exclude_freq_range)))
            if len(exclude_freq_range) == 0:
                continue  # no ranges to add
            # convert MS freqency ranges to image frame
            field = msobj.fields[fieldid]
            direction_ref = field.mdirection
            start_time = msobj.start_time
            end_time = msobj.end_time
            me = casatools.measures
            qa = casatools.quanta
            qmid_time = qa.quantity(start_time['m0'])
            qmid_time = qa.add(qmid_time, end_time['m0'])
            qmid_time = qa.div(qmid_time, 2.0)
            time_ref = me.epoch(rf=start_time['refer'],
                                v0=qmid_time)
            position_ref = msobj.antennas[antid].position

            # initialize
            me.done()
            me.doframe(time_ref)
            me.doframe(direction_ref)
            me.doframe(position_ref)

            def _to_imageframe(x):
                m = me.frequency(rf=spwobj.frame, v0=qa.quantity(x, 'Hz'))
                converted = me.measure(v=m, rf=to_frame)
                qout = qa.convert(converted['m0'], outunit='Hz')
                return qout['value']

            to_imageframe = numpy.vectorize(_to_imageframe)
            image_rms_freq_range.extend(to_imageframe(exclude_freq_range))

        #LOG.info("#####Overall LINE CHANNELS IN IMAGE FRAME = {}".format(str(image_rms_freq_range)))
        if len(image_rms_freq_range) == 0:
            return image_rms_freq_range

        return self._merge_ranges(numpy.reshape(image_rms_freq_range, (len(image_rms_freq_range)//2, 2), 'C'))

    def _merge_ranges(self, range_list):
        """
        A utility method to merge overlapping ranges in range_list.

        Argument
            range_list    : a list of ranges to merge, e.g., [ [min0,max0], [min1,max1], .... ]
                            each range in the list should be in ascending order (min0 <= max0)
                            there is no assumption in the order of ranges, e.g., min0 w.r.t min1
        Returns
            a list of merged ranges
            e.g., [[min_merged0,max_marged0], [min_merged1,max_merged1], ....]
        """
        #LOG.info("#####Merge ranges: {}".format(str(range_list)))
        num_range = len(range_list)
        if num_range == 0:
            return []
        merged = [range_list[0][0:2]]
        for i in range(1, num_range):
            segment = range_list[i]
            if len(segment) < 2:
                raise ValueError("segments in range list much have 2 elements")
            overlap = -1
            for j in range(len(merged)):
                if segment[1] < merged[j][0] or segment[0] > merged[j][1]:  # no overlap
                    continue
                else:
                    overlap = j
                    break
            if overlap < 0:
                merged.append(segment[0:2])
            else:
                merged[j][0] = min(merged[j][0], segment[0])
                merged[j][1] = max(merged[j][1], segment[1])
        # Check if further merge is necessary
        while len(merged) < num_range:
            num_range = len(merged)
            merged = self._merge_ranges(merged)
        #LOG.info("#####Merged: {}".format(str(merged)))
        return merged

    def get_imagename(self, source, spwids, antenna=None, asdm=None, stokes=None):
        context = self.inputs.context
        is_nro = sdutils.is_nro(context)
        namer = filenamer.Image()
        if self.inputs.is_ampcal:
            nameroot = asdm
            if nameroot is None:
                raise ValueError('ASDM uid must be provided to construct ampcal image name')
        elif is_nro:
            nameroot = ''
        else:
            nameroot = context.project_structure.ousstatus_entity_id
            if nameroot == 'unknown':
                nameroot = 'oussid'
        nameroot = filenamer.sanitize(nameroot)
        namer._associations.asdm(nameroot)
        #output_dir = context.output_dir
        #if output_dir:
        #    namer.output_dir(output_dir)
        if not is_nro:
            namer.stage(context.stage)

        namer.source(source)
        if self.inputs.is_ampcal:
            namer.intent(self.inputs.mode.lower())
        elif is_nro:
            pass
        else:
            namer.science()
        namer.spectral_window(spwids[0])
        if stokes is None:
            stokes = self.stokes
        namer.polarization(stokes)
        # so far we always create native resolution, full channel image
        namer.specmode('cube')
        #namer.spectral_image()
        namer._associations.format('image.sd')
        #namer.single_dish()
        namer.antenna(antenna)
        # iteration is necessary for exportdata
        namer.iteration(0)
        imagename = namer.get_filename()
        return imagename

    def calculate_theoretical_image_rms(self, infiles, antids, fieldids, spwids,
                                        pols, cell, bandwidth, imageunit, datatable_dict):
        """
        Calculate theoretical RMS of an image (PIPE-657).
        
        Parameters:
            infiles: a list of MS names
            antids: a list of antenna IDs, e.g., [3, 3]
            fieldids: a list of field IDs, e.g., [1, 1]
            spwids: a list of SpW IDs, e.g., [17, 17]
            pols: a list of polarization strings, e.g., [['XX', 'YY'], ['XX', 'YY']]
            cell: cell size of an image
            bandwidth: channel width of an image
            imageunit: the brightness unit of image. If unit is not 'K', Jy/K factor is used to convert unit (need Jy/K factor applied in a previous stage)
        Note: the number of elements in antids, fieldids, spws, and pols should be equal to that of infiles
        Retruns:
            A quantum value of theoretical image RMS.
            The value of quantity will be negative when calculation is aborted, i.e., -1.0 Jy/beam
        """
        cqa = casatools.quanta
        failed_rms = cqa.quantity(-1, imageunit)
        if len(infiles) == 0:
            LOG.error('No MS given to calculate a theoretical RMS. Aborting calculation of theoretical thermal noise.')
            return failed_rms
        assert len(infiles) == len(antids)
        assert len(infiles) == len(fieldids)
        assert len(infiles) == len(spwids)
        assert len(infiles) == len(pols)
        sq_rms = 0.0
        N = 0.0
        time_unit = 's'
        ang_unit = cqa.getunit(cell[0])
        cx_val = cqa.getvalue(cell[0])[0]
        cy_val = cqa.getvalue(cqa.convert(cell[1], ang_unit))[0]
        bandwidth = numpy.abs(bandwidth)
        context = self.inputs.context
        is_nro = sdutils.is_nro(context)
        for (infile, antid, fieldid, spwid, pol_names) in zip(infiles, antids, fieldids, spwids, pols):
            msobj = context.observing_run.get_ms(sdutils.get_parent_ms_name(context, infile))
            dd_corrs = msobj.get_data_description(spw=spwid).corr_axis
            polids = [dd_corrs.index(p) for p in pol_names if p in dd_corrs]
            field_name = msobj.get_fields(field_id=fieldid)[0].name
            error_msg = 'Aborting calculation of theoretical thermal noise of Field {} and SpW {}'.format(field_name, spwid)
            if msobj.observing_pattern[antid][spwid][fieldid] != 'RASTER':
                LOG.warn('Unable to calculate RMS of non-Raster map. '+error_msg)
                return failed_rms
            LOG.info('Processing MS {}, Field {}, SpW {}, Antenna {}, Pol {}'.format(os.path.basename(infile),
                                                                                     field_name,
                                                                                     spwid,
                                                                                     msobj.get_antenna(antid)[0].name,
                                                                                     str(pol_names)))
            dt = datatable_dict[msobj.basename]
            _index_list = common.get_index_list_for_ms(dt, [msobj.basename], [antid], [fieldid],
                                                       [spwid], srctype=0)
            if len(_index_list) == 0: #this happens when permanent flag is set to all selection.
                LOG.info('No unflagged row in DataTable. Skipping further calculation.')
                continue
            # effective BW
            with casatools.MSMDReader(infile) as msmd:
                ms_chanwidth = numpy.abs(msmd.chanwidths(spwid).mean())
                ms_effbw = msmd.chaneffbws(spwid).mean()
                ms_nchan = msmd.nchan(spwid)
                nchan_avg = sensitivity_improvement.onlineChannelAveraging(infile, spwid, msmd)
            if bandwidth/ms_chanwidth < 1.1: # imaging by the original channel
                effBW = ms_effbw
                LOG.info('Using an MS effective bandwidth, {} kHz'.format(effBW*0.001))
                #else: # pre-Cycle 3 alma data
                #    effBW = ms_chanwidth * sensitivity_improvement.windowFunction('hanning', channelAveraging=nchan_avg,
                #                                                                  returnValue='EffectiveBW')
                #    LOG.info('Using an estimated effective bandwidth {} kHz'.format(effBW*0.001))
            else:
                image_map_chan = bandwidth/ms_chanwidth
                effBW = ms_chanwidth * sensitivity_improvement.windowFunction('hanning', channelAveraging=nchan_avg,
                                                                              returnValue='EffectiveBW', useCAS8534=True,
                                                                              spwchan=ms_nchan, nchan=image_map_chan)
                LOG.info('Using an adjusted effective bandwidth of image, {} kHz'.format(effBW*0.001))
            # obtain average Tsys
            mean_tsys_per_pol = dt.getcol('TSYS').take(_index_list, axis=-1).mean(axis=-1)
            LOG.info('Mean Tsys = {} K'.format(str(mean_tsys_per_pol)))
            # obtain Wx, and Wy
            raster_info = _analyze_raster_pattern(dt, msobj, fieldid, spwid, antid, polids[0])
            width = cqa.getvalue(cqa.convert(raster_info.width, ang_unit))[0]
            height = cqa.getvalue(cqa.convert(raster_info.height, ang_unit))[0]
            # obtain T_OS,f
            unit = dt.getcolkeyword('EXPOSURE', 'UNIT')
            t_on_tot = cqa.getvalue(cqa.convert(cqa.quantity(dt.getcol('EXPOSURE').take(_index_list, axis=-1).sum(), unit), time_unit))[0]
            # flagged fraction
            full_intent = utils.to_CASA_intent(msobj, 'TARGET')
            flagdata_summary_job = casa_tasks.flagdata(vis=infile, mode='summary',
                                                       antenna='{}&&&'.format(antid),
                                                       field=str(fieldid),
                                                       spw=str(spwid), intent=full_intent,
                                                       spwcorr=False, fieldcnt=False,
                                                       name='summary')
            flag_stats = self._executor.execute(flagdata_summary_job)
            frac_flagged = flag_stats['spw'][str(spwid)]['flagged']/flag_stats['spw'][str(spwid)]['total']
            # the actual time on source
            t_on_act = t_on_tot * (1.0-frac_flagged)
            LOG.info('The actual on source time = {} {}'.format(t_on_act, time_unit))
            LOG.info('- total time on source = {} {}'.format(t_on_tot, time_unit))
            LOG.info('- flagged Fraction = {} %'.format(100*frac_flagged))
            # obtain calibration tables applied
            calto = callibrary.CalTo(vis=msobj.name, field=str(fieldid))
            calst = context.callibrary.get_calstate(calto)
            # obtain T_sub,on, T_sub,off
            t_sub_on = cqa.getvalue(cqa.convert(raster_info.row_duration, time_unit))[0]
            sky_field = msobj.calibration_strategy['field_strategy'][fieldid]
            try:
                skytab = ''
                caltabs = context.callibrary.applied.get_caltable('ps')
                ### For some reasons, sky caltable is not registered to calstate
                for cto, cfrom in context.callibrary.applied.merged().items():
                    if cto.vis == msobj.name and (cto.field == '' or fieldid in [f.id for f in msobj.get_fields(name=cto.field)]):
                        for cf in cfrom:
                            if cf.gaintable in caltabs:
                                skytab=cf.gaintable
                                break
            except:
                LOG.error('Could not find a sky caltable applied. '+error_msg)
                raise
            if not os.path.exists(skytab):
                LOG.warn('Could not find a sky caltable applied. '+error_msg)
                return failed_rms
            LOG.info('Searching OFF scans in {}'.format(os.path.basename(skytab)))
            with casatools.TableReader(skytab) as tb:
                t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}&&FIELD_ID=={}'.format(spwid, antid, sky_field), columns='INTERVAL')
                if t.nrows == 0:
                    LOG.warn('No sky caltable row found for spw {}, antenna {}, field {} in {}. {}'.format(spwid, antid, sky_field, os.path.basename(skytab), error_msg))
                    return failed_rms
                unit = t.getcolkeyword('INTERVAL', 'QuantumUnits')[0]
                t_sub_off = cqa.getvalue(cqa.convert(cqa.quantity(t.getcol('INTERVAL').mean(), unit), time_unit))[0]
            LOG.info('Subscan Time ON = {} {}, OFF = {} {}'.format(t_sub_on, time_unit, t_sub_off, time_unit))
            # obtain factors by convolution function (THIS ASSUMES SF kernel with either convsupport = 6 (ALMA) or 3 (NRO)
            # TODO: Ggeneralize factor for SF, and Gaussian convolution function
            conv2d = 0.3193 if is_nro else 0.1597
            conv1d = 0.5592 if is_nro else 0.3954
            if imageunit == 'K':
                jy_per_k = 1.0
                LOG.info('No Kelvin to Jansky conversion was performed to the image.')
            else:
                # obtain Jy/k factor
                try:
                    k2jytab = ''
                    caltabs = context.callibrary.applied.get_caltable('amp')
                    found = caltabs.intersection(calst.get_caltable('amp'))
                    if len(found) == 0:
                        LOG.warn('Could not find a Jy/K caltable applied. '+error_msg)
                        return failed_rms
                    if len(found) > 1:
                        LOG.warn('More than one Jy/K caltables are found.')
                    k2jytab = found.pop()
                    LOG.info('Searching Jy/K factor in {}'.format(os.path.basename(k2jytab)))
                except:
                    LOG.error('Could not find a Jy/K caltable applied. '+error_msg)
                    raise
                if not os.path.exists(k2jytab):
                    LOG.warn('Could not find a Jy/K caltable applied. '+error_msg)
                    return failed_rms
                with casatools.TableReader(k2jytab) as tb:
                    t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}'.format(spwid, antid), columns='CPARAM')
                    if t.nrows == 0:
                        LOG.warn('No Jy/K caltable row found for spw {}, antenna {} in {}. {}'.format(spwid, antid, os.path.basename(k2jytab), error_msg))
                        return failed_rms
                    tc = t.getcol('CPARAM')
                    jy_per_k = (1./tc.mean(axis=-1).real**2).mean()
                    LOG.info('Jy/K factor = {}'.format(jy_per_k))
            ang = cqa.getvalue(cqa.convert(raster_info.scan_angle, 'rad'))[0] + 0.5*numpy.pi
            c_proj = numpy.sqrt( (cy_val* numpy.sin(ang))**2 + (cx_val*numpy.cos(ang))**2)
            inv_variant_on = effBW * numpy.abs(cx_val * cy_val) * t_on_act / width / height
            inv_variant_off = effBW * c_proj * t_sub_off * t_on_act / t_sub_on / height
            
            for ipol in polids:
                sq_rms += (jy_per_k*mean_tsys_per_pol[ipol])**2 * (conv2d**2/inv_variant_on + conv1d**2/inv_variant_off) 
                N += 1.0

        theoretical_rms = numpy.sqrt(sq_rms)/N
        LOG.info('Theoretical RMS of image = {} {}'.format(theoretical_rms, imageunit))
        return cqa.quantity(theoretical_rms, imageunit)
    

def _analyze_raster_pattern(datatable, msobj, fieldid, spwid, antid, polid):
    """
    Analyze raster scan pattern from pointing in DataTable
    Parameters:
        datatable: DataTable instance
        msobj: MS class instance to process
        fieldid: a field ID to process
        spwid: an SpW ID to process
        antid: an antenna ID to process
        polid: a polarization ID to process
    Returns a named Tuple
    """
    _index_list = common.get_index_list_for_ms(datatable, [msobj.name], [antid], [fieldid],
                                                       [spwid], srctype=0)
    timestamp = datatable.getcol('TIME').take(_index_list, axis=-1)
    ra = datatable.getcol('OFS_RA').take(_index_list, axis=-1)
    dec = datatable.getcol('OFS_DEC').take(_index_list, axis=-1)
    exposure = datatable.getcol('EXPOSURE').take(_index_list, axis=-1)
    map_center_dec = datatable.getcol('DEC').take(_index_list, axis=-1).mean()
    radec_unit = datatable.getcolkeyword('OFS_RA', 'UNIT')
    assert radec_unit == datatable.getcolkeyword('OFS_DEC', 'UNIT')
    exp_unit = datatable.getcolkeyword('EXPOSURE', 'UNIT')
    gap_s, gap_l = rasterutil.find_time_gap(timestamp) #gap_s stores the last index in a raster row.
    gap_r = rasterutil.find_raster_gap(timestamp, ra, dec, gap_s)
    start_idx = 0
    duration = []
    num_integration = []
    delta_ra = []
    delta_dec = []
    center_ra = []
    center_dec = []
    height_list = []
    first_row = None # RA and Dec of the first raster row
    
    cqa = casatools.quanta
    map_center_dec = cqa.getvalue(cqa.convert(cqa.quantity(map_center_dec, datatable.getcolkeyword('DEC', 'UNIT')),'rad'))[0]
    dec_factor = numpy.abs(numpy.cos(map_center_dec))
    # loop over raster rows
    for end_idx in gap_s:
        duration.append(numpy.sum(exposure[start_idx:end_idx+1]))
        num_integration.append(end_idx-start_idx+1)
        delta_ra.append((ra[end_idx]-ra[start_idx])*dec_factor)
        delta_dec.append(dec[end_idx]-dec[start_idx])
        cra = ra[start_idx:end_idx+1].mean()
        cdec = dec[start_idx:end_idx+1].mean()
        center_ra.append(cra)
        center_dec.append(cdec)
        if first_row is None: first_row = (cra, cdec)
        if end_idx in gap_r:
            height_list.append( numpy.hypot((first_row[0]-cra)*dec_factor, first_row[1]-cdec) )
            first_row = None
        start_idx = end_idx +1
    if len(height_list) == 0: # only one iteration of map
        height_list.append( numpy.hypot((first_row[0]-center_ra[-1])*dec_factor, first_row[1]-center_dec[-1]) )
    center_ra = numpy.array(center_ra)
    center_dec = numpy.array(center_dec)
    row_sep_ra = (center_ra[1:]-center_ra[:-1])*dec_factor
    row_sep_dec = center_dec[1:]-center_dec[:-1]
    row_separation = numpy.median(numpy.hypot(row_sep_ra, row_sep_dec))
    # find complate raster
    num_row_int = rasterutil.find_most_frequent(num_integration)
    complete_idx = numpy.where(num_integration >= num_row_int)
    # raster scan parameters (TODO: projection?)
    row_duration = numpy.array(duration)[complete_idx].mean()
    row_delta_ra = numpy.abs(delta_ra)[complete_idx].mean()
    row_delta_dec = numpy.abs(delta_dec)[complete_idx].mean()
    width = numpy.hypot(row_delta_ra, row_delta_dec)
    sign_ra = +1.0 if delta_ra[complete_idx[0][0]] >= 0 else -1.0
    sign_dec = +1.0 if delta_dec[complete_idx[0][0]] >= 0 else -1.0
    scan_angle = math.atan2(sign_dec*row_delta_dec, sign_ra*row_delta_ra)
    hight = numpy.max(height_list)
    raster_info = RasterInfo(cqa.quantity(width, radec_unit), cqa.quantity(hight, radec_unit),
                             cqa.quantity(scan_angle, 'rad'), cqa.quantity(row_separation, radec_unit),
                             cqa.quantity(row_duration, exp_unit))
    LOG.info('Raster Information')
    LOG.info('- Scan Extent: [{}, {}] (scan direction: {})'.format(cqa.tos(raster_info.width),
                                                                   cqa.tos(raster_info.height),
                                                                   cqa.tos(raster_info.scan_angle)))
    LOG.info('- Raster row separation = {}'.format(cqa.tos(raster_info.row_separation)))
    LOG.info('- Raster row scan duration = {}'.format(cqa.tos(cqa.convert(raster_info.row_duration, 's'))))
    return raster_info
        
    