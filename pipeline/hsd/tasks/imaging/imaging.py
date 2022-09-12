"""Imaging stage."""

from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import collections
import math
import os
from numbers import Number

import numpy
from scipy import interpolate

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.imageheader as imageheader
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataTable, DataType
from pipeline.domain import MeasurementSet
from pipeline.extern import sensitivity_improvement
from pipeline.h.heuristics import fieldnames
from pipeline.hsd.heuristics import rasterscan
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.hsd.tasks.imaging import gridding
from pipeline.hsd.tasks.imaging import sdcombine
from pipeline.hsd.tasks.imaging import weighting
from pipeline.hsd.tasks.imaging import worker
from pipeline.hsd.tasks.imaging import resultobjects
from pipeline.hsd.tasks.imaging import detectcontamination
from pipeline.hsd.tasks import common
from pipeline.hsd.tasks.baseline import baseline
from pipeline.hsd.tasks.common import compress
from pipeline.hsd.tasks.common import direction_utils
from pipeline.hsd.tasks.common import rasterutil
from pipeline.hsd.tasks.common import utils as sdutils

if TYPE_CHECKING:
    from pipeline.infrastructure import Context
    from resultobjects import SDImagingResults

LOG = infrastructure.get_logger(__name__)

# SensitivityInfo:
#     sensitivity: Sensitivity of an image
#     representative: True if the image is of the representative SpW (regardless of source)
#     frequency_range: frequency ranges from which the sensitivity is calculated
#     to_export: True if the sensitivity shall be exported to aqua report. (to avoid exporting NRO sensitivity in K)
SensitivityInfo = collections.namedtuple('SensitivityInfo', 'sensitivity representative frequency_range to_export')
# RasterInfo: center_ra, center_dec = R.A. and Declination of map center
#             width=map extent along scan, height=map extent perpendicular to scan
#             angle=scan direction w.r.t. horizontal coordinate, row_separation=separation between raster rows.
RasterInfo = collections.namedtuple('RasterInfo', 'center_ra center_dec width height scan_angle row_separation row_duration')
# Reference MS in combined list
REF_MS_ID = 0


class SDImagingInputs(vdp.StandardInputs):
    """Inputs for imaging task class."""
    # Search order of input vis
    processing_data_type = [DataType.BASELINED, DataType.ATMCORR,
                            DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

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
    def _finalize_worker_result(cls,
                                context: 'Context',
                                result: 'SDImagingResults',
                                sourcename: str,
                                spwlist: List[int],
                                antenna: str,
                                specmode: str,
                                imagemode: str,
                                stokes: str,
                                validsp: List[List[int]],
                                rms: List[List[float]],
                                edge: List[int],
                                reduction_group_id: int,
                                file_index: List[int],
                                assoc_antennas: List[int],
                                assoc_fields: List[int],
                                assoc_spws: List[int],
                                sensitivity_info: Optional[SensitivityInfo]=None,
                                theoretical_rms: Optional[Dict]=None):
        """
        Fanalize the worker result.

        Args:
            context            : Pipeline context
            result             : SDImagingResults instance
            sourcename         : Name of the source
            spwlist            : List of SpWs
            antenna            : Antenna name
            specmode           : Specmode for tsdimaging
            imagemode          : Image mode
            stokes             : Stokes parameter
            validsp            : # of combined spectra
            rms                : Rms values
            edge               : Edge channels
            reduction_group_id : Reduction group ID
            file_index         : MS file index
            assoc_antennas     : List of associated antennas
            assoc_fields       : List of associated fields
            assoc_spws         : List of associated SpWs
            sensitivity_info   : Sensitivity information
            theoretical_rms    : Theoretical RMS
        Returns:
            (none)
        """
        # override attributes for image item
        # the following attribute is currently hard-coded
        sourcetype = 'TARGET'

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
        # check if data is NRO
        is_nro = sdutils.is_nro(context)
        if is_nro:
            virtspw = False
        else:
            virtspw = True
        for name in (imagename, imagename+'.weight'):
            imageheader.set_miscinfo(name=name,
                                     spw=','.join(map(str, spwlist)),
                                     virtspw=virtspw,
                                     field=image_item.sourcename,
                                     nfield=1,
                                     type='singledish',
                                     iter=1,  # nominal
                                     intent=sourcetype,
                                     specmode=specmode,
                                     is_per_eb=False,
                                     context=context)

        # finally replace task attribute with the top-level one
        result.task = cls

    def prepare(self):
        common_inputs = self.inputs
        common_context = common_inputs.context
        common_reduction_group = common_context.observing_run.ms_reduction_group
        common_infiles = common_inputs.infiles
        common_restfreq_list = common_inputs.restfreq
        # list of ms to process
        common_ms_list = common_inputs.ms
        common_ms_names = [msobj.name for msobj in common_ms_list]
        common_in_spw = common_inputs.spw
        common_args_spw = sdutils.convert_spw_virtual2real(common_context, common_in_spw)
        # in_field is comma-separated list of target field names that are
        # extracted from all input MSs
        common_in_field = common_inputs.field
#         antennalist = inputs.antennalist
        common_imagemode = common_inputs.mode.upper()
        common_cqa = casa_tools.quanta

        # check if data is NRO
        common_is_nro = sdutils.is_nro(common_context)

        # task returns ResultsList
        common_results = resultobjects.SDImagingResults()
        # search results and retrieve edge parameter from the most
        # recent SDBaselineResults if it exists
        common_getresult = lambda r: r.read() if hasattr(r, 'read') else r
        common_registered_results = [common_getresult(r) for r in common_context.results]
        common_baseline_stage = -1
        for __stage in range(len(common_registered_results) - 1, -1, -1):
            if isinstance(common_registered_results[__stage], baseline.SDBaselineResults):
                common_baseline_stage = __stage
        if common_baseline_stage > 0:
            common_edge = list(common_registered_results[common_baseline_stage].outcome['edge'])
            LOG.info('Retrieved edge information from SDBaselineResults: {}'.format(common_edge))
        else:
            LOG.info('No SDBaselineResults available. Set edge as [0,0]')
            common_edge = [0, 0]
        # dt_dict: key=input MS, value=datatable corresponding to the MS
        common_dt_dict = dict((__ms.basename, DataTable(sdutils.get_data_table_path(common_context, __ms)))
                       for __ms in common_ms_list)

        # loop over reduction group (spw and source combination)
        for redgrp_group_id, redgrp_group_desc in common_reduction_group.items():
            LOG.debug('Processing Reduction Group {}'.format(redgrp_group_id))
            LOG.debug('Group Summary:')
            for __group in redgrp_group_desc:
                LOG.debug('\t{}: Antenna {:d} ({}) Spw {:d} Field {:d} ({})'.format(__group.ms.basename,
                                                                                    __group.antenna_id, __group.antenna_name,
                                                                                    __group.spw_id,
                                                                                    __group.field_id, __group.field_name))
            # Which group in group_desc list should be processed

            # fix for CAS-9747
            # There may be the case that observation didn't complete so that some of
            # target fields are missing in MS. In this case, directly pass in_field
            # to get_valid_ms_members causes trouble. As a workaround, ad hoc pre-selection
            # of field name is applied here.
            # 2017/02/23 TN
            redgrp_field_sel = ''
            if len(common_in_field) == 0:
                # fine, just go ahead
                redgrp_field_sel = common_in_field
            elif redgrp_group_desc.field_name in [x.strip('"') for x in common_in_field.split(',')]:
                # pre-selection of the field name
                redgrp_field_sel = redgrp_group_desc.field_name
            else:
                # no field name is included in in_field, skip
                LOG.info('Skip reduction group {:d}'.format(redgrp_group_id))
                continue

            redgrp_member_list = list(common.get_valid_ms_members(redgrp_group_desc, common_ms_names, common_inputs.antenna, redgrp_field_sel, common_args_spw))
            LOG.trace('group {}: member_list={}'.format(redgrp_group_id, redgrp_member_list))

            # skip this group if valid member list is empty
            if len(redgrp_member_list) == 0:
                LOG.info('Skip reduction group {:d}'.format(redgrp_group_id))
                continue

            redgrp_member_list.sort()  # list of group_desc IDs to image
            redgrp_antenna_list = [redgrp_group_desc[i].antenna_id for i in redgrp_member_list]
            redgrp_spwid_list = [redgrp_group_desc[i].spw_id for i in redgrp_member_list]
            common_ms_list = [redgrp_group_desc[i].ms for i in redgrp_member_list]
            redgrp_fieldid_list = [redgrp_group_desc[i].field_id for i in redgrp_member_list]
            __temp_dd_list = [common_ms_list[i].get_data_description(spw=redgrp_spwid_list[i])
                            for i in range(len(redgrp_member_list))]
            redgrp_channelmap_range_list = [redgrp_group_desc[i].channelmap_range for i in redgrp_member_list]
            # this becomes list of list [[poltypes for ms0], [poltypes for ms1], ...]
#             polids_list = [[ddobj.get_polarization_id(corr) for corr in ddobj.corr_axis \
#                             if corr in self.required_pols ] for ddobj in temp_dd_list]
            redgrp_pols_list = [[__corr for __corr in __ddobj.corr_axis
                          if __corr in self.required_pols] for __ddobj in __temp_dd_list]
            del __temp_dd_list

            # NRO specific
            redgrp_correlations = None
            if common_is_nro:
                __correlations = []
                for __c in redgrp_pols_list:
                    if __c not in __correlations:
                        __correlations.append(__c)
                assert len(__correlations) == 1
                redgrp_correlations = ''.join(__correlations[0])

            LOG.debug('Members to be processed:')
            for i in range(len(redgrp_member_list)):
                LOG.debug('\t{}: Antenna {} Spw {} Field {}'.format(common_ms_list[i].basename,
                                                                    redgrp_antenna_list[i],
                                                                    redgrp_spwid_list[i],
                                                                    redgrp_fieldid_list[i]))

            # image is created per antenna (science) or per asdm and antenna (ampcal)
            redgrp_image_group = {}
            for (__msobj, __ant, __spwid, __fieldid, __pollist, __chanmap) in zip(common_ms_list, redgrp_antenna_list,
                                                                      redgrp_spwid_list, redgrp_fieldid_list,
                                                                      redgrp_pols_list, redgrp_channelmap_range_list):
                __identifier = __msobj.fields[__fieldid].name
                __antenna = __msobj.antennas[__ant].name
                __identifier += ('.'+__antenna)
                # create image per asdm and antenna for ampcal
                if common_inputs.is_ampcal:
                    __asdm_name = common.asdm_name_from_ms(__msobj)
                    __identifier += ('.'+__asdm_name)
                if __identifier in redgrp_image_group:
                    redgrp_image_group[__identifier].append([__msobj, __ant, __spwid, __fieldid, __pollist, __chanmap])
                else:
                    redgrp_image_group[__identifier] = [[__msobj, __ant, __spwid, __fieldid, __pollist, __chanmap]]
            LOG.debug('image_group={}'.format(redgrp_image_group))

            # loop over antennas
            redgrp_combined_infiles = []
            redgrp_combined_antids = []
            redgrp_combined_fieldids = []
            redgrp_combined_spws = []
            redgrp_combined_v_spws = []
            redgrp_tocombine_images = []
            redgrp_tocombine_org_directions = []
            redgrp_tocombine_specmodes = []
            redgrp_combined_pols = []
            redgrp_combined_rms_exclude = []

            # for combined images for NRO
            redgrp_tocombine_images_nro = []
            redgrp_tocombine_org_directions_nro = []

            redgrp_coord_set = False
            for redgrp_name, redgrp_members in redgrp_image_group.items():
                redgrp_msobjs = [x[0] for x in redgrp_members]
                redgrp_antids = [x[1] for x in redgrp_members]
                redgrp_spwids = [x[2] for x in redgrp_members]
                redgrp_fieldids = [x[3] for x in redgrp_members]
                redgrp_polslist = [x[4] for x in redgrp_members]
                redgrp_chanmap_range_list = [x[5] for x in redgrp_members]
                LOG.info("Processing image group: {}".format(redgrp_name))
                for idx in range(len(redgrp_msobjs)):
                    LOG.info("\t{}: Antenna {:d} ({}) Spw {} Field {:d} ({})"
                             "".format(redgrp_msobjs[idx].basename, redgrp_antids[idx], redgrp_msobjs[idx].antennas[redgrp_antids[idx]].name,
                                       redgrp_spwids[idx], redgrp_fieldids[idx], redgrp_msobjs[idx].fields[redgrp_fieldids[idx]].name))

                # reference data is first MS
                redgrp_ref_ms = redgrp_msobjs[0]
                redgrp_ant_name = redgrp_ref_ms.antennas[redgrp_antids[0]].name
                # for ampcal
                redgrp_asdm = None
                if common_inputs.is_ampcal:
                    redgrp_asdm = common.asdm_name_from_ms(redgrp_ref_ms)

                # source name
                redgrp_source_name = redgrp_group_desc.field_name.replace(' ', '_')

                # specmode
                __ref_field = redgrp_fieldids[0]
                __is_eph_obj = redgrp_ref_ms.get_fields(field_id=__ref_field)[0].source.is_eph_obj
                redgrp_specmode = 'cubesource' if __is_eph_obj else 'cube'

                # filenames for gridding
                common_infiles = [__ms.name for __ms in redgrp_msobjs]

                LOG.debug('infiles={}'.format(common_infiles))

                # image name
                # image name should be based on virtual spw id
                redgrp_v_spwids = [common_context.observing_run.real2virtual_spw_id(s, m) for s, m in zip(redgrp_spwids, redgrp_msobjs)]
                __v_spwids_unique = numpy.unique(redgrp_v_spwids)
                assert len(__v_spwids_unique) == 1
                redgrp_imagename = self.get_imagename(redgrp_source_name, __v_spwids_unique, redgrp_ant_name, redgrp_asdm, specmode=redgrp_specmode)
                LOG.info("Output image name: {}".format(redgrp_imagename))
                redgrp_imagename_nro = None
                if common_is_nro:
                    redgrp_imagename_nro = self.get_imagename(redgrp_source_name, __v_spwids_unique, redgrp_ant_name, redgrp_asdm, stokes=redgrp_correlations, specmode=redgrp_specmode)
                    LOG.info("Output image name for NRO: {}".format(redgrp_imagename_nro))

                # pick restfreq from restfreq_list
                if isinstance(common_restfreq_list, list):
                    __v_spwid = common_context.observing_run.real2virtual_spw_id(redgrp_spwids[0], redgrp_msobjs[0])
                    __v_spwid_list = [
                        common_context.observing_run.real2virtual_spw_id(int(i), redgrp_msobjs[0])
                        for i in common_args_spw[redgrp_msobjs[0].name].split(',')
                    ]
                    __v_idx = __v_spwid_list.index(__v_spwid)
                    if len(common_restfreq_list) > __v_idx:
                        redgrp_restfreq = common_restfreq_list[__v_idx]
                        if redgrp_restfreq is None:
                            redgrp_restfreq = ''
                        LOG.info( "Picked restfreq = '{}' from {}".format(redgrp_restfreq, common_restfreq_list) )
                    else:
                        redgrp_restfreq = ''
                        LOG.warning( "No restfreq for spw {} in {}. Applying default value.".format(__v_spwid, common_restfreq_list) )
                else:
                    redgrp_restfreq = common_restfreq_list
                    LOG.info("Processing with restfreq = {}".format(redgrp_restfreq))

                # Step 1.
                # Initialize weight column based on baseline RMS.
                __origin_ms = [msobj.origin_ms for msobj in redgrp_msobjs]
                __work_ms = [msobj.name for msobj in redgrp_msobjs]
                __weighting_inputs = vdp.InputsContainer(weighting.WeightMS, common_context,
                                                       infiles=__origin_ms, outfiles=__work_ms,
                                                       antenna=redgrp_antids, spwid=redgrp_spwids, fieldid=redgrp_fieldids)
                __weighting_task = weighting.WeightMS(__weighting_inputs)
                self._executor.execute(__weighting_task, merge=False, datatable_dict=common_dt_dict)

                # Step 2.
                # Imaging
                # Image per antenna, source
                LOG.info('Imaging Source {}, Ant {} Spw {:d}'.format(redgrp_source_name, redgrp_ant_name, redgrp_spwids[0]))
                # map coordinate (use identical map coordinate per spw)
                if not redgrp_coord_set:
                    # PIPE-313: evaluate map extent using pointing data from all the antenna in the data
                    __dummyids = [None for _ in redgrp_antids]
                    __image_coord = worker.ImageCoordinateUtil(common_context, common_infiles, __dummyids, redgrp_spwids, redgrp_fieldids)
                    if not __image_coord:  # No valid data is found
                        continue
                    redgrp_coord_set = True
                    (redgrp_phasecenter, redgrp_cellx, redgrp_celly, redgrp_nx, redgrp_ny, redgrp_org_direction) = __image_coord

                # register data for combining
                redgrp_combined_infiles.extend(common_infiles)
                redgrp_combined_antids.extend(redgrp_antids)
                redgrp_combined_fieldids.extend(redgrp_fieldids)
                redgrp_combined_spws.extend(redgrp_spwids)
                redgrp_combined_v_spws.extend(redgrp_v_spwids)
                redgrp_combined_pols.extend(redgrp_polslist)

                redgrp_stokes_list = [self.stokes]
                __imagename_list = [redgrp_imagename]
                if common_is_nro:
                    redgrp_stokes_list.append(redgrp_correlations)
                    __imagename_list.append(redgrp_imagename_nro)

                __imager_results = []
                for __stokes, __imagename in zip(redgrp_stokes_list, __imagename_list):
                    __imager_inputs = worker.SDImagingWorker.Inputs(common_context, common_infiles,
                                                                  outfile=__imagename,
                                                                  mode=common_imagemode,
                                                                  antids=redgrp_antids,
                                                                  spwids=redgrp_spwids,
                                                                  fieldids=redgrp_fieldids,
                                                                  restfreq=redgrp_restfreq,
                                                                  stokes=__stokes,
                                                                  edge=common_edge,
                                                                  phasecenter=redgrp_phasecenter,
                                                                  cellx=redgrp_cellx,
                                                                  celly=redgrp_celly,
                                                                  nx=redgrp_nx, ny=redgrp_ny,
                                                                  org_direction=redgrp_org_direction)
                    __imager_task = worker.SDImagingWorker(__imager_inputs)
                    __imager_result = self._executor.execute(__imager_task)
                    __imager_results.append(__imager_result)
                # per-antenna image (usually Stokes I)
                redgrp_imager_result = __imager_results[0]
                # per-antenna correlation image (XXYY/RRLL)
                redgrp_imager_result_nro = __imager_results[1] if common_is_nro else None

                if redgrp_imager_result.outcome is not None:
                    # Imaging was successful, proceed following steps

                    # add image list to combine
                    if os.path.exists(redgrp_imagename) and os.path.exists(redgrp_imagename+'.weight'):
                        redgrp_tocombine_images.append(redgrp_imagename)
                        redgrp_tocombine_org_directions.append(redgrp_org_direction)
                        redgrp_tocombine_specmodes.append(redgrp_specmode)
                    # Additional Step.
                    # Make grid_table and put rms and valid spectral number array
                    # to the outcome.
                    # The rms and number of valid spectra is used to create RMS maps.
                    LOG.info('Additional Step. Make grid_table')
                    redgrp_imagename = redgrp_imager_result.outcome['image'].imagename
                    with casa_tools.ImageReader(redgrp_imagename) as ia:
                        __cs = ia.coordsys()
                        __dircoords = [i for i in range(__cs.naxes())
                                     if __cs.axiscoordinatetypes()[i] == 'Direction']
                        __cs.done()
                        redgrp_nx = ia.shape()[__dircoords[0]]
                        redgrp_ny = ia.shape()[__dircoords[1]]

                    redgrp_observing_pattern = redgrp_msobjs[0].observing_pattern[redgrp_antids[0]][redgrp_spwids[0]][redgrp_fieldids[0]]
                    redgrp_grid_task_class = gridding.gridding_factory(redgrp_observing_pattern)
                    redgrp_validsps = []
                    redgrp_rmss = []
                    redgrp_grid_input_dict = {}
                    for (__msobj, __antid, __spwid, __fieldid, __poltypes, _dummy) in redgrp_members:
                        __msname = __msobj.name # Use parent ms
                        for p in __poltypes:
                            if p not in redgrp_grid_input_dict:
                                redgrp_grid_input_dict[p] = [[__msname], [__antid], [__fieldid], [__spwid]]
                            else:
                                redgrp_grid_input_dict[p][0].append(__msname)
                                redgrp_grid_input_dict[p][1].append(__antid)
                                redgrp_grid_input_dict[p][2].append(__fieldid)
                                redgrp_grid_input_dict[p][3].append(__spwid)

                    # Generate grid table for each POL in image (per ANT,
                    # FIELD, and SPW, over all MSes)
                    for __pol, __member in redgrp_grid_input_dict.items():
                        __mses = __member[0]
                        __antids = __member[1]
                        __fieldids = __member[2]
                        __spwids = __member[3]
                        __pols = [__pol for i in range(len(__mses))]
                        __gridding_inputs = redgrp_grid_task_class.Inputs(common_context, infiles=__mses,
                                                                 antennaids=__antids,
                                                                 fieldids=__fieldids,
                                                                 spwids=__spwids,
                                                                 poltypes=__pols,
                                                                 nx=redgrp_nx, ny=redgrp_ny)
                        __gridding_task = redgrp_grid_task_class(__gridding_inputs)
                        __gridding_result = self._executor.execute(__gridding_task, merge=False,
                                                                 datatable_dict=common_dt_dict)

                        # Extract RMS and number of spectra from grid_tables
                        if isinstance(__gridding_result.outcome, compress.CompressedObj):
                            __grid_table = __gridding_result.outcome.decompress()
                        else:
                            __grid_table = __gridding_result.outcome
                        redgrp_validsps.append([r[6] for r in __grid_table])
                        redgrp_rmss.append([r[8] for r in __grid_table])
                        del __grid_table

                    # define RMS ranges in image
                    LOG.info("Calculate spectral line and deviation mask frequency ranges in image.")
                    with casa_tools.ImageReader(redgrp_imagename) as ia:
                        __cs = ia.coordsys()
                        __frequency_frame = __cs.getconversiontype('spectral')
                        __cs.done()
                        __rms_exclude_freq = self._get_rms_exclude_freq_range_image(
                            __frequency_frame, redgrp_chanmap_range_list, common_edge, redgrp_msobjs, redgrp_antids, redgrp_spwids, redgrp_fieldids)
                        LOG.info("The spectral line and deviation mask frequency ranges = {}".format(str(__rms_exclude_freq)))
                    redgrp_combined_rms_exclude.extend(__rms_exclude_freq)

                    __file_index = [common.get_ms_idx(common_context, name) for name in common_infiles]
                    self._finalize_worker_result(common_context, redgrp_imager_result,
                                                 sourcename=redgrp_source_name, spwlist=redgrp_v_spwids, antenna=redgrp_ant_name, specmode=redgrp_specmode,
                                                 imagemode=common_imagemode, stokes=self.stokes, validsp=redgrp_validsps, rms=redgrp_rmss, edge=common_edge,
                                                 reduction_group_id=redgrp_group_id, file_index=__file_index,
                                                 assoc_antennas=redgrp_antids, assoc_fields=redgrp_fieldids, assoc_spws=redgrp_v_spwids)

                    if common_inputs.is_ampcal:
                        if len(common_infiles) == 1 and (redgrp_asdm not in ['', None]):
                            redgrp_imager_result.outcome['vis'] = redgrp_asdm
#                         # to register exported_ms to each scantable instance
#                         outcome['export_results'] = export_results

                    # NRO doesn't need per-antenna Stokes I images
                    if not common_is_nro:
                        common_results.append(redgrp_imager_result)

                if redgrp_imager_result_nro is not None and redgrp_imager_result_nro.outcome is not None:
                    # Imaging was successful, proceed following steps

                    # add image list to combine
                    if os.path.exists(redgrp_imagename_nro) and os.path.exists(redgrp_imagename_nro+'.weight'):
                        redgrp_tocombine_images_nro.append(redgrp_imagename_nro)
                        redgrp_tocombine_org_directions_nro.append(redgrp_org_direction)
                        redgrp_tocombine_specmodes.append(redgrp_specmode)

                    __file_index = [common.get_ms_idx(common_context, name) for name in common_infiles]
                    self._finalize_worker_result(common_context, redgrp_imager_result_nro,
                                                 sourcename=redgrp_source_name, spwlist=redgrp_v_spwids, antenna=redgrp_ant_name, specmode=redgrp_specmode,
                                                 imagemode=common_imagemode, stokes=redgrp_stokes_list[1], validsp=redgrp_validsps, rms=redgrp_rmss, edge=common_edge,
                                                 reduction_group_id=redgrp_group_id, file_index=__file_index,
                                                 assoc_antennas=redgrp_antids, assoc_fields=redgrp_fieldids, assoc_spws=redgrp_v_spwids)

                    common_results.append(redgrp_imager_result_nro)

            if common_inputs.is_ampcal:
                LOG.info("Skipping combined image for the amplitude calibrator.")
                continue

            # Make combined image
            if len(redgrp_tocombine_images) == 0:
                LOG.warning("No valid image to combine for Source {}, Spw {:d}".format(redgrp_source_name, redgrp_spwids[0]))
                continue
            # reference MS
            redgrp_ref_ms = common_context.observing_run.get_ms(name=redgrp_combined_infiles[REF_MS_ID])

            # image name
            # image name should be based on virtual spw id
            redgrp_combined_v_spws_unique = numpy.unique(redgrp_combined_v_spws)
            assert len(redgrp_combined_v_spws_unique) == 1
            redgrp_imagename = self.get_imagename(redgrp_source_name, redgrp_combined_v_spws_unique, specmode=redgrp_specmode)

            # Step 3.
            # Imaging of all antennas
            LOG.info('Combine images of Source {} Spw {:d}'.format(redgrp_source_name, redgrp_combined_v_spws[REF_MS_ID]))
            __combine_inputs = sdcombine.SDImageCombineInputs(common_context, inimages=redgrp_tocombine_images,
                                                            outfile=redgrp_imagename,
                                                            org_directions=redgrp_tocombine_org_directions,
                                                            specmodes=redgrp_tocombine_specmodes)
            __combine_task = sdcombine.SDImageCombine(__combine_inputs)
            redgrp_imager_result = self._executor.execute(__combine_task)

            if redgrp_imager_result.outcome is not None:
                # Imaging was successful, proceed following steps

                # Additional Step.
                # Make grid_table and put rms and valid spectral number array
                # to the outcome
                # The rms and number of valid spectra is used to create RMS maps
                LOG.info('Additional Step. Make grid_table')
                postprocess_imagename = redgrp_imager_result.outcome['image'].imagename
                postprocess_org_direction = redgrp_imager_result.outcome['image'].org_direction
                with casa_tools.ImageReader(postprocess_imagename) as ia:
                    __cs = ia.coordsys()
                    __dircoords = [i for i in range(__cs.naxes())
                                 if __cs.axiscoordinatetypes()[i] == 'Direction']
                    __cs.done()
                    postprocess_nx = ia.shape()[__dircoords[0]]
                    postprocess_ny = ia.shape()[__dircoords[1]]
                postprocess_observing_pattern = redgrp_ref_ms.observing_pattern[redgrp_combined_antids[REF_MS_ID]][redgrp_combined_spws[REF_MS_ID]][redgrp_combined_fieldids[REF_MS_ID]]
                postprocess_grid_task_class = gridding.gridding_factory(postprocess_observing_pattern)
                postprocess_validsps = []
                postprocess_rmss = []
                postprocess_grid_input_dict = {}
                for (__msname, __antid, __spwid, __fieldid, __poltypes) in zip(redgrp_combined_infiles, redgrp_combined_antids, redgrp_combined_spws,
                                                                     redgrp_combined_fieldids, redgrp_combined_pols):
                    # msobj = context.observing_run.get_ms(name=common.get_parent_ms_name(context,msname)) # Use parent ms
                    # ddobj = msobj.get_data_description(spw=spwid)
                    for p in __poltypes:
                        if p not in postprocess_grid_input_dict:
                            postprocess_grid_input_dict[p] = [[__msname], [__antid], [__fieldid], [__spwid]]
                        else:
                            postprocess_grid_input_dict[p][0].append(__msname)
                            postprocess_grid_input_dict[p][1].append(__antid)
                            postprocess_grid_input_dict[p][2].append(__fieldid)
                            postprocess_grid_input_dict[p][3].append(__spwid)

                for __pol, __member in postprocess_grid_input_dict.items():
                    __mses = __member[0]
                    __antids = __member[1]
                    __fieldids = __member[2]
                    __spwids = __member[3]
                    __pols = [__pol for i in range(len(__mses))]
                    __gridding_inputs = postprocess_grid_task_class.Inputs(common_context, infiles=__mses,
                                                             antennaids=__antids,
                                                             fieldids=__fieldids,
                                                             spwids=__spwids,
                                                             poltypes=__pols,
                                                             nx=postprocess_nx, ny=postprocess_ny)
                    __gridding_task = postprocess_grid_task_class(__gridding_inputs)
                    __gridding_result = self._executor.execute(__gridding_task, merge=False,
                                                             datatable_dict=common_dt_dict)
                    # Extract RMS and number of spectra from grid_tables
                    if isinstance(__gridding_result.outcome, compress.CompressedObj):
                        __grid_table = __gridding_result.outcome.decompress()
                    else:
                        __grid_table = __gridding_result.outcome
                    postprocess_validsps.append([r[6] for r in __grid_table])
                    postprocess_rmss.append([r[8] for r in __grid_table])
                    del __grid_table

                # calculate RMS of line free frequencies in a combined image
                LOG.info('Calculate sensitivity of combined image')
                with casa_tools.ImageReader(postprocess_imagename) as ia:
                    postprocess_cs = ia.coordsys()
                    postprocess_faxis = postprocess_cs.findaxisbyname('spectral')
                    postprocess_chan_width = postprocess_cs.increment()['numeric'][postprocess_faxis]
                    postprocess_brightnessunit = ia.brightnessunit()
                    postprocess_beam = ia.restoringbeam()
                postprocess_qcell = list(postprocess_cs.increment(format='q', type='direction')['quantity'].values())  # cs.increment(format='s', type='direction')['string']

                # Define image channels to calculate statistics
                postprocess_include_channel_range = self._get_stat_chans(postprocess_imagename, redgrp_combined_rms_exclude, common_edge)
                postprocess_stat_chans = convert_range_list_to_string(postprocess_include_channel_range)

                # Define region to calculate statistics
                postprocess_raster_infos = self.get_raster_info_list(common_context, redgrp_combined_infiles,
                                                         redgrp_combined_antids,
                                                         redgrp_combined_fieldids,
                                                         redgrp_combined_spws, common_dt_dict)
                postprocess_region = self._get_stat_region(postprocess_raster_infos, postprocess_org_direction, postprocess_beam)

                # Image statistics
                if postprocess_region is None:
                    LOG.warning('Could not get valid region of interest to calculate image statistics.')
                    postprocess_image_rms = -1.0
                else:
                    __statval = calc_image_statistics(postprocess_imagename, postprocess_stat_chans, postprocess_region)
                    if len(__statval['rms']):
                        postprocess_image_rms = __statval['rms'][0]
                        LOG.info("Statistics of line free channels ({}): RMS = {:f} {}, Stddev = {:f} {}, Mean = {:f} {}".format(postprocess_stat_chans, __statval['rms'][0], postprocess_brightnessunit, __statval['sigma'][0], postprocess_brightnessunit, __statval['mean'][0], postprocess_brightnessunit))
                    else:
                        LOG.warning('Could not get image statistics. Potentially no valid pixel in region of interest.')
                        postprocess_image_rms = -1.0
                # Theoretical RMS
                LOG.info('Calculating theoretical RMS of image, {}'.format(postprocess_imagename))
                postprocess_theoretical_rms = self.calculate_theoretical_image_rms(redgrp_combined_infiles, redgrp_combined_antids,
                                                                       redgrp_combined_fieldids, redgrp_combined_spws,
                                                                       redgrp_combined_pols, postprocess_raster_infos, postprocess_qcell,
                                                                       postprocess_chan_width, postprocess_brightnessunit,
                                                                       common_dt_dict)

                # estimate
                __rep_bw = redgrp_ref_ms.representative_target[2]
                (__rep_source_name, __rep_spwid) = redgrp_ref_ms.get_representative_source_spw()
                postprocess_is_representative_spw = (__rep_spwid == redgrp_combined_spws[REF_MS_ID] and __rep_bw is not None)
                postprocess_is_representative_source_spw = (__rep_spwid == redgrp_combined_spws[REF_MS_ID]) and \
                                               (__rep_source_name == utils.dequote(redgrp_source_name))
                if postprocess_is_representative_spw:
                    # skip estimate if data is Cycle 2 and earlier + th effective BW is nominal (= chan_width)
                    __spwobj = redgrp_ref_ms.get_spectral_window(__rep_spwid)
                    if common_cqa.time(redgrp_ref_ms.start_time['m0'], 0, ['ymd', 'no_time'])[0] < '2015/10/01' and \
                            __spwobj.channels.chan_effbws[0] == numpy.abs(__spwobj.channels.chan_widths[0]):
                        postprocess_is_representative_spw = False
                        LOG.warning("Cycle 2 and earlier project with nominal effective band width. Reporting RMS at native resolution.")
                    else:
                        if not common_cqa.isquantity(__rep_bw): # assume Hz
                            __rep_bw = common_cqa.quantity(__rep_bw, 'Hz')
                        LOG.info("Estimate RMS in representative bandwidth: {:f}kHz (native: {:f}kHz)".format(common_cqa.getvalue(common_cqa.convert(common_cqa.quantity(__rep_bw), 'kHz'))[0], postprocess_chan_width*1.e-3))
                        __factor = sensitivity_improvement.sensitivityImprovement(redgrp_ref_ms.name, __rep_spwid, common_cqa.tos(__rep_bw))
                        if __factor is None:
                            LOG.warning('No image RMS improvement because representative bandwidth is narrower than native width')
                            __factor = 1.0
                        LOG.info("Image RMS improvement of factor {:f} estimated. {:f} => {:f} {}".format(__factor, postprocess_image_rms, postprocess_image_rms/__factor, postprocess_brightnessunit))
                        postprocess_image_rms = postprocess_image_rms / __factor
                        postprocess_chan_width = numpy.abs(common_cqa.getvalue(common_cqa.convert(common_cqa.quantity(__rep_bw), 'Hz'))[0])
                        postprocess_theoretical_rms['value'] = postprocess_theoretical_rms['value'] / __factor
                elif __rep_bw is None:
                    LOG.warning(
                        "Representative bandwidth is not available. Skipping estimate of sensitivity in representative band width.")
                elif __rep_spwid is None:
                    LOG.warning(
                        "Representative SPW is not available. Skipping estimate of sensitivity in representative band width.")

                # calculate channel and frequency ranges of line free channels
                __ref_pixel = postprocess_cs.referencepixel()['numeric']
                __freqs = []
                for __ichan in postprocess_include_channel_range:
                    __ref_pixel[postprocess_faxis] = __ichan
                    __freqs.append(postprocess_cs.toworld(__ref_pixel)['numeric'][postprocess_faxis])
                postprocess_cs.done()
                if len(__freqs) > 1 and __freqs[0] > __freqs[1]:  # LSB
                    __freqs.reverse()
                postprocess_stat_freqs = str(', ').join(['{:f}~{:f}GHz'.format(__freqs[__iseg]*1.e-9, __freqs[__iseg+1]*1.e-9)
                                             for __iseg in range(0, len(__freqs), 2)])

                __file_index = [common.get_ms_idx(common_context, name) for name in redgrp_combined_infiles]
                __sensitivity = Sensitivity(array='TP',
                                          intent='TARGET',
                                          field=redgrp_source_name,
                                          spw=str(redgrp_combined_v_spws[REF_MS_ID]),
                                          is_representative=postprocess_is_representative_source_spw,
                                          bandwidth=common_cqa.quantity(postprocess_chan_width, 'Hz'),
                                          bwmode='repBW',
                                          beam=postprocess_beam, cell=postprocess_qcell,
                                          sensitivity=common_cqa.quantity(postprocess_image_rms, postprocess_brightnessunit))
                __theoretical_noise = Sensitivity(array='TP',
                                                intent='TARGET',
                                                field=redgrp_source_name,
                                                spw=str(redgrp_combined_v_spws[REF_MS_ID]),
                                                is_representative=postprocess_is_representative_source_spw,
                                                bandwidth=common_cqa.quantity(postprocess_chan_width, 'Hz'),
                                                bwmode='repBW',
                                                beam=postprocess_beam, cell=postprocess_qcell,
                                                sensitivity=postprocess_theoretical_rms)
                __sensitivity_info = SensitivityInfo(__sensitivity, postprocess_is_representative_spw, postprocess_stat_freqs, (not common_is_nro))
                self._finalize_worker_result(common_context, redgrp_imager_result,
                                             sourcename=redgrp_source_name, spwlist=redgrp_combined_v_spws, antenna='COMBINED',  specmode=redgrp_specmode,
                                             imagemode=common_imagemode, stokes=self.stokes, validsp=postprocess_validsps, rms=postprocess_rmss, edge=common_edge,
                                             reduction_group_id=redgrp_group_id, file_index=__file_index,
                                             assoc_antennas=redgrp_combined_antids, assoc_fields=redgrp_combined_fieldids, assoc_spws=redgrp_combined_v_spws,
                                             sensitivity_info=__sensitivity_info, theoretical_rms=__theoretical_noise)

                # PIPE-251: detect contamination
                detectcontamination.detect_contamination(common_context, redgrp_imager_result.outcome['image'])

                common_results.append(redgrp_imager_result)

            # NRO specific: generate combined image for each correlation
            if common_is_nro:
                if len(redgrp_tocombine_images_nro) == 0:
                    LOG.warning("No valid image to combine for Source {}, Spw {:d}".format(redgrp_source_name, redgrp_spwids[0]))
                    continue

                # image name
                # image name should be based on virtual spw id
                postprocess_imagename = self.get_imagename(redgrp_source_name, redgrp_combined_v_spws_unique, stokes=redgrp_correlations, specmode=redgrp_specmode)

                # Step 3.
                # Imaging of all antennas
                LOG.info('Combine images of Source {} Spw {:d}'.format(redgrp_source_name, redgrp_combined_v_spws[REF_MS_ID]))
                __combine_inputs = sdcombine.SDImageCombineInputs(common_context, inimages=redgrp_tocombine_images_nro,
                                                                outfile=postprocess_imagename,
                                                                org_directions=redgrp_tocombine_org_directions_nro,
                                                                specmodes=redgrp_tocombine_specmodes)
                __combine_task = sdcombine.SDImageCombine(__combine_inputs)
                redgrp_imager_result = self._executor.execute(__combine_task)

                if redgrp_imager_result.outcome is not None:
                    # Imaging was successful, proceed following steps

                    __file_index = [common.get_ms_idx(common_context, name) for name in redgrp_combined_infiles]
                    self._finalize_worker_result(common_context, redgrp_imager_result,
                                                 sourcename=redgrp_source_name, spwlist=redgrp_combined_v_spws, antenna='COMBINED', specmode=redgrp_specmode,
                                                 imagemode=common_imagemode, stokes=redgrp_stokes_list[1], validsp=postprocess_validsps, rms=postprocess_rmss, edge=common_edge,
                                                 reduction_group_id=redgrp_group_id, file_index=__file_index,
                                                 assoc_antennas=redgrp_combined_antids, assoc_fields=redgrp_combined_fieldids, assoc_spws=redgrp_combined_v_spws)

                    common_results.append(redgrp_imager_result)

        return common_results

    def analyse(self, result):
        return result

    def _get_rms_exclude_freq_range_image(self, to_frame, chanmap_ranges, edge,
                                          msobj_list, antid_list, spwid_list, fieldid_list):
        """
        Return a combined list of frequency ranges.

        This method combines deviation mask, channel map ranges, and edges.

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
            if len(exclude_range) == 1 and exclude_range[0] == [0, spwobj.num_channels-1]:
                # deviation mask is full channel range when all data are flagged
                LOG.warning("Ignoring DEVIATION MASK of {} (SPW {:d}, FIELD {:d}, ANT {:d}). Possibly all data flagged".format(
                    msobj.basename, spwid, fieldid, antid))
                exclude_range = []
            if edge[0] > 0: exclude_range.append([0, edge[0]-1])
            if edge[1] > 0: exclude_range.append([spwobj.num_channels-edge[1], spwobj.num_channels-1])
            if len(channelmap_range) > 0:
                exclude_range.extend(channelmap_range)
            # check the validity of channel number and fix it when out of range
            min_chan = 0
            max_chan = spwobj.num_channels - 1
            exclude_channel_range = [[max(min_chan, x[0]), min(max_chan, x[1])]
                                     for x in merge_ranges(exclude_range)]
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
            me = casa_tools.measures
            qa = casa_tools.quanta
            qmid_time = qa.quantity(start_time['m0'])
            qmid_time = qa.add(qmid_time, end_time['m0'])
            qmid_time = qa.div(qmid_time, 2.0)
            time_ref = me.epoch(rf=start_time['refer'],
                                v0=qmid_time)
            position_ref = msobj.antennas[antid].position

            if to_frame == 'REST':
                mse = casa_tools.ms
                mse.open( msobj.name )
                obstime = qa.time( qmid_time, form='ymd' )[0]
                v_to   = mse.cvelfreqs( spwids=[spwid], obstime=obstime, outframe='SOURCE' )
                v_from = mse.cvelfreqs( spwids=[spwid], obstime=obstime, outframe=spwobj.frame )
                mse.close()
                _to_imageframe = interpolate.interp1d( v_from, v_to,
                                                       kind='linear',
                                                       bounds_error=False,
                                                       fill_value='extrapolate' )
            else:
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

            image_rms_freq_range.extend( map(_to_imageframe, exclude_freq_range) )
            me.done()

        #LOG.info("#####Overall LINE CHANNELS IN IMAGE FRAME = {}".format(str(image_rms_freq_range)))
        if len(image_rms_freq_range) == 0:
            return image_rms_freq_range

        return merge_ranges(numpy.reshape(image_rms_freq_range, (len(image_rms_freq_range)//2, 2), 'C'))

    def get_imagename(self, source: str, spwids: List[int],
                      antenna: str=None, asdm: str=None, stokes: str=None, specmode: str='cube') -> str:
        """
        Generate image filename.

        Args:
            source   : Source name
            spwids   : SpW IDs
            antenna  : Antenna name
            asdm     : ASDM
            stokes   : Stokes parameter
            specmode : specmode for tsdimaging
        Returns:
            image filename
        Raises:
            ValueError if asdm is not provided for ampcal
        """
        context = self.inputs.context
        is_nro = sdutils.is_nro(context)
        if is_nro:
            namer = filenamer.Image(virtspw=False)
        else:
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
        namer.specmode( specmode )
        # so far we always create native resolution, full channel image
        #namer.spectral_image()
        namer._associations.format('image.sd')
        #namer.single_dish()
        namer.antenna(antenna)
        # iteration is necessary for exportdata
        namer.iteration(0)
        imagename = namer.get_filename()
        return imagename

    def _get_stat_chans(self, imagename: str,
                        combined_rms_exclude: List[Tuple[float, float]],
                        edge: Tuple[int, int]=(0, 0)) -> List[int]:
        """
        Return a list of channel ranges to calculate image statistics.

        Args:
            imagename: A name of image
            combined_rms_exclude: A list of frequency ranges to exclude
            edge: The left and right edge channels to exclude
        Retruns:
            A 1-d list of channel ranges to INCLUDE in calculation of image
            statistics, e.g., [imin0, imax0, imin0, imax0, ...]
        """
        with casa_tools.ImageReader(imagename) as ia:
            cs = ia.coordsys()
            faxis = cs.findaxisbyname('spectral')
            num_chan = ia.shape()[faxis]
        exclude_chan_ranges = convert_frequency_ranges_to_channels(combined_rms_exclude, cs, num_chan)
        LOG.info("Merged spectral line channel ranges of combined image = {}".format(str(exclude_chan_ranges)))
        include_chan_ranges = invert_ranges(exclude_chan_ranges, num_chan, edge)
        LOG.info("Line free channel ranges of image to calculate RMS = {}".format(str(include_chan_ranges)))
        return include_chan_ranges

    def _get_stat_region(self, raster_infos: List[RasterInfo],
                         org_direction: Optional[dict],
                         beam: dict) -> Optional[str]:
        """
        Retrun region to calculate statistics.

        Median width, height, and position angle is adopted as a reference
        map extent and then the width and height will be shrinked by 2 beam
        size in each direction.

        Arg:
            raster_infos: A list of RasterInfo to calculate region from.
            org_direction: A measure of direction of origin for ephemeris obeject.
            beam: Beam size dictionary of image.

        Retruns:
            Region expression string of a rotating box.
            Returns None if no valid region of interest is defined.
        """
        cqa = casa_tools.quanta
        beam_unit = cqa.getunit(beam['major'])
        assert cqa.getunit(beam['minor']) == beam_unit
        beam_size = numpy.sqrt(cqa.getvalue(beam['major'])*cqa.getvalue(beam['minor']))[0]
        center_unit = 'deg'
        angle_unit = None
        for r in raster_infos:
            if r is None: continue
            angle_unit = cqa.getunit(r.scan_angle)
            break
        if angle_unit is None:
            LOG.warning('No valid raster information available.')
            return None

        def __value_in_unit(quantity: dict, unit: str) -> float:
            # Get value(s) of quantity in a specified unit
            return cqa.getvalue(cqa.convert(quantity, unit))

        def __extract_values(value: str, unit: str) -> Number:
            # Extract valid values of specified attributes in list
            return [__value_in_unit(getattr(r, value), unit) for r in raster_infos if r is not None]

        rep_width = numpy.nanmedian(__extract_values('width', beam_unit))
        rep_height = numpy.nanmedian(__extract_values('height', beam_unit))
        rep_angle = numpy.nanmedian([cqa.getvalue(r.scan_angle) for r in raster_infos if r is not None])
        center_ra = numpy.nanmedian(__extract_values('center_ra', center_unit))
        center_dec = numpy.nanmedian(__extract_values('center_dec', center_unit))
        width = rep_width - beam_size
        height = rep_height - beam_size
        if width <= 0 or height <= 0: # No valid region selected.
            return None
        if org_direction is not None:
            (center_ra, center_dec) = direction_utils.direction_recover(center_ra,
                                                                        center_dec,
                                                                        org_direction)
        center = [cqa.tos(cqa.quantity(center_ra, center_unit)),
                  cqa.tos(cqa.quantity(center_dec, center_unit))]

        region = "rotbox[{}, [{}{}, {}{}], {}{}]".format(center,
                                                         width, beam_unit,
                                                         height, beam_unit,
                                                         rep_angle, angle_unit)
        return region

    def get_raster_info_list(self, context: 'Context', infiles: List[str],
                             antids: List[int], fieldids: List[int],
                             spwids: List[int],
                             datatable_dict: Dict[str, DataTable]) -> List[RasterInfo]:
        """
        Retrun a list of raster information.

        Each raster infromation is analyzed for element wise combination of
        infile, antenna, field, and SpW IDs in input parameter lists.

        Args:
            context: Pipeline context to work on
            infiles: A list of MS names
            antids: A list of antenna IDs. Must be the same lengh as infile.
            fieldids: A list of field IDs. Must be the same lengh as infile.
            spwids: A list of SpW IDs. Must be the same lengh as infile.
            datatable_dict: A dictionary of MS name (key) and DataTable
                instance (value) pair

        Returns:
            A list of RasterInfo. If raster information could not be obtained,
            the corresponding elements in the list will be None.
        """
        assert len(infiles) == len(antids)
        assert len(infiles) == len(fieldids)
        assert len(infiles) == len(spwids)
        raster_info_list = []
        for (infile, antid, fieldid, spwid) in zip(infiles, antids, fieldids, spwids):
            msobj = context.observing_run.get_ms(name=infile)
            # Non raster data set.
            if msobj.observing_pattern[antid][spwid][fieldid] != 'RASTER':
                f = msobj.get_fields(field_id=fieldid)[0]
                LOG.warning('Not a raster map: field {} in {}'.format(f.name, msobj.basename))
                raster_info_list.append(None)
            dt = datatable_dict[msobj.basename]
            try:
                raster_info_list.append(_analyze_raster_pattern(dt, msobj, fieldid, spwid, antid))
            except Exception:
                f = msobj.get_fields(field_id=fieldid)[0]
                a = msobj.get_antenna(antid)[0]
                LOG.info('Could not get raster information of field {}, Spw {}, Ant {}, MS {}. Potentially be because all data are flagged.'.format(f.name, spwid, a.name, msobj.basename))
                raster_info_list.append(None)
        assert len(infiles) == len(raster_info_list)
        return raster_info_list

    def calculate_theoretical_image_rms(self, infiles, antids, fieldids, spwids,
                                        pols, raster_infos, cell, bandwidth,
                                        imageunit, datatable_dict):
        """
        Calculate theoretical RMS of an image (PIPE-657).

        Parameters:
            infiles: a list of MS names
            antids: a list of antenna IDs, e.g., [3, 3]
            fieldids: a list of field IDs, e.g., [1, 1]
            spwids: a list of SpW IDs, e.g., [17, 17]
            pols: a list of polarization strings, e.g., [['XX', 'YY'], ['XX', 'YY']]
            raster_infos: a list of RasterInfo
            cell: cell size of an image
            bandwidth: channel width of an image
            imageunit: the brightness unit of image. If unit is not 'K', Jy/K factor is used to convert unit (need Jy/K factor applied in a previous stage)
        Note: the number of elements in antids, fieldids, spws, and pols should be equal to that of infiles
        Retruns:
            A quantum value of theoretical image RMS.
            The value of quantity will be negative when calculation is aborted, i.e., -1.0 Jy/beam
        """
        cqa = casa_tools.quanta
        failed_rms = cqa.quantity(-1, imageunit)
        if len(infiles) == 0:
            LOG.error('No MS given to calculate a theoretical RMS. Aborting calculation of theoretical thermal noise.')
            return failed_rms
        assert len(infiles) == len(antids)
        assert len(infiles) == len(fieldids)
        assert len(infiles) == len(spwids)
        assert len(infiles) == len(pols)
        assert len(infiles) == len(raster_infos)
        sq_rms = 0.0
        N = 0.0
        time_unit = 's'
        ang_unit = cqa.getunit(cell[0])
        cx_val = cqa.getvalue(cell[0])[0]
        cy_val = cqa.getvalue(cqa.convert(cell[1], ang_unit))[0]
        bandwidth = numpy.abs(bandwidth)
        context = self.inputs.context
        is_nro = sdutils.is_nro(context)
        for (infile, antid, fieldid, spwid, pol_names, raster_info) in zip(infiles, antids, fieldids, spwids, pols, raster_infos):
            msobj = context.observing_run.get_ms(name=infile)
            callist = context.observing_run.get_measurement_sets_of_type([DataType.REGCAL_CONTLINE_ALL])
            calmsobj = sdutils.match_origin_ms(callist, msobj.origin_ms)
            dd_corrs = msobj.get_data_description(spw=spwid).corr_axis
            polids = [dd_corrs.index(p) for p in pol_names if p in dd_corrs]
            field_name = msobj.get_fields(field_id=fieldid)[0].name
            error_msg = 'Aborting calculation of theoretical thermal noise of Field {} and SpW {}'.format(field_name, spwid)
            if msobj.observing_pattern[antid][spwid][fieldid] != 'RASTER':
                LOG.warning('Unable to calculate RMS of non-Raster map. '+error_msg)
                return failed_rms
            LOG.info('Processing MS {}, Field {}, SpW {}, Antenna {}, Pol {}'.format(msobj.basename,
                                                                                     field_name,
                                                                                     spwid,
                                                                                     msobj.get_antenna(antid)[0].name,
                                                                                     str(pol_names)))
            if raster_info is None:
                LOG.warning('Raster scan analysis failed. Skipping further calculation.')
                continue

            dt = datatable_dict[msobj.basename]
            _index_list = common.get_index_list_for_ms(dt, [msobj.origin_ms], [antid], [fieldid],
                                                       [spwid])
            if len(_index_list) == 0: #this happens when permanent flag is set to all selection.
                LOG.info('No unflagged row in DataTable. Skipping further calculation.')
                continue
            # effective BW
            with casa_tools.MSMDReader(infile) as msmd:
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
            calto = callibrary.CalTo(vis=calmsobj.name, field=str(fieldid))
            calst = context.callibrary.applied.trimmed(context, calto)
            # obtain T_sub,on, T_sub,off
            t_sub_on = cqa.getvalue(cqa.convert(raster_info.row_duration, time_unit))[0]
            sky_field = calmsobj.calibration_strategy['field_strategy'][fieldid]
            try:
                skytab = ''
                caltabs = context.callibrary.applied.get_caltable('ps')
                ### For some reasons, sky caltable is not registered to calstate
                for cto, cfrom in context.callibrary.applied.merged().items():
                    if cto.vis == calmsobj.name and (cto.field == '' or fieldid in [f.id for f in calmsobj.get_fields(name=cto.field)]):
                        for cf in cfrom:
                            if cf.gaintable in caltabs:
                                skytab = cf.gaintable
                                break
            except:
                LOG.error('Could not find a sky caltable applied. '+error_msg)
                raise
            if not os.path.exists(skytab):
                LOG.warning('Could not find a sky caltable applied. '+error_msg)
                return failed_rms
            LOG.info('Searching OFF scans in {}'.format(os.path.basename(skytab)))
            with casa_tools.TableReader(skytab) as tb:
                interval_unit = tb.getcolkeyword('INTERVAL', 'QuantumUnits')[0]
                t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}&&FIELD_ID=={}'.format(spwid, antid, sky_field), columns='INTERVAL')
                if t.nrows == 0:
                    LOG.warning('No sky caltable row found for spw {}, antenna {}, field {} in {}. {}'.format(
                        spwid, antid, sky_field, os.path.basename(skytab), error_msg))
                    t.close()
                    return failed_rms
                try:
                    interval = t.getcol('INTERVAL')
                finally:
                    t.close()
            t_sub_off = cqa.getvalue(cqa.convert(cqa.quantity(interval.mean(), interval_unit), time_unit))[0]
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
                    caltabs = context.callibrary.applied.get_caltable(('amp', 'gaincal'))
                    found = caltabs.intersection(calst.get_caltable(('amp', 'gaincal')))
                    if len(found) == 0:
                        LOG.warning('Could not find a Jy/K caltable applied. '+error_msg)
                        return failed_rms
                    if len(found) > 1:
                        LOG.warning('More than one Jy/K caltables are found.')
                    k2jytab = found.pop()
                    LOG.info('Searching Jy/K factor in {}'.format(os.path.basename(k2jytab)))
                except:
                    LOG.error('Could not find a Jy/K caltable applied. '+error_msg)
                    raise
                if not os.path.exists(k2jytab):
                    LOG.warning('Could not find a Jy/K caltable applied. '+error_msg)
                    return failed_rms
                with casa_tools.TableReader(k2jytab) as tb:
                    t = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}'.format(spwid, antid), columns='CPARAM')
                    if t.nrows == 0:
                        LOG.warning('No Jy/K caltable row found for spw {}, antenna {} in {}. {}'.format(spwid,
                                    antid, os.path.basename(k2jytab), error_msg))
                        t.close()
                        return failed_rms
                    try:
                        tc = t.getcol('CPARAM')
                    finally:
                        t.close()
                    jy_per_k = (1./tc.mean(axis=-1).real**2).mean()
                    LOG.info('Jy/K factor = {}'.format(jy_per_k))
            ang = cqa.getvalue(cqa.convert(raster_info.scan_angle, 'rad'))[0] + 0.5*numpy.pi
            c_proj = numpy.sqrt( (cy_val * numpy.sin(ang))**2 + (cx_val * numpy.cos(ang))**2 )
            inv_variant_on = effBW * numpy.abs(cx_val * cy_val) * t_on_act / width / height
            inv_variant_off = effBW * c_proj * t_sub_off * t_on_act / t_sub_on / height

            for ipol in polids:
                sq_rms += (jy_per_k*mean_tsys_per_pol[ipol])**2 * (conv2d**2/inv_variant_on + conv1d**2/inv_variant_off)
                N += 1.0

        if N == 0:
            LOG.warning('No rms estimate is available.')
            return failed_rms

        theoretical_rms = numpy.sqrt(sq_rms)/N
        LOG.info('Theoretical RMS of image = {} {}'.format(theoretical_rms, imageunit))
        return cqa.quantity(theoretical_rms, imageunit)


def _analyze_raster_pattern(datatable: DataTable, msobj: MeasurementSet,
                            fieldid: int, spwid: int, antid: int) -> RasterInfo:
    """
    Analyze raster scan pattern from pointing in DataTable.

    Args:
        datatable: DataTable instance
        msobj: MS class instance to process
        fieldid: a field ID to process
        spwid: an SpW ID to process
        antid: an antenna ID to process
        polid: a polarization ID to process
    Returns: a named Tuple of RasterInfo
    """
    origin_basename = os.path.basename(msobj.origin_ms)
    metadata = rasterutil.read_datatable(datatable)
    pflag = metadata.pflag
    ra = metadata.ra
    dec = metadata.dec
    exposure = datatable.getcol('EXPOSURE')
    timetable = datatable.get_timetable(ant=antid, spw=spwid, field_id=fieldid, ms=origin_basename, pol=None)
    # dtrow_list is a list of numpy array holding datatable rows separated by raster rows
    # [[r00, r01, r02, ...], [r10, r11, r12, ...], ...]
    dtrow_list_nomask = rasterutil.extract_dtrow_list(timetable)
    dtrow_list = [rows[pflag[rows]] for rows in dtrow_list_nomask if numpy.any(pflag[rows] == True)]
    radec_unit = datatable.getcolkeyword('OFS_RA', 'UNIT')
    assert radec_unit == datatable.getcolkeyword('OFS_DEC', 'UNIT')
    exp_unit = datatable.getcolkeyword('EXPOSURE', 'UNIT')
    try:
        gap_r = rasterscan.find_raster_gap(ra, dec, dtrow_list)
    except Exception:
        LOG.warning('Failed to detect gaps between raster scans. Fall back to time domain analysis. Result might not be correct.')
        try:
            dtrow_list_large = rasterutil.extract_dtrow_list(timetable, for_small_gap=False)
            se_small = [(v[0], v[-1]) for v in dtrow_list]
            se_large = [(v[0], v[-1]) for v in dtrow_list_large]
            gap_r = []
            for sl, el in se_large:
                for i, (ss, es) in enumerate(se_small):
                    if ss == sl:
                        gap_r.append(i)
                        break
            gap_r.append(len(dtrow_list))
        except Exception:
            LOG.warning('Could not find gaps between raster scans. No result is produced.')
            return None

    cqa = casa_tools.quanta
    idx_all = numpy.concatenate(dtrow_list)
    mean_dec = numpy.mean(datatable.getcol('DEC')[idx_all])
    dec_unit = datatable.getcolkeyword('DEC', 'UNIT')
    map_center_dec = cqa.getvalue(
        cqa.convert(cqa.quantity(mean_dec, dec_unit), 'rad')
    )[0]
    dec_factor = numpy.abs(numpy.cos(map_center_dec))

    ndata = len(dtrow_list)
    duration = numpy.fromiter(
        map(lambda x: exposure[x].sum(), dtrow_list),
        dtype=float, count=ndata
    )
    num_integration = numpy.fromiter(
        map(len, dtrow_list), dtype=int, count=ndata
    )
    center_ra = numpy.fromiter(
        map(lambda x: ra[x].mean(), dtrow_list),
        dtype=float, count=ndata
    )
    center_dec = numpy.fromiter(
        map(lambda x: dec[x].mean(), dtrow_list),
        dtype=float, count=ndata
    )
    delta_ra = numpy.fromiter(
        map(lambda x: (ra[x[-1]] - ra[x[0]]) * dec_factor, dtrow_list),
        dtype=float, count=ndata
    )
    delta_dec = numpy.fromiter(
        map(lambda x: dec[x[-1]] - dec[x[0]], dtrow_list),
        dtype=float, count=ndata
    )
    height_list = []
    for s, e in zip(gap_r[:-1], gap_r[1:]):
        start_ra = center_ra[s]
        start_dec = center_dec[s]
        end_ra = center_ra[e - 1]
        end_dec = center_dec[e - 1]
        height_list.append(
            numpy.hypot((start_ra - end_ra) * dec_factor, start_dec - end_dec)
        )
    LOG.debug('REFACTOR: ant %s, spw %s, field %s', antid, spwid, fieldid)
    LOG.debug('REFACTOR: map_center_dec=%s, dec_factor=%s', map_center_dec, dec_factor)
    LOG.debug('REFACTOR: duration=%s', list(duration))
    LOG.debug('REFACTOR: num_integration=%s', list(num_integration))
    LOG.debug('REFACTOR: delta_ra=%s', list(delta_ra))
    LOG.debug('REFACTOR: delta_dec=%s', list(delta_dec))
    LOG.debug('REFACTOR: center_ra=%s', list(center_ra))
    LOG.debug('REFACTOR: center_dec=%s', list(center_dec))
    LOG.debug('REFACTOR: height_list=%s', list(height_list))
    LOG.debug('REFACTOR: dtrows')
    for rows in dtrow_list:
        LOG.debug('REFACTOR:   %s', list(rows))
    center_ra = numpy.array(center_ra)
    center_dec = numpy.array(center_dec)
    row_sep_ra = (center_ra[1:]-center_ra[:-1])*dec_factor
    row_sep_dec = center_dec[1:]-center_dec[:-1]
    row_separation = numpy.median(numpy.hypot(row_sep_ra, row_sep_dec))
    # find complate raster
    num_row_int = rasterutil.find_most_frequent(num_integration)
    complete_idx = numpy.where(num_integration >= num_row_int)
    # raster scan parameters
    row_duration = numpy.array(duration)[complete_idx].mean()
    row_delta_ra = numpy.abs(delta_ra)[complete_idx].mean()
    row_delta_dec = numpy.abs(delta_dec)[complete_idx].mean()
    width = numpy.hypot(row_delta_ra, row_delta_dec)
    sign_ra = +1.0 if delta_ra[complete_idx[0][0]] >= 0 else -1.0
    sign_dec = +1.0 if delta_dec[complete_idx[0][0]] >= 0 else -1.0
    scan_angle = math.atan2(sign_dec*row_delta_dec, sign_ra*row_delta_ra)
    hight = numpy.max(height_list)
    center = (cqa.quantity(0.5*(center_ra.min()+center_ra.max()), radec_unit),
              cqa.quantity(0.5*(center_dec.min()+center_dec.max()), radec_unit))
    raster_info = RasterInfo(center[0], center[1],
                             cqa.quantity(width, radec_unit), cqa.quantity(hight, radec_unit),
                             cqa.quantity(scan_angle, 'rad'), cqa.quantity(row_separation, radec_unit),
                             cqa.quantity(row_duration, exp_unit))
    LOG.info('Raster Information')
    LOG.info('- Map Center: [{}, {}]'.format(cqa.angle(raster_info.center_ra, prec=8, form='time')[0],
                                             cqa.angle(raster_info.center_dec, prec=8)[0]))
    LOG.info('- Scan Extent: [{}, {}] (scan direction: {})'.format(cqa.tos(raster_info.width),
                                                                   cqa.tos(raster_info.height),
                                                                   cqa.tos(raster_info.scan_angle)))
    LOG.info('- Raster row separation = {}'.format(cqa.tos(raster_info.row_separation)))
    LOG.info('- Raster row scan duration = {}'.format(cqa.tos(cqa.convert(raster_info.row_duration, 's'))))
    return raster_info


def calc_image_statistics(imagename: str, chans: str, region: str) -> dict:
    """
    Retrun image statistics with channel and region selection.

    Args:
        imagename: Path to image to calculate statistics
        chans: Channel range selection string, e.g., '0~110;240~300'
        region: Region definition string.

    Returns:
        A dictionary of statistic values returned by ia.statistics.
    """
    LOG.info("Calculateing image statistics of chans='{}', region='{}' in {}".format(chans, region, imagename))
    rg = casa_tools.regionmanager
    with casa_tools.ImageReader(imagename) as ia:
        cs = ia.coordsys()
        try:
            chan_sel = rg.frombcs(csys=cs.torecord(), shape=ia.shape(), chans=chans)
        finally:
            cs.done()
            rg.done()
        subim = ia.subimage(region=chan_sel)
        try:
            stat = subim.statistics(region=region)
        finally:
            subim.close()
    return stat


### Utility methods to calcluate channel ranges
def convert_frequency_ranges_to_channels(range_list: List[Tuple[float, float]],
                                         cs, num_chan: int) -> List[Tuple[int, int]]:
    """
    Convert frequency ranges to channel ones.

    Args:
        range_list: A list of min/max frequency ranges,
            e.g., [[fmin0,fmax0],[fmin1, fmax1],...]
        cs: A coordinate system to convert world values to pixel one
        num_chan: the number of channels in frequency axis
    Returns:
        A list of min/max channels, e.g., [[imin0, imax0],[imin1,imax1],...]
    """
    faxis = cs.findaxisbyname('spectral')
    ref_world = cs.referencevalue()['numeric']
    LOG.info("Aggregated spectral line frequency ranges of combined image = {}".format(str(range_list)))
    channel_ranges = [] # should be list for sort
    for segment in range_list:
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
        channel_ranges.append([max(int(min_chan), 0),
                               min(int(max_chan), num_chan-1)])
    channel_ranges.sort()
    return merge_ranges(channel_ranges)


def convert_range_list_to_string(range_list: List[int]) -> str:
    """
    Convert a list of index ranges to string.

    Args:
        range_list: A list of ranges, e.g., [imin0, imax0, imin1, imax1, ...]
    Retruns:
        A string in form, e.g., 'imin0~imax0;imin1~imax1'
    Examples:
        >>> convert_range_list_to_string( [5, 10, 15, 20] )
        '5~10;15~20'
    """
    stat_chans = str(';').join([ '{:d}~{:d}'.format(range_list[iseg], range_list[iseg+1]) for iseg in range(0, len(range_list), 2) ])
    return stat_chans


def merge_ranges(range_list: List[Tuple[Number, Number]]) -> List[Tuple[Number, Number]]:
    """
    Merge overlapping ranges in range_list.

    Args:
        range_list    : a list of ranges to merge, e.g., [ [min0,max0], [min1,max1], .... ]
                        each range in the list should be in ascending order (min0 <= max0)
                        there is no assumption in the order of ranges, e.g., min0 w.r.t min1
    Returns:
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
        merged = merge_ranges(merged)
    #LOG.info("#####Merged: {}".format(str(merged)))
    return merged


def invert_ranges(id_range_list: List[Tuple[int, int]],
                  num_ids: int, edge: Tuple[int, int]) -> List[int]:
    """
    Return invert ID ranges.

    Args:
        id_range_list: A list of min/max ID ranges to invert. The list should
            be sorted in the ascending order of min IDs.
        num_ids: A number of IDs to consider
        edge: The left and right edges to exclude

    Returns:
        A 1-d list of inverted ranges

    Examples:
        >>> id_range_list = [[5,10],[15,20]]
        >>> num_ids = 30
        >>> edge = (2,3)
        >>> invert_ranges(id_range_list, num_ids, edge)
        [2, 4, 11, 14, 21, 26]
    """
    inverted_list = []
    if len(id_range_list) == 0:
        inverted_list = [edge[0], num_ids-1-edge[1]]
    else:
        if id_range_list[0][0] > edge[0]:
            inverted_list.extend([edge[0], id_range_list[0][0]-1])
        for j in range(len(id_range_list)-1):
            start_include = id_range_list[j][1]+1
            end_include = id_range_list[j+1][0]-1
            if start_include <= end_include:
                inverted_list.extend([start_include, end_include])
        if id_range_list[-1][1] + 1 < num_ids-1-edge[1]:
            inverted_list.extend([id_range_list[-1][1] + 1, num_ids-1-edge[1]])
    return inverted_list
