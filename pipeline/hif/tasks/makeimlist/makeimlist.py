import copy
import os
import operator
import collections

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import imageparams_factory
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from .cleantarget import CleanTarget
from .resultobjects import MakeImListResult

LOG = infrastructure.get_logger(__name__)


class MakeImListInputs(vdp.StandardInputs):
    # Must use empty data type list to allow for user override and
    # automatic determination depending on specmode, field and spw.
    processing_data_type = []

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
    per_eb = vdp.VisDependentProperty(default=False)
    calcsb = vdp.VisDependentProperty(default=False)
    datatype = vdp.VisDependentProperty(default='')
    datacolumn = vdp.VisDependentProperty(default='')
    parallel = vdp.VisDependentProperty(default='automatic')
    robust = vdp.VisDependentProperty(default=None)
    uvtaper = vdp.VisDependentProperty(default=None)

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

    def get_spw_hm_cell(self, spwlist):
        """If possible obtain spwlist specific hm_cell, otherwise return generic value.

        hif_checkproductsize() task determines the mitigation parameters. It does not know, however, about the
        set spwlist in the hif_makeimlist call and determines mitigation parameters per band (complete spw set).
        The band containing the set spwlist is determined by checking whether spwlist is a subset of the band
        spw list. The mitigation parameters found for the matching band are applied to the set spwlist.

        If no singluar band (spw set) is found that would contain spwlist, then the default hm_cell heuristics is
        returned.

        TODO: refactor and make hif_checkproductsize() (or a new task) spwlist aware."""
        mitigated_hm_cell = None
        multi_target_size_mitigation = self.context.size_mitigation_parameters.get('multi_target_size_mitigation', {})
        if multi_target_size_mitigation:
            multi_target_spwlist = [spws for spws in multi_target_size_mitigation.keys() if set(spwlist.split(',')).issubset(set(spws.split(',')))]
            if len(multi_target_spwlist) == 1:
                mitigated_hm_cell = multi_target_size_mitigation.get(multi_target_spwlist[0], {}).get('hm_cell')
        if mitigated_hm_cell not in [None, {}]:
            return mitigated_hm_cell
        else:
            return self.hm_cell

    def get_spw_hm_imsize(self, spwlist):
        """If possible obtain spwlist specific hm_imsize, otherwise return generic value.

        TODO: refactor and make hif_checkproductsize() (or a new task) spwlist aware."""
        mitigated_hm_imsize = None
        multi_target_size_mitigation = self.context.size_mitigation_parameters.get('multi_target_size_mitigation', {})
        if multi_target_size_mitigation:
            multi_target_spwlist = [spws for spws in multi_target_size_mitigation.keys() if set(spwlist.split(',')).issubset(set(spws.split(',')))]
            if len(multi_target_spwlist) == 1:
                mitigated_hm_imsize = multi_target_size_mitigation.get(multi_target_spwlist[0], {}).get('hm_imsize')
        if mitigated_hm_imsize not in [None, {}]:
            return mitigated_hm_imsize
        else:
            return self.hm_imsize

    def __init__(self, context, output_dir=None, vis=None, imagename=None, intent=None, field=None, spw=None,
                 contfile=None, linesfile=None, uvrange=None, specmode=None, outframe=None, hm_imsize=None,
                 hm_cell=None, calmaxpix=None, phasecenter=None, nchan=None, start=None, width=None, nbins=None,
                 robust=None, uvtaper=None, clearlist=None, per_eb=None, calcsb=None, datatype= None,
                 datacolumn=None, parallel=None, known_synthesized_beams=None):
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
        self.datatype = datatype
        self.datacolumn = datacolumn
        self.parallel = parallel
        self.known_synthesized_beams = known_synthesized_beams


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

        qaTool = casa_tools.quanta

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

        # datatype and datacolumn are mutually exclusive
        if inputs.datatype not in ('', None) and inputs.datacolumn not in (None, ''):
            msg = '"datatype" and "datacolumn" are mutually exclusive'
            LOG.error(msg)
            result.error = True
            result.error_msg = msg
            return result

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        if inputs.intent == 'TARGET':
            if inputs.specmode in ('mfs', 'cont'):
                specmode_datatypes = [DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
            else:
                specmode_datatypes = [DataType.SELFCAL_LINE_SCIENCE, DataType.REGCAL_LINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]
        else:
            specmode_datatypes = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

        # Check against any user input for datatype to make sure that the
        # correct initial vis list is chosen (e.g. for REGCAL_CONTLINE_ALL and RAW).
        known_datatypes_str = [str(v).replace('DataType.', '') for v in DataType]
        explicit_user_datatypes = False
        if inputs.datatype not in ('', None):
            # Consider every comma separated user value just once
            user_datatypes = list(set([datatype.strip().upper() for datatype in inputs.datatype.split(',')]))
            if all(datatype not in ('BEST', 'ALL', 'SELFCAL', 'REGCAL') for datatype in user_datatypes):
                datatype_checklist = [datatype not in known_datatypes_str for datatype in user_datatypes]
                if any(datatype_checklist):
                    msg = 'Undefined data type(s): {}'.format(','.join(d for d, c in zip(user_datatypes, datatype_checklist) if c))
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result
                explicit_user_datatypes = True
                # Use only intersection of specmode and user data types
                specmode_datatypes = specmode_datatypes and [eval(f'DataType.{datatype}') for datatype in user_datatypes]
        else:
            user_datatypes = []

        specmode_datatypes_str = [str(datatype).replace('DataType.', '') for datatype in specmode_datatypes]

        datacolumn = inputs.datacolumn

        global_datatype = None
        global_datatype_info = 'N/A'
        global_datacolumn = inputs.datacolumn
        selected_datatypes = [global_datatype]
        selected_datatypes_info = [global_datatype_info]
        automatic_datatype_choice = False

        # Select the correct vis list
        if inputs.vis in ('', [''], [], None):
            (ms_objects_and_columns, selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=specmode_datatypes, msonly=False)
            # Check for changing vis lists.
            if explicit_user_datatypes:
                for user_datatype in user_datatypes:
                    (sub_ms_objects_and_columns, sub_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=[eval(f'DataType.{user_datatype}')], msonly=False)
                    if set(ms_objects_and_columns) != set(sub_ms_objects_and_columns):
                        msg = 'Requested data types lead to multiple vis lists. Please run hif_makeimlist with data type selections per kind of MS (targets, targets_line, etc.).'
                        LOG.error(msg)
                        result.error = True
                        result.error_msg = msg
                        return result
            global_datatype = f'{str(selected_datatype).replace("DataType.", "")}'
            global_datatype_info = global_datatype
            selected_datatypes = [global_datatype]
            selected_datatypes_info = [global_datatype_info]
            automatic_datatype_choice = True

            if ms_objects_and_columns == collections.OrderedDict():
                result.set_info({'msg': 'No data found. No imaging targets were created.',
                                 'intent': inputs.intent,
                                 'specmode': inputs.specmode})
                result.contfile = None
                result.linesfile = None
                return result

            global_columns = list(ms_objects_and_columns.values())

            if inputs.datatype in ('', None):
                # Log these messages only if there is no user data type
                LOG.info(f'Using data type {str(selected_datatype).replace("DataType.", "")} for imaging.')

                if selected_datatype == DataType.RAW:
                    LOG.warn('Falling back to raw data for imaging.')

                if not all(global_column == global_columns[0] for global_column in global_columns):
                    LOG.warn(f'Data type based column selection changes among MSes: {",".join(f"{k.basename}: {v}" for k,v in ms_objects_and_columns.items())}.')

            if inputs.datacolumn not in (None, ''):
                global_datacolumn = inputs.datacolumn
                LOG.info(f'Manual override of datacolumn to {global_datacolumn}. Data type based datacolumn would have been "{"data" if global_columns[0] == "DATA" else "corrected"}".')
            else:
                if global_columns[0] == 'DATA':
                    global_datacolumn = 'data'
                elif global_columns[0] == 'CORRECTED_DATA':
                    global_datacolumn = 'corrected'
                else:
                    LOG.warn(f'Unknown column name {global_columns[0]}')
                    global_datacolumn = ''

            datacolumn = global_datacolumn
            inputs.vis = [k.basename for k in ms_objects_and_columns.keys()]

        # Handle user supplied data type requests
        if inputs.datatype not in ('', None):
            # Extract all available data types for the vis list
            vislist_datatypes_str = []
            for vis in inputs.vis:
                ms_object = inputs.context.observing_run.get_ms(vis)
                # Collect the intersection of data types across the vis list
                vislist_datatypes_str = vislist_datatypes_str + [str(datatype).replace('DataType.', '') for datatype in ms_object.data_column]
            vislist_datatypes_str = list(set(vislist_datatypes_str))
            # Intersection of specmode based and vis based datatypes gives
            # list of actually available data types for this call.
            available_datatypes_str = specmode_datatypes_str and vislist_datatypes_str

            if 'BEST' in user_datatypes:
                if user_datatypes != ['BEST']:
                    msg = '"BEST" and all other options are mutually exclusive'
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result

                # Automatic choice with fallback per source/spw selection
                user_datatypes = [global_datatype]
                user_datatypes_info = [global_datatype_info]
                automatic_datatype_choice = global_datatype is not None
                LOG.info(f'Using data type {global_datatype} for imaging.')
            elif 'ALL' in user_datatypes:
                if user_datatypes != ['BEST']:
                    msg = '"ALL" and all other options are mutually exclusive'
                    LOG.error(msg)
                    result.error = True
                    result.error_msg = msg
                    return result

                # All SELFCAL and REGCAL choices available for this vis list
                # List selfcal first, then regcal
                user_datatypes = [datatype for datatype in available_datatypes_str if 'SELFCAL' in datatype]
                user_datatypes = user_datatypes + [datatype for datatype in available_datatypes_str if 'REGCAL' in datatype]
                user_datatypes_info = [datatype for datatype in user_datatypes]
                automatic_datatype_choice = False
            else:
                user_datatypes = [datatype.strip().upper() for datatype in inputs.datatype.split(',')]
                if 'REGCAL' in user_datatypes or 'SELFCAL' in user_datatypes:
                    # Check if any explicit data types are given
                    if any(datatype in specmode_datatypes for datatype in user_datatypes):
                        msg = '"REGCAL"/"SELFCAL" and explicit data types are mutually exclusive'
                        LOG.error(msg)
                        result.error = True
                        result.error_msg = msg
                        return result

                    # Expand SELFCAL and REGCAL to explicit data types for this vis list
                    expanded_user_datatypes = []
                    # List selfcal first, then regcal
                    if 'SELFCAL' in user_datatypes:
                        expanded_user_datatypes = expanded_user_datatypes + [datatype for datatype in available_datatypes_str if 'SELFCAL' in datatype]
                    if 'REGCAL' in user_datatypes:
                        expanded_user_datatypes = expanded_user_datatypes + [datatype for datatype in available_datatypes_str if 'REGCAL' in datatype]
                    user_datatypes = expanded_user_datatypes
                    automatic_datatype_choice = False
                else:
                    # Explicit individual data types
                    datatype_checklist = [datatype not in known_datatypes_str for datatype in user_datatypes]
                    if any(datatype_checklist):
                        msg = 'Undefined data type(s): {}'.format(','.join(d for d, c in zip(user_datatypes, datatype_checklist) if c))
                        LOG.error(msg)
                        result.error = True
                        result.error_msg = msg
                        return result
                    automatic_datatype_choice = False
                user_datatypes_info = [datatype for datatype in user_datatypes]

            selected_datatypes = user_datatypes
            selected_datatypes_info = user_datatypes_info

        image_heuristics_factory = imageparams_factory.ImageParamsHeuristicsFactory()

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

        # Get representative target information
        repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, maxAllowedBeamAxialRatio, sensitivityGoal = self.heuristics.representative_target()

        # representative target case
        if inputs.specmode == 'repBW':
            repr_target_mode = True
            image_repr_target = False

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
                spwids = sorted(inputs.context.observing_run.virtual_science_spw_ids, key=int)
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
            for k, v in band.items():
                if str(k) in spwlist:
                    band_spws.setdefault(v, []).append(k)
        else:
            band_spws = {None: 0}

        # Need to record if there are targets for a vislist
        have_targets = {}

        max_num_targets = 0

        for selected_datatype, selected_datatype_info in zip(selected_datatypes, selected_datatypes_info):
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
                        spwlist_local = [','.join(map(str, sorted(map(int, spwlist))))]
                    else:
                        spwlist_local = spwlist

                    # get list of field_ids/intents to be cleaned
                    if (not repr_target_mode) or (repr_target_mode and image_repr_target):
                        field_intent_list = self.heuristics.field_intent_list(
                          intent=inputs.intent, field=inputs.field)
                    else:
                        continue

                    # Expand cont spws
                    if inputs.specmode == 'cont':
                        spwids = spwlist_local[0].split(',')
                    else:
                        spwids = spwlist_local

                    # Generate list of observed vis/field/spw combinations
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
                                    # Get a field domain object. Make sure that it has the necessary intent. Otherwise the list of spw IDs
                                    # will not match with the available science spw IDs.
                                    # Using all intents (inputs.intent) here. Further filtering is performed in the next block.
                                    if ms_domain_obj.get_fields(field_intent[0], intent=inputs.intent) != []:
                                        field_domain_obj = ms_domain_obj.get_fields(field_intent[0], intent=inputs.intent)[0]
                                        # Get all science spw IDs for this field and record the ones that are present in this MS
                                        field_science_spwids = [spw_domain_obj.id for spw_domain_obj in field_domain_obj.valid_spws if spw_domain_obj.id in ms_science_spwids]
                                        # Record the virtual spwids
                                        spwids_per_vis_and_field = [
                                            inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj)
                                            for spwid in field_science_spwids
                                            if inputs.context.observing_run.real2virtual_spw_id(spwid, ms_domain_obj) in list(map(int, spwids))]
                                    else:
                                        spwids_per_vis_and_field = []
                                except Exception as e:
                                    LOG.error(e)
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

                    # Save original vislist_field_spw_combinations dictionary to be able to generate
                    # proper messages if the vis list changes when falling back to a different data
                    # type for a given source/spw combination later on. The vislist_field_spw_combinations
                    # dictionary is possibly being modified on-the-fly below.
                    original_vislist_field_spw_combinations = copy.deepcopy(vislist_field_spw_combinations)

                    # Remove bad spws and record actual vis/field/spw combinations containing data.
                    # Record all spws with actual data in a global list.
                    # Need all spw keys (individual and cont) to distribute the
                    # cell and imsize heuristic results which work on the
                    # highest/lowest frequency spw only.
                    all_spw_keys = []
                    if field_intent_list != set([]):
                        valid_data = {}
                        filtered_spwlist = []
                        valid_data[str(vislist)] = {}
                        for vis in vislist:
                            ms_domain_obj = inputs.context.observing_run.get_ms(vis)
                            valid_data[vis] = {}
                            for field_intent in field_intent_list:
                                valid_data[vis][field_intent] = {}
                                if field_intent not in valid_data[str(vislist)]:
                                    valid_data[str(vislist)][field_intent] = {}
                                # Check only possible field/spw combinations to speed up
                                if vislist_field_spw_combinations.get(field_intent[0], None) is not None:
                                    # Check if this field is present in the current MS and has the necessary intent.
                                    # Using get_fields(name=...) since it does not throw an exception if the field is not found.
                                    if ms_domain_obj.get_fields(name=field_intent[0], intent=field_intent[1]) != []:
                                        observed_vis_list = vislist_field_spw_combinations.get(field_intent[0], None).get('vislist', None)
                                        observed_spwids_list = vislist_field_spw_combinations.get(field_intent[0], None).get('spwids', None)
                                        if observed_vis_list is not None and observed_spwids_list is not None:
                                            # Save spws in main list
                                            all_spw_keys.extend(map(str, observed_spwids_list))
                                            # Also save cont selection
                                            all_spw_keys.append(','.join(map(str, observed_spwids_list)))
                                            for observed_spwid in map(str, observed_spwids_list):
                                                valid_data[vis][field_intent][str(observed_spwid)] = self.heuristics.has_data(field_intent_list=[field_intent], spwspec=observed_spwid, vislist=[vis])[field_intent]
                                                if not valid_data[vis][field_intent][str(observed_spwid)] and vis in observed_vis_list:
                                                    LOG.warning('Data for EB {}, field {}, spw {} is completely flagged.'.format(
                                                        os.path.basename(vis), field_intent[0], observed_spwid))
                                                # Aggregated value per vislist (replace with lookup pattern later)
                                                if str(observed_spwid) not in valid_data[str(vislist)][field_intent]:
                                                    valid_data[str(vislist)][field_intent][str(observed_spwid)] = valid_data[vis][field_intent][str(observed_spwid)]
                                                else:
                                                    valid_data[str(vislist)][field_intent][str(observed_spwid)] = valid_data[str(vislist)][field_intent][str(observed_spwid)] or valid_data[vis][field_intent][str(observed_spwid)]
                                                if valid_data[vis][field_intent][str(observed_spwid)]:
                                                    filtered_spwlist.append(observed_spwid)
                        filtered_spwlist = sorted(list(set(filtered_spwlist)), key=int)
                    else:
                        continue

                    # Collapse cont spws
                    if inputs.specmode == 'cont':
                        filtered_spwlist_local = [','.join(filtered_spwlist)]
                    else:
                        filtered_spwlist_local = filtered_spwlist

                    if filtered_spwlist_local == [] or filtered_spwlist_local == ['']:
                        LOG.error('No spws left for vis list {}'.format(','.join(os.path.basename(vis) for vis in vislist)))
                        continue

                    # Parse hm_cell to get optional pixperbeam setting
                    cell = inputs.get_spw_hm_cell(filtered_spwlist_local[0])
                    if isinstance(cell, str):
                        pixperbeam = float(cell.split('ppb')[0])
                        cell = []
                    else:
                        pixperbeam = 5.0

                    # Add actual, possibly reduced cont spw combination to be able to properly populate the lookup tables later on
                    if inputs.specmode == 'cont':
                        all_spw_keys.append(','.join(filtered_spwlist))
                    # Keep only unique entries
                    all_spw_keys = list(set(all_spw_keys))

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

                    if min_freq_spwid == -1 or max_freq_spwid == -1:
                        LOG.error('Could not determine min/max frequency spw IDs for %s.' % (str(filtered_spwlist_local)))
                        continue

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

                    # Get field specific uvrange value
                    uvrange = {}
                    bl_ratio = {}
                    for field_intent in field_intent_list:
                        for spwspec in filtered_spwlist_local:
                            if inputs.uvrange not in (None, [], ''):
                                uvrange[(field_intent[0], spwspec)] = inputs.uvrange
                            else:
                                try:
                                    (uvrange[(field_intent[0], spwspec)], bl_ratio[(field_intent[0], spwspec)]) = \
                                        self.heuristics.uvrange(field=field_intent[0], spwspec=spwspec)
                                except Exception as e:
                                    # problem defining uvrange
                                    LOG.warning(e)
                                    pass

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
                                if (vislist_field_spw_combinations.get(field_intent[0], None) is not None and
                                        vislist_field_spw_combinations[field_intent[0]].get('spwids', None) is not None and
                                        spwspec in list(map(str, vislist_field_spw_combinations[field_intent[0]]['spwids']))):
                                    actual_field_intent_list.append(field_intent)

                            synthesized_beams[spwspec], known_synthesized_beams = self.heuristics.synthesized_beam(
                                field_intent_list=actual_field_intent_list, spwspec=spwspec, robust=robust, uvtaper=uvtaper,
                                pixperbeam=pixperbeam, known_beams=known_synthesized_beams, force_calc=calcsb,
                                parallel=parallel, shift=True)

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
                                field_ids = self.heuristics.field(field_intent[1], field_intent[0], vislist=vislist_field_spw_combinations[field_intent[0]]['vislist'])
                                phasecenters[field_intent[0]] = self.heuristics.phasecenter(field_ids, vislist=vislist_field_spw_combinations[field_intent[0]]['vislist'])
                            except Exception as e:
                                # problem defining center
                                LOG.warning(e)
                                pass
                    else:
                        for field_intent in field_intent_list:
                            phasecenters[field_intent[0]] = phasecenter

                    # if imsize not set then use heuristic code to calculate the
                    # centers for each field/spwspec
                    imsize = inputs.get_spw_hm_imsize(filtered_spwlist_local[0])
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
                                    field_ids = self.heuristics.field(field_intent[1], field_intent[0], vislist=vislist_field_spw_combinations[field_intent[0]]['vislist'])
                                    # Image size (FOV) may be determined depending on the fractional bandwidth of the
                                    # selected spectral windows. In continuum spectral mode pass the spw list string
                                    # to imsize heuristics (used only for VLA), otherwise pass None to disable the feature.
                                    imsize_spwlist = filtered_spwlist_local if inputs.specmode == 'cont' else None
                                    himsize = self.heuristics.imsize(
                                        fields=field_ids, cell=cells[spwspec], primary_beam=largest_primary_beams[spwspec],
                                        sfpblimit=sfpblimit, centreonly=False, vislist=vislist_field_spw_combinations[field_intent[0]]['vislist'],
                                        spwspec=imsize_spwlist)
                                    if field_intent[1] in [
                                            'PHASE',
                                            'BANDPASS',
                                            'AMPLITUDE',
                                            'FLUX',
                                            'CHECK',
                                            'POLARIZATION',
                                            'POLANGLE',
                                            'POLLEAKAGE'
                                            ]:
                                        himsize = [min(npix, inputs.calmaxpix) for npix in himsize]
                                    imsizes[(field_intent[0], spwspec)] = himsize
                                    if imsizes[(field_intent[0], spwspec)][0] > max_x_size:
                                        max_x_size = imsizes[(field_intent[0], spwspec)][0]
                                    if imsizes[(field_intent[0], spwspec)][1] > max_y_size:
                                        max_y_size = imsizes[(field_intent[0], spwspec)][1]
                                except Exception as e:
                                    # problem defining imsize
                                    LOG.warning(e)
                                    pass

                            if max_x_size == 1 or max_y_size == 1:
                                LOG.error('imsize of [{:d}, {:d}] for field {!s} intent {!s} spw {!s} is degenerate.'.format(max_x_size, max_y_size, field_intent[0], field_intent[1], min_freq_spwlist))
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
                            for spwspec in filtered_spwlist_local:
                                try:
                                    nchans[(field_intent[0], spwspec)], widths[(field_intent[0], spwspec)] = \
                                      self.heuristics.nchan_and_width(field_intent=field_intent[1], spwspec=spwspec)
                                except Exception as e:
                                    # problem defining nchan and width
                                    LOG.warning(e)
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

                    # Sort field/intent list alphabetically considering the intent as the first
                    # and the source name as the second key.
                    sorted_field_intent_list = sorted(field_intent_list, key=operator.itemgetter(1,0))

                    # In case of TARGET intent place representative source first in the list.
                    if 'TARGET' in inputs.intent:
                        sorted_field_intent_list = utils.place_repr_source_first(sorted_field_intent_list, repr_source)

                    for field_intent in sorted_field_intent_list:
                        mosweight = self.heuristics.mosweight(field_intent[1], field_intent[0])
                        for spwspec in filtered_spwlist_local:
                            # The field/intent and spwspec loops still cover the full parameter
                            # space. Here we filter the actual combinations.
                            valid_field_spwspec_combination = False
                            actual_spwids = []
                            if vislist_field_spw_combinations[field_intent[0]].get('spwids', None) is not None:
                                for spwid in spwspec.split(','):
                                    if valid_data[str(vislist)].get(field_intent, None):
                                        if valid_data[str(vislist)][field_intent].get(str(spwid), None):
                                            if int(spwid) in vislist_field_spw_combinations[field_intent[0]]['spwids']:
                                                valid_field_spwspec_combination = True
                                                actual_spwids.append(spwid)
                            if not valid_field_spwspec_combination:
                                continue

                            # For 'cont' mode we still need to restrict the virtual spw ID list to just
                            # the ones that were actually observed for this field.
                            adjusted_spwspec = ','.join(map(str, actual_spwids))

                            spwspec_ok = True
                            actual_spwspec_list = []
                            spwsel = {}
                            all_continuum = True
                            cont_ranges_spwsel_dict = {}
                            all_continuum_spwsel_dict = {}
                            spwsel_spwid_dict = {}

                            # Check if the globally selected data type is available for this field/spw combination.
                            if selected_datatype is not None and inputs.datacolumn in ('', None):
                                if automatic_datatype_choice:
                                    # In automatic mode check for source/spw specific fall back to next available data type.
                                    (local_ms_objects_and_columns, local_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=specmode_datatypes, msonly=False, source=field_intent[0], spw=adjusted_spwspec)
                                else:
                                    # In manual mode check determine the data column for the current data type.
                                    (local_ms_objects_and_columns, local_selected_datatype) = inputs.context.observing_run.get_measurement_sets_of_type(dtypes=[eval(f'DataType.{selected_datatype}')], msonly=False, source=field_intent[0], spw=adjusted_spwspec)
                                local_selected_datatype_str = str(local_selected_datatype).replace('DataType.', '')
                                local_selected_datatype_info = local_selected_datatype_str
                                local_columns = list(local_ms_objects_and_columns.values())

                                if local_selected_datatype_str != selected_datatype:
                                    if automatic_datatype_choice:
                                        LOG.warn(f'Data type {selected_datatype} is not available for field {field_intent[0]} SPW {adjusted_spwspec}. Falling back to data type {local_selected_datatype_str}.')
                                        local_selected_datatype_info = f'{local_selected_datatype_str} instead of {selected_datatype}'
                                    else:
                                        # Manually selected data type unavailable -> skip making an imaging target
                                        LOG.warn(f'Data type {selected_datatype} is not available for field {field_intent[0]} SPW {adjusted_spwspec} in the chosen vis list.')
                                        continue

                                if not all(local_column == local_columns[0] for local_column in local_columns):
                                    LOG.warn(f'Data type based column selection changes among MSes: {",".join(f"{k.basename}: {v}" for k,v in local_ms_objects_and_columns.items())}.')

                                if inputs.datacolumn not in (None, ''):
                                    local_datacolumn = global_datacolumn
                                    local_selected_datatype_info = global_datatype_info
                                    LOG.info(f'Manual override of datacolumn to {global_datacolumn}. Data type based datacolumn would have been "{"data" if local_columns[0] == "DATA" else "corrected"}".')
                                else:
                                    if local_columns[0] == 'DATA':
                                        local_datacolumn = 'data'
                                    elif local_columns[0] == 'CORRECTED_DATA':
                                        local_datacolumn = 'corrected'
                                    else:
                                        LOG.warn(f'Unknown column name {local_columns[0]}')
                                        local_datacolumn = ''

                                datacolumn = local_datacolumn

                                if vislist_field_spw_combinations[field_intent[0]]['vislist'] != [k.basename for k in local_ms_objects_and_columns.keys()]:
                                    if original_vislist_field_spw_combinations[field_intent[0]]['vislist'] != [k.basename for k in local_ms_objects_and_columns.keys()]:
                                        if automatic_datatype_choice and local_selected_datatype_str != selected_datatype:
                                            LOG.warn(f'''Modifying vis list from {original_vislist_field_spw_combinations[field_intent[0]]['vislist']} to {[k.basename for k in local_ms_objects_and_columns.keys()]} for fallback data type {local_selected_datatype_str}.''')
                                        else:
                                            LOG.warn(f'''Modifying vis list from {original_vislist_field_spw_combinations[field_intent[0]]['vislist']} to {[k.basename for k in local_ms_objects_and_columns.keys()]} for data type {local_selected_datatype_str}.''')
                                    vislist_field_spw_combinations[field_intent[0]]['vislist'] = [k.basename for k in local_ms_objects_and_columns.keys()]
                            else:
                                datacolumn = global_datacolumn

                                local_selected_datatype = None
                                local_selected_datatype_info = 'N/A'

                            # Save the specific vislist in a copy of the heuristics object tailored to the
                            # current imaging target
                            target_heuristics = copy.deepcopy(self.heuristics)
                            target_heuristics.vislist = vislist_field_spw_combinations[field_intent[0]]['vislist']

                            for spwid in adjusted_spwspec.split(','):
                                cont_ranges_spwsel_dict[spwid], all_continuum_spwsel_dict[spwid] = target_heuristics.cont_ranges_spwsel()
                                spwsel_spwid_dict[spwid] = cont_ranges_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, 'NONE')

                            no_cont_ranges = False
                            if (field_intent[1] == 'TARGET' and specmode == 'cont' and
                                    all([v == 'NONE' for v in spwsel_spwid_dict.values()])):
                                LOG.warning('No valid continuum ranges were found for any spw. Creating an aggregate continuum'
                                            ' image from the full bandwidth from all spws, but this should be used with'
                                            ' caution.')
                                no_cont_ranges = True

                            for spwid in adjusted_spwspec.split(','):
                                spwsel_spwid = spwsel_spwid_dict[spwid]
                                if field_intent[1] == 'TARGET' and not no_cont_ranges:
                                    if spwsel_spwid == 'NONE':
                                        if specmode == 'cont':
                                            LOG.warning('Spw {!s} will not be used in creating the aggregate continuum image'
                                                        ' of {!s} because no continuum range was found.'
                                                        ''.format(spwid, field_intent[0]))
                                        else:
                                            LOG.warning('Spw {!s} will not be used for {!s} because no continuum range was'
                                                        ' found.'.format(spwid, field_intent[0]))
                                            spwspec_ok = False
                                        continue
                                    #elif (spwsel_spwid == ''):
                                    #    LOG.warning('Empty continuum frequency range for %s, spw %s. Run hif_findcont ?' % (field_intent[0], spwid))

                                all_continuum = all_continuum and all_continuum_spwsel_dict[spwid].get(utils.dequote(field_intent[0]), {}).get(spwid, False)

                                if spwsel_spwid in ('ALL', '', 'NONE'):
                                    spwsel_spwid_freqs = ''
                                    if target_heuristics.is_eph_obj(field_intent[0]):
                                        spwsel_spwid_refer = 'SOURCE'
                                    else:
                                        spwsel_spwid_refer = 'LSRK'
                                else:
                                    spwsel_spwid_freqs, spwsel_spwid_refer = spwsel_spwid.split()

                                if spwsel_spwid_refer not in ('LSRK', 'SOURCE'):
                                    LOG.warning('Frequency selection is specified in %s but must be in LSRK or SOURCE'
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
                                    if '*' in nbins_dict:
                                        nbin = nbins_dict['*']
                                    else:
                                        nbin = nbins_dict[spwspec]
                                except:
                                    LOG.warning('Could not determine binning factor for spw %s. Using default channel width.'
                                                '' % adjusted_spwspec)
                                    nbin = -1
                            else:
                                nbin = -1

                            if spwspec_ok and (field_intent[0], spwspec) in imsizes and ('invalid' not in cells[spwspec]):
                                LOG.debug(
                                  'field:%s intent:%s spw:%s cell:%s imsize:%s phasecenter:%s' %
                                  (field_intent[0], field_intent[1], adjusted_spwspec,
                                   cells[spwspec], imsizes[(field_intent[0], spwspec)],
                                   phasecenters[field_intent[0]]))

                                # Remove MSs that do not contain data for the given field/intent combination
                                # FIXME: This should already have been filtered above. This filter is just from the domain objects.
                                scanidlist, visindexlist = target_heuristics.get_scanidlist(vislist_field_spw_combinations[field_intent[0]]['vislist'],
                                                                                            field_intent[0], field_intent[1])
                                domain_filtered_vislist = [vislist_field_spw_combinations[field_intent[0]]['vislist'][i] for i in visindexlist]
                                if inputs.specmode == 'cont':
                                    filtered_vislist = domain_filtered_vislist
                                else:
                                    # Filter MSs with fully flagged field/spw selections
                                    filtered_vislist = [v for v in domain_filtered_vislist if valid_data[v][field_intent][str(adjusted_spwspec)]]

                                # Save the filtered vislist
                                target_heuristics.vislist = filtered_vislist

                                # Get list of antenna IDs
                                antenna_ids = target_heuristics.antenna_ids(inputs.intent)
                                # PIPE-964: The '&' at the end of the antenna input was added to not to consider the cross
                                #  baselines by default. The cross baselines with antennas not listed (for TARGET images
                                #  the antennas with the minority antenna sizes are not listed) could be added in some
                                #  future configurations by removing this character.
                                antenna = [','.join(map(str, antenna_ids.get(os.path.basename(v), '')))+'&'
                                           for v in filtered_vislist]

                                target = CleanTarget(
                                    antenna=antenna,
                                    field=field_intent[0],
                                    intent=field_intent[1],
                                    spw=actual_spwspec,
                                    spwsel_lsrk=spwsel,
                                    spwsel_all_cont=all_continuum,
                                    num_all_spws=num_all_spws,
                                    num_good_spws=num_good_spws,
                                    cell=cells[spwspec],
                                    imsize=imsizes[(field_intent[0], spwspec)],
                                    phasecenter=phasecenters[field_intent[0]],
                                    specmode=inputs.specmode,
                                    gridder=target_heuristics.gridder(field_intent[1], field_intent[0]),
                                    imagename=imagename,
                                    start=inputs.start,
                                    width=widths[(field_intent[0], spwspec)],
                                    nbin=nbin,
                                    nchan=nchans[(field_intent[0], spwspec)],
                                    robust=robust,
                                    uvrange=uvrange[(field_intent[0], spwspec)],
                                    bl_ratio=bl_ratio[(field_intent[0], spwspec)],
                                    uvtaper=uvtaper,
                                    stokes='I',
                                    heuristics=target_heuristics,
                                    vis=filtered_vislist,
                                    datacolumn=datacolumn,
                                    datatype_info=local_selected_datatype_info,
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
                                                                                for k, v in have_targets.items()
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
    ('PHASE', 'cont'): 'phase calibrator',
    ('BANDPASS', 'mfs'): 'bandpass calibrator',
    ('BANDPASS', 'cont'): 'bandpass calibrator',
    ('AMPLITUDE', 'mfs'): 'flux calibrator',
    ('AMPLITUDE', 'cont'): 'flux calibrator',
    ('POLARIZATION', 'mfs'): 'polarization calibrator',
    ('POLARIZATION', 'cont'): 'polarization calibrator',
    ('POLANGLE', 'mfs'): 'polarization calibrator',
    ('POLANGLE', 'cont'): 'polarization calibrator',
    ('POLLEAKAGE', 'mfs'): 'polarization calibrator',
    ('POLLEAKAGE', 'cont'): 'polarization calibrator',
    ('CHECK', 'mfs'): 'check source',
    ('CHECK', 'cont'): 'check source',
    ('TARGET', 'mfs'): 'target per-spw continuum',
    ('TARGET', 'cont'): 'target aggregate continuum',
    ('TARGET', 'cube'): 'target cube',
    ('TARGET', 'repBW'): 'representative bandwidth target cube'
}

_SIDEBAR_SUFFIX = {
    ('PHASE', 'mfs'): 'cals',
    ('PHASE', 'cont'): 'cals',
    ('BANDPASS', 'mfs'): 'cals',
    ('BANDPASS', 'cont'): 'cals',
    ('AMPLITUDE', 'mfs'): 'cals',
    ('AMPLITUDE', 'cont'): 'cals',
    ('POLARIZATION', 'mfs'): 'cals',
    ('POLARIZATION', 'cont'): 'cals',
    ('POLANGLE', 'mfs'): 'cals',
    ('POLANGLE', 'cont'): 'cals',
    ('POLLEAKAGE', 'mfs'): 'cals',
    ('POLLEAKAGE', 'cont'): 'cals',
    ('CHECK', 'mfs'): 'checksrc',
    ('CHECK', 'cont'): 'checksrc',
    ('TARGET', 'mfs'): 'mfs',
    ('TARGET', 'cont'): 'cont',
    ('TARGET', 'cube'): 'cube',
    ('TARGET', 'repBW'): 'cube_repBW'
}
