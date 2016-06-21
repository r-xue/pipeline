from __future__ import absolute_import

import os
import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.sdfilenamer as filenamer
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils
from pipeline.hif.heuristics import fieldnames
from . import gridding
from . import weighting
from . import worker
from . import sdcombine
from .. import common
from ..baseline import baseline

LOG = infrastructure.get_logger(__name__)

class SDImagingInputs(basetask.StandardInputs): #common.SingleDishInputs
    """
    Inputs for imaging
    """
    @basetask.log_equivalent_CASA_call
    def __init__(self, context, mode=None, infiles=None,
                 field=None, spw=None):
        self._init_properties(vars())
        self.vis = infiles
        self.infiles = self.vis
#         self._to_list(['infiles'])
#         for key in ['spw']:
#             val = getattr(self, key)
#             if val is None or (val[0] == '[' and val[-1] == ']'):
#                 self._to_list([key])
        if self.mode is None: self.mode='line'
#         self._to_list(['infiles', 'iflist', 'pollist'])

    @property
    def msid_list(self):
        """
        Returns MS index in context observing run specified as infiles.
        """
        ms_names = [ms.name for ms in self.context.observing_run.measurement_sets]
        return map(ms_names.index, map(os.path.abspath, self.infiles))

    @property
    def antenna(self):
        return ''
#         if self._antenna is not None:
#             return self._antenna
#   
#         antennas = self.ms.get_antenna(self._antenna)
#         return ','.join([str(a.id) for a in antennas])
#   
#     @antenna.setter
#     def antenna(self, value):
#         if value is None:
#             # use all antenna by default
#             value = ''
#         self._antenna = value

    @property
    def field(self):
        if not callable(self._field):
            return self._field
        # filters field with intents in self.intent
        fields = set()
        for idx in self.msid_list:
            # This assumes the same fields in all MSes
            msobj = self.context.observing_run.measurement_sets[idx]
            # this will give something like '0542+3243,0343+242'
            intent_fields = self._field(msobj, self.intent)
            fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    @field.setter
    def field(self, value):
        if value is None:
            value = fieldnames.IntentFieldnames()
        self._field = value

    @property
    def intent(self):
        return "TARGET"

    @property
    def spw(self):
        if self._spw is not None:
            return self._spw
        # filters science spws by default (assumes the same spw setting for all MSes)
        msobj = self.context.observing_run.measurement_sets[self.msid_list[0]]
        science_spws = msobj.get_spectral_windows(self._spw, with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @spw.setter
    def spw(self, value):
        self._spw = value


class SDImagingResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(SDImagingResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(SDImagingResults, self).merge_with_context(context)
        LOG.todo('need to decide what is done in SDImagingResults.merge_with_context')
        
        if self.outcome.has_key('export_results'):
            self.outcome['export_results'].merge_with_context(context)

    def _outcome_name(self):
        # return [image.imagename for image in self.outcome]
        return self.outcome['image'].imagename

class SDImaging(common.SingleDishTaskTemplate):
    Inputs = SDImagingInputs

    @common.datatable_setter
    def prepare(self):
        context = self.inputs.context
        reduction_group = context.observing_run.ms_reduction_group
        infiles = self.inputs.infiles
        # list of ms to process
        ms_list = [context.observing_run.measurement_sets[idx] for idx in self.inputs.msid_list]
        ms_names = [msobj.name for msobj in ms_list]
        args = self.inputs.to_casa_args()
#         antennalist = self.inputs.antennalist
        imagemode = self.inputs.mode.upper()
        logrecords = []
         
        # task returns ResultsList
        results = basetask.ResultsList()
        # search results and retrieve edge parameter from the most
        # recent SDBaselineResults if it exists
        getresult = lambda r : r.read() if hasattr(r, 'read') else r
        registered_results = [getresult(r) for r in context.results]
        baseline_stage = -1
        for stage in xrange(len(registered_results) - 1, -1, -1):
            if isinstance(registered_results[stage], baseline.SDBaselineResults):
                baseline_stage = stage
        if baseline_stage > 0:
            edge = list(registered_results[baseline_stage].outcome['edge'])
            LOG.info('Retrieved edge information from SDBaselineResults: %s' % (edge))
        else:
            LOG.info('No SDBaselineResults available. Set edge as [0,0]')
            edge = [0, 0]
         
        # loop over reduction group (spw and source combination)
        for (group_id, group_desc) in reduction_group.items():
            LOG.debug('Processing Reduction Group %s'%(group_id))
            LOG.debug('Group Summary:')
            for m in group_desc:
                LOG.debug('\t%s: Antenna %s Spw %s Field %s'%(os.path.basename(m.ms.work_data),
                                                              m.ms.antennas[m.antenna].name,
                                                              m.spw, m.ms.fields[m.field_id].name))
            #member_list = list(common.get_valid_members(group_desc, file_index, iflist))
#             pols_list = list(common.pol_filter(group_desc, self.inputs.get_pollist))
            # which group in group_desc list should be processed
            member_list = list(common.get_valid_ms_members(group_desc, ms_names, self.inputs.antenna, args['spw'], args['field']))
            LOG.debug('group %s: member_list=%s'%(group_id, member_list))
             
            # skip this group if valid member list is empty
            if len(member_list) == 0:
                LOG.info('Skip reduction group %d'%(group_id))
                continue
 
            member_list.sort() #list of group_desc IDs to image
            antenna_list = [group_desc[i].antenna for i in member_list]
            spwid_list = [group_desc[i].spw for i in member_list]
            ms_list = [group_desc[i].ms for i in member_list]
            fieldid_list = [group_desc[i].field_id for i in member_list]
             
            LOG.debug('Members to be processed:')
            for i in xrange(len(member_list)):
                LOG.debug('\t%s: Antenna %s Spw %s Field %s'%(ms_list[i].work_data, antenna_list[i], spwid_list[i], fieldid_list[i]))
 
            #continue
 
            # image is created per antenna (science) or per asdm and antenna (ampcal)
            image_group = {}
            for (msobj, ant, spwid, fieldid) in zip(ms_list, antenna_list, spwid_list, fieldid_list):
                field_name = msobj.fields[fieldid].name
                identifier = field_name
                antenna = msobj.antennas[ant].name
                identifier += ('.'+antenna)
                # create image per asdm and antenna for ampcal
                if imagemode=='AMPCAL':
                    asdm_name = common.asdm_name_from_ms(msobj)
                    identifier += ('.'+asdm_name)
                if identifier in image_group.keys():
                    image_group[identifier].append([msobj, ant, spwid, fieldid])
                else:
                    image_group[identifier] = [[msobj, ant, spwid, fieldid]]
            LOG.info('image_group=%s' % (image_group))
 
#             # loop over antennas
#             combined_infiles = []
#             combined_antids = []
#             combined_fieldids = []
#             combined_spws = []
#             tocombine_images = []
#  
#             srctype = None
#             coord_set = False
#             for (name, _members) in image_group.items():
#                 msobjs =  map(lambda x: x[0], _members)
#                 antids = map(lambda x: x[1], _members)
#                 spwids = map(lambda x: x[2], _members)
#                 fieldids = map(lambda x: x[3], _members)
#                  
#                 # reference data is first MS
#                 ref_ms = msobjs[0]
#                 ant_name = ref_ms.antennas[antids[0]].name
#                 # for ampcal
#                 asdm = None
#                 if imagemode=='AMPCAL':
#                     asdm = common.asdm_name_from_ms(ref_ms)
#  
# #                 # SRCTYPE for ON-SOURCE
# #                 srctype = st.calibration_strategy['srctype']
#  
#                 # source name
# #                 target_sources = [v for v in st.source.values() \
# #                                   if 'TARGET' in v.intents]
#                 source_name =  group_desc.field_name.replace(' ', '_')
#                  
#                 # filenames for gridding
#                 infiles = [ms.work_data for ms in msobjs]
#  
# #                 LOG.debug('filenames=%s' % (filenames))
#                 LOG.debug('infiles=%s' % (infiles))
#                  
#                 # image name
#                 namer = filenamer.Image()
#                 namer.casa_image()
#                 namer.source(source_name)
#                 namer.antenna_name(ant_name)
#                 namer.asdm(asdm)
#                 namer.spectral_window(spwids[0])
# #                 namer.polarization('I' if imagemode=='AMPCAL' else common.polstring(net_pols))
#                 namer.polarization('I')
#                 imagename = namer.get_filename()
#                 
#                 # Step 1.
#                 # Initialize weight column based on baseline RMS.
#                 LOG.info('Set weights based on baseline RMS')
#                 for i in xrange(len(msobjs)):
#                     msobj = msobjs[i]
#                     antid = antids[i]
#                     spwid = spwids[i]
#                     fieldid = fieldids[i]
#                     LOG.debug('Setting weight for %s Antenna %s Spw %s Field'%(msobj.name, antid,spwid, fieldid))
#                     original_ms = msobj.name
#                     work_ms = msobj.work_data
#                     ###???
#                     spwtype = msobj.spectral_windows[19].type
#                     weighting_inputs = weighting.WeightMS.Inputs(context, infile=original_ms, 
#                                                                  outfile=work_ms, antenna=antid,
#                                                                  spwid=spwid, field=fieldid, spwtype=spwtype, 
#                                                                  onsourceid=srctype)
#                     weighting_task = weighting.WeightMS(weighting_inputs)
#                     weighting_result = self._executor.execute(weighting_task, merge=True)
#                     logrecords.extend(weighting_result.logrecords)
#  
#                 # Step 2.
#                 # Imaging
#                 # Image per antenna, source
#                 LOG.info('Imaging Source %s, Ant %s' % (source_name, ant_name))
#                 # map coordinate (use identical map coordinate per spw)
#                 if not coord_set:
#                     phasecenter, cellx, celly, nx, ny = worker.ALMAImageCoordinateUtil(context, self.datatable, infiles, antids, spwids, fieldids, srctype)
#                     coord_set = True
#  
#                 # register data for combining
#                 combined_infiles.extend(infiles)
#                 combined_antids.append(antids)
#                 combined_fieldids.extend(fieldids)
#                 combined_spws.extend(spwids)
#                  
#                 imager_inputs = worker.SDImagingWorker.Inputs(context, infiles=infiles, 
#                                                               outfile=imagename, mode=imagemode,
#                                                               antids=antids, spwids=spwids,
#                                                               fieldids=fieldids,
#                                                               onsourceid=srctype, edge=edge,
#                                                               phasecenter=phasecenter,
#                                                               cellx=cellx, celly=celly,
#                                                               nx=nx, ny=ny)
#                 imager_task = worker.SDImagingWorker(imager_inputs)
#                 imager_result = self._executor.execute(imager_task, merge=True)
#                  
#                 if imager_result.outcome is not None:
#                     # Imaging was successful, proceed following steps
#                     logrecords.extend(imager_result.logrecords)
#  
#                     # add image list to combine
#                     if os.path.exists(imagename) and os.path.exists(imagename+'.weight'):
#                         tocombine_images.append(imagename)
#  
#                     # Additional Step.
#                     # Make grid_table and put rms and valid spectral number array 
#                     # to the outcome.
#                     # The rms and number of valid spectra is used to create RMS maps.
#                     validsps = []
#                     rmss = []
# #                     if imagemode != 'AMPCAL':
#                     LOG.info('Additional Step. Make grid_table')
#                     with casatools.ImageReader(imager_result.outcome) as ia:
#                         cs = ia.coordsys()
#                         dircoords = [i for i in xrange(cs.naxes())
#                                      if cs.axiscoordinatetypes()[i] == 'Direction']
#                         nx = ia.shape()[dircoords[0]]
#                         ny = ia.shape()[dircoords[1]]
# 
#                     observing_pattern = ms.observing_pattern[andids[0][spwids[0]][fieldids[0]]
#                     grid_task_class = gridding.gridding_factory(observing_pattern)
#                     grid_tables = []
#                     grid_input_dict = {}
#                     for (msobj, antid, spwid, fieldid) in _members:
#                         msname = msobj.name # Use parent ms
#                         ddobj = msobj.get_data_description(spw=spwid)
#                         pol = map(ddobj.get_polarization_id, map(ddobj.get_polarization_label, xrange(ddobj.num_polarizations)))
#                         for p in pol:
#                             if not grid_input_dict.has_key(p):
#                                 grid_input_dict[p] = [[msname], [antid], [spwid], [fieldid]]
#                             else:
#                                 grid_input_dict[p][0].append(msname)
#                                 grid_input_dict[p][1].append(antid)
#                                 grid_input_dict[p][2].append(spwid)
#                                 grid_input_dict[p][3].append(fieldid)
#  
#                     for (pol,member) in grid_input_dict.items():
#                         _mses = member[0]
#                         _antids = member[1]
#                         _spwids = member[2]
#                         _fieldids = member[3]
#                         _pols = [[pol] for i in xrange(len(_mses))]
#                         gridding_inputs = grid_task_class.Inputs(context, msname=_mses, 
#                                                                  antennaid=_antids, 
#                                                                  spwid=_spwids,
#                                                                  fieldids=_fieldids,
#                                                                  polid=_pols,
#                                                                  nx=nx, ny=ny)
#                         gridding_task = grid_task_class(gridding_inputs)
#                         gridding_result = self._executor.execute(gridding_task, merge=True)
#                         grid_tables.append(gridding_result.outcome)
#                         logrecords.extend(gridding_result.logrecords)
#                     for i in xrange(len(grid_input_dict)):
#                         validsps.append([r[6] for r in grid_tables[i]])
#                         rmss.append([r[8] for r in grid_tables[i]])
#  
#                     image_item = imagelibrary.ImageItem(imagename=imagename,
#                                                         sourcename=source_name,
#                                                         spwlist=spwids,
#                                                         specmode='cube',
#                                                         sourcetype='TARGET')
#                     image_item.antenna = name
#                     outcome = {}
#                     outcome['image'] = image_item
#                     outcome['imagemode'] = imagemode
#                     outcome['validsp'] = validsps
#                     outcome['rms'] = rmss
#                     outcome['edge'] = edge
#                     outcome['reduction_group_id'] = group_id
#                     outcome['file_index'] = map(common.get_parent_ms_idx, infiles)
#                     outcome['assoc_spws'] = spwids
#                     outcome['assoc_pols'] = pols
#                     if imagemode == 'AMPCAL':
#                         if len(infiles)==1 and (asdm not in ['', None]): outcome['vis'] = asdm
#                         # to register exported_ms to each scantable instance
#                         outcome['export_results'] = export_results
#  
#                     result = SDImagingResults(task=self.__class__,
#                                               success=True,
#                                               outcome=outcome)
#                     result.task = self.__class__
#  
#                     result.stage_number = self.inputs.context.task_counter 
#                                                  
#                     results.append(result)
#                      
#             # derive Jy/K factor and store it in a file
#             if imagemode == 'AMPCAL':
#                 LOG.info("Skipping combined image for the amplitude calibrator.")
#                 continue
#  
#             # Make combined image
#             # reference MS
#             ms = context.observing_run.measurement_sets[combined_infiles[0]]
#              
#             # image name
#             namer = filenamer.Image()
#             namer.casa_image()
#             namer.source(source_name)
#             namer.spectral_window(combined_spws[0])
#             namer.polarization('I')
#             imagename = namer.get_filename()
#  
#             # Step 3.
#             # Imaging of all antennas
#             LOG.info('Combine images of Source %s' % (source_name))
#             if False:
#                 imager_inputs = worker.SDImagingWorker.Inputs(context, infiles=combined_infiles, 
#                                                               outfile=imagename, mode=imagemode,
#                                                               antids=combined_antids,
#                                                               spwids=combined_spws,
#                                                               fieldids=combined_fieldids,
#                                                               onsourceid=srctype, edge=edge,
#                                                               phasecenter=phasecenter,
#                                                               cellx=cellx, celly=celly,
#                                                               nx=nx, ny=ny)
#                 imager_task = worker.SDImagingWorker(imager_inputs)
#                 imager_result = self._executor.execute(imager_task, merge=True)
#             else:
#                 combine_inputs = sdcombine.SDImageCombineInputs(context, inimages=tocombine_images,
#                                                                 outfile=imagename)
#                 combine_task = sdcombine.SDImageCombine(combine_inputs)
#                 imager_result = self._executor.execute(combine_task, merge=True)
#              
#             if imager_result.outcome is not None:
#                 # Imaging was successful, proceed following steps
#                 logrecords.extend(imager_result.logrecords)
#      
#                 # Additional Step.
#                 # Make grid_table and put rms and valid spectral number array 
#                 # to the outcome
#                 # The rms and number of valid spectra is used to create RMS maps
#                 LOG.info('Additional Step. Make grid_table')
#                 with casatools.ImageReader(imager_result.outcome) as ia:
#                     cs = ia.coordsys()
#                     dircoords = [i for i in xrange(cs.naxes())
#                                  if cs.axiscoordinatetypes()[i] == 'Direction']
#                     nx = ia.shape()[dircoords[0]]
#                     ny = ia.shape()[dircoords[1]]
#                 validsps = []
#                 rmss = []
#                 observing_pattern =  ms.observing_pattern[andids[0][spwids[0]][fieldids[0]]
#                 grid_task_class = gridding.gridding_factory(observing_pattern)
#                 grid_tables = []
#                 grid_input_dict = {}
#                 for (msname, antid, spwid, fieldid) in zip(combined_infiles,combined_antids,combined_spws,combined_fieldids):
#                     msobj = context.observing_run.get_ms(name=common.get_parent_ms_name(context,name)) # Use parent ms
#                     ddobj = msobj.get_data_description(spw=spwid)
#                     pol = map(ddobj.get_polarization_id, map(ddobj.get_polarization_label, xrange(ddobj.num_polarizations)))
#                     for p in pol:
#                         if not grid_input_dict.has_key(p):
#                             grid_input_dict[p] = [[msname], [antid], [spwid], [fieldid]]
#                         else:
#                             grid_input_dict[p][0].append(msname)
#                             grid_input_dict[p][1].append(antid)
#                             grid_input_dict[p][2].append(spwid)
#                             grid_input_dict[p][3].append(fieldid)
#  
#                 for (pol,member) in grid_input_dict.items():
#                     _mses = member[0]
#                     _antids = member[1]
#                     _spwids = member[2]
#                     _fieldids = member[3]
#                     _pols = [[pol] for i in xrange(len(_mses))]
#                     gridding_inputs = grid_task_class.Inputs(context, msname=_mses, 
#                                                              antennaid=_antids, 
#                                                              spwid=_spwids,
#                                                              fieldids=_fieldids,
#                                                              polid=_pols,
#                                                              nx=nx, ny=ny)
#                     gridding_task = grid_task_class(gridding_inputs)
#                     gridding_result = self._executor.execute(gridding_task, merge=True)
#                     logrecords.extend(gridding_result.logrecords)
#                     grid_tables.append(gridding_result.outcome)
#                 for i in xrange(len(grid_input_dict)):
#                     validsps.append([r[6] for r in grid_tables[i]])
#                     rmss.append([r[8] for r in grid_tables[i]])
#                  
#                 image_item = imagelibrary.ImageItem(imagename=imagename,
#                                                     sourcename=source_name,
#                                                     spwlist=combined_spws,
#                                                     specmode='cube',
#                                                     sourcetype='TARGET')
#                 image_item.antenna = 'COMBINED'
#                 outcome = {}
#                 outcome['image'] = image_item
#                 outcome['imagemode'] = imagemode
#                 outcome['validsp'] = validsps
#                 outcome['rms'] = rmss
#                 outcome['edge'] = edge
#                 outcome['reduction_group_id'] = group_id
#                 outcome['file_index'] = combined_indices
#                 outcome['assoc_spws'] = combined_spws
#                  
#                 # to register exported_ms to each scantable instance
#                 outcome['export_results'] = export_results
#                 outcome['reduction_group_id'] = group_id
#                 result = SDImagingResults(task=self.__class__,
#                                           success=True,
#                                           outcome=outcome)
#                 result.task = self.__class__
#  
#                 result.stage_number = self.inputs.context.task_counter 
#                      
#                 results.append(result)
#                                      
#         LOG.todo('logrecords for SDImagingResults must be handled properly')
#         # only add logrecords to first result
#         if len(results) > 0:
#             results[0].logrecords = logrecords
#             for r in results[1:]:   
#                 r.logrecords = []

        return results
    
    def analyse(self, result):
        return result
    
