"""Inspection module for importdata."""

import os
import re
from typing import Any, Dict, Optional, Set, Tuple, Union

import numpy
import pipeline.infrastructure as infrastructure
from pipeline.domain.datatable import DataTableImpl
from pipeline.domain.measurementset import MeasurementSet
from pipeline.domain.singledish import MSReductionGroupDesc
from pipeline.hsd.heuristics.rasterscan import RasterScanHeuristicsResult, RasterScanHeuristicsFailure
from pipeline.hsd.tasks.common.inspection_util import (inspect_reduction_group,
                                                       set_beam_size)
# import pipeline.domain.singledish as singledish
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.launcher import Context

from ... import heuristics
from . import reader

LOG = infrastructure.get_logger(__name__)


class SDInspection(object):
    """Inspection class for hsd_importdata."""

    def __init__(self, context: Context, table_name: str, ms: Optional[MeasurementSet]=None, hm_rasterscan: str = 'time'):
        """Initialise SDInspection class.

        Args:
            context: pipeline context
            table_name: path to DataTable corresponding to ms
            ms: MeasurementSet object for inspection
            hm_rasterscan: Heuristics method for raster scan analysis (default: 'time')
        """
        self.context = context
        self.table_name = table_name
        self.ms = ms
        self.hm_rasterscan = hm_rasterscan.lower()

        if self.hm_rasterscan not in ['time', 'direction']:
            raise ValueError("hm_rasterscan must be either 'time' or 'direction'")

    def execute(self) -> Tuple[Dict[int, MSReductionGroupDesc], Dict[str, Union[str, Dict]]]:
        """Execute inspection process.

        This method calls execute method of reader.py.

        Return:
            reduction_group: dict of reduction group IDs (key) and MSReductionGroupDesc (value)
            org_directions: dict of org_direction
        """

        # per ms inspection: beam size and calibration strategy
        LOG.debug('inspect_beam_size')
        set_beam_size(self.ms)
        LOG.debug('inspect_calibration_strategy')
        self._inspect_calibration_strategy()

        # per ms inspection: reduction group
        LOG.debug('inspect_reduction_group')
        reduction_group = inspect_reduction_group(self.ms)

        # generate MS-based DataTable
        LOG.debug('register meta data to DataTable')
        table_name = self.table_name
        worker = reader.MetaDataReader(context=self.context, ms=self.ms, table_name=table_name)
        invalid_pointing_data, msglist = worker.generate_flagdict_for_invalid_pointing_data()
        LOG.debug('table_name=%s' % table_name)

        # worker.set_name(ms.name)
        org_directions = worker.execute() if os.path.exists(self.ms.name) else None

        datatable = worker.get_datatable()
        datatable.exportdata(minimal=False)

        appended_row = worker.appended_row
        nrow = datatable.nrow
        startrow = nrow - appended_row
        LOG.debug('%s rows are appended (total %s, startrow %s)' % (appended_row, nrow, startrow))

        # MS-wide inspection: data grouping
        LOG.debug('_group_data: ms = %s' % self.ms.basename)
        position_group_id = datatable.position_group_id
        time_group_id_small = datatable.time_group_id_small
        time_group_id_large = datatable.time_group_id_large
        grouping_result = self._group_data(datatable, position_group_id,
                                           time_group_id_small, time_group_id_large,
                                           startrow=startrow, nrow=appended_row)

        # merge grouping result with MS-based DataTable
        position_group = grouping_result['POSGRP']
        LOG.debug('len(position_group) = %s appended_row = %s' % (len(position_group), appended_row))
        LOG.debug('position_group = %s' % position_group)
        datatable.putcol('POSGRP', position_group[startrow:], startrow=startrow, nrow=appended_row)

        time_gap = grouping_result['TIMEGAP']

        def _g():
            yield 'POSGRP_REP', None
            yield 'POSGRP_LIST', None
            yield 'TIMEGAP_S', time_gap[0]
            yield 'TIMEGAP_L', time_gap[1]

        for key, value in _g():
            LOG.debug('key, value = %s, %s' % (key, value))
            if value is None:
                value = grouping_result[key]
                LOG.debug('updated value = %s' % value)
            else:
                mskey = self.ms.basename.replace('.', '_')
                value = {mskey: value}
            if datatable.haskeyword(key):
                LOG.debug('Updating %s' % key)
                LOG.debug('before: %s' % (datatable.getkeyword(key)))
                current_value = datatable.getkeyword(key)
                current_value.update(value)
                datatable.putkeyword(key, current_value)
            else:
                LOG.debug('Adding %s' % key)
                datatable.putkeyword(key, value)
            LOG.debug('after: %s' % (datatable.getkeyword(key)))
        # datatable.putkeyword('POSGRP_LIST', grouping_result['POSGRP_LIST'])
        time_group = grouping_result['TIMEGRP']
        time_group_list = grouping_result['TIMEGRP_LIST']
        # datatable.putkeyword('TIMEGAP_S', time_gap[0])
        # datatable.putkeyword('TIMEGAP_L', time_gap[1])
        for group_id, member_list in reduction_group.items():
            for member in member_list:
                ms = member.ms
                ant = member.antenna_id
                spw = member.spw_id
                field_id = member.field_id
                LOG.info('Adding time table for Reduction Group %s (ms %s antenna %s spw %s field_id %s)' %
                         (group_id, ms.basename, ant, spw, field_id))
                datatable.set_timetable(ant, spw, None, time_group_list[ant][spw][field_id],
                                        numpy.array(time_group[0]), numpy.array(time_group[1]),
                                        ms=ms.basename, field_id=field_id)
        datatable.exportdata(minimal=False)

        # PIPE-646 & PIPE-647
        # generate flag commands (only for ALMA data)
        is_alma = self.ms.antenna_array.name == 'ALMA'
        # TODO: heuristics to detect raster scan
        # apply pointing flag only for OTF raster
        is_raster = True
        timedomain_rsh_result = RasterScanHeuristicsResult(self.ms)
        if is_alma and is_raster:
            worker.generate_flagcmd(timedomain_rsh_result)

        directional_rsh_result = grouping_result['RASTERHEURISTICSRESULT']
        return reduction_group, org_directions, msglist, directional_rsh_result, timedomain_rsh_result

#     def _inspect_reduction_group(self):
#         reduction_group = {}
#         group_spw_names = {}
#         ms = self.ms
#         science_windows = ms.get_spectral_windows(science_windows_only=True)
#         assert hasattr(ms, 'calibration_strategy')
#         field_strategy = ms.calibration_strategy['field_strategy']
#         for field_id in field_strategy:
#             fields = ms.get_fields(field_id)
#             assert len(fields) == 1
#             field = fields[0]
#             field_name = field.name
#             for spw in science_windows:
#                 spw_name = spw.name
#                 nchan = spw.num_channels
#                 min_frequency = float(spw._min_frequency.value)
#                 max_frequency = float(spw._max_frequency.value)
#                 if len(spw_name) > 0:
#                     # grouping by name
#                     match = self.__find_match_by_name(spw_name, field_name, group_spw_names)
#                 else:
#                     # grouping by frequency range
#                     match = self.__find_match_by_coverage(nchan, min_frequency, max_frequency,
#                                                           reduction_group, fraction=0.99, field_name=field_name)
#                 if match == False:
#                     # add new group
#                     key = len(reduction_group)
#                     group_spw_names[key] = (spw_name, field_name)
#                     newgroup = singledish.MSReductionGroupDesc(spw_name=spw_name,
#                                                                min_frequency=min_frequency,
#                                                                max_frequency=max_frequency,
#                                                                nchan=nchan,
#                                                                field=field)
#                     reduction_group[key] = newgroup
#                 else:
#                     key = match
#                 ### Check existance of antenna, spw, field combination in MS
#                 ddid = ms.get_data_description(spw=spw)
#                 with casa_tools.TableReader(ms.name) as tb:
#                     subtb = tb.query('DATA_DESC_ID==%d && FIELD_ID==%d' % (ddid.id, field.id),
#                                      columns='ANTENNA1')
#                     valid_antid = set(subtb.getcol('ANTENNA1'))
#                     subtb.close()
# #                 myms = casa_tools.ms
# #                 valid_antid = myms.msseltoindex(vis=ms.name, spw=spw.id,
# #                                                 field=field_id, baseline='*&&&')['antenna1']
# #                 for antenna in ms.antennas:
# #                         reduction_group[key].add_member(ms, antenna.id, spw.id, field_id)
#                 for ant_id in valid_antid:
#                     reduction_group[key].add_member(ms, ant_id, spw.id, field_id)
#
#         return reduction_group

    def __select_data(self, datatable: DataTableImpl, startrow: int=0, nrow: int=-1) -> Tuple[Dict[int, Set[int]],
                                                                                              Dict[int, Set[int]],
                                                                                              Dict[int, Set[int]]]:
        """Inspect DataTable and return row IDs grouped by antenna, spw, and field.

        Args:
            datatable: DataTable to inspect
            startrow: the row ID in DataTable to start inspection
            nrow: number of rows to inspect
        Returns:
            Dict of antenna id/spectral window/field id
        """
        ms_name = self.ms.name
        filename = datatable.getkeyword('FILENAME')
        assert os.path.basename(ms_name) == os.path.basename(filename)

        by_antenna = {}
        by_spw = {}
        by_field = {}
        ant = datatable.getcol('ANTENNA', startrow=startrow, nrow=nrow)
        spw = datatable.getcol('IF', startrow=startrow, nrow=nrow)
        field_id = datatable.getcol('FIELD_ID', startrow=startrow, nrow=nrow)
        srctype = datatable.getcol('SRCTYPE', startrow=startrow, nrow=nrow)
        LOG.trace('ant=%s' % ant)
        LOG.trace('spw=%s' % spw)
        if nrow < 0:
            nrow = datatable.nrow - startrow
        LOG.debug('nrow = %s' % nrow)
        for i in range(nrow):
            if srctype[i] != 0:
                continue

            thisant = ant[i]
            thisspw = spw[i]
            thisfield = field_id[i]

            spw_domain = self.ms.spectral_windows[thisspw]
            #LOG.debug('spw.name=\'%s\''%(spw_domain.name))
            #LOG.debug('spw.intents=%s'%(spw_domain.intents))
            if (re.search(r'^WVR#', spw_domain.name) is not None or
                    re.search(r'#CH_AVG$', spw_domain.name) is not None or
                    'TARGET' not in spw_domain.intents):
                continue

            if thisant not in by_antenna:
                by_antenna[thisant] = set()
            by_antenna[thisant].add(i + startrow)

            if thisspw not in by_spw:
                by_spw[thisspw] = set()
            by_spw[thisspw].add(i + startrow)

            if thisfield not in by_field:
                by_field[thisfield] = set()
            by_field[thisfield].add(i + startrow)

        return by_antenna, by_spw, by_field

    def _group_data(self, datatable: DataTableImpl, position_group_id: int, time_group_id_small: int, time_group_id_large: int,
                    startrow: int=0, nrow: int=-1) -> Dict[str, Union[numpy.array, Dict[int, Any]]]:
        """Inspect DataTable and generate time and position groups.

        Args:
            datatable: DataTable to inspect
            position_group_id: position group id
            time_group_id_small: time group range id, min
            time_group_id_large: time group range id, max
            startrow: the row ID in DataTable to start inspection
            nrow: number of rows to inspect
        Returns:
            Dict of grouping
        """
        ms_ant_map = {}
        id_ant_map = {}
        ant_offset = 0
        ms = self.ms
        nant = len(ms.antennas)
        for a in range(nant):
            key = a + ant_offset
            ms_ant_map[key] = ms
            id_ant_map[key] = ms.antennas[a].id
        ant_offset += nant

        if nrow < 0:
            nrow = datatable.nrow - startrow
        by_antenna, by_spw, by_field = self.__select_data(datatable, startrow=startrow, nrow=nrow)
        LOG.trace('by_antenna=%s' % by_antenna)
        LOG.trace('by_spw=%s' % by_spw)
        LOG.trace('len(by_antenna)=%s len(by_spw)=%s' % (len(by_antenna), len(by_spw)))

        qa = casa_tools.quanta

        pos_heuristic2 = heuristics.GroupByPosition2()
        obs_heuristic2 = heuristics.ObservingPattern2()
        time_heuristic2 = heuristics.GroupByTime2()
        merge_heuristic2 = heuristics.MergeGapTables2()
        raster_heuristic = heuristics.RasterScanHeuristic()
        ra = numpy.asarray(datatable.getcol('RA'))
        dec = numpy.asarray(datatable.getcol('DEC'))
        offset_ra = numpy.asarray(datatable.getcol('OFS_RA'))
        offset_dec = numpy.asarray(datatable.getcol('OFS_DEC'))
#         row = numpy.asarray(datatable.getcol('ROW'))
        elapsed = numpy.asarray(datatable.getcol('ELAPSED'))
        beam = numpy.asarray(datatable.getcol('BEAM'))
        posgrp = numpy.zeros(datatable.nrow, dtype=numpy.int32) - 1
        timegrp = [numpy.zeros(datatable.nrow, dtype=numpy.int32) - 1,
                   numpy.zeros(datatable.nrow, dtype=numpy.int32) - 1]
        posgrp_rep = {}
        posgrp_list = {}
        timegrp_list = {}
        timegap = [{}, {}]
        last_ra = None
        last_dec = None
        last_time = None
        pos_dict = None
        pos_gap = None
        time_table = None
        time_gap = None
        merge_table = None
        merge_gap = None
        observing_pattern = {}

        posgrp_id = position_group_id
        LOG.debug('POSGRP: starting ID is %s' % posgrp_id)
        timegrp_id = [time_group_id_small, time_group_id_large]
        LOG.debug('TIMEGRP: starting ID is %s' % timegrp_id)

        ms = self.ms
        rasterscan_heuristics_result = RasterScanHeuristicsResult(ms)
        for ant, vant in by_antenna.items():
            LOG.debug('Start ant %s' % ant)
            pattern_dict = {}
            # ms = ms_ant_map[ant]
            observatory = ms.antenna_array.name
            _beam_size = ms.beam_sizes[id_ant_map[ant]]
            for i in (0, 1):
                timegap[i][ant] = {}
            posgrp_list[ant] = {}
            timegrp_list[ant] = {}
            for spw, vspw in by_spw.items():
                LOG.debug('Start spw %s' % spw)
                try:
                    spw_domain = ms.get_spectral_window(spw_id=spw)
                except KeyError:
                    continue
                pattern_dict[spw] = {}
                posgrp_list[ant][spw] = {}
                timegrp_list[ant][spw] = {}
                for i in (0, 1):
                    timegap[i][ant][spw] = {}
                # beam radius
                radius = qa.mul(_beam_size[spw], 0.5)
                r_combine = radius
                r_allowance = qa.mul(radius, 0.1)

                for field_id, vfield in by_field.items():
                    pattern_dict[spw][field_id] = None
                    for i in (0, 1):
                        timegap[i][ant][spw][field_id] = None
                    posgrp_list[ant][spw][field_id] = []
                    timegrp_list[ant][spw][field_id] = None

                    # for (pol,vpol) in self.by_pol.items():
                    id_list = numpy.fromiter(vant & vspw & vfield, dtype=numpy.int32)
                    if len(id_list) == 0:
                        continue
                    id_list.sort()
                    LOG.debug('id_list=%s' % id_list)
#                     row_sel = numpy.take(row, id_list)
                    ra_sel = numpy.take(ra, id_list)
                    dec_sel = numpy.take(dec, id_list)
                    time_sel = numpy.take(elapsed, id_list)
                    beam_sel = numpy.take(beam, id_list)

                    # new GroupByPosition with translation
                    update_pos = (last_ra is None or
                                  len(ra_sel) != len(last_ra) or
                                  len(dec_sel) != len(last_dec) or
                                  not (all(ra_sel == last_ra) and
                                       all(dec_sel == last_dec)))
                    if update_pos:
                        (pos_dict, pos_gap) = pos_heuristic2(ra_sel, dec_sel,
                                                             r_combine, r_allowance)
                        last_ra = ra_sel
                        last_dec = dec_sel

                        # ObsPatternAnalysis
                        # 2014/02/04 TN
                        # Temporary workaround for TP acceptance data issue
                        # Observing pattern is always 'RASTER' for ALMA
                        if observatory == 'ALMA':
                            pattern = 'RASTER'
                        else:
                            pattern = obs_heuristic2(pos_dict)

                    # prepare for Self.Datatable
                    # posgrp_list[ant][spw][pol] = []
                    LOG.debug('pos_dict = %s' % pos_dict)
                    LOG.debug('last_ra = %s last_dec = %s' % (last_ra, last_dec))
                    for k, v in pos_dict.items():
                        if v[0] == -1:
                            continue
                        LOG.debug('POSGRP_REP: add %s as a representative of group %s' % (id_list[v[0]], posgrp_id))
                        posgrp_rep[int(posgrp_id)] = int(id_list[v[0]])
                        for i in v:
                            _id = id_list[i]
                            posgrp[_id] = posgrp_id
                        posgrp_list[ant][spw][field_id].append(posgrp_id)
                        posgrp_id += 1
                    #

                    raster_heuristic_ok = False
                    if pattern == 'RASTER' and self.hm_rasterscan == 'direction':
                        LOG.info('Performing RasterScanHeuristics for raster scan pattern')
                        try:
                            sra_sel = numpy.take(offset_ra, id_list)
                            sdec_sel = numpy.take(offset_dec, id_list)
                            merge_table, merge_gap = raster_heuristic(sra_sel, sdec_sel)
                            raster_heuristic_ok = True
                        except RasterScanHeuristicsFailure as e:
                            LOG.debug('{} : EB:{}:{}'.format(e, ms.execblock_id, ms.antennas[ant].name) +
                                      'This often happens when pointing pattern deviates from regular raster. You may want to check the pointings in observation.')
                            rasterscan_heuristics_result.set_result_fail(ant, spw, field_id)
                            raster_heuristic_ok = False

                    if pattern != 'RASTER' or self.hm_rasterscan == 'time' or raster_heuristic_ok is False:
                        # new GroupByTime with translation
                        time_diff = time_sel[1:] - time_sel[:-1]
                        update_time = (last_time is None or
                                       len(time_diff) != len(last_time) or
                                       not all(time_diff == last_time))
                        if update_time:
                            (time_table, time_gap) = time_heuristic2(time_sel, time_diff)
                            last_time = time_diff

                        # new MergeGapTable with translation
                        if update_pos or update_time:
                            (merge_table, merge_gap) = merge_heuristic2(time_gap, time_table, pos_gap, beam_sel)

                    # prepare for Self.Datatable
                    keys = ['small', 'large']
                    grp_list = {}
                    for idx, key in enumerate(keys):
                        tmp = []
                        table = merge_table[idx]
                        for item in table:
                            for i in item:
                                timegrp[idx][id_list[i]] = timegrp_id[idx]
                            tmp.append(timegrp_id[idx])
                            timegrp_id[idx] = timegrp_id[idx] + 1
                        grp_list[key] = tmp
                        gap = merge_gap[idx]
                        gap_id = []
                        for v in gap:
                            gap_id.append(id_list[v])
                        timegap[idx][ant][spw][field_id] = gap_id
                    timegrp_list[ant][spw][field_id] = grp_list
                    ###

                    pattern_dict[spw][field_id] = pattern

            # register observing pattern to domain object
            # self[ant].pattern = pattern_dict
            observing_pattern[ant] = pattern_dict

        grouping_result = {}
        grouping_result['POSGRP'] = posgrp
        grouping_result['POSGRP_REP'] = posgrp_rep
        grouping_result['POSGRP_LIST'] = posgrp_list
        grouping_result['TIMEGRP_LIST'] = timegrp_list
        grouping_result['TIMEGRP'] = timegrp
        grouping_result['TIMEGAP'] = timegap
        grouping_result['RASTERHEURISTICSRESULT'] = rasterscan_heuristics_result
        # grouping_result['OBSERVING_PATTERN'] = observing_pattern

        ms.observing_pattern = observing_pattern

        return grouping_result

    def _inspect_calibration_strategy(self):
        """Inspect calibration strategy and set it to MeasurementSet."""
        ms = self.ms
        tsys_transfer = []
        calibration_type_heuristic = heuristics.CalibrationTypeHeuristics()
        spwmap_heuristic = heuristics.TsysSpwMapHeuristics()
        calibration_type = calibration_type_heuristic(ms.name)
        science_windows = ms.get_spectral_windows(science_windows_only=True)
        tsys_windows = [spw for spw in ms.spectral_windows
                        if 'ATMOSPHERE' in spw.intents and
                        re.search(r'(CH_AVG|SQLD|WVR)', spw.name) is None]
        LOG.debug('tsys_windows={spws}'.format(spws=[spw.id for spw in tsys_windows]))
        TOL = 1.0e-3
        for spwa in tsys_windows:
            if spwa in science_windows:
                # identical spw, skip (not necessary to transfer Tsys)
                continue
            fmina = float(spwa._min_frequency.value)
            fmaxa = float(spwa._max_frequency.value)
            for spwt in science_windows:
                if spwa == spwt:
                    # identical spw, skip (not necessary to transfer Tsys)
                    continue
                elif spwa.baseband != spwt.baseband:
                    # different baseband, skip
                    continue
                else:
                    fmint = float(spwt._min_frequency.value)
                    fmaxt = float(spwt._max_frequency.value)
                    dfmin = (fmint - fmina) / fmina
                    dfmax = (fmaxt - fmaxa) / fmaxa
                    LOG.trace('(fmina,fmaxa) = (%s, %s)' % (fmina, fmaxa))
                    LOG.trace('(fmint,fmaxt) = (%s, %s)' % (fmint, fmaxt))
                    LOG.trace('dfmin = %s, dfmax=%s, TOL = %s' % (dfmin, dfmax, TOL))
                    if dfmin >= -TOL and dfmax <= TOL:
                        tsys_transfer.append([spwa.id, spwt.id])
        do_tsys_transfer = len(tsys_transfer) > 0
        spwmap = spwmap_heuristic(ms, tsys_transfer)

        # field mapping (for multi-source EB)
        # {target field: reference field}
        target_fields = ms.get_fields(intent='TARGET')
        reference_fields = ms.get_fields(intent='REFERENCE')
        field_map = {}
        for target in target_fields:
            target_name = target.name
            LOG.debug('target name: \'%s\'' % target_name)

            if len(reference_fields) == 0:
                field_map[target.id] = target.id
                continue

            for reference in reference_fields:
                reference_name = reference.name
                LOG.debug('reference name: \'%s\'' % reference_name)
#                 tpattern = '^%s_[0-9]$'%(target_name)
#                 rpattern = '^%s_[0-9]$'%(reference_name)
                if target_name == reference_name:
                    field_map[target.id] = reference.id
                elif _check_offsource_fieldname_maching(reference_name, target_name):
                    field_map[target.id] = reference.id
        calibration_strategy = {'tsys': do_tsys_transfer,
                                'tsys_strategy': spwmap,
                                'calmode': calibration_type,
                                'field_strategy': field_map}
        ms.calibration_strategy = calibration_strategy

#     def _inspect_beam_size(self):
#         ms = self.ms
#         beam_size_heuristic = heuristics.SingleDishBeamSize()
#         beam_sizes = {}
#         for antenna in ms.antennas:
#             diameter = antenna.diameter
#             antenna_id = antenna.id
#             beam_size_for_antenna = {}
#             for spw in ms.spectral_windows:
#                 spw_id = spw.id
#                 center_frequency = float(spw.centre_frequency.convert_to(measures.FrequencyUnits.GIGAHERTZ).value)
#                 beam_size = beam_size_heuristic(diameter=diameter, frequency=center_frequency)
#                 beam_size_quantity = casa_tools.quanta.quantity(beam_size, 'arcsec')
#                 beam_size_for_antenna[spw_id] = beam_size_quantity
#             beam_sizes[antenna_id] = beam_size_for_antenna
#         ms.beam_sizes = beam_sizes

#     def __find_match_by_name(self, spw_name, field_name, group_names):
#         match = False
#         for group_key, names in group_names.items():
#             group_spw_name = names[0]
#             group_field_name = names[1]
#             if group_spw_name == '':
#                 raise RuntimeError("Got empty group spectral window name")
#             elif spw_name == group_spw_name and field_name == group_field_name:
#                 match = group_key
#                 break
#         return match
#
#     def __find_match_by_coverage(self, nchan, min_frequency, max_frequency, reduction_group, fraction=0.99,
#                                  field_name=None):
#         if fraction <= 0 or fraction > 1.0:
#             raise ValueError("overlap fraction should be between 0.0 and 1.0")
#         LOG.warning("Creating reduction group by frequency overlap. This may not be proper if observation dates extend"
#                  " over long period.")
#         match = False
#         for group_key, group_desc in reduction_group.items():
#             group_field_name = group_desc.field
#             if field_name is not None and group_field_name != field_name:
#                 continue
#             group_range = group_desc.frequency_range
#             group_nchan = group_desc.nchan
#             overlap = max(0.0, min(group_range[1], max_frequency) - max(group_range[0], min_frequency))
#             width = max(group_range[1], max_frequency) - min(group_range[0], min_frequency)
#             coverage = overlap/width
#             if nchan == group_nchan and coverage >= fraction:
#                 match = group_key
#                 break
#         return match


def _check_offsource_fieldname_maching(name1: str, name2: str) -> bool:
    """
    Return True if two fieldnames follow the naming rule of OFF SOURCE.

    if name1 is 'M100', then name2 should be 'M100_OFF_[ID]' (or 'M100_[ID]' in old pattern)
    Note the method returns False for the exact match, i.e., name1 == name2.

    Args:
        name1: matching string 1
        name2: matching string 2
    Returns:
        boolean matched them
    """
    trim_name = lambda s: s[1:-1] if s.startswith('"') and s.endswith('"') else s
    name1 = trim_name(name1)
    name2 = trim_name(name2)
    pos1 = name1.find(name2)
    pos2 = name2.find(name1)
    # extract suffix part of field name
    if pos1 == 0 and len(name1) > len(name2):
        # name1 looks like name2 + suffix, try pattern matching for suffix
        suffix = name1[len(name2):]
    elif pos2 == 0 and len(name1) < len(name2):
        # name2 looks like name1 + suffix, try pattern matching for suffix
        suffix = name2[len(name1):]
    else:  # field names do not match
        return False
    # Check if the field name matches to pattern
    off_pattern = '^_OFF_[0-9]+$'
    old_pattern = '^_[0-9]+$'  # old and unofficial field name pattern
    for pattern in (off_pattern, old_pattern):
        # is_match = lambda s: re.match(pattern, s) is not None
        if re.match(pattern, suffix) is not None:
            if pattern == old_pattern:
                LOG.warning("OFF source field identified using old field name heuristics. You may want to review field"
                            " mapping carefully.")
            return True

    return False
