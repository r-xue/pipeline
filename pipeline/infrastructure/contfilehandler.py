#!/usr/bin/env python
"""
Class to handle continuum frequency range files.

The text files contain ranges per source and spw using CASA syntax.
The keyword "NONE" can be written in case of non-detection of a continuum
frequency range.
"""

import re

import numpy as np
import collections

from . import casa_tools
from . import utils
from . import logging

from typing import Union, Tuple, List, Dict, Any, Generator

LOG = logging.get_logger(__name__)


class ContFileHandler(object):

    def __init__(self, filename, warn_nonexist=False):
        self.filename = filename
        self.p = re.compile(r'([\d.]*)(~)([\d.]*)(\D*)')
        self.cont_ranges = self.read(warn_nonexist=warn_nonexist)

    def read(self, skip_none=False, warn_nonexist=False):

        if self.filename is None:
            return {}

        cont_ranges = {'fields': {}, 'version': 2}

        try:
            cont_region_data = [item.replace('\n', '') for item in open(self.filename, 'r').readlines() if item != '\n']
        except:
            cont_region_data = []
            if warn_nonexist:
                LOG.warning('Could not read file %s. Using empty selection.' % self.filename)

        for item in cont_region_data:
            try:
                if ((item.find('SpectralWindow:') == -1) and
                        (item.find('SPW') == -1) and
                        (item.find('~') == -1) and
                        (item != 'NONE') and
                        (item != 'ALL')):
                    if item.find('Field:') == 0:
                        field_name = item.split('Field:')[1].strip()
                    else:
                        field_name = item
                    field_name = utils.dequote(field_name).strip()
                    if field_name != '':
                        cont_ranges['fields'][field_name] = {}
                elif item.find('SPW') == 0:
                    cont_ranges['version'] = 1
                    spw_id = item.split('SPW')[1].strip()
                    cont_ranges['fields'][field_name][spw_id] = []
                elif item.find('SpectralWindow:') == 0:
                    cont_ranges['version'] = 2
                    spw_id = item.split('SpectralWindow:')[1].strip()
                    cont_ranges['fields'][field_name][spw_id] = []
                elif item == 'ALL':
                    cont_ranges['fields'][field_name][spw_id].append('ALL')
                elif (item == 'NONE') and not skip_none:
                    cont_ranges['fields'][field_name][spw_id].append('NONE')
                else:
                    cont_regions = self.p.findall(item.replace(';', ''))
                    for cont_region in cont_regions:
                        if cont_ranges['version'] == 1:
                            unit = cont_region[3]
                            refer = 'TOPO'
                        elif cont_ranges['version'] == 2:
                            unit, refer = cont_region[3].split()
                        fLow = casa_tools.quanta.convert('%s%s' % (cont_region[0], unit), 'GHz')['value']
                        fHigh = casa_tools.quanta.convert('%s%s' % (cont_region[2], unit), 'GHz')['value']
                        cont_ranges['fields'][field_name][spw_id].append({'range': (fLow, fHigh), 'refer': refer})
            except:
                pass

        for fkey in cont_ranges['fields']:
            for skey in cont_ranges['fields'][fkey]:
                if cont_ranges['fields'][fkey][skey] == []:
                    cont_ranges['fields'][fkey][skey] = ['NONE']

        return cont_ranges

    def write(self, cont_ranges=None):

        if self.filename is None:
            return

        if cont_ranges is None:
            cont_ranges = self.cont_ranges

        fd = open(self.filename, 'w+')
        if cont_ranges != {}:
            for field_name in cont_ranges['fields']:
                if cont_ranges['version'] == 1:
                    fd.write('%s\n\n' % (field_name.replace('"', '')))
                elif cont_ranges['version'] == 2:
                    fd.write('Field: %s\n\n' % (field_name.replace('"', '')))
                for spw_id in cont_ranges['fields'][field_name]:
                    if cont_ranges['version'] == 1:
                        fd.write('SPW%s\n' % spw_id)
                    elif cont_ranges['version'] == 2:
                        fd.write('SpectralWindow: %s\n' % spw_id)
                    if cont_ranges['fields'][field_name][spw_id] in ([], ['NONE']):
                        fd.write('NONE\n')
                    elif cont_ranges['fields'][field_name][spw_id] == ['ALL']:
                        fd.write('ALL\n')
                    else:
                        for freq_range in cont_ranges['fields'][field_name][spw_id]:
                            if freq_range == 'NONE':
                                fd.write('NONE\n')
                            elif freq_range == 'ALL':
                                fd.write('ALL\n')
                            else:
                                if cont_ranges['version'] == 1:
                                    fd.write('%.10f~%.10fGHz\n' % (float(freq_range['range'][0]), float(freq_range['range'][1])))
                                elif cont_ranges['version'] == 2:
                                    fd.write('%.10f~%.10fGHz %s\n' % (float(freq_range['range'][0]), float(freq_range['range'][1]),
                                                                freq_range['refer']))
                    fd.write('\n')
        fd.close()

    def get_merged_selection(self, field_name, spw_id, cont_ranges=None):

        field_name = str(field_name)
        spw_id = str(spw_id)

        if cont_ranges is None:
            cont_ranges = self.cont_ranges

        all_continuum = False
        if field_name in cont_ranges['fields']:
            if spw_id in cont_ranges['fields'][field_name]:
                if cont_ranges['fields'][field_name][spw_id] not in (['ALL'], [], ['NONE']):
                    merged_cont_ranges = utils.merge_ranges(
                        [cont_range['range'] for cont_range in cont_ranges['fields'][field_name][spw_id] if isinstance(cont_range, dict)])
                    cont_ranges_spwsel = ';'.join(['%.10f~%.10fGHz' % (float(spw_sel_interval[0]), float(spw_sel_interval[1]))
                                                   for spw_sel_interval in merged_cont_ranges])
                    refers = np.array([cont_range['refer']
                                       for cont_range in cont_ranges['fields'][field_name][spw_id] if isinstance(cont_range, dict)])
                    if (refers == 'TOPO').all():
                        refer = 'TOPO'
                    elif (refers == 'LSRK').all():
                        refer = 'LSRK'
                    elif (refers == 'SOURCE').all():
                        refer = 'SOURCE'
                    else:
                        refer = 'UNDEFINED'
                    cont_ranges_spwsel = '%s %s' % (cont_ranges_spwsel, refer)
                    if 'ALL' in cont_ranges['fields'][field_name][spw_id]:
                        all_continuum = True
                elif cont_ranges['fields'][field_name][spw_id] == ['ALL']:
                    cont_ranges_spwsel = 'ALL'
                    all_continuum = True
                else:
                    cont_ranges_spwsel = 'NONE'
            else:
                cont_ranges_spwsel = ''
        else:
            cont_ranges_spwsel = ''

        return cont_ranges_spwsel, all_continuum

    def to_topo(self, selection: str, msnames: List[str], fields: List[Union[int, str]], spw_id: Union[int, str], observing_run: Any, ctrim: int = 0, ctrim_nchan: int = -1) -> Tuple[List[str], List[str], Dict]:

        frame_freq_selection, refer = selection.split()
        if refer not in ('LSRK', 'SOURCE'):
            LOG.error('Original reference frame must be LSRK or SOURCE.')
            raise Exception('Original reference frame must be LSRK or SOURCE.')

        if len(msnames) != len(fields):
            LOG.error('MS names and fields lists must match in length.')
            raise Exception('MS names and fields lists must match in length.')

        spw_id = int(spw_id)

        qaTool = casa_tools.quanta
        suTool = casa_tools.synthesisutils

        freq_ranges = []
        aggregate_frame_bw = '0.0GHz'
        cont_regions = self.p.findall(frame_freq_selection.replace(';', ''))
        for cont_region in cont_regions:
            fLow = qaTool.convert('%s%s' % (cont_region[0], cont_region[3]), 'Hz')['value']
            fHigh = qaTool.convert('%s%s' % (cont_region[2], cont_region[3]), 'Hz')['value']
            freq_ranges.append((fLow, fHigh))
            delta_f = qaTool.sub('%sHz' % fHigh, '%sHz' % fLow)
            aggregate_frame_bw = qaTool.add(aggregate_frame_bw, delta_f)

        topo_chan_selections = []
        topo_freq_selections = []
        for i in range(len(msnames)):
            msname = msnames[i]
            real_spw_id = observing_run.virtual2real_spw_id(spw_id, observing_run.get_ms(msname))
            field = int(fields[i])
            topo_chan_selection = []
            topo_freq_selection = []
            try:
                if field != -1:
                    for freq_range in freq_ranges:
                        if refer == 'LSRK':
                            result = suTool.advisechansel(msname=msname, fieldid=field, spwselection=str(real_spw_id),
                                                          freqstart=freq_range[0], freqend=freq_range[1], freqstep=100.,
                                                          freqframe='LSRK')
                        else:
                            result = suTool.advisechansel(msname=msname, fieldid=field, spwselection=str(real_spw_id),
                                                          freqstart=freq_range[0], freqend=freq_range[1], freqstep=100.,
                                                          freqframe='SOURCE', ephemtable='TRACKFIELD')
                        suTool.done()
                        spw_index = result['spw'].tolist().index(real_spw_id)
                        start = result['start'][spw_index]
                        stop = start + result['nchan'][spw_index] - 1
                        # Optionally skip edge channels
                        if ctrim_nchan != -1:
                            start = start if (start >= ctrim) else ctrim
                            stop = stop if (stop < (ctrim_nchan-ctrim)) else ctrim_nchan-ctrim-1
                        if stop >= start:
                            topo_chan_selection.append((start, stop))
                            result = suTool.advisechansel(msname=msname, fieldid=field,
                                                          spwselection='%d:%d~%d' % (real_spw_id, start, stop),
                                                          freqframe='TOPO', getfreqrange=True)
                            fLow = float(qaTool.getvalue(qaTool.convert(result['freqstart'], 'GHz')))
                            fHigh = float(qaTool.getvalue(qaTool.convert(result['freqend'], 'GHz')))
                            topo_freq_selection.append((fLow, fHigh))
            except Exception as e:
                LOG.info('Cannot calculate TOPO range for MS %s Field %s SPW %s. Exception: %s' % (msname, field, real_spw_id, e))

            topo_chan_selections.append(';'.join('%d~%d' % (item[0], item[1]) for item in topo_chan_selection))
            topo_freq_selections.append('%s TOPO' % (';'.join('%.10f~%.10fGHz' %
                                                              (float(item[0]), float(item[1])) for item in topo_freq_selection)))

        return topo_freq_selections, topo_chan_selections, aggregate_frame_bw


def contfile_to_spwsel(vis, context, contfile='cont.dat', use_realspw=True):
    """Translate continuum ranges specified in contfile to frequency selection string.

    By default (use_realspw=True), the frequency selection string is in real SPWs. The return is 
    a dictionary with field names with keys and spwsel as values, e.g.:
        {'04287+1801': '20:327.464~328.183GHz;328.402~329.136GHz,26:340.207~340.239GHz;340.280~340.313GHz'}
    If the frequencies specified in the contfile are in LSRK, they will be converted to TOPO.
    """

    contfile_handler = ContFileHandler(contfile)
    contdict = contfile_handler.read(warn_nonexist=False)

    #if contdict == {}:
    #    #LOG.error(contfile + " is empty, does not exist or cannot be read.")
    #    LOG.info('cont.dat file not present.  Default to VLA Continuum Heuristics.')
    #    return {}

    m = context.observing_run.get_ms(vis)

    fielddict = {}

    for field in contdict['fields']:
        spwstring = ''
        for spw in contdict['fields'][field]:
            if contdict['fields'][field][spw][0]['refer'] == 'LSRK':
                LOG.info("Converting from LSRK to TOPO...")
                # Convert from LSRK to TOPO
                sname = field
                fieldobj = m.get_fields(name=field)
                fieldobjlist = [fieldobjitem for fieldobjitem in fieldobj]
                field_id = str(fieldobjlist[0].id)

                cranges_spwsel = collections.OrderedDict()

                cranges_spwsel[sname] = collections.OrderedDict()
                cranges_spwsel[sname][spw], _ = contfile_handler.get_merged_selection(sname, spw)

                freq_ranges, chan_ranges, aggregate_lsrk_bw = contfile_handler.to_topo(
                    cranges_spwsel[sname][spw], [vis], [field_id], int(spw),
                    context.observing_run)
                freq_ranges_list = freq_ranges[0].split(';')
                spwstring = spwstring + spw + ':'
                for freqrange in freq_ranges_list:
                    spwstring = spwstring + freqrange.replace(' TOPO', '') + ';'
                spwstring = spwstring[:-1]
                spwstring = spwstring + ','

            if contdict['fields'][field][spw][0]['refer'] == 'TOPO':
                LOG.info("Using TOPO frequency specified in {!s}".format(contfile))
                spwstring = spwstring + spw + ':'
                for freqrange in contdict['fields'][field][spw]:
                    spwstring = spwstring + str(freqrange['range'][0]) + '~' + str(freqrange['range'][1]) + 'GHz;'
                spwstring = spwstring[:-1]
                spwstring = spwstring + ','
        spwstring = spwstring[:-1]  # Remove appending semicolon

        if use_realspw:
            spwstring = context.observing_run.get_real_spwsel([spwstring], [vis])
        fielddict[field] = spwstring[0]

    LOG.info("Using frequencies in TOPO reference frame:")
    for field, spwsel in fielddict.items():
        LOG.info("    Field: {!s}   SPW: {!s}".format(field, spwsel))

    return fielddict