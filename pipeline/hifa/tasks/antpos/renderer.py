import collections
import os
import numpy

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class T2_4MDetailsALMAAntposRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='almaantpos.mako', 
                 description='Correct for antenna position offsets',
                 always_rerender=False):
        super(T2_4MDetailsALMAAntposRenderer, self).__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        table_rows = make_antpos_table(pipeline_context, results)
        # Sort by total offset, from highest-to-lowest, see: PIPE-77
        table_rows_by_offset = make_antpos_table(pipeline_context, results, sort_by=lambda x: float(getattr(x, 'total')), reverse=True)
        threshold = 0.49 # FIXME, temporary test value, needs to default to representative_frequency and then be converted to meters for comparison with the x,y,z offsets
        mako_context.update({'table_rows': table_rows,
                             'table_rows_by_offset': table_rows_by_offset,
                             'threshold': threshold})


AntposTR = collections.namedtuple('AntposTR', 'vis antenna x y z total total_in_wavelengths')


def make_antpos_table(context, results, sort_by=lambda x: getattr(x, 'antenna'), reverse=False): #FIXME: add docs and type hinting

    # Will hold all the antenna offset table rows for the results
    rows = []

    # Loop over the results
    for single_result in results:
        vis_cell = os.path.basename(single_result.inputs['vis'])

        # Construct the antenna list and the xyz offsets
        antenna_list = single_result.antenna.split(',')
        xyzoffsets_list = make_xyzoffsets_list(single_result.offsets)

        # No offsets
        if len(antenna_list) is 0 or len(antenna_list) != len(xyzoffsets_list):
            continue

        # Loop over the individual antennas and offsets
        for item in zip (antenna_list, xyzoffsets_list):
            antname = item[0]
            xoffset = '%0.2e' % item[1][0]
            yoffset = '%0.2e' % item[1][1]
            zoffset = '%0.2e' % item[1][2]
            total_offset = '%0.2f' % item[1][3]
            total_offset_in_wavelengths = '%0.2f' % item[1][4]
            tr = AntposTR(vis_cell, antname, xoffset, yoffset, zoffset, total_offset, total_offset_in_wavelengths)
            rows.append(tr)

    rows.sort(key=sort_by, reverse=reverse)
    return utils.merge_td_columns(rows)


def make_xyzoffsets_list (offsets_list):
    if len(offsets_list) is 0:
        return []

    xyz_list = []
    for i in range (0, len(offsets_list), 3):
        x, y, z = offsets_list[i], offsets_list[i+1], offsets_list[i+2]
        # PIPE-77: Add total offset in mm and also in units of wavelength
        total_offset = numpy.linalg.norm([x, y, z]) * 1000.0
        total_offset_in_wavelengths = total_offset #FIXME fix units
        xyz_list.append((x, y, z, total_offset, total_offset_in_wavelengths))
    return xyz_list


