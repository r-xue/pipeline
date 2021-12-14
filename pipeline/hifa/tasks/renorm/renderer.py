import os
import collections
import shutil
import re
import xml.etree.ElementTree as ET

from pipeline.infrastructure.utils import weblog

import pipeline.h.tasks.common.displays.image as image
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

LOG = logging.get_logger(__name__)


class T2_4MDetailsRenormRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the Lowgainflag task.
    """
    def __init__(self, uri='renorm.mako', 
                 description='Renormalize',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % result.stage_number)

        (table_rows,
         mako_context['alerts_info']) = make_renorm_table(pipeline_context, result, weblog_dir)

        mako_context.update({
            'table_rows': table_rows,
            'weblog_dir': weblog_dir
        })

TR = collections.namedtuple('TR', 'vis source spw max pdf')

def make_renorm_table(context, results, weblog_dir):

    # Will hold all the input and output MS(s)
    rows = []
    alert = []

    scale_factors = []
    # Loop over the results
    for result in results:
        threshold = result.threshold
        vis = os.path.basename(result.inputs['vis'])
        if result.alltdm:
            alert = ['No FDM spectral windows are present, '
                     'so the amplitude scale does not need to be '
                     'assessed for renormalization.']
        for source, source_stats in result.stats.items():
            for spw, spw_stats in source_stats.items():

                # print(source, spw, source_stats)
                maxrn = spw_stats.get('max_rn')
                scale_factors.append(maxrn)
                if maxrn:
                    maxrn_field = f"{spw_stats.get('max_rn'):.8} ({spw_stats.get('max_rn_field')})"
                else:
                    maxrn_field = ""

                pdf = spw_stats.get('pdf_summary')
                pdf_path = f"RN_plots/{pdf}"
                if os.path.exists(pdf_path):
                    LOG.trace(f"Copying {pdf_path} to {weblog_dir}")
                    shutil.copy(pdf_path, weblog_dir)   # copy pdf file across to weblog directory
                    pdf_path = pdf_path.replace('RN_plots', f'stage{result.stage_number}')
                    pdf_path_link = f'<a href="{pdf_path}" download="{pdf}">PDF</a>'
                else:
                    pdf_path_link = ""

                specplot = spw_stats.get('spec_plot')
                tr = TR(vis, source, spw, maxrn_field, pdf_path_link)
                rows.append(tr)

    merged_rows = utils.merge_td_columns(rows, num_to_merge=2)
    merged_rows = [list(row) for row in merged_rows]  # convert tuples to mutable lists

    for row, _ in enumerate(merged_rows):
        mm = re.search(r'<td[^>]*>(\d+.\d*) \(\d+\)', merged_rows[row][-2])
        if mm:  # do we have a pattern match?
            scale_factor = scale_factors[row]
            if scale_factor > threshold:

                for col in (-3, -2, -1):
                    cell = ET.fromstring(merged_rows[row][col])
                    innermost_child = getchild(cell)
                    innermost_child.set('class','danger alert-danger')
                    merged_rows[row][col] = ET.tostring(innermost_child)


    return merged_rows, alert

def getchild(el):
    if el.findall('td'):
        return getchild(el[0])
    else:
        return el