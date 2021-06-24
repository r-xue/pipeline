import os
import collections
import shutil

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

        table_rows = make_renorm_table(pipeline_context, result, weblog_dir)

        mako_context.update({
            'table_rows': table_rows,
            'weblog_dir': weblog_dir
        })

TR = collections.namedtuple('TR', 'vis source spw max pdf')

def make_renorm_table(context, results, weblog_dir):

    # Will hold all the input and output MS(s)
    rows = []

    # Loop over the results
    for result in results:
        vis = os.path.basename(result.inputs['vis'])
        for source, source_stats in result.stats.items():
            for spw, spw_stats in source_stats.items():
                # print(source, spw, source_stats)
                maxrn = spw_stats.get('max_rn')
                if maxrn:
                    maxrn_field = f"{spw_stats.get('max_rn'):.4} ({spw_stats.get('max_rn_field')})"
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

    return utils.merge_td_columns(rows)
