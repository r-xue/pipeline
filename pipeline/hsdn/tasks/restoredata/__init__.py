from __future__ import absolute_import

import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.pipelineqa as pipelineqa
import pipeline.infrastructure.renderer.weblog as weblog

from .restoredata import NRORestoreData

weblog.add_renderer(NRORestoreData, basetemplates.T2_4MDetailsDefaultRenderer( \
    description='Restore Calibrated Data'), group_by=weblog.UNGROUPED)


