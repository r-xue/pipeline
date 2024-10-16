import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog

from .restoredata import SDRestoreData

weblog.add_renderer(SDRestoreData, basetemplates.T2_4MDetailsDefaultRenderer(description='Restore Calibrated Data'),
                    group_by=weblog.UNGROUPED)


