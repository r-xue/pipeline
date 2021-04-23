import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog

from .atmcorr import SerialSDATMCorrection

SDATMCorrection = SerialSDATMCorrection

weblog.add_renderer(SDATMCorrection, basetemplates.T2_4MDetailsDefaultRenderer(description='Restore Calibrated Data'),
                    group_by=weblog.UNGROUPED)
