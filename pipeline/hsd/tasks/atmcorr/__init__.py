import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog

from .atmcorr import SerialSDATMCorrection

SDATMCorrection = SerialSDATMCorrection

weblog.add_renderer(
    SDATMCorrection,
    basetemplates.T2_4MDetailsDefaultRenderer(
        description='Apply correction for atmospheric effects',
        uri='hsd_atmcor.mako', always_rerender=True),
    group_by=weblog.UNGROUPED
)
