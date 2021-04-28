import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog

from .atmcorr import SerialSDATMCorrection
from .renderer import T2_4MDetailsSingleDishATMCorRenderer

SDATMCorrection = SerialSDATMCorrection

weblog.add_renderer(
    SDATMCorrection,
    T2_4MDetailsSingleDishATMCorRenderer(always_rerender=True),
    group_by=weblog.UNGROUPED
)
