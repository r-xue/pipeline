import pipeline.infrastructure.renderer.weblog as weblog

from .atmcor import SerialSDATMCorrection
from .renderer import T2_4MDetailsSingleDishATMCorRenderer

SDATMCorrection = SerialSDATMCorrection

weblog.add_renderer(
    SDATMCorrection,
    T2_4MDetailsSingleDishATMCorRenderer(always_rerender=True),
    group_by=weblog.UNGROUPED
)
