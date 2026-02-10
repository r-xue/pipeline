import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from . import renderer
from .timegaincal import SerialTimeGaincal, TimeGaincal
# from pipeline.hif.tasks.gaincal import renderer

weblog.add_renderer(TimeGaincal, renderer.T2_4MDetailsGaincalRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialTimeGaincal, renderer.T2_4MDetailsGaincalRenderer(), group_by=weblog.UNGROUPED)
