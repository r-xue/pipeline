import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import applycal
from . import qa
from . import renderer
from .applycal import Applycal, SerialApplycal
from .applycal import ApplycalResults

qaadapter.registry.register_to_flagging_topic(applycal.ApplycalResults)

weblog.add_renderer(Applycal, renderer.T2_4MDetailsApplycalRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialApplycal, renderer.T2_4MDetailsApplycalRenderer(), group_by=weblog.UNGROUPED)
