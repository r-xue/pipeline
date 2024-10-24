import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import polcal
from . import qa
from . import renderer
from .polcal import Polcal

qaadapter.registry.register_to_calibration_topic(polcal.PolcalResults)

weblog.add_renderer(Polcal,
                    renderer.T2_4MDetailsPolcalRenderer(),
                    group_by=weblog.UNGROUPED)
