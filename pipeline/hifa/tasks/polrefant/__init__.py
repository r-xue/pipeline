import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa
from . import renderer
from . import resultobjects
from .polrefant import PolRefAnt

__all__ = [
    'PolRefAnt'
]

qaadapter.registry.register_to_calibration_topic(resultobjects.PolRefAntResults)

weblog.add_renderer(PolRefAnt, renderer.T2_4MDetailsPolRefAntRenderer(), group_by=weblog.UNGROUPED)
