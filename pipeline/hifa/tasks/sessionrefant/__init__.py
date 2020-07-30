import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa
from . import renderer
from . import resultobjects
from .sessionrefant import SessionRefAnt

__all__ = [
    'SessionRefAnt'
]

qaadapter.registry.register_to_calibration_topic(resultobjects.SessionRefAntResults)

weblog.add_renderer(SessionRefAnt, renderer.T2_4MDetailsSessionRefAntRenderer(), group_by=weblog.UNGROUPED)