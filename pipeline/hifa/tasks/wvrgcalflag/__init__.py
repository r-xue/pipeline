import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from . import renderer
from . import resultobjects
from .wvrgcalflag import SerialWvrgcalflag, Wvrgcalflag

qaadapter.registry.register_to_calibration_topic(resultobjects.WvrgcalflagResults)

weblog.add_renderer(Wvrgcalflag, renderer.T2_4MDetailsWvrgcalflagRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialWvrgcalflag, renderer.T2_4MDetailsWvrgcalflagRenderer(), group_by=weblog.UNGROUPED)
