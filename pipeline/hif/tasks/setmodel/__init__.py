import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from pipeline.h.tasks.common import commonfluxresults
from . import qa
from . import renderer
from .setjy import Setjy
from .setmodel import SerialSetModels, SetModels

qaadapter.registry.register_to_calibration_topic(commonfluxresults.FluxCalibrationResults)

weblog.add_renderer(Setjy, renderer.T2_4MDetailsSetjyRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialSetModels, renderer.T2_4MDetailsSetjyRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SetModels, renderer.T2_4MDetailsSetjyRenderer(), group_by=weblog.UNGROUPED)