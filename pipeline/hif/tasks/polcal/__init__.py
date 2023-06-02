import pipeline.infrastructure.renderer.qaadapter as qaadapter
from .polcalworker import PolcalWorker
from . import polcalworker

qaadapter.registry.register_to_calibration_topic(polcalworker.PolcalResults)
