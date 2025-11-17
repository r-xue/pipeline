import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from pipeline.hif.tasks.bandpass import common
from . import qa
from . import renderer
from .almaphcorbandpass import SerialALMAPhcorBandpass, ALMAPhcorBandpass, SessionALMAPhcorBandpass

qaadapter.registry.register_to_calibration_topic(common.BandpassResults)

weblog.add_renderer(ALMAPhcorBandpass, renderer.T2_4MDetailsBandpassRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialALMAPhcorBandpass, renderer.T2_4MDetailsBandpassRenderer(), group_by=weblog.UNGROUPED)