import pipeline.infrastructure.renderer.qaadapter as qaadapter
from . import flagdeterbase
from . import qa
from .flagdatasetter import FlagdataSetter

qaadapter.registry.register_to_flagging_topic(flagdeterbase.FlagDeterBaseResults)
