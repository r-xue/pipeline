from __future__ import absolute_import
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.pipelineqa as pipelineqa
import pipeline.infrastructure.renderer.weblog as weblog

from .msbaseline import SDMSBaseline
from . import msbaseline
from . import renderer
from . import qa

pipelineqa.registry.add_handler(qa.SDBaselineQAHandler())
pipelineqa.registry.add_handler(qa.SDBaselineListQAHandler())

#qaadapter.registry.register_to_calibration_topic(msbaseline.SDMSBaselineResults)
qaadapter.registry.register_to_miscellaneous_topic(msbaseline.SDMSBaselineResults)

weblog.add_renderer(SDMSBaseline, renderer.T2_4MDetailsSingleDishBaselineRenderer(), group_by=weblog.UNGROUPED)
