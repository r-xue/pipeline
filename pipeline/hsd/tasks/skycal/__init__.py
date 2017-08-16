from __future__ import absolute_import

import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .skycal import SDSkyCal
from . import skycal
from . import renderer

qaadapter.registry.register_to_calibration_topic(skycal.SDSkyCalResults)

weblog.add_renderer(SDSkyCal, renderer.T2_4MDetailsSingleDishSkyCalRenderer(), group_by=weblog.UNGROUPED)
