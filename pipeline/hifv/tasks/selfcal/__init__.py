from __future__ import absolute_import
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .selfcal import Selfcal
from . import selfcal
from . import renderer

# qaadapter.registry.register_to_dataset_topic(selfcal.SelfcalResults)

# weblog.add_renderer(Selfcal, basetemplates.T2_4MDetailsDefaultRenderer(uri='selfcal.mako',
#                         description='Selfcal'), group_by=weblog.UNGROUPED)

weblog.add_renderer(Selfcal, renderer.T2_4MDetailsselfcalRenderer(), group_by=weblog.UNGROUPED)
