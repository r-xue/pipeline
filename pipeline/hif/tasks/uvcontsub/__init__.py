# import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
# from ..applycal import applycal
from pipeline.h.tasks.applycal import applycal
from . import qa
from . import renderer
from . import uvcontsub
from .uvcontsub import UVcontSub

#qaadapter.registry.register_to_dataset_topic(uvcontfit.UVcontFitResults)
qaadapter.registry.register_to_dataset_topic(uvcontsub.UVcontSubResults)
#qaadapter.registry.register_to_dataset_topic(applycal.ApplycalResults)

#weblog.add_renderer(UVcontFit, renderer.T2_4MDetailsUVcontFitRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(UVcontSub, renderer.T2_4MDetailsUVcontSubRenderer(), group_by=weblog.UNGROUPED)
# weblog.add_renderer(UVcontSub,
#                     basetemplates.T2_4MDetailsDefaultRenderer(description='Continuum subtract the TARGET data'),
#                     group_by=weblog.UNGROUPED)
