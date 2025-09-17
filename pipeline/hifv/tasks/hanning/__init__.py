from pipeline.infrastructure.renderer import qaadapter, weblog

from .hanning import Hanning
from . import hanning
from . import renderer
from . import qa

qaadapter.registry.register_to_dataset_topic(hanning.HanningResults)

weblog.add_renderer(Hanning, renderer.T2_4DetailsHanningRenderer(uri='hanning.mako',
                                                                 description='VLA Hanning Smoothing'),
                    group_by=weblog.UNGROUPED)
