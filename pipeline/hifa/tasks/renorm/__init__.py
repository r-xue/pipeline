import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .renorm import Renorm
from . import renorm
from . import renderer

#qaadapter.registry.register_to_dataset_topic(renorm.RenormResults)

weblog.add_renderer(Renorm,
                    renderer.T2_4MDetailsRenormRenderer(uri='renorm.mako', description='Renorm'),
                    group_by=weblog.UNGROUPED)
