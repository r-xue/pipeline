import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .analyzestokescubes import Analyzestokescubes
from . import analyzestokescubes

#qaadapter.registry.register_to_dataset_topic(analyzestokescubes.AnalyzestokescubesResults)

weblog.add_renderer(Analyzestokescubes,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='analyzestokescubes.mako',
                                                              description='Analyzestokescubes'),
                    group_by=weblog.UNGROUPED)
