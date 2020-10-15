import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .vlassmasking import Vlassmasking
from . import vlassmasking

#qaadapter.registry.register_to_dataset_topic(vlassmasking.VlassmaskingResults)

weblog.add_renderer(Vlassmasking,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='vlassmasking.mako',
                                                              description='Vlassmasking'),
                    group_by=weblog.UNGROUPED)
