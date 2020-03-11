import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .targetflag import Targetflag
from . import targetflag

#qaadapter.registry.register_to_dataset_topic(targetflag.TargetflagResults)

weblog.add_renderer(Targetflag,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='targetflag.mako',
                                                              description='Targetflag'),
                    group_by=weblog.UNGROUPED)
