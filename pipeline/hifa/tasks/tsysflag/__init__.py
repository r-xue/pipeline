import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.h.tasks.tsysflag.renderer as super_renderer

from .tsysflag import ALMATsysflag

weblog.add_renderer(ALMATsysflag, super_renderer.T2_4MDetailsTsysflagRenderer(), group_by=weblog.UNGROUPED)
