import pipeline.infrastructure.renderer.weblog as weblog

from .tsysflagcontamination import TsysFlagContamination
from .renderer import T2_4MDetailsTsysflagContaminationRenderer

weblog.add_renderer(
    TsysFlagContamination,
    T2_4MDetailsTsysflagContaminationRenderer(),
    group_by=weblog.UNGROUPED,
)
