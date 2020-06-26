import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from .almaimportdata import ALMAImportData
from .renderer import T2_4MDetailsALMAImportDataRenderer

weblog.add_renderer(ALMAImportData, T2_4MDetailsALMAImportDataRenderer(), group_by=weblog.UNGROUPED)
