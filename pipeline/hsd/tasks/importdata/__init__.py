# import pipeline.hif.tasks.importdata.renderer as super_renderer
import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from . import renderer
from .importdata import SerialSDImportData as SerialSDImportData
from .importdata import HpcSDImportData as HpcSDImportData
from .importdata import SDImportDataResults as SDImportDataResults
import pipeline.infrastructure.renderer.qaadapter as qaadapter

SDImportData = HpcSDImportData

qaadapter.registry.register_to_dataset_topic(SDImportDataResults)

# # use the standard ImportData renderer to render ALMAImportData results
# weblog.add_renderer(SDImportData, super_renderer.T2_4MDetailsImportDataRenderer(uri='hsd_importdata.mako'), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialSDImportData, renderer.T2_4MDetailsSingleDishImportDataRenderer(uri='hsd_importdata.mako'),
                    group_by=weblog.UNGROUPED)
weblog.add_renderer(HpcSDImportData, renderer.T2_4MDetailsSingleDishImportDataRenderer(uri='hsd_importdata.mako'),
                    group_by=weblog.UNGROUPED)
