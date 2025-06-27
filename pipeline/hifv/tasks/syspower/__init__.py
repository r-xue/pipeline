import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .syspower import Syspower
from . import syspower
from . import renderer
from . import qa

qaadapter.registry.register_to_dataset_topic(syspower.SyspowerResults)

weblog.add_renderer(Syspower, renderer.T2_4MDetailssyspowerRenderer(), group_by=weblog.UNGROUPED)
