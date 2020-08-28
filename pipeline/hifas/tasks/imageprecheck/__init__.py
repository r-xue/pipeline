import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.hifa.tasks.imageprecheck.renderer as renderer
import pipeline.hifa.tasks.imageprecheck.qa as qa

from . import imageprecheck
from .imageprecheck import ImagePreCheck

qaadapter.registry.register_to_dataset_topic(imageprecheck.ImagePreCheckResults)

weblog.add_renderer(ImagePreCheck,
                    renderer.T2_4MDetailsCheckProductSizeRenderer(uri='srdp_imageprecheck.mako',
                                                                  description='SRDP ImagePreCheck'),
                    group_by=weblog.UNGROUPED)
