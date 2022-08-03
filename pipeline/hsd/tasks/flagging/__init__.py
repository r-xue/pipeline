import pipeline.h.tasks.flagging.renderer as super_renderer
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from .flagdeteralmasd import FlagDeterALMASingleDish, HpcFlagDeterALMASingleDish
from . import renderer
from . import flagdeteralmasd

qaadapter.registry.register_to_flagging_topic(flagdeteralmasd.FlagDeterALMASingleDishResults)

# Use generic deterministic flagging renderer for ALMA SD
# deterministic flagging.
weblog.add_renderer(FlagDeterALMASingleDish,
                    renderer.T2_4MDetailsFlagDeterAlmaSdRenderer(description='ALMA SD deterministic flagging', always_rerender=False),
                    group_by=weblog.UNGROUPED)
weblog.add_renderer(HpcFlagDeterALMASingleDish,
                    renderer.T2_4MDetailsFlagDeterAlmaSdRenderer(description='ALMA SD deterministic flagging', always_rerender=False),
                    group_by=weblog.UNGROUPED)
