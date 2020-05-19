import os
import shutil

import pipeline.h.tasks.common.displays.flagging as flagging
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
import pipeline.h.tasks.flagging.renderer as super_renderer
from pipeline.hif.tasks.common import flagging_renderer_utils as flagutils

LOG = logging.get_logger(__name__)

class T2_4MDetailsFlagDeterAlmaSdRenderer(super_renderer.T2_4MDetailsFlagDeterBaseRenderer):
    def __init__(self, uri='flagdeterbase.mako',
                 description='Deterministic flagging', always_rerender=False):

        super(T2_4MDetailsFlagDeterAlmaSdRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        super(T2_4MDetailsFlagDeterAlmaSdRenderer, self).update_mako_context(
            mako_context, pipeline_context, result
        )

        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % result.stage_number)

        # copy flagpointing.txt
        for r in result:
            src = r.inputs['filepointing']
            if os.path.exists(src):
                LOG.trace('Copying %s to %s' % (src, weblog_dir))
                shutil.copy(src, weblog_dir)

        # insert pointing agent
        agent = mako_context['agents']
        pos = agent.index('online')
        agent.insert(pos + 1, 'pointing')

        # update mako_context
        mako_context.update({'agents': agent})
