import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

__all__ = [
    'T2_4MDetailsPolRefAntRenderer'
]

LOG = logging.get_logger(__name__)


class T2_4MDetailsPolRefAntRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='polrefant.mako', description='Select polarisation reference antennas',
                 always_rerender=False):
        super(T2_4MDetailsPolRefAntRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # Collect information about which MSes were evaluated, corresponding
        # session names, and final chosen reference antenna, for display in
        # table in task weblog page.
        sessions = {}
        for result in results:
            for session_name in result.refant:
                sessions[session_name] = {
                    'vislist': result.refant[session_name]['vislist'],
                    'refant': result.refant[session_name]['refant'],
                }

        # Update the mako context.
        mako_context.update({
            'sessions': sessions,
        })
