import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

__all__ = [
    'T2_4MDetailsPolRefAntRenderer'
]

LOG = logging.get_logger(__name__)


class T2_4MDetailsPolRefAntRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='polrefant.mako', description='Select polarisation reference antennas',
                 always_rerender=False):
        super(T2_4MDetailsPolRefAntRenderer, self).__init__(uri=uri, description=description,
                                                            always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        pass
