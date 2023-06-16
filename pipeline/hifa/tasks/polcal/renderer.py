import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

LOG = logging.get_logger(__name__)


class T2_4MDetailsPolcalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the Polcal task.
    """
    def __init__(self, uri='polcal.mako',
                 description='Polarisation Calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # As a multi-vis task, there is only 1 Result for Polcal.
        result = results[0]

        # Initialize required output for weblog.
        session_names = []
        vislists = {}
        refants = {}
        polfields = {}

        # Retrieve info for each session.
        for session_name, session_results in result.session.items():
            # Store session name and corresponding vislist.
            session_names.append(session_name)
            vislists[session_name] = session_results['vislist']

            # Store pol cal field name and refant.
            refants[session_name] = session_results['refant']
            polfields[session_name] = session_results['polcal_field_name']

        # Update the mako context.
        mako_context.update({
            'session_names': session_names,
            'vislists': vislists,
            'refants': refants,
            'polfields': polfields,
        })
