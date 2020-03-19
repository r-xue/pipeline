"""
Created on 18 Mar 2020

@author: Dirk Muders (MPIfR)
"""
import collections


import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.hif.tasks.correctedampflag.renderer import T2_4MDetailsCorrectedampflagRenderer
from pipeline.infrastructure import basetask

LOG = logging.get_logger(__name__)

FlagTotal = collections.namedtuple('FlagSummary', 'flagged total')


class T2_4MDetailsTargetflagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the TargetFlag task.
    """
    def __init__(self, uri='targetflag.mako',
                 description='Target outlier flagging',
                 always_rerender=False):
        super(T2_4MDetailsTargetflagRenderer, self).__init__(uri=uri, description=description,
                                                             always_rerender=always_rerender)


        # Attach correctedampflag renderer.
        self.cafrenderer = T2_4MDetailsCorrectedampflagRenderer(uri=uri, description=description,
                                                                always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):

        #
        # Get flagging reports, summaries
        #
        cafresults = basetask.ResultsList()
        for result in results:
            for cafresult in result.cafresults.values():
                cafresults.append(cafresult)
        cafresults.stage_number = results.stage_number
        self.cafrenderer.update_mako_context(mako_context, pipeline_context, cafresults)
