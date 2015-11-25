'''
Created on 8 Sep 2014

@author: sjw
'''
import contextlib
import json
import math
import os

import pipeline.infrastructure.logging as logging
from . import weblog

LOG = logging.get_logger(__name__)


class T2_4MDetailsDefaultRenderer(object):
    def __init__(self, uri='t2-4m_details-generic.html',
                 description='No description set for this task',
                 always_rerender=False):
        self.uri = uri
        # TODO remove template access after conversion
        self.description = description
        self.always_rerender = always_rerender

    def get_display_context(self, context, result):
        mako_context = {'pcontext': context,
                        'result': result,
                        'casalog_url': self._get_log_url(context, result),
                        'dirname': 'stage%s' % result.stage_number}
        self.update_mako_context(mako_context, context, result)
        return mako_context

    def update_mako_context(self, mako_context, pipeline_context, result):
        LOG.trace('No-op update_mako_context for %s', self.__class__.__name__)
        
    def render(self, context, result):
        display_context = self.get_display_context(context, result)
        uri = self.uri
        template = weblog.TEMPLATE_LOOKUP.get_template(uri)
        return template.render(**display_context)

    def _get_log_url(self, context, result):
        """
        Get the URL of the stage log relative to the report directory.
        """
        stagelog_path = os.path.join(context.report_dir,
                                     'stage%s' % result.stage_number,
                                     'casapy.log')

        if not os.path.exists(stagelog_path):
            return None

        return os.path.relpath(stagelog_path, context.report_dir)        


class CommonRenderer(object):
    def __init__(self, uri, context, result):
        self.uri = uri
        self.context = context
        self.result = result
        # this should be overwritten by the superclass constructor, otherwise
        # an exception will occur when the template is rendered
        self.path = None

        # when the calling context is rendering a page of plots for all MSes,
        # the constructor is given a list of results rather than a single
        # result
        if isinstance(result, list):
            stage_number = result[0].stage_number
        else:
            stage_number = result.stage_number
        stage = 'stage%s' % stage_number

        self.dirname = os.path.join(context.report_dir, stage)

    def render(self):
        mako_context = {'pcontext': self.context,
                        'result': self.result,
                        'dirname': self.dirname}
        self.update_mako_context(mako_context)
        t = weblog.TEMPLATE_LOOKUP.get_template(self.uri)
        return t.render(**mako_context)

    def update_mako_context(self, mako_context):
        pass

    def get_file(self):
        if self.path is None:
            LOG.error('No path specified for renderer')
            raise IOError()
        
        if not os.path.exists(self.dirname):
            os.makedirs(self.dirname)
            
        file_obj = open(self.path, 'w')
        return contextlib.closing(file_obj)
    

class JsonPlotRenderer(CommonRenderer):
    def __init__(self, uri, context, result, plots, title, outfile):
        super(JsonPlotRenderer, self).__init__(uri, context, result)
        self.plots = plots
        self.title = title
        self.path = os.path.join(self.dirname, outfile)

        # all values set on this dictionary will be written to the JSON file
        d = {}
        for plot in plots:
            # calculate the relative pathnames as seen from the browser
            thumbnail_relpath = os.path.relpath(plot.thumbnail,
                                                self.context.report_dir)
            image_relpath = os.path.relpath(plot.abspath,
                                            self.context.report_dir)
            json_dict_for_plot = {'thumbnail' : thumbnail_relpath}
            # push the most commonly used filter parameters directly into the
            # JSON dictionary to save the extending classes from doing it in
            # update_json_dict 
            for param in ('vis', 'session', 'spw', 'scan', 'ant', 'baseband',
                          'field', 'pol'):
                if param in plot.parameters:
                    json_dict_for_plot[param] = str(plot.parameters[param])                    
            self.update_json_dict(json_dict_for_plot, plot)

            # Javascript JSON parser doesn't like Javascript floating point 
            # constants (NaN, Infinity etc.), so convert them to null. We  
            # do not omit the dictionary entry so that the plot is hidden
            # by the filters.
            for k, v in json_dict_for_plot.items():
                if isinstance(v, float):
                    if math.isnan(v) or math.isinf(v):
                        json_dict_for_plot[k] = 'null'

            d[image_relpath] = json_dict_for_plot

        self.json = json.dumps(d)
    
    def update_json_dict(self, d, plot):
        """
        Hook function that can be used by extending classes to extract extra
        parameters from the plot object and insert them into the JSON
        dictionary for that plot.
        """
        pass
         
    def update_mako_context(self, mako_context):
        super(JsonPlotRenderer, self).update_mako_context(mako_context)
        well_formed = self.json.replace("'", "\\'").replace('"', '&quot;')
        
        mako_context.update({'plots': self.plots,
                             'json': well_formed,
                             'plot_title': self.title})
