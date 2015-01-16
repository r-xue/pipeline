'''
Created on 11 Sep 2014

@author: sjw
'''
import os

import pipeline.infrastructure.displays.image as image
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

LOG = logging.get_logger(__name__)


class T2_4MDetailsBandpassFlagRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    '''
    Renders detailed HTML output for the Tsysflag task.
    '''
    def __init__(self, uri='bpflagchans.mako', 
                 description='Flag channels with bad bandpass calibration',
                 always_rerender=False):
        super(T2_4MDetailsBandpassFlagRenderer, self).__init__(uri=uri,
                description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        htmlreports = self.get_htmlreports(pipeline_context, results)        

        plots = {}
        for result in results:
            renderer = Antenna1VsChannelsPlotRenderer(pipeline_context, result)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
            
            vis = os.path.basename(result.inputs['vis'])
            plots[vis] = os.path.relpath(renderer.path, pipeline_context.report_dir)

        mako_context.update({'htmlreports' : htmlreports,
                             'plots'       : plots})

    def get_htmlreports(self, context, results):
        report_dir = context.report_dir
        weblog_dir = os.path.join(report_dir,
                                  'stage%s' % results.stage_number)

        htmlreports = {}
        for result in results:
            if not hasattr(result, 'table'):
                # empty result
                continue

            flagcmd_abspath = self._write_flagcmd_to_disk(weblog_dir, result)
            flagcmd_relpath = os.path.relpath(flagcmd_abspath, report_dir)
            table_basename = os.path.basename(result.table)
            htmlreports[table_basename] = flagcmd_relpath

        return htmlreports

    def _write_flagcmd_to_disk(self, weblog_dir, result):
        tablename = os.path.basename(result.table)
        filename = os.path.join(weblog_dir, '%s-flag_commands.txt' % tablename)
        flagcmds = [l.flagcmd for l in result.flagcmds()]
        with open(filename, 'w') as flagfile:
            flagfile.writelines(['# Flag commands for %s\n#\n' % tablename])
            flagfile.writelines(['%s\n' % cmd for cmd in flagcmds])
            if not flagcmds:
                flagfile.writelines(['# No flag commands generated\n'])
        return filename
    
    
class Antenna1VsChannelsPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Time vs Antenna1 plots for %s' % vis
        outfile = filenamer.sanitize('time_vs_antenna1-%s.html' % vis)

        stage = 'stage%s' % result.stage_number
        dirname = os.path.join(context.report_dir, stage)

        plotter = image.ImageDisplay()
        plots = plotter.plot(context=context, results=result, reportdir=dirname,
          dpi=1000)
        
        super(Antenna1VsChannelsPlotRenderer, self).__init__(
                'generic_x_vs_y_per_spw_and_pol_plots.mako', context, 
                result, plots, title, outfile)

