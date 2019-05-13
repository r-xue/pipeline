import os
import collections
import shutil
import numpy

import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils

from . import display as display
from ..common import utils as sdutils

LOG = logging.get_logger(__name__)

JyperKTRV = collections.namedtuple('JyperKTRV', 'virtualspw msname realspw antenna pol factor')
JyperKTR  = collections.namedtuple('JyperKTR',  'spw msname antenna pol factor')

class T2_4MDetailsSingleDishK2JyCalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='hsd_k2jycal.mako', 
                 description='Generate Kelvin to Jy calibration table.',
                 always_rerender=False):
        super(T2_4MDetailsSingleDishK2JyCalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):            
        reffile = None
        spw_factors = collections.defaultdict(lambda: [])
        valid_spw_factors = collections.defaultdict(lambda: collections.defaultdict(lambda: []))

        dovirtual = sdutils.require_virtual_spw_id_handling(context.observing_run)
        trfunc_r = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTR(_rspwid, _vis, _antenna, _pol, _factor)
        trfunc_v = lambda _vspwid, _vis, _rspwid, _antenna, _pol, _factor: JyperKTRV(_vspwid, _vis, _rspwid, _antenna, _pol, _factor)
        trfunc = trfunc_v if dovirtual else trfunc_r
        for r in results:
            # rearrange jyperk factors
            ms = context.observing_run.get_ms(name=r.vis)
            vis = ms.basename
            spw_band = {}
            for spw in ms.get_spectral_windows(science_windows_only=True):
                spwid = spw.id
                # virtual spw id
                vspwid = context.observing_run.real2virtual_spw_id(spwid, ms)
                ddid = ms.get_data_description(spw=spwid)
                if vspwid not in spw_band:
                    spw_band[vspwid] = spw.band
                for ant in ms.get_antenna():
                    ant_name = ant.name
                    corrs = list(map(ddid.get_polarization_label, range(ddid.num_polarizations)))
#                     # an attempt to collapse pol rows
#                     # corr_collector[factor] = [corr0, corr1, ...]
#                     corr_collector = collections.defaultdict(lambda: [])
                    for corr in corrs:
                        factor = self.__get_factor(r.factors, vis, spwid, ant_name, corr)
#                         corr_collector[factor].append(corr)
#                     for factor, corrlist in corr_collector.iteritems():
#                         corr = str(', ').join(corrlist)
                        jyperk = factor if factor is not None else 'N/A (1.0)'
#                         tr = JyperKTR(vis, spwid, ant_name, corr, jyperk)
                        tr = trfunc(vspwid, vis, spwid, ant_name, corr, jyperk)
                        spw_factors[vspwid].append(tr)
                        if factor is not None:
                            valid_spw_factors[vspwid][corr].append(factor)
            reffile = r.reffile
        stage_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        # histogram plots of Jy/K factors
        hist_plots = []
        for vspwid, valid_factors in valid_spw_factors.iteritems():
            if len(valid_factors) > 0:
                task = display.K2JyHistDisplay(stage_dir, vspwid, valid_factors, spw_band[vspwid])
                hist_plots += task.plot()
        # input Jy/K files
        reffile_copied = None
        if reffile is not None and os.path.exists(reffile):
            LOG.debug('copying %s to %s' % (reffile, stage_dir))
            shutil.copy2(reffile, stage_dir)
            reffile_copied = os.path.join(stage_dir, os.path.basename(reffile))
        # order table rows so that spw comes first
        row_values = []
        for factor_list in spw_factors.itervalues():
            row_values += list(factor_list)
        ctx.update({'jyperk_rows': utils.merge_td_columns(row_values),
                    'reffile': reffile_copied,
                    'jyperk_hist': hist_plots,
                    'dovirtual': dovirtual})

    @staticmethod
    def __get_factor(factor_dict, vis, spwid, ant_name, pol_name):
        """
        Returns a factor corresponding to vis, spwid, ant_name, and pol_name from
        a factor_dict[vis][spwid][ant_name][pol_name] = factor
        If factor_dict lack corresponding factor, the method returns None.
        """
        if (vis not in factor_dict or
                spwid not in factor_dict[vis] or
                ant_name not in factor_dict[vis][spwid] or
                pol_name not in factor_dict[vis][spwid][ant_name]):
            return None
        return factor_dict[vis][spwid][ant_name][pol_name]
