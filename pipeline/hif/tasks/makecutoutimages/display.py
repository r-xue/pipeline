import collections
import os

import matplotlib.pyplot as plt

import numpy as np
from matplotlib.pyplot import cm
from scipy.stats import median_absolute_deviation

import pipeline.infrastructure as infrastructure
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.h.tasks.common.displays.imhist import ImageHistDisplay
from pipeline.infrastructure import casa_tools
import pipeline.infrastructure.renderer.logger as logger
from pipeline.infrastructure.displays.plotstyle import matplotlibrc_formal


LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class CutoutimagesSummary(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG cutout images for weblog")
        plot_wrappers = []

        for subimagename in self.result.subimagenames:
            if '.psf.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-0.1, vmax=0.3))
            elif '.image.' in subimagename and '.pbcor' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.image_stats = image.statistics(robust=True)

            elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.residual_stats = image.statistics(robust=True)

            elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                # CAS-10345/PIPE-1189: use (-5*RMSmedian, 20*RMSmedian) as the colormap scaling range
                # We expect 'subimagename' here to be: X.image.pbcor.ttX.subim or X.image.pbcor.subim
                rms_subimagename = os.path.splitext(subimagename)[0]+'.rms.subim'
                with casa_tools.ImageReader(rms_subimagename) as image:
                    rms_median = image.statistics(robust=True).get('median')[0]
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean',
                                                           vmin=-5 * rms_median,
                                                           vmax=20 * rms_median))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_stats = image.statistics(robust=True)

            elif '.rms.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.rms_stats = image.statistics(robust=True)
                        arr = image.getchunk()
                        # PIPE-489 changed denominators to unmasked (non-zero) pixels
                        # get fraction of pixels <= 120 micro Jy VLASS technical goal.  ignore 0 (masked) values.
                        self.result.RMSfraction120 = (np.count_nonzero((arr != 0) & (arr <= 120e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 168 micro Jy VLASS SE goal.  ignore 0 (masked) values.
                        self.result.RMSfraction168 = (np.count_nonzero((arr != 0) & (arr <= 168e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # get fraction of pixels <= 200 micro Jy VLASS technical requirement.  ignore 0 (masked) values.
                        self.result.RMSfraction200 = (np.count_nonzero((arr != 0) & (arr <= 200e-6)) /
                                                      float(np.count_nonzero(arr != 0))) * 100
                        # PIPE-642: include the number and percentage of masked pixels in weblog
                        self.result.n_masked = np.count_nonzero(arr == 0)
                        self.result.pct_masked = (np.count_nonzero(arr == 0) / float(arr.size)) * 100

            elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pbcor_residual_stats = image.statistics(robust=True)

            elif '.pb.' in subimagename:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean', vmin=0.2, vmax=1.))
                plot_wrappers.append(ImageHistDisplay(self.context, subimagename,
                                                      x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                                      reportdir=stage_dir).plot())
                if '.tt1.' not in subimagename:
                    with casa_tools.ImageReader(subimagename) as image:
                        self.result.pb_stats = image.statistics(robust=True)
            else:
                plot_wrappers.append(sky.SkyDisplay().plot(self.context, subimagename,
                                                           reportdir=stage_dir, intent='',
                                                           collapseFunction='mean'))

        return [p for p in plot_wrappers if p is not None]


class VlassCubeCutoutimagesSummary(object):
    """A class for the VLASS-CUBE makecutout image summary plots."""
    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.result.stats = None

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.info("Making PNG cutout images for weblog")
        plot_wrappers = []

        # a nested dictionary container for all stats: stats[spw]['rms'] etc.
        stats = collections.OrderedDict()

        stats_stokes_list = []
        stats_reffreq_list = []

        for subimagename in self.result.subimagenames:

            with casa_tools.ImageReader(subimagename) as image:

                image_miscinfo = image.miscinfo()
                virtspw = image_miscinfo['virtspw']
                if virtspw not in stats:
                    stats[virtspw] = collections.OrderedDict()

                if '.psf.' in subimagename:
                    plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                                          reportdir=stage_dir, intent='', stokes_list=['I'],
                                                                          collapseFunction='mean',
                                                                          vmin=-0.1, vmax=0.3))

                elif '.image.' in subimagename and '.pbcor' not in subimagename:
                    # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                    if '.tt1.' not in subimagename:
                        # for non-pbcor image, we use a PB-based mask as the image/residual from tclean()  could be unmasked with
                        # artifically low-amp pixels at edges.
                        pbname = subimagename.replace('.image.', '.pb.')
                        item_stats = self._get_stats(
                            image, items=['peak', 'madrms', 'max/madrms'], mask=f'mask("{pbname}")')
                        stats[virtspw]['image'] = item_stats
                        stats_stokes_list = sky.SkyDisplay.get_stokes(subimagename)
                        cs = image.coordsys()
                        stats_reffreq_list.append(cs.referencevalue(format='n')['numeric'][3])
                        beam = image.restoringbeam(channel=0, polarization=0)
                        stats[virtspw]['beam'] = {'bmaj': beam['major']['value'],
                                                  'bmin': beam['minor']['value'], 'bpa': beam['positionangle']['value']}

                elif '.residual.' in subimagename and '.pbcor.' not in subimagename:
                    # PIPE-491/1163: report non-pbcor stats and don't display images; don't save stats from .tt1
                    if '.tt1.' not in subimagename:
                        # for non-pbcor image, we use a PB-based mask as the image/residual from tclean()  could be unmasked with
                        # artifically low-amp pixels at edges.
                        pbname = subimagename.replace('.residual.', '.pb.')
                        item_stats = self._get_stats(
                            image, items=['peak', 'madrms', 'max/madrms'], mask=f'mask("{pbname}")')
                        stats[virtspw]['residual'] = item_stats

                elif '.image.pbcor.' in subimagename and '.rms.' not in subimagename:
                    # CAS-10345/PIPE-1189: use (-5*RMSmedian, 20*RMSmedian) as the colormap scaling range
                    # We expect 'subimagename' here to be: X.image.pbcor.ttX.subim or X.image.pbcor.subim
                    rms_subimagename = os.path.splitext(subimagename)[0]+'.rms.subim'
                    with casa_tools.ImageReader(rms_subimagename) as image:
                        rms_median = image.statistics(robust=True).get('median')[0]
                    plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                             reportdir=stage_dir, intent='', stokes_list=None,
                                                             collapseFunction='mean',
                                                             vmin=-5 * rms_median,
                                                             vmax=20 * rms_median)
                    for plot in plots:
                        plot.parameters['type'] = 'image.pbcor'
                    plot_wrappers.extend(plots)
                    if '.tt1.' not in subimagename:
                        with casa_tools.ImageReader(subimagename) as image:
                            self.result.pbcor_stats = image.statistics(robust=True)

                elif '.rms.' in subimagename:
                    plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                             reportdir=stage_dir, intent='', stokes_list=None,
                                                             collapseFunction='mean')
                    for plot in plots:
                        plot.parameters['type'] = 'image.pbcor.rms'
                    plot_wrappers.extend(plots)

                    if '.tt1.' not in subimagename:
                        item_stats = self._get_stats(image, items=['max', 'median', 'pct<800e-6', 'pct_masked'])
                        stats[virtspw]['rms'] = item_stats

                elif '.residual.pbcor.' in subimagename and not subimagename.endswith('.rms'):
                    plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                             reportdir=stage_dir, intent='', stokes_list=None,
                                                             collapseFunction='mean')
                    for plot in plots:
                        plot.parameters['type'] = 'residual.pbcor'
                    plot_wrappers.extend(plots)

                elif '.pb.' in subimagename:
                    plots = sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                             reportdir=stage_dir, intent='', stokes_list=['I'],
                                                             collapseFunction='mean', vmin=0.2, vmax=1.)
                    for plot in plots:
                        plot.parameters['type'] = 'pb'
                    plot_wrappers.extend(plots)
                    plot_hist = ImageHistDisplay(self.context, subimagename,
                                                 x_axis='Primary Beam Response', y_axis='Num. of Pixel',
                                                 reportdir=stage_dir).plot()
                    plot_hist.parameters['virtspw'] = plot_wrappers[-1].parameters['virtspw']
                    plot_hist.parameters['band'] = plot_wrappers[-1].parameters['band']
                    plot_hist.parameters['type'] = plot_wrappers[-1].parameters['type']
                    plot_hist.parameters['stokes'] = 'I'
                    plot_wrappers.append(plot_hist)
                    if '.tt1.' not in subimagename:
                        item_stats = self._get_stats(image, items=['max', 'min', 'median'])
                        stats[virtspw]['pb'] = item_stats
                else:
                    plot_wrappers.extend(sky.SkyDisplay().plot_per_stokes(self.context, subimagename,
                                                                          reportdir=stage_dir, intent='', stokes_list=None,
                                                                          collapseFunction='mean'))

        self.result.stats = stats
        self.result.stats_stokes = stats_stokes_list
        self.result.stats_reffreq = stats_reffreq_list
        self._get_stats_summary()

        plot_wrappers = [p for p in plot_wrappers if p is not None]
        for p in plot_wrappers:
            p.parameters['order_idx'] = 10
            if p.parameters['type'] == 'image.pbcor':
                p.parameters['order_idx'] = 1
            if p.parameters['type'] == 'residual.pbcor':
                p.parameters['order_idx'] = 2
            if p.parameters['type'] == 'image.pbcor.rms':
                p.parameters['order_idx'] = 3
            if p.parameters['type'] == 'pb':
                p.parameters['order_idx'] = 4
            if p.parameters['type'] == 'psf':
                p.parameters['order_idx'] = 4

        return plot_wrappers

    def _get_stats_summary(self):
        """Reorganize the raw stats (spw,imtype) into a layout (imtype,property) more convenient for tabulation."""

        stats_summary = collections.OrderedDict()
        for spw, stats_spw in self.result.stats.items():
            for imtype, stats_spw_imtype in stats_spw.items():
                for item, value in stats_spw_imtype.items():
                    if imtype not in stats_summary:
                        stats_summary[imtype] = collections.OrderedDict()
                    if item not in stats_summary[imtype]:
                        stats_summary[imtype][item] = {'spw': [], 'value': []}
                    stats_summary[imtype][item]['spw'].append(spw)
                    stats_summary[imtype][item]['value'].append(value)

        for imtype, stats_summary_imtype in stats_summary.items():
            for item, item_details in stats_summary_imtype.items():
                value_arr = np.array(item_details['value'])         # shape=(n_spw, n_pol)
                spw_arr = np.array(item_details['spw'])             # shape=(n_spw,)
                stats_summary[imtype][item]['spwwise_madrms']=median_absolute_deviation(value_arr,axis=0)*1.4826    # shape=(n_spw,)
                stats_summary[imtype][item]['spwwise_mean']=np.mean(value_arr,axis=0)                               # shape=(n_spw,)
                stats_summary[imtype][item]['range'] = np.percentile(value_arr, (0, 100))
                idx_maxdev = np.argmax(value_arr-np.median(value_arr, axis=0), axis=0)
                stats_summary[imtype][item]['spw_outlier'] = spw_arr[idx_maxdev]

        self.result.stats_summary = stats_summary

        return

    def _get_stats(self, image, items=['min', 'max'], mask=None):
        """Extract the desired stats properties (per Stokes) from an ia.statistics() return."""

        imstats = image.statistics(robust=True, axes=[0, 1, 3], mask=mask)
        stats = collections.OrderedDict()

        for item in items:
            if item.lower() == 'madrms':
                stats['madrms'] = imstats['medabsdevmed']*1.4826  # see CAS-9631
            elif item.lower() == 'max/madrms':
                stats['max/madrms'] = imstats['max']/imstats['medabsdevmed']*1.4826  # see CAS-9631
            elif item.lower() == 'maxabs':
                stats['maxabs'] = np.maximum(np.abs(imstats['max']), np.abs(imstats['min']))
            elif 'pct<' in item:
                threshold = float(item.replace('pct<', ''))
                imstats_threshold = image.statistics(robust=True, axes=[0, 1, 3], includepix=[0, threshold], mask=mask)
                if len(imstats_threshold['npts']) == 0:
                    # if no pixel is selected from the restricted pixel value range, the return of ia.statitics() would be empty.
                    imstats_threshold['npts'] = np.zeros(4)
                stats[item] = imstats_threshold['npts']/imstats['npts']
            elif item.lower() == 'pct_masked':
                im_shape = (imstats['trc']-imstats['blc'])+1
                stats[item] = 1.-imstats['npts']/im_shape[0]/im_shape[1]
            elif item.lower() == 'peak':  # Here 'peak' means the pixel value with largest deviation from zero.
                stats[item] = np.where(np.abs(imstats['max']) > np.abs(imstats['min']), imstats['max'], imstats['min'])
            elif item.lower() == 'peak/madrms':
                peak = np.where(np.abs(imstats['max']) > np.abs(imstats['min']), imstats['max'], imstats['min'])
                madrms = imstats['medabsdevmed']*1.4826  # see CAS-9631
                stats['peak/madrms'] = peak/madrms
            else:
                stats[item] = imstats[item.lower()]

        return stats


class VlassCubeCutoutRmsSummary(object):
    """A class for the VLASS-CUBE makecutout rms-vs-frequency summary plots."""
    def __init__(self, context, result):
        self.context = context
        self.result = result

    @matplotlibrc_formal
    def plot(self):

        stage_dir = os.path.join(self.context.report_dir,
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        plots = []

        figfile = os.path.join(stage_dir, 'image_madrms_vs_freq.png')
        plot = self._plot_rms(figfile=figfile, imtype='image', item='madrms')
        plots.append(plot)

        figfile = os.path.join(stage_dir, 'image_rms_vs_spw.png')
        plot = self._plot_rms(figfile=figfile, imtype='rms', item='median')
        plots.append(plot)

        return [p for p in plots if p is not None]

    def _plot_rms(self, figfile, imtype='rms', item='median'):

        x = np.array(self.result.stats_reffreq)/1e9
        y = np.array(self.result.stats_summary[imtype][item]['value'])*1e3
        spw_labels = np.array(self.result.stats_summary[imtype][item]['spw'])

        LOG.debug(f'Creating the {imtype}_{item} vs. Frequency plot.')

        try:

            fig, ax = plt.subplots(figsize=(8, 6))
            cmap = cm.get_cmap('rainbow_r')
            for idx, stokes in enumerate(self.result.stats_stokes):
                color_idx = idx/len(self.result.stats_stokes)
                ax.plot(x, y[:, idx], color=cmap(color_idx), label=f'$\it{stokes}$')
                ax.scatter(x, y[:, idx], color=cmap(color_idx), alpha=0.75, s=300, edgecolors='black')
                if stokes == 'I':
                    for idx_spw in range(len(x)):
                        text = ax.annotate(spw_labels[idx_spw], (x[idx_spw], y[idx_spw, idx]),
                                           ha='center', va='center', fontsize=9.)
                        text.set_alpha(.7)

            ax.set_xlabel('Frequency [GHz]')
            ax.set_ylabel(r'RMS$_{\rm median}$ [mJy/beam]')
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            ax.set_xlim(1.9, 4.1)
            ax.set_ylim(ylim)

            # 'RflagDevHeuristic' is only imported on-demand; if it's imported during module initialization,
            # a circular import will introduce problem (hif->hifv->hif)
            from pipeline.hifv.heuristics.rfi import RflagDevHeuristic
            sefd = RflagDevHeuristic._get_vla_sefd()

            for band, sefd_per_band in sefd.items():
                sefd_x = sefd_per_band[:, 0]/1e3
                sefd_y = sefd_per_band[:, 1]
                if np.mean(x) > np.min(sefd_x) and np.mean(x) < np.max(sefd_x):
                    LOG.info(f'Selecting Band {band} for the SEFD-based rms prediction.')
                    sefd_spw = np.interp(x, sefd_x, sefd_y)
                    scale = np.median(np.divide(y[:, 1:], sefd_spw[:, np.newaxis]))
                    ax.plot(sefd_x, sefd_y*scale, color='gray', label=r'SEFD$_{\rm norm}$', linestyle='-')

            # ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            ax.legend(fontsize=12, labelspacing=0.5)

            fig.tight_layout()
            fig.savefig(figfile)
            plt.close(fig)

            plot = logger.Plot(figfile,
                               x_axis='Frequency',
                               y_axis=imtype+'_'+item,
                               parameters={})

        except Exception as ex:
           LOG.warning("Could not create plot {}".format(figfile))
           LOG.warning(ex)
           plot = None

        return plot
