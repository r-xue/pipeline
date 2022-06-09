import collections
import copy
import os

import matplotlib
import matplotlib.pyplot as plt

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.common.displays import sky as sky
from pipeline.infrastructure import casa_tools
from .plot_spectra import plot_spectra

LOG = infrastructure.get_logger(__name__)

# class used to transfer image statistics through to plotting routines
ImageStats = collections.namedtuple('ImageStats', 'rms max')


class CleanSummary(object):
    def __init__(self, context, result, image_stats):
        self.context = context
        self.result = result
        self.image_stats = image_stats

    def plot(self):
        stage_dir = os.path.join(self.context.report_dir, 
                                 'stage%d' % self.result.stage_number)
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        LOG.trace('Plotting')
        plot_wrappers = []

        # this class can handle a list of results from hif_cleanlist, or a single 
        # result from hif_clean if we make the single result from the latter
        # the member of a list
        if hasattr(self.result, 'results'):
            results = self.result.results
        else:
            results = [self.result]

        for r in results:
            if r.empty():
                continue

            extension = '.tt0' if r.multiterm else ''

            # psf map
            plot_wrappers.append(sky.SkyDisplay().plot(self.context, r.psf + extension,
                                                            reportdir=stage_dir, intent=r.intent,
                                                            collapseFunction='mean'))

            # flux map
            plot_wrappers.append(sky.SkyDisplay().plot(self.context,
                                                            r.flux + extension, reportdir=stage_dir, intent=r.intent,
                                                            collapseFunction='mean'))

            for i, iteration in [(k, r.iterations[k]) for k in sorted(r.iterations)]:
                # process image for this iteration
                if 'image' in iteration:
                    collapse_function = 'max' if (('cube' in iteration.get('image', '')) or ('repBW' in iteration.get('image', ''))) else 'mean'

                    # PB corrected
                    image_path = iteration['image'].replace('.image', '.image%s' % (extension))
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, image_path, reportdir=stage_dir, intent=r.intent,
                                                   collapseFunction=collapse_function))

                    # Non PB corrected
                    image_path = image_path.replace('.pbcor', '')

                    # CAS-8847: For the non-pbcor MOM8 image displayed in the weblog set the color range to: +1
                    # sigmaCube to max([peakCube,+8sigmaCube]). The pbcor MOM8 images are ok as is for now.
                    extra_args = {}
                    if collapse_function == 'max':
                        if image_path not in self.image_stats:
                            LOG.trace('No cached image statistics found for {!s}'.format(image_path))
                            with casa_tools.ImageReader(image_path) as image:
                                stats = image.statistics(robust=False)
                                image_rms = stats.get('rms')[0]
                                image_max = stats.get('max')[0]
                                self.image_stats[image_path] = ImageStats(rms=image_rms, max=image_max)

                        image_stats = self.image_stats[image_path]
                        image_rms = image_stats.rms
                        image_max = image_stats.max

                        extra_args = {
                            'vmin': image_rms,
                            'vmax': max([image_max, 8*image_rms])
                        }

                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, image_path, reportdir=stage_dir, intent=r.intent,
                                                   collapseFunction=collapse_function, **extra_args))

                # residual for this iteration
                plot_wrappers.append(
                    sky.SkyDisplay().plot(self.context, iteration['residual'] + extension, reportdir=stage_dir,
                                               intent=r.intent))

                # model for this iteration (currently only last but allow for others in future)
                if 'model' in iteration and os.path.exists(iteration['model'] + extension):
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration['model'] + extension, reportdir=stage_dir,
                                                   intent=r.intent, **{'cmap': copy.copy(matplotlib.cm.seismic)}))

                # MOM0_FC for this iteration (currently only last but allow for others in future).
                if 'mom0_fc' in iteration and os.path.exists(iteration['mom0_fc'] + extension):
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration['mom0_fc'] + extension, reportdir=stage_dir,
                                                   intent=r.intent))

                # MOM8_FC for this iteration (currently only last but allow for others in future).
                if 'mom8_fc' in iteration and os.path.exists(iteration['mom8_fc'] + extension):
                    # PIPE-197: For MOM8_FC image displayed in the weblog set the color range
                    # from (median-MAD) to 10 * sigma, where sigma is from the annulus minus cleanmask
                    # masked mom8_fc image and median, MAD are from the unmasked mom8_fc image
                    if iteration['cube_sigma_fc_chans'] is not None:
                        extra_args = {
                            'vmin': iteration['mom8_fc_image_median_all'] - iteration['mom8_fc_image_mad'],
                            'vmax': 10 * iteration['cube_sigma_fc_chans'],
                            'mom8_fc_peak_snr': iteration['mom8_fc_peak_snr']
                        }
                        # in case min >= max, set min=image_min and max=image_max
                        if extra_args['vmin'] >= extra_args['vmax']:
                            extra_args['vmin'] = iteration['mom8_fc_image_min']
                            extra_args['vmax'] = iteration['mom8_fc_image_max']
                    else:
                        extra_args = {}

                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration['mom8_fc'] + extension, reportdir=stage_dir,
                                                   intent=r.intent, **extra_args))

                # cleanmask - not for iter 0
                if i > 0:
                    collapse_function = 'max' if (('cube' in iteration.get('cleanmask', '')) or ('repBW' in iteration.get('cleanmask', ''))) else 'mean'
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration.get('cleanmask', ''), reportdir=stage_dir,
                                              intent=r.intent, collapseFunction=collapse_function,
                                              **{'cmap': copy.copy(matplotlib.cm.YlOrRd)}))

                # cube spectra for this iteration
                if ('cube' in iteration.get('image', '')) or ('repBW' in iteration.get('image', '')):
                    imagename = r.image_robust_rms_and_spectra['nonpbcor_imagename']
                    with casa_tools.ImageReader(imagename) as image:
                        miscinfo = image.miscinfo()

                    parameters = {k: miscinfo[k] for k in ['virtspw', 'iter'] if k in miscinfo}
                    parameters['field'] = '%s (%s)' % (miscinfo['field'], miscinfo['intent'])
                    parameters['type'] = 'spectra'
                    try:
                        parameters['prefix'] = miscinfo['filnam01']
                    except:
                        parameters['prefix'] = None

                    virtual_spw = parameters['virtspw']
                    ref_ms = self.context.observing_run.measurement_sets[0]
                    real_spw = self.context.observing_run.virtual2real_spw_id(virtual_spw, ref_ms)
                    real_spw_obj = ref_ms.get_spectral_window(real_spw)
                    if real_spw_obj.receiver is not None and real_spw_obj.freq_lo is not None:
                        rec_info = {'type': real_spw_obj.receiver, 'LO1': real_spw_obj.freq_lo[0].str_to_precision(12)}
                    else:
                        LOG.warning('Could not determine receiver type. Assuming TSB.')
                        rec_info = {'type': 'TSB', 'LO1': '0GHz'}

                    plotfile = '%s.spectrum.png' % (os.path.join(stage_dir, os.path.basename(imagename)))
                    field_id = int(r.field_ids[0].split(',')[0])
                    plot_spectra(r.image_robust_rms_and_spectra, rec_info, plotfile, ref_ms.name, str(real_spw), field_id)

                    plot_wrappers.append(logger.Plot(plotfile, parameters=parameters))

        return [p for p in plot_wrappers if p is not None]


class TcleanMajorCycleSummaryFigure(object):
    """Tclean major cycle summery statistics plot with two panels, contains:

    Flux density cleaned vs. major cycle
    Plot of Peak Residual per major cycle

    Note that as of 04.2021 no clear tclean return dictionary was available and the unit
    of the flux and RMS was determined empirically.

    See PIPE-991."""

    def __init__(self, context, result, major_cycle_stats):
        self.context = context
        self.majorcycle_stats = major_cycle_stats
        self.reportdir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        self.filebase = result.targets[0]['imagename'].replace('STAGENUMBER', '%s_0' % result.stage_number)
        self.figfile = self._get_figfile()
        self.units = ['Jy', 'Jy/pixel']
        self.title = 'Major cycle statistics'
        self.xlabel = 'Minor iterations done'
        self.ylabel = ['Flux density cleaned [%s]' % self.units[0], 'Peak residual [%s]' % self.units[1]]
        self.unitfactor = [1.0, 1.0]

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing tclean major cycle summary plot')
            return self._get_plot_object()

        LOG.info('Creating major cycle statistics plot.')

        fig, (ax0, ax1) = plt.subplots(2, 1, )
        fig.set_dpi(150.0)

        ax0.set_title(self.title, fontsize=10)
        ax1.set_xlabel(self.xlabel, fontsize=8)
        ax0.set_ylabel(self.ylabel[0], fontsize=8)
        ax1.set_ylabel(self.ylabel[1], fontsize=8)

        ax0.tick_params(axis='both', which='both', labelsize=8)
        ax1.tick_params(axis='both', which='both', labelsize=8)

        ax0.set_yscale('log')
        ax1.set_yscale('log')

        x0 = 0
        for iter, item in self.majorcycle_stats.items():
            if item['nminordone_array'] is not None:
                # get quantities
                x = item['nminordone_array'] + x0
                ax0_y = item['totalflux_array'] * self.unitfactor[0]
                ax1_y = item['peakresidual_array'] * self.unitfactor[1]
                # increment last iteration
                x0 = x[-1]
                # scatter plot
                ax0.plot(x, ax0_y, 'b+')
                ax1.plot(x, ax1_y, 'b+')
                # Vertical line and annotation at major cycle end
                ax0.axvline(x0, linewidth=1, linestyle='dotted', color='k')
                ax1.axvline(x0, linewidth=1, linestyle='dotted', color='k')
                ax0.annotate(f'iter{iter}', xy=(x0, ax0.get_ylim()[0]), xycoords='data',
                             xytext=(-10, 6), textcoords='offset points', size=8, rotation=90)

        fig.tight_layout()
        fig.savefig(self.figfile)
        plt.close()

        return self._get_plot_object()

    def _get_figfile(self):
        return os.path.join(self.reportdir,
                            'major_cycle_stats.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis=self.xlabel,
                           y_axis='/'.join(self.ylabel))

