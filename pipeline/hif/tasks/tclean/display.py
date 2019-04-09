from __future__ import absolute_import
import collections
import os
import copy
import matplotlib

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.renderer.logger as logger

from pipeline.h.tasks.common.displays import sky as sky

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
                    collapse_function = 'max' if (('cube' in iteration['image']) or ('repBW' in iteration['cleanmask'])) else 'mean'

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
                            with casatools.ImageReader(image_path) as image:
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
                                                   intent=r.intent, **{'cmap': copy.deepcopy(matplotlib.cm.seismic)}))

                # MOM0_FC for this iteration (currently only last but allow for others in future).
                if 'mom0_fc' in iteration and os.path.exists(iteration['mom0_fc'] + extension):
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration['mom0_fc'] + extension, reportdir=stage_dir,
                                                   intent=r.intent))

                # MOM8_FC for this iteration (currently only last but allow for others in future).
                if 'mom8_fc' in iteration and os.path.exists(iteration['mom8_fc'] + extension):
                    # CAS-8847: For MOM8_FC image displayed in the weblog set the color range to: +1
                    # sigmaCube to max([peakCube,+8sigmaCube]).
                    image_path = iteration['image'].replace('.image', '.image%s' % (extension))
                    image_path = image_path.replace('.pbcor', '')
                    extra_args = {}
                    if image_path not in self.image_stats:
                        LOG.trace('No cached image statistics found for {!s}'.format(image_path))
                        with casatools.ImageReader(image_path) as image:
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
                        sky.SkyDisplay().plot(self.context, iteration['mom8_fc'] + extension, reportdir=stage_dir,
                                                   intent=r.intent, **extra_args))

                # cleanmask and cube spectra for this iteration - not for iter 0
                if i > 0:
                    collapse_function = 'max' if (('cube' in iteration['cleanmask']) or ('repBW' in iteration['cleanmask'])) else 'mean'
                    plot_wrappers.append(
                        sky.SkyDisplay().plot(self.context, iteration['cleanmask'], reportdir=stage_dir,
                                                   intent=r.intent, collapseFunction=collapse_function,
                                                   **{'cmap': copy.deepcopy(matplotlib.cm.YlOrRd)}))

                    if 'cube' in iteration['cleanmask']:
                        imagename = r.image_robust_rms_and_spectra['nonpbcor_imagename']
                        with casatools.ImageReader(r.image_robust_rms_and_spectra['nonpbcor_imagename']) as image:
                            miscinfo = image.miscinfo()

                        parameters = {k: miscinfo[k] for k in ['spw', 'iter'] if k in miscinfo}
                        parameters['field'] = '%s (%s)' % (miscinfo['field'], miscinfo['intent'])
                        parameters['type'] = 'spectra'

                        virtual_spw = parameters['spw']
                        imaging_mss = [m for m in self.context.observing_run.measurement_sets if m.is_imaging_ms]
                        if imaging_mss != []:
                            ref_ms = imaging_mss[0]
                        else:
                            ref_ms = self.context.observing_run.measurement_sets[0]
                        real_spw = self.context.observing_run.virtual2real_spw_id(virtual_spw, ref_ms)
                        real_spw_obj = ref_ms.get_spectral_window(real_spw)
                        if real_spw_obj.receiver is not None and real_spw_obj.freq_lo is not None:
                            rec_info = {'type': real_spw_obj.receiver, 'LO1': real_spw_obj.freq_lo[0].str_to_precision(12)}
                            print 'DEBUG_DM:', rec_info, rec_info['type']=='DSB'
                        else:
                            LOG.warn('Could not determine receiver type. Assuming TSB.')
                            rec_info = {'type': 'TSB', 'LO1': '0GHz'}

                        plotfile = '%s.spectrum.png' % (os.path.join(stage_dir, os.path.basename(imagename)))

                        plot_spectra(r.image_robust_rms_and_spectra, rec_info, plotfile)

                        plot_wrappers.append(logger.Plot(plotfile, parameters=parameters))

        return [p for p in plot_wrappers if p is not None]
