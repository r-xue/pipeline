import os.path

import numpy as np

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


def analyse_clean_result(multiterm, model, restored, residual, flux, cleanmask, pblimit_image=0.2,
                         pblimit_cleanmask=0.3, cont_freq_ranges=None):

    if flux == '':
        flux = None

    if multiterm:
        extension = '.tt0'
    else:
        extension = ''

    # get the sum of the model image to find how much flux has been
    # cleaned
    model_sum = None
    if model is not None:
        with casatools.ImageReader(model+extension) as image:
            model_stats = image.statistics(robust=False)
            model_sum = model_stats['sum'][0]
            LOG.debug('Sum of model: %s' % model_sum)

    LOG.debug('Fixing coordsys of flux and cleanmask')
    with casatools.ImageReader(residual+extension) as image:
        csys = image.coordsys()
    if flux is not None:
        with casatools.ImageReader(flux+extension) as image:
            image.setcoordsys(csys.torecord())
    if cleanmask is not None and os.path.exists(cleanmask):
        with casatools.ImageReader(cleanmask) as image:
            image.setcoordsys(csys.torecord())

    with casatools.ImageReader(residual+extension) as image:
        # get the rms of the residual image inside the cleaned area
        LOG.todo('Cannot use dirname in mask')
        residual_cleanmask_rms = None

        if cleanmask is not None and os.path.exists(cleanmask):
            # Area inside clean mask
            statsmask = '"%s" > 0.1' % (os.path.basename(cleanmask))

            resid_clean_stats = image.statistics(mask=statsmask, robust=False)

            try:
                residual_cleanmask_rms = resid_clean_stats['rms'][0]
                LOG.info('Residual rms inside cleaned area: %s' % residual_cleanmask_rms)
            except:
                pass

        # and the rms of the residual image outside the cleaned area
        residual_non_cleanmask_rms = None

        if flux is not None and os.path.exists(flux+extension):
            have_mask = True
            # Default is annulus 0.2 < pb < 0.3
            statsmask = '("%s" > %f) && ("%s" < %f)' % (os.path.basename(flux)+extension, pblimit_image,
                                                        os.path.basename(flux)+extension, pblimit_cleanmask)
        elif cleanmask is not None and os.path.exists(cleanmask):
            have_mask = True
            # Area outside clean mask
            statsmask = '"%s" < 0.1' % (os.path.basename(cleanmask))
        else:
            have_mask = False
            statsmask = ''

        residual_stats = image.statistics(mask=statsmask, robust=False)

        try:
            residual_non_cleanmask_rms = residual_stats['rms'][0]
            if have_mask:
                LOG.info('Residual rms in the annulus: %s' % residual_non_cleanmask_rms)
            else:
                LOG.info('Residual rms across full area: %s' % residual_non_cleanmask_rms)
        except:
            pass

        # get the max, min of the residual image (avoiding the edges
        # where spikes can occur)
        if flux is not None and os.path.exists(flux+extension):
            residual_stats = image.statistics(
              mask='"%s" > %f' % (os.path.basename(flux)+extension, pblimit_image), robust=False)
        else:
            residual_stats = image.statistics(robust=False)

        try:
            residual_max = residual_stats['max'][0]
            residual_min = residual_stats['min'][0]
        except:
            residual_max = None
            residual_min = None

        LOG.info('Residual max: %s min: %s' % (residual_max, residual_min))

        residual_stats = image.statistics(robust=True)
        residual_robust_rms = residual_stats['medabsdevmed'][0] * 1.4826  # see CAS-9631
        LOG.debug('residual scaled MAD: %s' % residual_robust_rms)

    pbcor_image_min = None
    pbcor_image_max = None
    nonpbcor_image_non_cleanmask_rms = None
    nonpbcor_image_non_cleanmask_rms_min = None
    nonpbcor_image_non_cleanmask_rms_max = None
    if restored not in [None, '']:
        # get min and max of the pb-corrected cleaned result
        with casatools.ImageReader(restored.replace('.image', '.image%s' % extension)) as image:
            # define mask outside the cleaned area
            if flux is not None and os.path.exists(flux+extension):
                have_mask = True
                # Default is area pb > 0.3
                statsmask = '"%s" > %f' % (os.path.basename(flux)+extension, pblimit_cleanmask)
            elif cleanmask is not None and os.path.exists(cleanmask):
                have_mask = True
                # Area inside clean mask
                statsmask = '"%s" > 0.1' % (os.path.basename(cleanmask))
            else:
                have_mask = False
                statsmask = ''

            image_stats = image.statistics(mask=statsmask)

            pbcor_image_min = image_stats['min'][0]
            pbcor_image_max = image_stats['max'][0]

            if have_mask:
                LOG.debug('Clean pb-corrected image min in cleaned area: %s' % pbcor_image_min)
                LOG.debug('Clean pb-corrected image max in cleaned area: %s' % pbcor_image_max)
            else:
                LOG.debug('Clean pb-corrected image min in full area: %s' % pbcor_image_min)
                LOG.debug('Clean pb-corrected image max in full area: %s' % pbcor_image_max)

        # get RMS in non cleanmask area of non-pb-corrected cleaned result
        if restored.find('.image.pbcor') != -1:
            nonpbcor_imagename = restored.replace('.image.pbcor', '.image%s' % extension)
        else:
            nonpbcor_imagename = restored.replace('.image', '.image%s' % extension)

        with casatools.ImageReader(nonpbcor_imagename) as image:

            # define mask outside the cleaned area
            if flux is not None and os.path.exists(flux+extension):
                have_mask = True
                # Default is annulus 0.2 < pb < 0.3
                statsmask = '("%s" > %f) && ("%s" < %f)' % (os.path.basename(flux)+extension, pblimit_image,
                                                            os.path.basename(flux)+extension, pblimit_cleanmask)
            elif cleanmask is not None and os.path.exists(cleanmask):
                have_mask = True
                # Area outside clean mask
                statsmask = '"%s" < 0.1' % (os.path.basename(cleanmask))
            else:
                have_mask = False
                statsmask = ''

            try:
                # Get image RMS for all channels (this is for the weblog)
                image_stats = image.statistics(mask=statsmask, robust=False, axes=[0, 1, 2])

                # Filter continuum frequency ranges if given
                if cont_freq_ranges not in (None, ''):
                    cont_chan_ranges = utils.freq_selection_to_channels(nonpbcor_imagename, cont_freq_ranges)
                    cont_chan_indices = np.hstack([np.arange(start, stop+1) for start, stop in cont_chan_ranges])
                    nonpbcor_image_non_cleanmask_rms_vs_chan = image_stats['rms'][cont_chan_indices]
                else:
                    nonpbcor_image_non_cleanmask_rms_vs_chan = image_stats['rms']

                nonpbcor_image_non_cleanmask_rms_median = np.median(nonpbcor_image_non_cleanmask_rms_vs_chan)
                nonpbcor_image_non_cleanmask_rms_mean = np.mean(nonpbcor_image_non_cleanmask_rms_vs_chan)
                nonpbcor_image_non_cleanmask_rms_min = np.min(nonpbcor_image_non_cleanmask_rms_vs_chan)
                nonpbcor_image_non_cleanmask_rms_max = np.max(nonpbcor_image_non_cleanmask_rms_vs_chan)
                nonpbcor_image_non_cleanmask_rms = nonpbcor_image_non_cleanmask_rms_median
                if have_mask:
                    area_text = 'annulus'
                else:
                    area_text = 'full image'
                LOG.info('Clean image statistics (%s) for %s: rmsmedian: %s Jy/bm rmsmean: %s Jy/bm rmsmin:'
                         ' %s Jy/bm rmsmax: %s Jy/bm' % \
                    (area_text, os.path.basename(nonpbcor_imagename),
                     nonpbcor_image_non_cleanmask_rms_median,
                     nonpbcor_image_non_cleanmask_rms_mean,
                     nonpbcor_image_non_cleanmask_rms_min,
                     nonpbcor_image_non_cleanmask_rms_max))
            except Exception as e:
                nonpbcor_image_non_cleanmask_rms_min = \
                nonpbcor_image_non_cleanmask_rms_max = \
                nonpbcor_image_non_cleanmask_rms = \
                    -999.0
                LOG.warn('Exception while determining image RMS for %s: %s' % (nonpbcor_imagename, e))

    return (residual_cleanmask_rms, residual_non_cleanmask_rms, residual_min, residual_max,
            nonpbcor_image_non_cleanmask_rms_min, nonpbcor_image_non_cleanmask_rms_max,
            nonpbcor_image_non_cleanmask_rms, pbcor_image_min, pbcor_image_max, residual_robust_rms)
