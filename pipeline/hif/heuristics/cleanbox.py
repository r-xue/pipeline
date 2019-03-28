import os.path

import numpy as np

import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


def analyse_clean_result(multiterm, model, restored, residual, pb, cleanmask, pblimit_image=0.2,
                         pblimit_cleanmask=0.3, cont_freq_ranges=None):

    qaTool = casatools.quanta

    if pb == '':
        pb = None

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

    LOG.debug('Fixing coordsys of pb and cleanmask')
    with casatools.ImageReader(residual+extension) as image:
        csys = image.coordsys()
    if pb is not None:
        with casatools.ImageReader(pb+extension) as image:
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

        if pb is not None and os.path.exists(pb+extension):
            have_mask = True
            statsmask = '("%s" > %f) && ("%s" < %f)' % (os.path.basename(pb)+extension, pblimit_image,
                                                        os.path.basename(pb)+extension, pblimit_cleanmask)
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
        if pb is not None and os.path.exists(pb+extension):
            residual_stats = image.statistics(
              mask='"%s" > %f' % (os.path.basename(pb)+extension, pblimit_image), robust=False)
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
    nonpbcor_imagename = None
    nonpbcor_image_non_cleanmask_rms = None
    nonpbcor_image_non_cleanmask_rms_min = None
    nonpbcor_image_non_cleanmask_rms_max = None
    nonpbcor_image_non_cleanmask_robust_rms = None
    nonpbcor_image_non_cleanmask_freq_ch1 = None
    nonpbcor_image_non_cleanmask_freq_chN = None
    nonpbcor_image_non_cleanmask_freq_frame = None
    nonpbcor_image_cleanmask_spectrum = None
    nonpbcor_image_cleanmask_spectrum_pblimit = None
    nonpbcor_image_cleanmask_npoints = None
    if restored not in [None, '']:
        # get min and max of the pb-corrected cleaned result
        with casatools.ImageReader(restored.replace('.image', '.image%s' % extension)) as image:
            # define mask outside the cleaned area
            if pb is not None and os.path.exists(pb+extension):
                have_mask = True
                # Default is area pb > 0.3
                statsmask = '"%s" > %f' % (os.path.basename(pb)+extension, pblimit_cleanmask)
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

        # If possible use flattened clean mask for exclusion of areas for all spectral channels
        if cleanmask is not None and os.path.exists(cleanmask):
            flattened_mask = cleanmask.replace('.mask', '.mask.flattened')

            with casatools.ImageReader(cleanmask) as image:
                flattened_mask_image = image.collapse(function='max', axes=[2,3], outfile=flattened_mask)
                try:
                    npoints_mask = flattened_mask_image.statistics(mask='"%s" > 0.1' % (os.path.basename(flattened_mask)), robust=False)['npts']
                    if npoints_mask.shape != (0,):
                        nonpbcor_image_cleanmask_npoints = int(npoints_mask)
                    else:
                        nonpbcor_image_cleanmask_npoints = 0
                except:
                    nonpbcor_image_cleanmask_npoints = 0

        with casatools.ImageReader(nonpbcor_imagename) as image:
            # Get the image frequency axis for later plotting.
            imhead = image.summary(list=False)
            lcs = image.coordsys()
            try:
                freq_axis = image.coordsys().findaxisbyname('spectral')
            except:
                num_axes = image.shape().shape[0]
                if num_axes > 3:
                    LOG.warn("Can't find spectral axis. Assuming it is 3.")
                    freq_axis = 3
                elif num_axes > 2:
                    LOG.warn("Can't find spectral axis. Assuming it is 2.")
                    freq_axis = 2
                elif num_axes == 2:
                    LOG.error("No spectral axis found")
                    freq_axis = -1
            lcs.done()
            nonpbcor_image_non_cleanmask_freq_ch1 = qaTool.quantity(imhead['refval'][freq_axis] - imhead['refpix'][freq_axis] * imhead['incr'][freq_axis], imhead['axisunits'][freq_axis])
            nonpbcor_image_non_cleanmask_freq_chN = qaTool.quantity(imhead['refval'][freq_axis] + (imhead['shape'][freq_axis] - imhead['refpix'][freq_axis]) * imhead['incr'][freq_axis], imhead['axisunits'][freq_axis])
            # Get the spectral reference. Unfortunately this is coded in text
            # messages rather than a key/value pair. Hence the parsing code.
            try:
                for msg in imhead['messages'][1].split('\n'):
                    msg_l = msg.lower()
                    if 'spectral' in msg_l and 'reference' in msg_l:
                        nonpbcor_image_non_cleanmask_freq_frame = msg.split(':')[1].strip()
            except:
                LOG.warn('Cannot determine spectral reference in %s. Assuming it is LSRK.' % (nonpbcor_imagename))
                nonpbcor_image_non_cleanmask_freq_frame = 'LSRK'

            # define mask outside the cleaned area
            if pb is not None and os.path.exists(pb+extension) and cleanmask is not None and os.path.exists(cleanmask):
                have_mask = True
                # Annulus without clean mask
                statsmask = '("%s" < 0.1) && ("%s" > %f) && ("%s" < %f)' % \
                            (os.path.basename(flattened_mask), \
                             os.path.basename(pb)+extension, pblimit_image, \
                             os.path.basename(pb)+extension, pblimit_cleanmask)
            elif pb is not None and os.path.exists(pb+extension):
                have_mask = True
                # Full annulus
                statsmask = '("%s" > %f) && ("%s" < %f)' % (os.path.basename(pb)+extension, pblimit_image,
                                                            os.path.basename(pb)+extension, pblimit_cleanmask)
            elif cleanmask is not None and os.path.exists(cleanmask):
                have_mask = True
                # Area outside clean mask
                statsmask = '"%s" < 0.1' % (os.path.basename(flattened_mask))
            else:
                # Full area
                have_mask = False
                statsmask = ''

            try:
                # Get image RMS for all channels (this is for the weblog)
                image_stats = image.statistics(mask=statsmask, robust=True, axes=[0, 1, 2], algorithm='chauvenet', maxiter=5)

                # Filter continuum frequency ranges if given
                if cont_freq_ranges not in (None, ''):
                    # TODO: utils.freq_selection_to_channels uses casatools.image to get the frequency axis
                    #       and closes the global pipeline image tool. The context manager wrapped tool
                    #       used in this "with" statement is a different instance, so this is OK, but stacked
                    #       use of casatools.image might lead to unexpected results.
                    cont_chan_ranges = utils.freq_selection_to_channels(nonpbcor_imagename, cont_freq_ranges)
                    cont_chan_indices = np.hstack([np.arange(start, stop+1) for start, stop in cont_chan_ranges])
                    nonpbcor_image_non_cleanmask_rms_vs_chan = image_stats['rms'][cont_chan_indices]
                else:
                    nonpbcor_image_non_cleanmask_rms_vs_chan = image_stats['rms']

                nonpbcor_image_non_cleanmask_robust_rms = image_stats['medabsdevmed'] * 1.4826

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

            # Get the flux density spectrum in the clean mask area if available
            if nonpbcor_image_cleanmask_npoints is not None:
                if nonpbcor_image_cleanmask_npoints != 0:
                    # Area in flattened clean mask
                    spectrum_mask = '"%s" > 0.1' % (os.path.basename(flattened_mask))
                elif pb is not None:
                    # Area of pb > pblimit_cleanmask
                    spectrum_mask = '"%s" > %f' % (os.path.basename(pb)+extension, pblimit_cleanmask)
                    nonpbcor_image_cleanmask_spectrum_pblimit = pblimit_cleanmask
                else:
                    spectrum_mask = None
                nonpbcor_image_cleanmask_spectrum = image.getprofile(function='flux', mask=spectrum_mask, stretch=True, axis=freq_axis)['values']

    return (residual_cleanmask_rms, residual_non_cleanmask_rms, residual_min, residual_max,
            nonpbcor_image_non_cleanmask_rms_min, nonpbcor_image_non_cleanmask_rms_max,
            nonpbcor_image_non_cleanmask_rms, pbcor_image_min, pbcor_image_max, residual_robust_rms,
            {'nonpbcor_imagename': nonpbcor_imagename,
             'nonpbcor_image_non_cleanmask_freq_ch1': nonpbcor_image_non_cleanmask_freq_ch1,
             'nonpbcor_image_non_cleanmask_freq_chN': nonpbcor_image_non_cleanmask_freq_chN,
             'nonpbcor_image_non_cleanmask_freq_frame': nonpbcor_image_non_cleanmask_freq_frame,
             'nonpbcor_image_non_cleanmask_robust_rms': nonpbcor_image_non_cleanmask_robust_rms,
             'nonpbcor_image_cleanmask_spectrum': nonpbcor_image_cleanmask_spectrum,
             'nonpbcor_image_cleanmask_spectrum_pblimit': nonpbcor_image_cleanmask_spectrum_pblimit,
             'nonpbcor_image_cleanmask_npoints': nonpbcor_image_cleanmask_npoints,
             'cont_freq_ranges': cont_freq_ranges})
