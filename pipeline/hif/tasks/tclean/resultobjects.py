import collections
import copy
import os.path

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.utils as utils

from pipeline.h.tasks.common.displays import sky as sky

LOG = infrastructure.get_logger(__name__)


class BoxResult(basetask.Results):
    def __init__(self):
        super(BoxResult, self).__init__()        
        self.threshold = None
        self.sensitivity = None
        self.cleanmask = None

    def merge_with_context(self, context):
        pass

    def __repr__(self):
        return 'BoxResult <threshold=%s cleanmask=%s>' % (
         self.threshold, self.cleanmask)


class TcleanResult(basetask.Results):
    def __init__(self, vis=None, sourcename=None, intent=None, spw=None, orig_specmode=None, specmode=None,
                 multiterm=None, plotdir=None, imaging_mode=None, is_per_eb=None, is_eph_obj=None):
        super(TcleanResult, self).__init__()
        self.vis = vis
        self.sourcename = sourcename
        self.intent = intent
        self.spw = spw
        self.orig_specmode = orig_specmode
        self.specmode = specmode
        self.multiterm = multiterm
        self.plotdir = plotdir
        self._psf = None
        self._model = None
        self._flux = None
        self.iterations = collections.defaultdict(dict)
        self._pblimit_image = 0.2
        self._pblimit_cleanmask = 0.3
        self._aggregate_bw = 0.0
        self._eff_ch_bw = 0.0
        self._sensitivity = 0.0
        self._dr_corrected_sensitivity = 0.0
        self._threshold = 0.0
        self._dirty_dynamic_range = 0.0
        self._DR_correction_factor = 1.0
        self._maxEDR_used = False
        self._image_min = 0.0
        self._image_max = 0.0
        self._image_rms = 0.0
        self._image_rms_min = 0.0
        self._image_rms_max = 0.0
        self._image_robust_rms_and_spectra = None
        # Temporarily needed until CAS-8576 is fixed
        self._residual_max = 0.0
        self._tclean_stopcode = 0
        self._tclean_stopreason = None
        self._tclean_iterdone = 0
        # This should be automatic, but it does not yet work
        self.pipeline_casa_task = 'Tclean'
        # The tclean command for the weblog renderer
        self._tclean_command = 'tclean()'
        # Dummy settings for the weblog renderer
        self.results = [self]
        self.targets = ['']
        self.warning = None
        self.error = None
        # Used to make simple telescope-dependent decisions about weblog output
        self.imaging_mode = imaging_mode
        self.per_spw_cont_sensitivities_all_chan = None
        self.check_source_fit = None
        self.cube_all_cont = False
        self.bad_psf_channels = None
        self.is_per_eb = is_per_eb
        self.is_eph_obj = is_eph_obj
        # Store computed synthesized beams
        self.synthesized_beams = None
        # Store visibility amplitude ratio for VLA
        self.bl_ratio = None

    def merge_with_context(self, context):
        # Calculated beams for later stages
        if self.synthesized_beams is not None:
            if 'recalc' in self.synthesized_beams:
                context.synthesized_beams = copy.deepcopy(self.synthesized_beams)
                del context.synthesized_beams['recalc']
            else:
                utils.update_beams_dict(context.synthesized_beams, self.synthesized_beams)

        # Calculated sensitivities for later stages
        if self.per_spw_cont_sensitivities_all_chan is not None:
            if 'recalc' in result.per_spw_cont_sensitivities_all_chan:
                context.per_spw_cont_sensitivities_all_chan = copy.deepcopy(self.per_spw_cont_sensitivities_all_chan)
                del context.per_spw_cont_sensitivities_all_chan['recalc']
            else:
                utils.update_sens_dict(context.per_spw_cont_sensitivities_all_chan, self.per_spw_cont_sensitivities_all_chan)

        # Remove heuristics objects to avoid accumulating large amounts of unnecessary memory
        try:
            del self.inputs['image_heuristics']
        except:
            pass

    def empty(self):
        return not(self._psf or self._model or self._flux or 
          self.iterations!={})

    # this is used to generate a pipeline product, not used by weblog
    @property
    def imageplot(self):
        iters = sorted(self.iterations.keys())
        image = self.iterations[iters[-1]].get('image', None)
        imageplot = sky.plotfilename(image=image, reportdir=self.plotdir)
        return imageplot

    @property
    def flux(self):
        return self._flux

    def set_flux(self, image):
        if self._flux is None:
            self._flux = image

    @property
    def cleanmask(self, iter, image):
        iters = sorted(self.iterations.keys())
        if len(iters) > 0:
            return self.iterations[iters[-1]].get('cleanmask', None)
        else:
            return None

    def set_cleanmask(self, iter, image):
        self.iterations[iter]['cleanmask'] = image

    @property
    def imaging_params(self, iteration):
        return self.iterations[iteration].get('imaging_params', None)

    def set_imaging_params(self, iteration, imaging_parameters):
        self.iterations[iteration]['imaging_params'] = imaging_parameters

    @property
    def image(self):
        iters = sorted(self.iterations.keys())
        if len(iters) > 0:
            return self.iterations[iters[-1]].get('image', None)
        else:
            return None

    def set_image(self, iter, image):
        self.iterations[iter]['image'] = image

    @property
    def model(self):
        iters = sorted(self.iterations.keys())
        if len(iters) > 0:
            return self.iterations[iters[-1]].get('model', None)
        else:
            return None

    def set_model(self, iter, image):
        self.iterations[iter]['model'] = image

    @property
    def mom0_fc(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom0_fc')

    def set_mom0_fc(self, iter, image):
        '''
        Sets name of moment 0 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom0_fc'] = image

    @property
    def mom8_fc(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc')

    def set_mom8_fc(self, iter, image):
        '''
        Sets name of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc'] = image

    @property
    def mom8_fc_image_min(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_image_min')

    def set_mom8_fc_image_min(self, iter, image_min):
        '''
        Sets image minimum of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_image_min'] = image_min

    @property
    def mom8_fc_image_max(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_image_max')

    def set_mom8_fc_image_max(self, iter, image_max):
        '''
        Sets image maximum of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_image_max'] = image_max

    @property
    def mom8_fc_image_median_all(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_image_median_all')

    def set_mom8_fc_image_median_all(self, iter, image_median_all):
        '''
        Sets image median of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_image_median_all'] = image_median_all

    @property
    def mom8_fc_image_median_annulus(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_image_median_annulus')

    def set_mom8_fc_image_median_annulus(self, iter, image_median_annulus):
        '''
        Sets image annulus median of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_image_median_annulus'] = image_median_annulus

    @property
    def mom8_fc_image_mad(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_image_mad')

    def set_mom8_fc_image_mad(self, iter, image_mad):
        '''
        Sets image MAD of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_image_mad'] = image_mad

    @property
    def mom8_fc_cube_sigma(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_cube_sigma')

    def set_mom8_fc_cube_sigma(self, iter, cube_sigma):
        '''
        Sets sigma of cube computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_cube_sigma'] = cube_sigma

    @property
    def mom8_fc_cube_chanScaledMAD(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_cube_chanScaledMAD')

    def set_mom8_fc_cube_chanScaledMAD(self, iter, cube_chanScaledMAD):
        '''
        Sets channel scaled MAD of cube computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_cube_chanScaledMAD'] = cube_chanScaledMAD

    @property
    def mom8_fc_peak_snr(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_peak_snr')

    def set_mom8_fc_peak_snr(self, iter, peak_snr):
        '''
        Sets peak SNR of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_peak_snr'] = peak_snr

    @property
    def mom8_fc_outlier_threshold(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_outlier_threshold')

    def set_mom8_fc_outlier_threshold(self, iter, outlier_threshold):
        '''
        Sets outlier threshold of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_outlier_threshold'] = outlier_threshold

    @property
    def mom8_fc_n_pixels(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_n_pixels')

    def set_mom8_fc_n_pixels(self, iter, n_pixels):
        '''
        Sets number of unmasked pixels of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_n_pixels'] = n_pixels

    @property
    def mom8_fc_n_outlier_pixels(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8_fc_n_outlier_pixels')

    def set_mom8_fc_n_outlier_pixels(self, iter, n_outlier_pixels):
        '''
        Sets number of unmasked outlier pixels of moment 8 image computed from line-free channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8_fc_n_outlier_pixels'] = n_outlier_pixels

    @property
    def mom0(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom0')

    def set_mom0(self, iter, image):
        '''
        Sets name of moment 0 image computed from all channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom0'] = image

    @property
    def mom8(self):
        iters = sorted(self.iterations.keys())
        return self.iterations[iters[-1]].get('mom8')

    def set_mom8(self, iter, image):
        '''
        Sets name of moment 8 image computed from all channels of non-primary beam corrected cube
        image for iter iteration step.
        '''
        self.iterations[iter]['mom8'] = image

    @property
    def psf(self):
        return self._psf

    def set_psf(self, image):
        if self._psf is None:
            self._psf = image

    @property
    def residual(self):
        iters = sorted(self.iterations.keys())
        if len(iters) > 0:
            return self.iterations[iters[-1]].get('residual', None)
        else:
            return None

    def set_residual(self, iter, image):
        self.iterations[iter]['residual'] = image

    @property
    def pblimit_image(self):
        return self._pblimit_image

    def set_pblimit_image(self, pblimit_image):
        self._pblimit_image = pblimit_image

    @property
    def pblimit_cleanmask(self):
        return self._pblimit_cleanmask

    def set_pblimit_cleanmask(self, pblimit_cleanmask):
        self._pblimit_cleanmask = pblimit_cleanmask

    @property
    def aggregate_bw(self):
        return self._aggregate_bw

    def set_aggregate_bw(self, aggregate_bw):
        self._aggregate_bw = aggregate_bw

    @property
    def eff_ch_bw(self):
        return self._eff_ch_bw

    def set_eff_ch_bw(self, eff_ch_bw):
        self._eff_ch_bw = eff_ch_bw

    @property
    def sensitivity(self):
        return self._sensitivity

    def set_sensitivity(self, sensitivity):
        self._sensitivity = sensitivity

    @property
    def dr_corrected_sensitivity(self):
        return self._dr_corrected_sensitivity

    def set_dr_corrected_sensitivity(self, dr_corrected_sensitivity):
        self._dr_corrected_sensitivity = dr_corrected_sensitivity

    @property
    def threshold(self):
        return self._threshold

    def set_threshold(self, threshold):
        self._threshold = threshold

    @property
    def dirty_dynamic_range(self):
        return self._dirty_dynamic_range

    def set_dirty_dynamic_range(self, dirty_dynamic_range):
        self._dirty_dynamic_range = dirty_dynamic_range

    @property
    def DR_correction_factor(self):
        return self._DR_correction_factor

    def set_DR_correction_factor(self, DR_correction_factor):
        self._DR_correction_factor = DR_correction_factor

    @property
    def maxEDR_used(self):
        return self._maxEDR_used

    def set_maxEDR_used(self, maxEDR_used):
        self._maxEDR_used = maxEDR_used

    @property
    def image_min(self):
        return self._image_min

    def set_image_min(self, image_min):
        self._image_min = image_min

    @property
    def image_max(self):
        return self._image_max

    def set_image_max(self, image_max):
        self._image_max = image_max

    @property
    def image_rms(self):
        return self._image_rms

    def set_image_rms(self, image_rms):
        self._image_rms = image_rms

    @property
    def image_rms_min(self):
        return self._image_rms_min

    def set_image_rms_min(self, image_rms_min):
        self._image_rms_min = image_rms_min

    @property
    def image_rms_max(self):
        return self._image_rms_max

    def set_image_rms_max(self, image_rms_max):
        self._image_rms_max = image_rms_max

    @property
    def image_robust_rms_and_spectra(self):
        return self._image_robust_rms_and_spectra

    def set_image_robust_rms_and_spectra(self, image_robust_rms_and_spectra):
        self._image_robust_rms_and_spectra = image_robust_rms_and_spectra

    @property
    def tclean_command(self):
        return self._tclean_command

    def set_tclean_command(self, tclean_command):
        self._tclean_command = tclean_command

    @property
    def tclean_stopcode(self):
        return self._tclean_stopcode

    def set_tclean_stopcode(self, tclean_stopcode):
        self._tclean_stopcode = tclean_stopcode

    @property
    def tclean_stopreason(self):
        return self._tclean_stopreason

    def set_tclean_stopreason(self, tclean_stopcode):
        stopreasons = ['iteration limit',
                       'threshold',
                       'force stop',
                       'no change in peak residual across two major cycles',
                       'peak residual increased by more than 3 times from the previous major cycle',
                       'peak residual increased by more than 3 times from the minimum reached',
                       'zero mask',
                       'any combination of n-sigma and other valid exit criterion']
        assert 0 < tclean_stopcode <= len(stopreasons),\
            "tclean stop code {} does not index into stop reasons list".format(tclean_stopcode)
        self._tclean_stopreason = stopreasons[tclean_stopcode-1]

    @property
    def tclean_iterdone(self):
        return self._tclean_iterdone

    def set_tclean_iterdone(self, tclean_iterdone):
        self._tclean_iterdone = tclean_iterdone

    @property
    def nmajordone(self, iteration):
        return self.iterations[iteration].get('nmajordone', None)

    def set_nmajordone(self, iteration, nmajordone):
        self.iterations[iteration]['nmajordone'] = nmajordone

    @property
    # Cumulative minor iteration array
    def nminordone_array(self, iteration):
        return self.iterations[iteration].get('nminordone_array', None)

    def set_nminordone_array(self, iteration, nminordone_array):
        self.iterations[iteration]['nminordone_array'] = nminordone_array

    @property
    # Cleaned peak RMS as a function of minor iteration number
    def peakrms_array(self, iteration):
        return self.iterations[iteration].get('peakrms_array', None)

    def set_peakrms_array(self, iteration, peakrms_array):
        self.iterations[iteration]['peakrms_array'] = peakrms_array

    @property
    # Total cleaned flux as a function of minor iteration number
    def totalflux_array(self, iteration):
        return self.iterations[iteration].get('totalflux_array', None)

    def set_totalflux_array(self, iteration, totalflux_array):
        self.iterations[iteration]['totalflux_array'] = totalflux_array

    @property
    # Fractional flux outside of clean mask
    def outmaskratio(self):
        iters = sorted(self.iterations.keys())
        if len(iters) > 0:
            return self.iterations[iters[-1]].get('outmaskratio', None)
        else:
            return None

    def set_outmaskratio(self, iteration, outmaskratio):
        self.iterations[iteration]['outmaskratio'] = outmaskratio

    def __repr__(self):
        repr = 'Tclean:\n'
        if self._psf is not None:
            repr += ' psf: %s\n' % os.path.basename(self._psf)
        else:
            repr += ' psf: None'
        if self._flux is not None:
            repr += ' flux: %s\n' % os.path.basename(self._flux)
        else:
            repr += ' flux: None'

        items_to_print = ['image', 'residual', 'model', 'cleanmask', 'mom0_fc']
        str_len = max([len(item) for item in items_to_print])
        for k, v in self.iterations.items():
            repr += ' iteration %s:\n' % k
            for item in items_to_print:
                if item in v:
                    repr += '   %s : %s\n' % (item.ljust(str_len), os.path.basename(v[item]))

        return repr
