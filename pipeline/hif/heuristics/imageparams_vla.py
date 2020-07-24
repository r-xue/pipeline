import re

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.casatools as casatools
from pipeline.infrastructure import casa_tasks
import pipeline.domain.measures as measures

from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVLA(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile,
                                       linesfile, imaging_params)
        self.imaging_mode = 'VLA'

    def robust(self):
        """See PIPE-680 and CASR-543"""
        return 0.5

    def uvtaper(self, beam_natural=None, protect_long=None):
        return []

    def uvrange(self):
        """
        Restrict uvrange in case of very extended emission.

        If the amplitude of the shortest 5 per cent of the covered baselines
        is more than 2 times that of the 50-55 per cent baselines, then exclude
        the shortest 5 per cent of the baselines.

        See PIPE-681 and CASR-543.

        :return: None or string in the form of '> {x}klambda', where
            {x}=0.05*max(baseline)
        """
        def get_mean_amplitude(vis, uvrange=None, axis='amplitude', spw=None):
            stat_arg = {'vis': vis, 'uvrange': uvrange, 'axis': axis,
                        'useflags': True, 'spw': spw}
            job = casa_tasks.visstat(**stat_arg)
            stats = job.execute(dry_run=False)  # returns stat in meter

            # loop through keys to determine average from all spws
            mean = 0.0
            for k, v in stats.items():
                mean = mean + v['mean']

            return mean / float(len(stats))
        #
        qa = casatools.quanta
        #
        LOG.info('Computing uvrange heuristics for spwsids={:s}'.format(','.join([str(spw) for spw in self.spwids])))

        # Can it be that more than one visibility (ms file) is used?
        vis = self.vislist[0]
        ms = self.observing_run.get_ms(vis)

        # Determine the largest covered baseline in klambda. Assume that
        # the maximum baseline is associated with the highest frequency spw.
        light_speed = qa.getvalue(qa.convert(qa.constants('c'), 'm/s'))[0]
        max_mean_freq_Hz = 0.0   # spw mean frequency in the highest frequency spw
        real_spwids = []
        for spwid in self.spwids:
            real_spwid = self.observing_run.virtual2real_spw_id(spwid, ms)
            spw = ms.get_spectral_window(real_spwid)
            real_spwids.append(real_spwid)
            mean_freq_Hz = spw.mean_frequency.to_units(measures.FrequencyUnits.HERTZ)
            if float(mean_freq_Hz) > max_mean_freq_Hz:
                max_mean_freq_Hz = float(mean_freq_Hz)
                max_freq_spw = real_spwid
        # List of real spws
        real_spwids_str = ','.join([str(spw) for spw in real_spwids])

        # Check for maximum frequency
        if max_mean_freq_Hz == 0.0:
            LOG.warn("Highest frequency spw and largest baseline cannot be determined for spwids={:s}. "
                     "Using default uvrange.".format(','.join([str(spw) for spw in self.spwids])))
            return None
        # Get max baseline
        mean_wave_m = light_speed / max_mean_freq_Hz  # in meter
        job = casa_tasks.visstat(vis=vis, spw=str(max_freq_spw), axis='uvrange', useflags=True)
        uv_stat = job.execute(dry_run=False) # returns stat in meter
        max_bl = uv_stat['DATA_DESC_ID=%s' % max_freq_spw]['max'] / mean_wave_m

        # Define bin for lowest 5% of baselines (in wavelength units)
        uvll = 0.0
        uvul = 0.05 * max_bl

        uvrange_SBL = '{:0.1f}~{:0.1f}klambda'.format(uvll / 1000.0, uvul / 1000.0)
        mean_SBL = get_mean_amplitude(vis=vis, uvrange=uvrange_SBL, spw=real_spwids_str)

        # Range for  50-55% bin
        uvll = 0.5 * max_bl
        uvul = 0.5 * max_bl + 0.05 * max_bl
        uvrange_MBL = '{:0.1f}~{:0.1f}klambda'.format(uvll / 1000.0, uvul / 1000.0)
        mean_MBL = get_mean_amplitude(vis=vis, uvrange=uvrange_MBL, spw=real_spwids_str)

        # Compare amplitudes and decide on return value
        meanratio = mean_SBL / mean_MBL

        # Report results
        LOG.info('Mean amplitude in uvrange bins: {:s} is {:0.2E}Jy, '
                 '{:s} is {:0.2E}Jy, ratio={:0.2E}'.format(uvrange_SBL, mean_SBL, uvrange_MBL, mean_MBL, meanratio))
        if meanratio > 2.0:
            LOG.info('Selecting uvrange>{:0.1f}klambda to avoid very extended emission.'.format(0.05 * max_bl / 1000.0))
            return ">{:0.1f}klambda".format(0.05 * max_bl / 1000.0)
        else:
            # Use complete uvrange
            return '>0.0klambda'

    def pblimits(self, pb):
        """
        PB gain level at which to cut off normalizations (tclean parameter).

        See PIPE-674 and CASR-543
        """
        pblimit_image = -0.1
        pblimit_cleanmask = 0.3   # default value from base class in case pb=None

        return pblimit_image, pblimit_cleanmask

    def nterms(self, spwspec):
        """
        Determine nterms depending on the fractional bandwidth.
        Returns 1 if the fractional bandwidth is < 10 per cent, 2 otherwise.

        See PIPE-679 and CASR-543
        """
        if spwspec is None:
            return None
        # Fractional bandwidth
        fr_bandwidth = self.get_fractional_bandwidth(spwspec)
        if (fr_bandwidth >= 0.1):
            return 2
        else:
            return 1

    def deconvolver(self, specmode, spwspec):
        """See PIPE-679 and CASR-543"""
        return 'mtmfs'

    def niter_correction(self, niter, cell, imsize, residual_max, threshold, mask_frac_rad=0.0):
        """Adjustment of number of cleaning iterations due to mask size.

        See PIPE-682 and CASR-543 and base class method for parameter description."""
        if mask_frac_rad == 0.0:
            # Assume at most 25% of pixels are within the (circular) mask (PIPE-682).
            # 0.25 = mask_frac_rad**2 * pi / 4
            mask_frac_rad = 0.56

        return super().niter_correction(niter, cell, imsize, residual_max, threshold,
                                        mask_frac_rad=mask_frac_rad)

    def specmode(self):
        """See PIPE-683 and CASR-543"""
        return 'cont'

    def nsigma(self, iteration, hm_nsigma):
        """See PIPE-678 and CASR-543"""
        if hm_nsigma:
            return hm_nsigma
        else:
            return 5.0

    def threshold(self, iteration, threshold, hm_masking):
        """See PIPE-678 and CASR-543"""
        if iteration == 0 or hm_masking in ['none']:
            return '0.0mJy'
        else:
            return threshold

    def imsize(self, fields, cell, primary_beam, sfpblimit=None, max_pixels=None,
               centreonly=False, vislist=None, spwspec=None):
        """
        Image size heuristics for single fields and mosaics. The pixel count along x and y image dimensions
        is determined by the cell size, primary beam size and the spread of phase centers in case of mosaics.

        Frequency dependent image size may be computed for VLA imaging.

        For single fields, 18 GHz and above FOV extends to the first minimum of the primary beam Airy pattern.
        Below 18 GHz, FOV extends to the second minimum (incorporating the first sidelobes).

        See PIPE-675 and CASR-543

        :param fields: list of comma separated strings of field IDs per MS.
        :param cell: pixel (cell) size in arcsec.
        :param primary_beam: primary beam width in arcsec.
        :param sfpblimit: single field primary beam response. If provided then imsize is chosen such that the image
            edge is at normalised primary beam level equals to sfpblimit.
        :param max_pixels: maximum allowed pixel count, integer. The same limit is applied along both image axes.
        :param centreonly: if True, then ignore the spread of field centers.
        :param vislist: list of visibility path string to be used for imaging. If not set then use all visibilities
            in the context.
        :param spwspec: ID list of spectral windows used to create image product. List or string containing comma
            separated spw IDs list.
        :return: two element list of pixel count along x and y image axes.
        """
        if spwspec is not None:
            if type(spwspec) is not str:
                spwspec = ",".join(spwspec)
            freq_limits = self.get_min_max_freq(spwspec)
            # 18 GHz and above (K, Ka, Q VLA bands)
            if freq_limits['abs_min_freq'] >= 1.79e10:
                # equivalent to first minimum of the Airy diffraction pattern; m = 1.22.
                sfpblimit = 0.294
            else:
                # equivalent to second minimum of the Airy diffraction pattern; m = 2.233 in theta = m*lambda/D
                sfpblimit = 0.016

        return super().imsize(fields, cell, primary_beam, sfpblimit=sfpblimit, max_pixels=max_pixels,
                              centreonly=centreonly, vislist=vislist)

    def imagename(self, output_dir=None, intent=None, field=None, spwspec=None, specmode=None, band=None):
        try:
            nameroot = self.imagename_prefix
            if nameroot == 'unknown':
                nameroot = 'oussid'
            # need to sanitize the nameroot here because when it's added
            # to filenamer as an asdm, os.path.basename is run on it with
            # undesirable results.
            nameroot = filenamer.sanitize(nameroot)
        except:
            nameroot = 'oussid'
        namer = filenamer.Image()
        namer._associations.asdm(nameroot)

        if output_dir:
            namer.output_dir(output_dir)

        namer.stage('STAGENUMBER')
        if intent:
            namer.intent(intent)
        if field:
            namer.source(field)
        if specmode != 'cont' and spwspec:
            # find all the spwids present in the list
            p = re.compile(r"[ ,]+(\d+)")
            spwids = p.findall(' %s' % spwspec)
            spwids = list(set(spwids))
            spw = '_'.join(map(str, sorted(map(int, spwids))))
            namer.spectral_window(spw)
        if specmode == 'cont' and band:
            namer.band('{}_band'.format(band))
        if specmode:
            namer.specmode(specmode)

        # filenamer returns a sanitized filename (i.e. one with
        # illegal characters replace by '_'), no need to check
        # the name components individually.
        imagename = namer.get_filename()
        return imagename
