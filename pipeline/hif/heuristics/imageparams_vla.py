import re

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.filenamer as filenamer

from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVLA(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile,
                                       linesfile, imaging_params)
        self.imaging_mode = 'VLA'

    def robust(self):
        return 0.5

    def uvtaper(self, beam_natural=None, protect_long=None):
        return []

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
            abs_min_frequency, abs_max_frequency = self.get_min_max_freq(spwspec)
            # 18 GHz and above (Ku, K, Ka, Q VLA bands)
            if abs_min_frequency >= 1.8e10:
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
