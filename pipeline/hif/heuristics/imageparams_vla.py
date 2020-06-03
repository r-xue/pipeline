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
        # Determine min and max frequency of selected spectral windows.
        abs_min_frequency = 1.0e15
        abs_max_frequency = 0.0
        msname = self.vislist[0]
        ms = self.observing_run.get_ms(name=msname)
        for spwid in spwspec.split(','):
            real_spwid = self.observing_run.virtual2real_spw_id(spwid, self.observing_run.get_ms(msname))
            spw = ms.get_spectral_window(real_spwid)
            min_frequency = float(spw.min_frequency.to_units(measures.FrequencyUnits.HERTZ))
            if (min_frequency < abs_min_frequency):
                abs_min_frequency = min_frequency
            max_frequency = float(spw.max_frequency.to_units(measures.FrequencyUnits.HERTZ))
            if (max_frequency > abs_max_frequency):
                abs_max_frequency = max_frequency
        # Fractional bandwidth
        if (2.0 * (abs_max_frequency - abs_min_frequency) / (abs_min_frequency + abs_max_frequency) > 0.1):
            return 2
        else:
            return 1

    def deconvolver(self, specmode, spwspec):
        """See PIPE-679 and CASR-543"""
        return 'mtmfs'

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
