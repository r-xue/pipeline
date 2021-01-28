import re

import numpy

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVlassSeCont(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile,
                                       linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CONT'
        # Update it explicitly when populating context.clean_list_pending (i.e. in hif_editimlist)
        self.vlass_stage = 0

    # niter
    def niter_correction(self, niter, cell, imsize, residual_max, threshold, residual_robust_rms, mask_frac_rad=0.0):
        if niter:
            return int(niter)
        else:
            return 20000

    def niter(self):
        return self.niter_correction(None, None, None, None, None, None)

    def deconvolver(self, specmode, spwspec):
        return 'mtmfs'

    def robust(self):
        if self.vlass_stage == 3:
            return 1.0
        else:
            return -2.0

    def gridder(self, intent, field):
        # TODO: should be user switchable between awproject and mosaic
        return 'awproject'

    def cell(self, beam=None, pixperbeam=None):
        return ['0.6arcsec']

    def imsize(self, fields=None, cell=None, primary_beam=None, sfpblimit=None, max_pixels=None, centreonly=None,
               vislist=None, spwspec=None):
        return [16384, 16384]

    def reffreq(self):
        return '3.0GHz'

    def cyclefactor(self, iteration):
        return 3.0

    def cycleniter(self, iteration):
        if self.vlass_stage == 3 and iteration > 0:
            return 3000
        else:
            return 5000

    def scales(self, iteration=None):
        if self.vlass_stage == 3 and iteration and iteration > 1:
            return [0, 5, 12]
        else:
            return [0]

    def uvtaper(self, beam_natural=None, protect_long=None):
        if self.vlass_stage == 3:
            return ''
        else:
            return '3arcsec'

    def uvrange(self, field=None, spwspec=None):
        return '<12km', None

    def mask(self, results_list=None):
        """Tier-1 mask name to be used for computing Tier-1 and Tier-2 combined mask.

            Obtain the mask name from the latest MakeImagesResult object in context.results.
            If not found, then return empty string (as base heuristics)."""
        if results_list and type(results_list) is list:
            for result in results_list:
                result_meta = result.read()
                if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                        'hifv_vlassmasking'):
                    return [r.combinedmask for r in result_meta][0]

        # In case hif_makeimages result was not found or results_list was not provided
        return ""

    def buffer_radius(self):
        return 1000.

    def specmode(self):
        return 'mfs'

    def intent(self):
        return 'TARGET'

    def nterms(self, spwspec):
        return 2

    def stokes(self):
        return 'I'

    def pb_correction(self):
        return False

    def conjbeams(self):
        return True

    def get_sensitivity(self, ms_do, field, intent, spw, chansel, specmode, cell, imsize, weighting, robust, uvtaper):
        return 0.0, None, None

    def find_fields(self, distance='0deg', phase_center=None, matchregex=''):

        # Created STM 2016-May-16 use center direction measure
        # Returns list of fields from msfile within a rectangular box of size distance

        qa = casa_tools.quanta
        me = casa_tools.measures
        tb = casa_tools.table

        msfile = self.vislist[0]

        fieldlist = []

        phase_center = phase_center.split()
        center_dir = me.direction(phase_center[0], phase_center[1], phase_center[2])
        center_ra = center_dir['m0']['value']
        center_dec = center_dir['m1']['value']

        try:
            qdist = qa.toangle(distance)
            qrad = qa.convert(qdist, 'rad')
            maxrad = qrad['value']
        except:
            print('ERROR: cannot parse distance {}'.format(distance))
            return

        try:
            tb.open(msfile + '/FIELD')
        except:
            print('ERROR: could not open {}/FIELD'.format(msfile))
            return
        field_dirs = tb.getcol('PHASE_DIR')
        field_names = tb.getcol('NAME')
        tb.close()

        (nd, ni, nf) = field_dirs.shape
        print('Found {} fields'.format(nf))

        # compile field dictionaries
        ddirs = {}
        flookup = {}
        for i in range(nf):
            fra = field_dirs[0, 0, i]
            fdd = field_dirs[1, 0, i]
            rapos = qa.quantity(fra, 'rad')
            decpos = qa.quantity(fdd, 'rad')
            ral = qa.angle(rapos, form=["tim"], prec=9)
            decl = qa.angle(decpos, prec=10)
            fdir = me.direction('J2000', ral[0], decl[0])
            ddirs[i] = {}
            ddirs[i]['ra'] = fra
            ddirs[i]['dec'] = fdd
            ddirs[i]['dir'] = fdir
            fn = field_names[i]
            ddirs[i]['name'] = fn
            if fn in flookup:
                flookup[fn].append(i)
            else:
                flookup[fn] = [i]
        print('Cataloged {} fields'.format(nf))

        # Construct offset separations in ra,dec
        print('Looking for fields with maximum separation {}'.format(distance))
        nreject = 0
        skipmatch = matchregex == '' or matchregex == []
        for i in range(nf):
            dd = ddirs[i]['dir']
            dd_ra = dd['m0']['value']
            dd_dec = dd['m1']['value']
            sep_ra = abs(dd_ra - center_ra)
            if sep_ra > numpy.pi:
                sep_ra = 2.0 * numpy.pi - sep_ra
            # change the following to use dd_dec 2017-02-06
            sep_ra_sky = sep_ra * numpy.cos(dd_dec)

            sep_dec = abs(dd_dec - center_dec)

            ddirs[i]['offset_ra'] = sep_ra_sky
            ddirs[i]['offset_ra'] = sep_dec

            if sep_ra_sky <= maxrad:
                if sep_dec <= maxrad:
                    if skipmatch:
                        fieldlist.append(i)
                    else:
                        # test regex against name
                        foundmatch = False
                        fn = ddirs[i]['name']
                        for rx in matchregex:
                            mat = re.findall(rx, fn)
                            if len(mat) > 0:
                                foundmatch = True
                        if foundmatch:
                            fieldlist.append(i)
                        else:
                            nreject += 1

        print('Found {} fields within {}'.format(len(fieldlist), distance))
        if not skipmatch:
            print('Rejected {} distance matches for regex'.format(nreject))

        return fieldlist

    def keep_iterating(self, iteration, hm_masking, tclean_stopcode, dirty_dynamic_range, residual_max,
                       residual_robust_rms, field, intent, spw, specmode):
        """Determine if another tclean iteration is necessary."""

        if iteration == 0:
            return True, 'auto'
        elif iteration == 1:
            LOG.info('Final VLASS single epoch tclean call with no mask')
            return True, 'user'
        else:
            return False, 'user'

    def threshold(self, iteration, threshold, hm_masking):

        if hm_masking == 'auto':
            return '0.0mJy'
        elif hm_masking == 'none':
            if iteration in [0, 1]:
                return threshold
            else:
                return '0.0mJy'
        else:
            return threshold

    def nsigma(self, iteration, hm_nsigma):

        if hm_nsigma:
            return hm_nsigma

        if iteration == 0:
            return 0
        elif iteration == 1:
            return 2.0
        else:
            return 4.5

    def savemodel(self, iteration):
        # Model is saved in first imaging cycle last iteration
        if self.vlass_stage == 1 and iteration == 1:
            return 'modelcolumn'
        else:
            return 'none'

    def datacolumn(self):
        """Column parameter to be used as tclean argument
        """
        # First imaging stage use data column
        if self.vlass_stage == 1:
            return 'data'
        # Subsequent stages use the self-calibrated and corrected column
        else:
            return 'corrected'

    def wprojplanes(self):
        return 32

    def rotatepastep(self):
        return 5.0

    def get_autobox_params(self, iteration, intent, specmode, robust):

        """Default auto-boxing parameters."""

        sidelobethreshold = None
        noisethreshold = None
        lownoisethreshold = None
        negativethreshold = None
        if iteration == 0:
            minbeamfrac = 0.3
        else:
            minbeamfrac = 0.1
        growiterations = None
        dogrowprune = None
        minpercentchange = None
        fastnoise = None

        return (sidelobethreshold, noisethreshold, lownoisethreshold, negativethreshold, minbeamfrac,
                growiterations, dogrowprune, minpercentchange, fastnoise)

    def usepointing(self):
        """clean flag to use pointing table."""

        return True

    def get_cfcaches(self, cfcache: str):
        """Parses comma separated cfcache string

        Used to input wide band and non-wide band cfcaches at the same time in
        VLASS-SE-CONT imaging mode.
        """
        if ',' in cfcache:
            return [cfch.strip() for cfch in cfcache.split(',')]
        else:
            return cfcache, None

    def smallscalebias(self):
        """A numerical control to bias the scales when using multi-scale or mtmfs algorithms"""
        return 0.4

    def restoringbeam(self):
        """Tclean parameter"""
        return ['']

    def pointingoffsetsigdev(self):
        """Tclean parameter"""
        return [300, 30]


class ImageParamsHeuristicsVlassSeContAWPP001(ImageParamsHeuristicsVlassSeCont):
    """
    Special heuristics case when gridder is awproject and the wprojplanes parameter
    is set to 1 (in parent class it is 32).
    """

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristicsVlassSeCont.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params,
                                                  contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CONT-AWP-P001'
        # Update it explicitly when populating context.clean_list_pending (i.e. in hif_editimlist)
        self.vlass_stage = 0

    def wprojplanes(self):
        return 1

    def gridder(self, intent, field):
        return 'awproject'
