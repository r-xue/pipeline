import os

import numpy as np

import pipeline as pipeline
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.imageheader as imageheader
from pipeline import environment
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from .resultobjects import TcleanResult

LOG = infrastructure.get_logger(__name__)

# The basic clean tasks classes. Clean performs a single clean run.


class CleanBaseInputs(vdp.StandardInputs):

    # simple properties ------------------------------------------------------------------------------------------------

    antenna = vdp.VisDependentProperty(default='')
    datacolumn = vdp.VisDependentProperty(default='')
    deconvolver = vdp.VisDependentProperty(default='')
    cycleniter = vdp.VisDependentProperty(default=-999)
    cyclefactor = vdp.VisDependentProperty(default=-999.0)
    cfcache = vdp.VisDependentProperty(default='')
    field = vdp.VisDependentProperty(default='')
    gridder = vdp.VisDependentProperty(default='')
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='')
    iter = vdp.VisDependentProperty(default=0)
    mask = vdp.VisDependentProperty(default='')
    hm_dogrowprune = vdp.VisDependentProperty(default=True)
    hm_growiterations = vdp.VisDependentProperty(default=-999)
    hm_lownoisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_masking = vdp.VisDependentProperty(default='auto')
    hm_minbeamfrac = vdp.VisDependentProperty(default=-999.0)
    hm_minpercentchange = vdp.VisDependentProperty(default=-999.0)
    hm_minpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_maxpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_fastnoise = vdp.VisDependentProperty(default=True)
    hm_negativethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_noisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_sidelobethreshold = vdp.VisDependentProperty(default=-999.0)
    mosweight = vdp.VisDependentProperty(default=None)
    nchan = vdp.VisDependentProperty(default=-1)
    niter = vdp.VisDependentProperty(default=5000)
    hm_nsigma = vdp.VisDependentProperty(default=0.0)
    hm_perchanweightdensity = vdp.VisDependentProperty(default=False)
    hm_npixels = vdp.VisDependentProperty(default=0)
    nterms = vdp.VisDependentProperty(default=None)
    orig_specmode = vdp.VisDependentProperty(default='')
    outframe = vdp.VisDependentProperty(default='LSRK')
    parallel = vdp.VisDependentProperty(default='automatic')
    pblimit = vdp.VisDependentProperty(default=0.2)
    is_per_eb = vdp.VisDependentProperty(default=False)
    phasecenter = vdp.VisDependentProperty(default='')
    restoringbeam = vdp.VisDependentProperty(default='common')
    robust = vdp.VisDependentProperty(default=-999.0)
    savemodel = vdp.VisDependentProperty(default='none')
    scales = vdp.VisDependentProperty(default=None)
    sensitivity = vdp.VisDependentProperty(default=None)
    spwsel_all_cont = vdp.VisDependentProperty(default=None)
    start = vdp.VisDependentProperty(default='')
    stokes = vdp.VisDependentProperty(default='I')
    threshold = vdp.VisDependentProperty(default=None)
    usepointing = vdp.VisDependentProperty(default=None)
    uvrange = vdp.VisDependentProperty(default='')
    uvtaper = vdp.VisDependentProperty(default=None)
    weighting = vdp.VisDependentProperty(default='briggs')
    width = vdp.VisDependentProperty(default='')
    restfreq = vdp.VisDependentProperty(default=None)
    wprojplanes = vdp.VisDependentProperty(default=None)
    wbawp = vdp.VisDependentProperty(default=None)
    rotatepastep = vdp.VisDependentProperty(default=None)
    calcpsf = vdp.VisDependentProperty(default=None)
    calcres = vdp.VisDependentProperty(default=None)
    pbmask = vdp.VisDependentProperty(default=None)

    # properties requiring some logic ----------------------------------------------------------------------------------

    @vdp.VisDependentProperty
    def cell(self):
        return []

    @vdp.VisDependentProperty
    def imsize(self):
        return []

    @imsize.convert
    def imsize(self, value):
        if isinstance(value, str) and value.startswith('['):
            # Remove the characters [, ], and ' from the value.
            temp = value.translate(str.maketrans("[]'"))
            temp = temp.split(',')
            return list(map(int, temp))
        return value

    @vdp.VisDependentProperty
    def specmode(self, value):
        if 'TARGET' in self.intent:
            return 'cube'
        return 'mfs'

    @vdp.VisDependentProperty
    def spw(self):
        first_ms = self.context.observing_run.measurement_sets[0]
        return ','.join([spw.id for spw in first_ms.get_spectral_windows()])

    @vdp.VisDependentProperty
    def spwsel(self):
        return []

    def __init__(self, context, output_dir=None, vis=None, imagename=None, datacolumn=None, intent=None, field=None,
                 spw=None, spwsel=None, spwsel_all_cont=None, uvrange=None, orig_specmode=None, specmode=None, gridder=None, deconvolver=None,
                 uvtaper=None, nterms=None, cycleniter=None, cyclefactor=None, hm_minpsffraction=None,
                 hm_maxpsffraction=None, scales=None, outframe=None, imsize=None,
                 cell=None, phasecenter=None, nchan=None, start=None, width=None, stokes=None, weighting=None,
                 robust=None, restoringbeam=None, iter=None, mask=None, savemodel=None, hm_masking=None,
                 hm_sidelobethreshold=None, hm_noisethreshold=None, hm_lownoisethreshold=None, wprojplanes=None,
                 hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None, hm_dogrowprune=None,
                 hm_minpercentchange=None, hm_fastnoise=None, pblimit=None, niter=None, hm_nsigma=None,
                 hm_perchanweightdensity=None, hm_npixels=None, threshold=None, sensitivity=None, reffreq=None,
                 restfreq=None, conjbeams=None, is_per_eb=None, antenna=None, usepointing=None, mosweight=None,
                 result=None, parallel=None, heuristics=None, rotatepastep=None, cfcache=None, calcpsf=None,
                 calcres=None, wbawp=None, pbmask=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.imagename = imagename
        self.datacolumn = datacolumn
        self.intent = intent
        self.field = field
        self.spw = spw
        self.spwsel = spwsel
        self.spwsel_all_cont = spwsel_all_cont
        self.uvrange = uvrange
        self.savemodel = savemodel
        self.orig_specmode = orig_specmode
        self.specmode = specmode
        self.gridder = gridder
        self.deconvolver = deconvolver
        self.uvtaper = uvtaper
        self.nterms = nterms
        self.cycleniter = cycleniter
        self.cyclefactor = cyclefactor
        self.hm_minpsffraction = hm_minpsffraction
        self.hm_maxpsffraction = hm_maxpsffraction
        self.scales = scales
        self.outframe = outframe
        self.imsize = imsize
        self.cell = cell
        self.phasecenter = phasecenter
        self.nchan = nchan
        self.start = start
        self.width = width
        self.stokes = stokes
        self.weighting = weighting
        self.robust = robust
        self.restoringbeam = restoringbeam
        self.iter = iter
        self.mask = mask
        self.pbmask = pbmask

        self.hm_masking = hm_masking
        self.hm_sidelobethreshold = hm_sidelobethreshold
        self.hm_noisethreshold = hm_noisethreshold
        self.hm_lownoisethreshold = hm_lownoisethreshold
        self.hm_negativethreshold = hm_negativethreshold
        self.hm_minbeamfrac = hm_minbeamfrac
        self.hm_growiterations = hm_growiterations
        self.hm_dogrowprune = hm_dogrowprune
        self.hm_minpercentchange = hm_minpercentchange
        self.hm_fastnoise = hm_fastnoise

        self.cfcache = cfcache
        self.pblimit = pblimit
        self.niter = niter
        self.threshold = threshold
        self.hm_nsigma = hm_nsigma
        self.hm_perchanweightdensity = hm_perchanweightdensity
        self.hm_npixels = hm_npixels
        self.sensitivity = sensitivity
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.conjbeams = conjbeams
        self.result = result
        self.parallel = parallel
        self.is_per_eb = is_per_eb
        self.antenna = antenna
        self.usepointing = usepointing
        self.mosweight = mosweight
        self.wprojplanes = wprojplanes
        self.wbawp = wbawp
        self.rotatepastep = rotatepastep
        self.heuristics = heuristics
        self.calcpsf = calcpsf
        self.calcres = calcres


class CleanBase(basetask.StandardTaskTemplate):
    Inputs = CleanBaseInputs

    is_multi_vis_task = True

    def prepare(self):
        context = self.inputs.context
        inputs = self.inputs

        # Make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        # Set the data column
        targetmslist = [vis for vis in inputs.vis if context.observing_run.get_ms(name=vis).is_imaging_ms]
        if not inputs.datacolumn:
            if len(targetmslist) > 0:
                if inputs.specmode == 'cube':
                    inputs.datacolumn = 'corrected'
                else:
                    inputs.datacolumn = 'data'
            else:
                inputs.datacolumn = 'corrected'

        # Remove MSs that do not contain data for the given field(s)
        scanidlist, visindexlist = inputs.heuristics.get_scanidlist(inputs.vis, inputs.field, inputs.intent)
        inputs.vis = [inputs.vis[i] for i in visindexlist]
        inputs.spwsel = [inputs.spwsel[i] for i in visindexlist]

        # Initialize imaging results structure
        if not inputs.result:
            plotdir = os.path.join(inputs.context.report_dir,
                                   'stage%s' % inputs.context.stage.split('_')[0])
            result = TcleanResult(vis=inputs.vis,
                                  sourcename=inputs.field,
                                  intent=inputs.intent,
                                  spw=inputs.spw,
                                  orig_specmode=inputs.orig_specmode,
                                  specmode=inputs.specmode,
                                  multiterm=inputs.nterms if inputs.deconvolver == 'mtmfs' else None,
                                  plotdir=plotdir, imaging_mode=inputs.heuristics.imaging_mode,
                                  is_per_eb=inputs.is_per_eb,
                                  is_eph_obj=inputs.heuristics.is_eph_obj(inputs.field))
        else:
            result = inputs.result

        try:
            result = self._do_clean_cycle(scanidlist, result, iter=inputs.iter)
        except Exception as e:
            LOG.error('%s/%s/spw%s clean error: %s' % (inputs.field, inputs.intent, inputs.spw, str(e)))
            result.error = '%s/%s/spw%s clean error: %s' % (inputs.field, inputs.intent, inputs.spw, str(e))

        return result

    def analyse(self, result):
        return result

    def _do_clean_cycle(self, scanidlist=None, result=None, iter=1):
        """
        Compute a clean image.
        """
        if scanidlist is None:
            scanidlist = []

        context = self.inputs.context
        inputs = self.inputs

        # Derive names of clean products for this iteration
        old_model_name = result.model
        model_name = '%s.%s.iter%s.model' % (inputs.imagename, inputs.stokes, iter)
        if old_model_name is not None:
            if os.path.exists(old_model_name):
                if result.multiterm:
                    rename_image(old_name=old_model_name, new_name=model_name,
                                 extensions=['.tt%d' % nterm for nterm in range(result.multiterm)])
                else:
                    rename_image(old_name=old_model_name, new_name=model_name)

        if inputs.niter == 0 and not (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            image_name = ''
        else:
            image_name = '%s.%s.iter%s.image' % (
                inputs.imagename, inputs.stokes, iter)

        residual_name = '%s.%s.iter%s.residual' % (
            inputs.imagename, inputs.stokes, iter)
        psf_name = '%s.%s.iter%s.psf' % (
            inputs.imagename, inputs.stokes, iter)
        flux_name = '%s.%s.iter%s.pb' % (
            inputs.imagename, inputs.stokes, iter)
        mask_name = '%s.%s.iter%s.mask' % (
            inputs.imagename, inputs.stokes, iter)

        # Starting with CASA 4.7.79 tclean can calculate chanchunks automatically.
        chanchunks = -1

        parallel = all([mpihelpers.parse_mpi_input_parameter(inputs.parallel),
                        'TARGET' in inputs.intent])

        # Need to translate the virtual spw IDs to real ones
        real_spwsel = context.observing_run.get_real_spwsel(inputs.spwsel, inputs.vis)

        # Common tclean parameters
        tclean_job_parameters = {
            'vis':           inputs.vis,
            'imagename':     '%s.%s.iter%s' % (os.path.basename(inputs.imagename), inputs.stokes, iter),
            'datacolumn':    inputs.datacolumn,
            'antenna':       inputs.antenna,
            'field':         inputs.field,
            'spw':           real_spwsel,
            'intent':        utils.to_CASA_intent(inputs.ms[0], inputs.intent),
            'specmode':      inputs.specmode if inputs.specmode != 'cont' else 'mfs',
            'gridder':       inputs.gridder,
            'pblimit':       inputs.pblimit,
            'niter':         inputs.niter,
            'threshold':     inputs.threshold,
            'deconvolver':   inputs.deconvolver,
            'interactive':   0,
            'nchan':         inputs.nchan,
            'start':         inputs.start,
            'width':         inputs.width,
            'imsize':        inputs.imsize,
            'cell':          inputs.cell,
            'cfcache':       inputs.cfcache,
            'stokes':        inputs.stokes,
            'weighting':     inputs.weighting,
            'robust':        inputs.robust,
            'restoringbeam': inputs.restoringbeam,
            'uvrange':       inputs.uvrange,
            'savemodel':     inputs.savemodel,
            'perchanweightdensity':  inputs.hm_perchanweightdensity,
            'npixels':    inputs.hm_npixels,
            'chanchunks':    chanchunks,
            'parallel':      parallel,
            'wbawp':         inputs.wbawp
            }

        # Set special phasecenter and outframe for ephemeris objects.
        # Needs to be done here since the explicit coordinates are
        # used in heuristics methods upstream.
        if inputs.heuristics.is_eph_obj(inputs.field):
            tclean_job_parameters['phasecenter'] = 'TRACKFIELD'
            # 2018-08-13: Spectral tracking has been implemented via a new
            # specmode option (CAS-11766).
            if inputs.specmode == 'cube':
                tclean_job_parameters['specmode'] = 'cubesource'
            # 2018-04-19: 'REST' does not yet work (see CAS-8965, CAS-9997)
            #tclean_job_parameters['outframe'] = 'REST'
            tclean_job_parameters['outframe'] = ''
            # 2018-07-10: Parallel imaging of ephemeris objects does not
            # yet work (see CAS-11631)
            tclean_job_parameters['parallel'] = False
        else:
            tclean_job_parameters['phasecenter'] = inputs.phasecenter
            tclean_job_parameters['outframe'] = inputs.outframe

        if scanidlist not in [[], None]:
            tclean_job_parameters['scan'] = scanidlist

        # Set up masking parameters
        if inputs.hm_masking == 'auto':
            tclean_job_parameters['usemask'] = 'auto-multithresh'

            # get heuristics parameters
            (sidelobethreshold, noisethreshold, lownoisethreshold, negativethreshold, minbeamfrac, growiterations,
             dogrowprune, minpercentchange,
             fastnoise) = inputs.heuristics.get_autobox_params(iter, inputs.intent, inputs.specmode, inputs.robust)

            # Override individually with manual settings
            if inputs.hm_sidelobethreshold != -999.0:
                tclean_job_parameters['sidelobethreshold'] = inputs.hm_sidelobethreshold
            elif sidelobethreshold is not None:
                tclean_job_parameters['sidelobethreshold'] = sidelobethreshold

            if inputs.hm_noisethreshold != -999.0:
                tclean_job_parameters['noisethreshold'] = inputs.hm_noisethreshold
            elif noisethreshold is not None:
                tclean_job_parameters['noisethreshold'] = noisethreshold

            if inputs.hm_lownoisethreshold != -999.0:
                tclean_job_parameters['lownoisethreshold'] = inputs.hm_lownoisethreshold
            elif lownoisethreshold is not None:
                tclean_job_parameters['lownoisethreshold'] = lownoisethreshold

            if inputs.hm_negativethreshold != -999.0:
                tclean_job_parameters['negativethreshold'] = inputs.hm_negativethreshold
            elif negativethreshold is not None:
                tclean_job_parameters['negativethreshold'] = negativethreshold

            if inputs.hm_minbeamfrac != -999.0:
                tclean_job_parameters['minbeamfrac'] = inputs.hm_minbeamfrac
            elif minbeamfrac is not None:
                tclean_job_parameters['minbeamfrac'] = minbeamfrac

            if inputs.hm_growiterations != -999:
                tclean_job_parameters['growiterations'] = inputs.hm_growiterations
            elif growiterations is not None:
                tclean_job_parameters['growiterations'] = growiterations

            if inputs.hm_dogrowprune != -999:
                tclean_job_parameters['dogrowprune'] = inputs.hm_dogrowprune
            elif dogrowprune is not None:
                tclean_job_parameters['dogrowprune'] = dogrowprune

            if inputs.hm_minpercentchange != -999:
                tclean_job_parameters['minpercentchange'] = inputs.hm_minpercentchange
            elif minpercentchange is not None:
                tclean_job_parameters['minpercentchange'] = minpercentchange

            tclean_job_parameters['fastnoise'] = fastnoise

        else:
            tclean_job_parameters['fastnoise'] = inputs.hm_fastnoise
            if inputs.hm_masking != 'none' and inputs.mask == 'pb':
                # In manual cleaning mode decide for cleaning with pbmask according
                # to heuristic class method (see PIPE-977)
                tclean_job_parameters['usemask'] = 'pb'
                tclean_job_parameters['pbmask'] = inputs.pbmask if inputs.pbmask else inputs.heuristics.pbmask()
            elif (inputs.hm_masking != 'none') and (inputs.mask != ''):
                tclean_job_parameters['usemask'] = 'user'
                tclean_job_parameters['mask'] = inputs.mask

        # Show nterms parameter only if it is used.
        if result.multiterm:
            tclean_job_parameters['nterms'] = result.multiterm

        # Select whether to restore image
        if inputs.niter == 0 and not (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            tclean_job_parameters['restoration'] = False
            tclean_job_parameters['pbcor'] = False
        else:
            tclean_job_parameters['restoration'] = True
            tclean_job_parameters['pbcor'] = inputs.heuristics.pb_correction()

        # Re-use products from previous iteration.
        if iter > 0:
            tclean_job_parameters['restart'] = True
            tclean_job_parameters['calcpsf'] = False
            tclean_job_parameters['calcres'] = False

        # Allow setting calcpsf and calcres explicitly
        if type(inputs.calcpsf) is bool:
            tclean_job_parameters['calcpsf'] = inputs.calcpsf
        if type(inputs.calcres) is bool:
            tclean_job_parameters['calcres'] = inputs.calcres

        # Additional heuristics or task parameters
        if inputs.cyclefactor not in (None, -999):
            tclean_job_parameters['cyclefactor'] = inputs.cyclefactor
        else:
            # Call first and assign to variable to avoid calling slow methods twice
            cyclefactor = inputs.heuristics.cyclefactor(iter)
            if cyclefactor:
                tclean_job_parameters['cyclefactor'] = cyclefactor

        if inputs.hm_minpsffraction not in (None, -999):
            tclean_job_parameters['minpsffraction'] = inputs.hm_minpsffraction

        if inputs.hm_maxpsffraction not in (None, -999):
            tclean_job_parameters['maxpsffraction'] = inputs.hm_maxpsffraction

        if inputs.cycleniter not in (None, -999):
            tclean_job_parameters['cycleniter'] = inputs.cycleniter
        else:
            cycleniter = inputs.heuristics.cycleniter(iter)
            if cycleniter is not None:
                tclean_job_parameters['cycleniter'] = cycleniter

        if inputs.scales:
            tclean_job_parameters['scales'] = inputs.scales
        else:
            scales = inputs.heuristics.scales(iter)
            if scales:
                tclean_job_parameters['scales'] = scales

        if inputs.uvrange:
            tclean_job_parameters['uvrange'] = inputs.uvrange
        else:
            uvrange, _ = inputs.heuristics.uvrange(field=inputs.field, spwspec=inputs.spw)
            if uvrange:
                tclean_job_parameters['uvrange'] = uvrange

        if inputs.uvtaper:
            tclean_job_parameters['uvtaper'] = inputs.uvtaper
        else:
            uvtaper = inputs.heuristics.uvtaper(None)
            if uvtaper:
                tclean_job_parameters['uvtaper'] = uvtaper

        if inputs.reffreq:
            tclean_job_parameters['reffreq'] = inputs.reffreq
        else:
            reffreq = inputs.heuristics.reffreq()
            if reffreq:
                tclean_job_parameters['reffreq'] = reffreq

        if inputs.restfreq:
            tclean_job_parameters['restfreq'] = inputs.restfreq
        else:
            restfreq = inputs.heuristics.restfreq()
            if restfreq:
                tclean_job_parameters['restfreq'] = restfreq

        if inputs.conjbeams is not None:
            tclean_job_parameters['conjbeams'] = inputs.conjbeams
        else:
            conjbeams = inputs.heuristics.conjbeams()
            if conjbeams is not None:
                tclean_job_parameters['conjbeams'] = conjbeams

        if inputs.usepointing is not None:
            tclean_job_parameters['usepointing'] = inputs.usepointing
        else:
            usepointing = inputs.heuristics.usepointing()
            if usepointing is not None:
                tclean_job_parameters['usepointing'] = usepointing

        if inputs.mosweight is not None:
            tclean_job_parameters['mosweight'] = inputs.mosweight
        else:
            mosweight = inputs.heuristics.mosweight(inputs.intent, inputs.field)
            if mosweight is not None:
                tclean_job_parameters['mosweight'] = mosweight

        tclean_job_parameters['nsigma'] = inputs.heuristics.nsigma(iter, inputs.hm_nsigma)
        tclean_job_parameters['wprojplanes'] = inputs.heuristics.wprojplanes()
        tclean_job_parameters['rotatepastep'] = inputs.heuristics.rotatepastep()
        tclean_job_parameters['smallscalebias'] = inputs.heuristics.smallscalebias()
        tclean_job_parameters['usepointing'] = inputs.heuristics.usepointing()
        tclean_job_parameters['pointingoffsetsigdev'] = inputs.heuristics.pointingoffsetsigdev()

        # Up until CASA 5.2 it is necessary to run tclean calls with
        # restoringbeam == 'common' in two steps in HPC mode (CAS-10849).
        if (tclean_job_parameters['parallel'] == True) and \
           (tclean_job_parameters['specmode'] == 'cube') and \
           (tclean_job_parameters['restoration'] == True) and \
           (tclean_job_parameters['restoringbeam'] == 'common'):

            # CAS-11322 asks to temporarily leave restoration set to True
            # DMU, 2018-06-01
            #tclean_job_parameters['restoration'] = False
            tclean_job_parameters['restoringbeam'] = ''
            job = casa_tasks.tclean(**tclean_job_parameters)
            tclean_result = self._executor.execute(job)

            tclean_job_parameters['parallel'] = False
            tclean_job_parameters['niter'] = 0
            tclean_job_parameters['restoration'] = True
            tclean_job_parameters['restoringbeam'] = 'common'
            tclean_job_parameters['calcpsf'] = False
            tclean_job_parameters['calcres'] = False
            job = casa_tasks.tclean(**tclean_job_parameters)
            tclean_result2 = self._executor.execute(job)
        else:
            job = casa_tasks.tclean(**tclean_job_parameters)
            tclean_result = self._executor.execute(job)

        # Record last tclean command for weblog
        result.set_tclean_command(str(job))

        pbcor_image_name = '%s.%s.iter%s.image.pbcor' % (inputs.imagename, inputs.stokes, iter)

        if inputs.niter > 0:
            if 'stopcode' in tclean_result:
                # Serial tclean result
                tclean_stopcode = tclean_result['stopcode']
                tclean_iterdone = tclean_result['iterdone']
                tclean_nmajordone = tclean_result['nmajordone']
                tclean_nminordone = tclean_result['summaryminor'][0,:]
                tclean_peakresidual = tclean_result['summaryminor'][1,:]
                tclean_totalflux = tclean_result['summaryminor'][2, :]
                tclean_niter = tclean_result['niter']
            else:
                # Parallel tclean result structure is currently (2017-03) different
                tclean_stopcodes = [tclean_result[key][int(key.replace('node', ''))]['stopcode']
                                    for key in tclean_result]
                # Bump up any error condition (8, 6, 5, 4, 3, 1) to the
                # reporting level.
                if 8 in tclean_stopcodes:
                    tclean_stopcode = 8
                elif 6 in tclean_stopcodes:
                    tclean_stopcode = 6
                elif 5 in tclean_stopcodes:
                    tclean_stopcode = 5
                elif 4 in tclean_stopcodes:
                    tclean_stopcode = 4
                elif 3 in tclean_stopcodes:
                    tclean_stopcode = 3
                elif 1 in tclean_stopcodes:
                    tclean_stopcode = 1
                elif (np.array(tclean_stopcodes) == 7).all():
                    # If zero masks (stopcode 7) occur only in a subset of
                    # frequency regions, they should not be reported.
                    tclean_stopcode = 7
                elif 2 in tclean_stopcodes:
                    # Normal stop based on threshold
                    tclean_stopcode = 2
                else:
                    # This should not happen. Just a fallback. Will cause
                    # stop code reason evaluation to fail later on.
                    tclean_stopcode = 0
                # With CASA 6.2 (see CAS-9386) this else clause is not necessary. The pipeline 2021.1.1 release is
                # based on the CASA 6.1 series, however, even there the else clause does not seem to be triggered.
                # The variables are defined here for completeness, but tclean_nminordone, tclean_peakresidual and
                # tclean_totalflux are not guaranteed to have correct (expected) array shape.
                tclean_iterdone = sum([tclean_result[key][int(key.replace('node', ''))]['iterdone']
                                       for key in tclean_result])
                tclean_nmajordone = sum([tclean_result[key][int(key.replace('node', ''))]['nmajordone']
                                         for key in tclean_result])
                tclean_niter = max([tclean_result[key][int(key.replace('node', ''))]['niter']
                                    for key in tclean_result])
                tclean_nminordone = np.concatenate([tclean_result[key][int(key.replace('node', ''))]['summaryminor'][0,:]
                                                    for key in tclean_result])
                tclean_peakresidual = np.concatenate([tclean_result[key][int(key.replace('node', ''))]['summaryminor'][1,:]
                                                 for key in tclean_result])
                tclean_totalflux = np.concatenate([tclean_result[key][int(key.replace('node', ''))]['summaryminor'][2,:]
                                                   for key in tclean_result])

            LOG.info('tclean used %d iterations' % tclean_iterdone)
            if (tclean_stopcode == 1) and (tclean_iterdone >= tclean_niter):
                result.error = CleanBaseError('tclean reached niter limit. Field: %s SPW: %s' %
                                              (inputs.field, inputs.spw), 'Reached niter limit')
                LOG.warning('tclean reached niter limit of %d for %s / spw%s !' %
                            (tclean_niter, utils.dequote(inputs.field), inputs.spw))

            result.set_tclean_stopcode(tclean_stopcode)
            result.set_tclean_stopreason(tclean_stopcode)
            result.set_tclean_iterdone(tclean_iterdone)
            result.set_nmajordone(iter, tclean_nmajordone)
            result.set_nminordone_array(iter, tclean_nminordone)
            result.set_peakresidual_array(iter, tclean_peakresidual)
            result.set_totalflux_array(iter, tclean_totalflux)

            if tclean_stopcode in [5, 6]:
                result.error = CleanBaseError('tclean stopped to prevent divergence (stop code %d). Field: %s SPW: %s' %
                                              (tclean_stopcode, inputs.field, inputs.spw),
                                              'tclean stopped to prevent divergence.')
                LOG.warning('tclean stopped to prevent divergence (stop code %d). Field: %s SPW: %s' %
                            (tclean_stopcode, inputs.field, inputs.spw))

        if iter > 0 or (inputs.specmode == 'cube' and inputs.spwsel_all_cont):
            # Store the model.
            imageheader.set_miscinfo(name=model_name, spw=inputs.spw, field=inputs.field,
                                     type='model', iter=iter, multiterm=result.multiterm,
                                     intent=inputs.intent, specmode=inputs.specmode,
                                     is_per_eb=inputs.is_per_eb,
                                     context=context)
            result.set_model(iter=iter, image=model_name)

            # Always set info on the uncorrected image for plotting
            imageheader.set_miscinfo(name=image_name, spw=inputs.spw, field=inputs.field,
                                     type='image', iter=iter, multiterm=result.multiterm,
                                     intent=inputs.intent, specmode=inputs.specmode, robust=inputs.robust,
                                     is_per_eb=inputs.is_per_eb,
                                     context=context)

            # Store the PB corrected image.
            if os.path.exists('%s' % (pbcor_image_name.replace('.image.pbcor', '.image.tt0.pbcor' if result.multiterm else '.image.pbcor'))):
                imageheader.set_miscinfo(name=pbcor_image_name, spw=inputs.spw, field=inputs.field,
                                         type='pbcorimage', iter=iter, multiterm=result.multiterm,
                                         intent=inputs.intent, specmode=inputs.specmode, robust=inputs.robust,
                                         is_per_eb=inputs.is_per_eb,
                                         context=context)
                result.set_image(iter=iter, image=pbcor_image_name)
            else:
                result.set_image(iter=iter, image=image_name)

        # Store the residual.
        if os.path.exists('%s' % (residual_name.replace('.residual', '.residual.tt0' if result.multiterm else '.residual'))):
            imageheader.set_miscinfo(name=residual_name, spw=inputs.spw, field=inputs.field,
                                     type='residual', iter=iter, multiterm=result.multiterm,
                                     intent=inputs.intent, specmode=inputs.specmode,
                                     is_per_eb=inputs.is_per_eb,
                                     context=context)
            result.set_residual(iter=iter, image=residual_name)

        # Store the PSF.
        imageheader.set_miscinfo(name=psf_name, spw=inputs.spw, field=inputs.field,
                                 type='psf', iter=iter, multiterm=result.multiterm,
                                 intent=inputs.intent, specmode=inputs.specmode,
                                 is_per_eb=inputs.is_per_eb,
                                 context=context)
        result.set_psf(image=psf_name)

        # Store the flux image.
        imageheader.set_miscinfo(name=flux_name, spw=inputs.spw, field=inputs.field,
                                 type='flux', iter=iter, multiterm=result.multiterm,
                                 intent=inputs.intent, specmode=inputs.specmode,
                                 is_per_eb=inputs.is_per_eb,
                                 context=context)
        result.set_flux(image=flux_name)

        # Make sure mask has path name
        if os.path.exists(inputs.mask):
            imageheader.set_miscinfo(name=inputs.mask, spw=inputs.spw, field=inputs.field,
                                     type='cleanmask', iter=iter,
                                     intent=inputs.intent, specmode=inputs.specmode,
                                     is_per_eb=inputs.is_per_eb,
                                     context=context)
            result.set_cleanmask(iter=iter, image=inputs.mask)
        elif os.path.exists(mask_name):
            # Use mask made by tclean
            imageheader.set_miscinfo(name=mask_name, spw=inputs.spw, field=inputs.field,
                                     type='cleanmask', iter=iter,
                                     intent=inputs.intent, specmode=inputs.specmode,
                                     is_per_eb=inputs.is_per_eb,
                                     context=context)
            result.set_cleanmask(iter=iter, image=mask_name)

        # Keep threshold and sensitivity for QA and weblog
        result.set_threshold(inputs.threshold)
        result.set_sensitivity(inputs.sensitivity)
        result.set_imaging_params(iter, tclean_job_parameters)

        return result


def rename_image(old_name, new_name, extensions=['']):
    """
    Rename an image
    """
    if old_name is not None:
        for extension in extensions:
            with casa_tools.ImageReader('%s%s' % (old_name, extension)) as image:
                image.rename(name=new_name, overwrite=True)


class CleanBaseError(object):
    """Clean Base Error Class to transfer detailed messages for weblog
    reporting.
    """

    def __init__(self, longmsg='', shortmsg=''):
        self.longmsg = longmsg
        self.shortmsg = shortmsg
