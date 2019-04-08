"""A pipeline task to add to a list of images to be made by hif_makeimages()

The hif_editimlist() task typically uses a parameter file as input.  Depending
on the use case, there will usually be a minimal set of input parameters
defined in this file.  Each set of image parameters gets stored in the global
context in the clean_list_pending attribute.

Example:
    A common case is providing a list of VLASS image parameters via a file::

        CASA <1>: hif_editimlist(parameter_file='vlass_QLIP_parameters.list')

    The ``vlass_QLIP_parameters.list`` file might contain something like the
    following::

        phasecenter='J2000 12:16:04.600 +059.24.50.300'
        imagename='QLIP_image'

    An equivalent way to invoke the above example would be::

        CASA <2>: hif_editimlist(phasecenter='J2000 12:16:04.600 +059.24.50.300',
                                 imagename='QLIP_image')

Any imaging parameters that are not specified when hif_editimlist() is called,
either as a task parameter or via a parameter file, will have a default value
or heuristic applied.

Todo:
    * In the future this task will be modified to allow editing the parameters
    of an existing context.clean_list_pending entry.

"""

from __future__ import absolute_import
from __future__ import print_function  # get python 3 print function

import ast
import os
import copy

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
from pipeline.hif.heuristics import imageparams_factory
from pipeline.hif.tasks.makeimlist.cleantarget import CleanTarget
from pipeline.infrastructure import task_registry
from .resultobjects import EditimlistResult

LOG = infrastructure.get_logger(__name__)


class EditimlistInputs(vdp.StandardInputs):
    search_radius_arcsec = vdp.VisDependentProperty(default=1000.0)
    conjbeams = vdp.VisDependentProperty(default=True)
    cyclefactor = vdp.VisDependentProperty(default='')
    cycleniter = vdp.VisDependentProperty(default='')
    deconvolver = vdp.VisDependentProperty(default='')
    editmode = vdp.VisDependentProperty(default='')
    imaging_mode = vdp.VisDependentProperty(default='')
    imagename = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='')
    gridder = vdp.VisDependentProperty(default='')
    mask = vdp.VisDependentProperty(default=None)
    nbin = vdp.VisDependentProperty(default=-1)
    nchan = vdp.VisDependentProperty(default=-1)
    niter = vdp.VisDependentProperty(default=20000)
    nterms = vdp.VisDependentProperty(default=2)
    parameter_file = vdp.VisDependentProperty(default='')
    phasecenter = vdp.VisDependentProperty(default='')
    reffreq = vdp.VisDependentProperty(default='')
    restfreq = vdp.VisDependentProperty(default='')
    robust = vdp.VisDependentProperty(default=-999.0)
    scales = vdp.VisDependentProperty(default='')
    specmode = vdp.VisDependentProperty(default='')
    start = vdp.VisDependentProperty(default='')
    stokes = vdp.VisDependentProperty(default='')
    threshold = vdp.VisDependentProperty(default='')
    threshold_nsigma = vdp.VisDependentProperty(default=4.0)
    uvtaper = vdp.VisDependentProperty(default='')
    uvrange = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default='')
    sensitivity = vdp.VisDependentProperty(default=0.0)

    @vdp.VisDependentProperty
    def cell(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @cell.convert
    def cell(self, val):
        if isinstance(val, str):
            val = [val]
        return val

    @vdp.VisDependentProperty
    def imsize(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @imsize.convert
    def imsize(self, val):
        if isinstance(val, str):
            val = [val]
        return val

    @vdp.VisDependentProperty
    def field(self):
        # mutable object, so should not use VisDependentProperty(default=[])
        return []

    @field.convert
    def field(self, val):
        if isinstance(val, str):
            val = [val]
        return val

    @vdp.VisDependentProperty
    def spw(self):
        return ''

    @spw.convert
    def spw(self, val):
        # Use str() method to catch single spwid case via PPR which maps to int.
        return str(val)

    def __init__(self, context, output_dir=None, vis=None,
                 search_radius_arcsec=None, cell=None, conjbeams=None,
                 cyclefactor=None, cycleniter=None, deconvolver=None,
                 editmode=None, field=None, imaging_mode=None,
                 imagename=None, imsize=None, intent=None, gridder=None,
                 mask=None, nbin=None, nchan=None, niter=None, nterms=None,
                 parameter_file=None, phasecenter=None, reffreq=None, restfreq=None,
                 robust=None, scales=None, specmode=None, spw=None,
                 start=None, stokes=None, threshold=None, threshold_nsigma=None,
                 uvtaper=None, uvrange=None, width=None, sensitivity=None):

        super(EditimlistInputs, self).__init__()
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.search_radius_arcsec = search_radius_arcsec
        self.cell = cell
        self.conjbeams = conjbeams
        self.cyclefactor = cyclefactor
        self.cycleniter = cycleniter
        self.deconvolver = deconvolver
        self.editmode = editmode
        self.field = field
        self.imaging_mode = imaging_mode
        self.imagename = imagename
        self.imsize = imsize
        self.intent = intent
        self.gridder = gridder
        self.mask = mask
        self.nbin = nbin
        self.nchan = nchan
        self.niter = niter
        self.nterms = nterms
        self.parameter_file = parameter_file
        self.phasecenter = phasecenter
        self.reffreq = reffreq
        self.restfreq = restfreq
        self.robust = robust
        self.scales = scales
        self.specmode = specmode
        self.spw = spw
        self.start = start
        self.stokes = stokes
        self.threshold = threshold
        self.threshold_nsigma = threshold_nsigma
        self.uvtaper = uvtaper
        self.uvrange = uvrange
        self.width = width
        self.sensitivity = sensitivity

        keys_to_consider = ('field', 'intent', 'spw', 'cell', 'deconvolver', 'imsize',
                            'phasecenter', 'specmode', 'gridder', 'imagename', 'scales',
                            'start', 'width', 'nbin', 'nchan', 'uvrange', 'stokes', 'nterms',
                            'robust', 'uvtaper', 'niter', 'cyclefactor', 'cycleniter', 'mask',
                            'search_radius_arcsec', 'threshold', 'imaging_mode', 'reffreq', 'restfreq',
                            'editmode', 'threshold_nsigma', 'sensitivity', 'conjbeams')

        self.keys_to_change = []
        keydict = self.as_dict()
        for key in keys_to_consider:
            # print key, eval(key)
            if keydict[key] is not None:
                self.keys_to_change.append(key)


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
api.ImagingMeasurementSetsPreferred.register(EditimlistInputs)


@task_registry.set_equivalent_casa_task('hif_editimlist')
class Editimlist(basetask.StandardTaskTemplate):
    # 'Inputs' will be used later in execute_task().
    #   See h/cli/utils.py and infrastructure/argmagger.py
    Inputs = EditimlistInputs

    # TODO:  check to see if I should set this to False
    is_multi_vis_task = True

    def prepare(self):

        inp = self.inputs

        # get the class inputs as a dictionary
        inpdict = inp.as_dict()
        LOG.debug(inp.as_dict())

        # if a file is given, read whatever parameters are defined in the file
        if inp.parameter_file:
            if os.access(inp.parameter_file, os.R_OK):
                with open(inp.parameter_file) as parfile:
                    for line in parfile:
                        # ignore comment lines or lines that don't contain '='
                        if line.startswith('#') or '=' not in line:
                            continue
                        # split key=value into a key, value components
                        parameter, value = line.partition('=')[::2]
                        # strip whitespace
                        parameter = parameter.strip()
                        value = value.strip()
                        # all params come in as strings.  evaluate it to set it to the proper type
                        value = ast.literal_eval(value)

                        # use this information to change the values in inputs
                        LOG.debug("Setting inputdict['{k}'] to {v} {t}".format(k=parameter, v=value, t=type(value)))
                        inpdict[parameter] = value
                        inp.keys_to_change.append(parameter)
            else:
                LOG.error('Input parameter file is not readable: {fname}'.format(fname=inp.parameter_file))

        # now construct the list of imaging command parameter lists that must
        # be run to obtain the required images
        result = EditimlistResult()

        # will default to adding a new image list entry
        inpdict.setdefault('editmode', 'add')

        # Use the ms object from the context to change field ids to fieldnames, if needed
        # TODO think about how to handle multiple MSs
        ms = inp.context.observing_run.get_ms(inp.vis[0])
        fieldnames = []

        if inpdict['field']:
            # assume field entries are either all integers or all strings, but not a mix
            if isinstance(inpdict['field'][0], int):
                fieldobj = ms.get_fields(field_id=inpdict['field'][0])
                for fieldname in fieldobj.name:
                    fieldnames.append(fieldname)
            else:
                for fieldname in inpdict['field']:
                    fieldnames.append(fieldname)

            if len(fieldnames) > 1:
                fieldnames = [','.join(fieldnames)]
        # fieldnames is now a list of fieldnames: ['fieldA', 'fieldB', ...]

        imlist_entry = CleanTarget()  # initialize a target structure for clean_list_pending

        img_mode = 'VLASS-QL' if not inpdict['imaging_mode'] else inpdict['imaging_mode']
        result.img_mode = img_mode

        # The default spw range for VLASS is 2~17. hif_makeimages() needs a csv list.
        # We set the imlist_entry spw before the heuristics object because the heursitics class
        # uses it in initialization.
        if img_mode in ('VLASS-QL', 'VLASS-SE-CONT', 'VLASS-SE-CUBE', 'VLASS-SE-TAPER'):
            if not inpdict['spw']:
                imlist_entry['spw'] = ','.join([str(x) for x in range(2, 18)])
            else:
                if 'MHz' in inpdict['spw']:
                    # map the center frequencies (MHz) to spw ids
                    cfreq_spw = {}
                    spws = ms.get_spectral_windows(science_windows_only=True)
                    for spw_ii in spws:
                        centre_freq = int(spw_ii.centre_frequency.to_units(measures.FrequencyUnits.MEGAHERTZ))
                        spwid = spw_ii.id
                        cfreq_spw[centre_freq] = spwid

                    user_freqs = inpdict['spw'].split(',')
                    spws = []
                    for uf in user_freqs:
                        uf_int = int(uf.replace('MHz', ''))
                        spws.append(cfreq_spw[uf_int])
                    imlist_entry['spw'] = ','.join([str(x) for x in spws])
                else:
                    imlist_entry['spw'] = inpdict['spw']
        else:
            imlist_entry['spw'] = inpdict['spw']

        # phasecenter is required user input (not determined by heuristics)
        imlist_entry['phasecenter'] = inpdict['phasecenter']

        iph = imageparams_factory.ImageParamsHeuristicsFactory()
        th = imlist_entry['heuristics'] = iph.getHeuristics(vislist=inp.vis, spw=imlist_entry['spw'],
                                                            observing_run=inp.context.observing_run,
                                                            imagename_prefix=inp.context.project_structure.ousstatus_entity_id,
                                                            proj_params=inp.context.project_performance_parameters,
                                                            imaging_params=inp.context.imaging_parameters,
                                                            imaging_mode=img_mode)

        # NB: the 'threshold_nsigma' parameter in hif_editimlist translates to
        #     the internal 'rms_sigma' in hif_makeimages
        threshold_set_by_user = bool(inpdict['threshold'])
        rms_nsigma_set_by_user = bool(inpdict['threshold_nsigma'])
        if threshold_set_by_user and not rms_nsigma_set_by_user:
            imlist_entry['threshold'] = inpdict['threshold']
        elif rms_nsigma_set_by_user and not threshold_set_by_user:
            imlist_entry['rms_nsigma'] = inpdict['threshold_nsigma']
        elif rms_nsigma_set_by_user and threshold_set_by_user:
            imlist_entry['threshold'] = inpdict['threshold']
            imlist_entry['rms_nsigma'] = inpdict['threshold_nsigma']
            LOG.warn("Both 'threshold' and 'threshold_nsigma' were specified.")
        else:  # neither set by user.  Use nsigma.
            imlist_entry['rms_nsigma'] = th.threshold_nsigma()

        imlist_entry['stokes'] = th.stokes() if not inpdict['stokes'] else inpdict['stokes']
        imlist_entry['conjbeams'] = th.conjbeams() if not inpdict['conjbeams'] else inpdict['conjbeams']
        imlist_entry['reffreq'] = th.reffreq() if not inpdict['reffreq'] else inpdict['reffreq']
        imlist_entry['restfreq'] = th.restfreq() if not inpdict['restfreq'] else inpdict['restfreq']
        # niter_correction is run again in tclean.py
        imlist_entry['niter'] = th.niter_correction(None, None, None, None, None) if not inpdict['niter'] else inpdict['niter']
        imlist_entry['cyclefactor'] = th.cyclefactor() if not inpdict['cyclefactor'] else inpdict['cyclefactor']
        imlist_entry['cycleniter'] = th.cycleniter() if not inpdict['cycleniter'] else int(inpdict['cycleniter'])
        imlist_entry['scales'] = th.scales() if not inpdict['scales'] else inpdict['scales']
        imlist_entry['uvtaper'] = th.uvtaper() if not inpdict['uvtaper'] else inpdict['uvtaper']
        imlist_entry['uvrange'] = th.uvrange() if not inpdict['uvrange'] else inpdict['uvrange']
        imlist_entry['deconvolver'] = th.deconvolver(None, None) if not inpdict['deconvolver'] else inpdict['deconvolver']
        imlist_entry['robust'] = th.robust() if inpdict['robust'] in (None, -999.0) else inpdict['robust']
        imlist_entry['mask'] = th.mask() if not inpdict['mask'] else inpdict['mask']
        imlist_entry['specmode'] = th.specmode() if not inpdict['specmode'] else inpdict['specmode']
        LOG.info('RADIUS')
        LOG.info(repr(inpdict['search_radius_arcsec']))
        LOG.info('default={d}'.format(d=not inpdict['search_radius_arcsec']
                                        and not isinstance(inpdict['search_radius_arcsec'], float)
                                        and not isinstance(inpdict['search_radius_arcsec'], int)))
        buffer_arcsec = th.buffer_radius() \
            if (not inpdict['search_radius_arcsec']
                and not isinstance(inpdict['search_radius_arcsec'], float)
                and not isinstance(inpdict['search_radius_arcsec'], int)) else inpdict['search_radius_arcsec']
        LOG.info("{k} = {v}".format(k='search_radius', v=buffer_arcsec))
        result.capture_buffer_size(buffer_arcsec)
        imlist_entry['intent'] = th.intent() if not inpdict['intent'] else inpdict['intent']
        imlist_entry['nterms'] = th.nterms() if not inpdict['nterms'] else inpdict['nterms']
        if 'ALMA' not in img_mode:
            imlist_entry['sensitivity'] = th.get_sensitivity(ms_do=None, field=None, intent=None, spw=None, 
                                                             chansel=None, specmode=None, cell=None, imsize=None,
                                                             weighting=None, robust=None,
                                                             uvtaper=None) if not inpdict['sensitivity'] else inpdict['sensitivity']
        # ---------------------------------------------------------------------------------- set cell (SRDP ALMA)
        ppb = 5.0  # pixels per beam
        if fieldnames:
            synthesized_beam, ksb = th.synthesized_beam(field_intent_list=[[fieldnames[0], 'TARGET']],
                                                        spwspec=imlist_entry['spw'],
                                                        robust=imlist_entry['robust'],
                                                        uvtaper=imlist_entry['uvtaper'],
                                                        pixperbeam=ppb,
                                                        known_beams=inp.context.synthesized_beams,
                                                        force_calc=False)
        else:
            synthesized_beam = None
        imlist_entry['cell'] = th.cell(beam=synthesized_beam,
                                       pixperbeam=ppb) if not inpdict['cell'] else inpdict['cell']
        # ----------------------------------------------------------------------------------  set imsize (SRDP ALMA)
        largest_primary_beam = th.largest_primary_beam_size(spwspec=imlist_entry['spw'], intent='TARGET')
        fieldids = th.field('TARGET', fieldnames)
        imlist_entry['imsize'] = th.imsize(fields=fieldids, cell=imlist_entry['cell'],
                                           primary_beam=largest_primary_beam,
                                           sfpblimit=0.2) if not inpdict['imsize'] else inpdict['imsize']

        # ------------------------------
        imlist_entry['nchan'] = inpdict['nchan']
        imlist_entry['nbin'] = inpdict['nbin']
        imlist_entry['start'] = inpdict['start']
        imlist_entry['width'] = inpdict['width']

        # for VLASS phasecenter is required user input (not determined by heuristics)
        imlist_entry['phasecenter'] = th.phasecenter(fieldids) if not inpdict['phasecenter'] else inpdict['phasecenter']

        # set the field name list in the image list target
        if fieldnames:
            imlist_entry['field'] = fieldnames[0]
        else:
            if not isinstance(imlist_entry['phasecenter'], type(None)):
                # TODO: remove the dependency on cell size being in arcsec

                # remove brackets and begin/end string characters
                # if cell is a list, get the first string element
                if isinstance(imlist_entry['cell'], type([])):
                    imlist_entry['cell'] = imlist_entry['cell'][0]
                imlist_entry['cell'] = imlist_entry['cell'].strip('[').strip(']')
                imlist_entry['cell'] = imlist_entry['cell'].replace("'", '')
                imlist_entry['cell'] = imlist_entry['cell'].replace('"', '')
                # We always search for fields in 1sq degree with a surrounding buffer
                mosaic_side_arcsec = 3600  # 1 degree
                dist = (mosaic_side_arcsec / 2.) + float(buffer_arcsec)
                dist_arcsec = str(dist) + 'arcsec'
                LOG.info("{k} = {v}".format(k='dist_arcsec', v=dist_arcsec))
                found_fields = imlist_entry['heuristics'].find_fields(distance=dist_arcsec,
                                                                      phase_center=imlist_entry['phasecenter'],
                                                                      matchregex=['^0', '^1', '^2'])
                if found_fields:
                    imlist_entry['field'] = ','.join(str(x) for x in found_fields)

        imlist_entry['gridder'] = th.gridder(imlist_entry['intent'], imlist_entry['field']) if not inpdict['gridder'] else inpdict['gridder']
        imlist_entry['imagename'] = th.imagename(intent=imlist_entry['intent'], field=imlist_entry['field'],
                                                 spwspec=imlist_entry['spw'], specmode=imlist_entry['specmode'],
                                                 band=None) if not inpdict['imagename'] else inpdict['imagename']

        for key, value in imlist_entry.iteritems():
            LOG.info("{k} = {v}".format(k=key, v=value))

        try:
            if imlist_entry['field']:
                # In the coarse cube case we want one entry per spw per stokes
                # so we want to loop over spw/stokes and create an imlist_entry for each
                if 'VLASS-SE-CUBE' == img_mode:
                    pols = imlist_entry['stokes']
                    spws = imlist_entry['spw'].split(',')
                    imagename = imlist_entry['imagename']
                    for spw in spws:
                        imlist_entry['spw'] = spw
                        imlist_entry['imagename'] = imagename + '.spw' + spw
                        for pol in pols:
                            imlist_entry['stokes'] = pol
                            # we make a deepcopy to get a unique object for each target
                            #  but also to reuse the original CleanTarget object since
                            #  we are only modifying two of the many fields
                            result.add_target(copy.deepcopy(imlist_entry))
                else:
                    result.add_target(imlist_entry)
            else:
                raise TypeError
        except TypeError:
            LOG.error('No fields to image.')

        # check for required user inputs
        if not imlist_entry['imagename']:
            LOG.error('No imagename provided.')

        if not imlist_entry['phasecenter']:
            LOG.error('No phasecenter provided.')

        return result

    def analyse(self, result):
        return result
