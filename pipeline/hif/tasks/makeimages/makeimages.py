import os
import tempfile

import pipeline.infrastructure as infrastructure
#import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.h.tasks.common.sensitivity import Sensitivity
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import exceptions
from pipeline.infrastructure import task_registry
from .resultobjects import MakeImagesResult
from ..tclean import Tclean
from ..tclean.resultobjects import TcleanResult

LOG = infrastructure.get_logger(__name__)


class MakeImagesInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.SELFCAL_LINE_SCIENCE, DataType.REGCAL_LINE_SCIENCE, DataType.SELFCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    calcsb = vdp.VisDependentProperty(default=False)
    cleancontranges = vdp.VisDependentProperty(default=False)
    hm_cleaning = vdp.VisDependentProperty(default='rms')
    hm_cyclefactor = vdp.VisDependentProperty(default=-999.0)
    hm_dogrowprune = vdp.VisDependentProperty(default=None)
    hm_growiterations = vdp.VisDependentProperty(default=-999)
    hm_lownoisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_masking = vdp.VisDependentProperty(default=None)
    hm_minbeamfrac = vdp.VisDependentProperty(default=-999.0)
    hm_minpercentchange = vdp.VisDependentProperty(default=-999.0)
    hm_minpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_maxpsffraction = vdp.VisDependentProperty(default=-999.0)
    hm_fastnoise = vdp.VisDependentProperty(default=None)
    hm_nsigma = vdp.VisDependentProperty(default=0.0)
    hm_perchanweightdensity = vdp.VisDependentProperty(default=None)
    hm_npixels = vdp.VisDependentProperty(default=0)
    hm_negativethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_noisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_sidelobethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_weighting = vdp.VisDependentProperty(default=None)
    masklimit = vdp.VisDependentProperty(default=2.0)
    parallel = vdp.VisDependentProperty(default='automatic')
    tlimit = vdp.VisDependentProperty(default=2.0)
    drcorrect = vdp.VisDependentProperty(default=-999.0)
    overwrite_on_export = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty(null_input=['', None, {}])
    def target_list(self):
        return self.context.clean_list_pending

    @tlimit.convert
    def tlimit(self, tlimit):
        if tlimit <= 0.0:
            raise ValueError('tlimit values must be larger than 0.0')
        else:
            return tlimit

    def __init__(self, context, output_dir=None, vis=None, target_list=None,
                 hm_masking=None, hm_sidelobethreshold=None, hm_noisethreshold=None,
                 hm_lownoisethreshold=None, hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None,
                 hm_dogrowprune=None, hm_minpercentchange=None, hm_fastnoise=None, hm_nsigma=None,
                 hm_perchanweightdensity=None, hm_npixels=None, hm_cyclefactor=None, hm_minpsffraction=None,
                 hm_maxpsffraction=None, hm_weighting=None, hm_cleaning=None, tlimit=None, drcorrect=None, masklimit=None,
                 cleancontranges=None, calcsb=None, hm_mosweight=None, overwrite_on_export=None,
                 parallel=None,
                 # Extra parameters
                 ):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.target_list = target_list
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
        self.hm_nsigma = hm_nsigma
        self.hm_perchanweightdensity = hm_perchanweightdensity
        self.hm_npixels = hm_npixels
        self.hm_cleaning = hm_cleaning
        self.hm_cyclefactor = hm_cyclefactor
        self.hm_minpsffraction = hm_minpsffraction
        self.hm_maxpsffraction = hm_maxpsffraction
        self.hm_weighting = hm_weighting
        self.tlimit = tlimit
        self.drcorrect = drcorrect
        self.masklimit = masklimit
        self.cleancontranges = cleancontranges
        self.calcsb = calcsb
        self.hm_mosweight = hm_mosweight
        self.parallel = parallel
        self.overwrite_on_export = overwrite_on_export


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(MakeImagesInputs)


@task_registry.set_equivalent_casa_task('hif_makeimages')
@task_registry.set_casa_commands_comment('A list of target sources is cleaned.')
class MakeImages(basetask.StandardTaskTemplate):
    Inputs = MakeImagesInputs

    is_multi_vis_task = True

    def prepare(self):
        inputs = self.inputs

        result = MakeImagesResult()
        result.overwrite = inputs.overwrite_on_export

        # Carry any message from hif_makeimlist (e.g. for missing PI cube target)
        result.set_info(inputs.context.clean_list_info)

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result.mitigation_error = True
                return result

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        with CleanTaskFactory(inputs, self._executor) as factory:
            task_queue = [(target, factory.get_task(target))
                          for target in inputs.target_list]

            for (target, task) in task_queue:
                try:
                    worker_result = task.get_result()
                except exceptions.PipelineException as ex:
                    result.add_result(TcleanResult(), target, outcome='failure')
                    LOG.error('Cleaning failure for field {!s} spw {!s} specmode {!s}.\nException from hif_tclean: {!s}'.format(
                        target['field'], target['spw'], target['specmode'], ex))
                else:
                    # Note add_result() removes 'heuristics' from worker_result
                    heuristics = target['heuristics']
                    result.add_result(worker_result, target, outcome='success')
                    # Export RMS of  sources              
                    if self._is_target_for_sensitivity(worker_result, heuristics):
                        s = self._get_image_rms_as_sensitivity(worker_result, target, heuristics)
                        if s is not None:
                            result.sensitivities_for_aqua.append(s)
                    del heuristics

        # set of descriptions
        if inputs.context.clean_list_info.get('msg', '') != '':
            target_list = [inputs.context.clean_list_info]
        else:
            target_list = inputs.target_list

        description = {
            # map specmode to description for every clean target
            _get_description_map(target['intent']).get(target['specmode'], 'Calculate clean products')
            for target in target_list
        }
        result.metadata['long description'] = ' / '.join(sorted(description))

        sidebar = {
            # map specmode to description for every clean target
            _get_sidebar_map(target['intent']).get(target['specmode'], '')
            for target in target_list
        }
        result.metadata['sidebar suffix'] = '/'.join(sidebar)

        return result

    def analyse(self, result):
        return result

    def _is_target_for_sensitivity(self, clean_result, heuristics):
        """
        Returns True if the clean target is one to export image sensitivity
        Conditions to export image sensitivities are
        - cubes generated by SRDP ALMA image cube recipe (specmode='cube')
        - all target images for ALMA if not SRDP
        - reprSrc and reprSpw (all specmode)
        """

        # SRDP ALMA optimized cube images
        if self.inputs.context.project_structure.recipe_name == 'hifa_cubeimage':
            # only cubes
            return clean_result.specmode == 'cube'

        # ALMA pipeline
        if heuristics.imaging_mode == 'ALMA':
            return clean_result.intent == 'TARGET'

        # Representative source and SpW
        _, repr_source, repr_spw, _, _, _, _, _, _, _ = heuristics.representative_target()
        if str(repr_spw) in clean_result.spw.split(',') and repr_source == utils.dequote(clean_result.sourcename):
            return True

        # Don't export image sensitivity for the other clean targets
        return False

    def _get_image_rms_as_sensitivity(self, result, target, heuristics):
        if not result.image:
            return None

        extension = 'tt0.' if result.multiterm else '' # Needed when nterms=2, see PIPE-1361
        # the tt0 needs to be inserted before the ending ".pbcor" in the image name
        index = result.image.find('pbcor')
        imname = result.image[:index] + extension + result.image[index:]

        if not os.path.exists(imname):
            return None

        cqa = casa_tools.quanta
        cell = target['cell'][0:2] if len(target['cell']) >= 2 else (target['cell'][0], target['cell'][0])
        # Image beam
        with casa_tools.ImageReader(imname) as image:
            restoringbeam = image.restoringbeam()
            csys = image.coordsys()
            chanwidth_of_image = csys.increment(format='q', type='spectral')['quantity']['*1']
            csys.done()
        # effectiveBW
        if result.specmode == 'cube': # use nbin for cube and repBW
            msobj = self.inputs.context.observing_run.get_ms(name=result.vis[0])
            nbin = target['nbin'] if target['nbin'] > 0 else 1
            SCF, physicalBW_of_1chan, effectiveBW_of_1chan = heuristics.get_bw_corr_factor(msobj, result.spw, nbin)
            effectiveBW_of_image = cqa.quantity(nbin / SCF**2 * effectiveBW_of_1chan, 'Hz')
        else: #continuum mode
            effectiveBW_of_image = result.aggregate_bw
        # antenna array (aligned definition with imageprecheck)
        diameters = list(heuristics.antenna_diameters().keys())
        array = ('%dm' % min(diameters))

        # Check if this sensitivity is for the representative source and SpW
        _, repr_source, repr_spw, _, _, _, _, _, _, _ = heuristics.representative_target()
        if str(repr_spw) in result.spw.split(',') and repr_source == utils.dequote(result.sourcename):
            is_representative = True
        else:
            is_representative = False

        return Sensitivity(array=array,
                           intent=target['intent'],
                           field=target['field'],
                           spw=result.spw,
                           is_representative=is_representative,
                           bandwidth=chanwidth_of_image,
                           effective_bw=effectiveBW_of_image,
                           bwmode=result.orig_specmode,
                           beam=restoringbeam,
                           cell=cell,
                           robust=target['robust'],
                           uvtaper=target['uvtaper'],
                           sensitivity=cqa.quantity(result.image_rms, 'Jy/beam'),
                           pbcor_image_min=cqa.quantity(result.image_min, 'Jy/beam'),
                           pbcor_image_max=cqa.quantity(result.image_max, 'Jy/beam'),
                           imagename=result.image.replace('.pbcor', ''),
                           datatype=result.datatype)


class CleanTaskFactory(object):
    def __init__(self, inputs, executor):
        self.__inputs = inputs
        self.__context = inputs.context
        self.__executor = executor
        self.__context_path = None

    def __enter__(self):
        # If there's a possibility that we'll submit MPI jobs, save the context
        # to disk ready for import by the MPI servers.
        if mpihelpers.mpiclient:
            # Use the tempfile module to generate a unique temporary filename,
            # which we use as the output path for our pickled context
            tmpfile = tempfile.NamedTemporaryFile(suffix='.context',
                                                  dir=self.__context.output_dir,
                                                  delete=True)
            self.__context_path = tmpfile.name
            tmpfile.close()

            self.__context.save(self.__context_path)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__context_path and os.path.exists(self.__context_path):
            os.unlink(self.__context_path)

    def get_task(self, target):
        """
        Create and return a SyncTask or AsyncTask for the clean job required
        to produce the clean target.

        The current algorithm generates Tier 0 clean jobs for calibrator
        images (=AsyncTask) and Tier 1 clean jobs for target images
        (=SyncTask).

        :param target: a clean job definition generated by MakeImList
        :return: a SyncTask or AsyncTask
        """
        task_args = self.__get_task_args(target)

        is_mpi_ready = mpihelpers.is_mpi_ready()
        is_cal_image = 'TARGET' not in target['intent']

        is_tier0_job = is_mpi_ready and is_cal_image
        # PIPE-1923 asks to temporarily turn off Tier-0 mode for
        # POLARIZATION intent when imaging IQUV because of a
        # potential CASA bug. This should be undone when this
        # bug is fixed.
        if target['intent'] == 'POLARIZATION' and target['stokes'] == 'IQUV':
            is_tier0_job = False
            if is_mpi_ready:
                LOG.info('Temporarily turning off Tier-0 parallelization for Stokes IQUV polarization calibrator imaging (PIPE-1923).')

        parallel_wanted = mpihelpers.parse_mpi_input_parameter(self.__inputs.parallel)

        # PIPE-1401: turn on the tier0 parallelization for individuals planes in the VLASS coarse cube imaging
        # Also see the disscussions in PIPE-1357
        vlass_se_cube_tier0_wanted = True
        is_vlass_se_cube = 'TARGET' in target['intent'] and self.__context.imaging_mode == 'VLASS-SE-CUBE'
        if all([vlass_se_cube_tier0_wanted, is_vlass_se_cube, is_mpi_ready]):
            is_tier0_job = True
            task_args['parallel'] = False

        if is_tier0_job and parallel_wanted:
            executable = mpihelpers.Tier0PipelineTask(Tclean,
                                                      task_args,
                                                      self.__context_path)
            return mpihelpers.AsyncTask(executable)
        else:
            inputs = Tclean.Inputs(self.__context, **task_args)
            task = Tclean(inputs)
            return mpihelpers.SyncTask(task, self.__executor)

    def __get_task_args(self, target):
        inputs = self.__inputs

        parallel_wanted = mpihelpers.parse_mpi_input_parameter(inputs.parallel)

        # request Tier 1 tclean parallelisation if the user requested it, this
        # is science target imaging, and we are running as an MPI client.
        parallel = all([parallel_wanted,
                        'TARGET' in target['intent'],
                        mpihelpers.is_mpi_ready()])

        image_heuristics = target['heuristics']

        task_args = dict(target)
        task_args.update({
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            # set the weighting type
            'weighting': inputs.hm_weighting,
            # other vals
            'tlimit': inputs.tlimit,
            'masklimit': inputs.masklimit,
            'cleancontranges': inputs.cleancontranges,
            'calcsb': inputs.calcsb,
            'parallel': parallel,
            'hm_perchanweightdensity': inputs.hm_perchanweightdensity,
            'hm_npixels': inputs.hm_npixels,
            'restoringbeam': image_heuristics.restoringbeam(),
        })

        if 'hm_nsigma' not in task_args:
            task_args['hm_nsigma'] = inputs.hm_nsigma

        if inputs.drcorrect not in (None, -999.0):
            task_args['drcorrect'] = inputs.drcorrect

        if target['robust'] not in (None, -999.0):
            task_args['robust'] = target['robust']
        else:
            task_args['robust'] = image_heuristics.robust()

        if target['uvtaper']:
            task_args['uvtaper'] = target['uvtaper']
        else:
            task_args['uvtaper'] = image_heuristics.uvtaper()

        # set the imager mode here (temporarily ...)
        if target['gridder'] is not None:
            task_args['gridder'] = target['gridder']
        else:
            task_args['gridder'] = image_heuristics.gridder(
                    task_args['intent'], task_args['field'])

        if inputs.hm_masking in (None, ''):
            if 'TARGET' in task_args['intent']:
                task_args['hm_masking'] = 'auto'
            elif task_args['intent'] == 'POLARIZATION' and task_args['stokes'] == 'IQUV':
                task_args['hm_masking'] = 'centralregion'
            else:
                task_args['hm_masking'] = 'auto'
        else:
            task_args['hm_masking'] = inputs.hm_masking

        if inputs.hm_masking == 'auto':
            task_args['hm_sidelobethreshold'] = inputs.hm_sidelobethreshold
            task_args['hm_noisethreshold'] = inputs.hm_noisethreshold
            task_args['hm_lownoisethreshold'] = inputs.hm_lownoisethreshold
            task_args['hm_negativethreshold'] = inputs.hm_negativethreshold
            task_args['hm_minbeamfrac'] = inputs.hm_minbeamfrac
            task_args['hm_growiterations'] = inputs.hm_growiterations
            task_args['hm_dogrowprune'] = inputs.hm_dogrowprune
            task_args['hm_minpercentchange'] = inputs.hm_minpercentchange
            task_args['hm_fastnoise'] = inputs.hm_fastnoise

        if inputs.hm_cleaning == '':
            task_args['hm_cleaning'] = 'rms'
        else:
            task_args['hm_cleaning'] = inputs.hm_cleaning

        if target['vis']:
            task_args['vis'] = target['vis']

        if target['is_per_eb']:
            task_args['is_per_eb'] = target['is_per_eb']

        if inputs.hm_mosweight not in (None, ''):
            task_args['mosweight'] = inputs.hm_mosweight
        elif target['mosweight'] not in (None, ''):
            task_args['mosweight'] = target['mosweight']
        else:
            task_args['mosweight'] = image_heuristics.mosweight(task_args['intent'], task_args['field'])


        if inputs.hm_cyclefactor not in (None, -999.0):
            # The tclean task argument was already called "cyclefactor"
            # before hm_cyclefactor was exposed in hif_makeimages. To
            # keep compatibility with hif_editimlist and cleantarget.py
            # we keep the name now. Could be refactored later.
            task_args['cyclefactor'] = inputs.hm_cyclefactor

        if inputs.hm_minpsffraction not in (None, -999.0):
            task_args['hm_minpsffraction'] = inputs.hm_minpsffraction

        if inputs.hm_maxpsffraction not in (None, -999.0):
            task_args['hm_maxpsffraction'] = inputs.hm_maxpsffraction

        return task_args


def _get_description_map(intent):
    if intent in ('PHASE', 'BANDPASS', 'AMPLITUDE'):
        return {
            'mfs': 'Make calibrator images',
            'cont': 'Make calibrator images'
        }
    elif intent in ('POLARIZATION', 'POLANGLE', 'POLLEAKAGE'):
        return {
            'mfs': 'Make polarization calibrator images',
            'cont': 'Make polarization calibrator images'
        }
    elif intent == 'CHECK':
        return {
            'mfs': 'Make check source images',
            'cont': 'Make check source images'
        }
    elif intent == 'TARGET':
        return {
            'mfs': 'Make target per-spw continuum images',
            'cont': 'Make target aggregate continuum images',
            'cube': 'Make target cubes',
            'repBW': 'Make representative bandwidth target cube'

        }
    else:
        return {}

def _get_sidebar_map(intent):
    if intent in ('PHASE', 'BANDPASS', 'AMPLITUDE'):
        return {
            'mfs': 'cals',
            'cont': 'cals'
        }
    elif intent in ('POLARIZATION', 'POLANGLE', 'POLLEAKAGE'):
        return {
            'mfs': 'pol',
            'cont': 'pol'
        }
    elif intent == 'CHECK':
        return {
            'mfs': 'checksrc',
            'cont': 'checksrc'
        }
    elif intent == 'TARGET':
        return {
            'mfs': 'mfs',
            'cont': 'cont',
            'cube': 'cube',
            'repBW': 'cube_repBW'
        }
    else:
        return {}
