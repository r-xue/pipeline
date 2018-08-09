from __future__ import absolute_import

import os
import tempfile
import types

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.api as api
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import exceptions
from pipeline.infrastructure import task_registry
from pipeline.h.tasks.common.sensitivity import Sensitivity
from .resultobjects import MakeImagesResult
from ..tclean import Tclean
from ..tclean.resultobjects import TcleanResult

LOG = infrastructure.get_logger(__name__)


class MakeImagesInputs(vdp.StandardInputs):
    calcsb = vdp.VisDependentProperty(default=False)
    cleancontranges = vdp.VisDependentProperty(default=False)
    hm_cleaning = vdp.VisDependentProperty(default='rms')
    hm_dogrowprune = vdp.VisDependentProperty(default=True)
    hm_growiterations = vdp.VisDependentProperty(default=-999)
    hm_lownoisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_masking = vdp.VisDependentProperty(default='auto')
    hm_minbeamfrac = vdp.VisDependentProperty(default=-999.0)
    hm_minpercentchange = vdp.VisDependentProperty(default=-999.0)
    hm_negativethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_noisethreshold = vdp.VisDependentProperty(default=-999.0)
    hm_sidelobethreshold = vdp.VisDependentProperty(default=-999.0)
    masklimit = vdp.VisDependentProperty(default=2.0)
    maxncleans = vdp.VisDependentProperty(default=10)
    parallel = vdp.VisDependentProperty(default='automatic')
    tlimit = vdp.VisDependentProperty(default=2.0)
    weighting = vdp.VisDependentProperty(default='briggs')

    @vdp.VisDependentProperty(null_input=['', None, {}])
    def target_list(self):
        return self.context.clean_list_pending

    def __init__(self, context, output_dir=None, vis=None, target_list=None,
                 hm_masking=None, hm_sidelobethreshold=None, hm_noisethreshold=None,
                 hm_lownoisethreshold=None, hm_negativethreshold=None, hm_minbeamfrac=None, hm_growiterations=None,
                 hm_dogrowprune=None, hm_minpercentchange=None,
                 hm_cleaning=None, tlimit=None, masklimit=None, maxncleans=None,
                 calcsb=None, cleancontranges=None, parallel=None,
                 # Extra parameters
                 weighting=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.target_list = target_list
        self.weighting = weighting
        self.hm_masking = hm_masking
        self.hm_sidelobethreshold = hm_sidelobethreshold
        self.hm_noisethreshold = hm_noisethreshold
        self.hm_lownoisethreshold = hm_lownoisethreshold
        self.hm_negativethreshold = hm_negativethreshold
        self.hm_minbeamfrac = hm_minbeamfrac
        self.hm_growiterations = hm_growiterations
        self.hm_dogrowprune = hm_dogrowprune
        self.hm_minpercentchange = hm_minpercentchange
        self.hm_cleaning = hm_cleaning
        self.tlimit = tlimit
        self.masklimit = masklimit
        self.maxncleans = maxncleans
        self.cleancontranges = cleancontranges
        self.calcsb = calcsb
        self.parallel = parallel


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
api.ImagingMeasurementSetsPreferred.register(MakeImagesInputs)


@task_registry.set_equivalent_casa_task('hif_makeimages')
@task_registry.set_casa_commands_comment('A list of target sources is cleaned.')
class MakeImages(basetask.StandardTaskTemplate):
    Inputs = MakeImagesInputs

    is_multi_vis_task = True

    def prepare(self):
        inputs = self.inputs

        result = MakeImagesResult()

        # Carry any message from hif_makeimlist (e.g. for missing PI cube target)
        result.set_info(inputs.context.clean_list_info)

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result.mitigation_error = True
                return result

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if type(inputs.vis) is not types.ListType:
            inputs.vis = [inputs.vis]

        with CleanTaskFactory(inputs, self._executor) as factory:
            task_queue = [(target, factory.get_task(target))
                          for target in inputs.target_list]

            for (target, task) in task_queue:
                try:
                    worker_result = task.get_result()
                except exceptions.PipelineException:
                    result.add_result(TcleanResult(), target, outcome='failure')
                else:
                    # Note add_result() removes 'heuristics' from worker_result
                    heuristics = target['heuristics']
                    result.add_result(worker_result, target, outcome='success')
                    # Export RMS (reprSrc, reprSpw only)
                    repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, sensitivityGoal = heuristics.representative_target()
                    if real_repr_target and str(repr_spw) == worker_result.spw and \
                    repr_source==worker_result.sourcename:
                        s = self._get_image_rms_as_sensitivity(worker_result, target, heuristics)
                        if s is not None:
                            result.sensitivities_for_aqua.append(s)

        # set of descriptions
        if inputs.context.clean_list_info.get('msg', '') != '':
            description = {
                _get_description_map(inputs.context.clean_list_info.get('intent', '')).get(inputs.context.clean_list_info.get('specmode', ''), 'Calculate clean products')  # map specmode to description..
            }
        else:
            description = {
                _get_description_map(target['intent']).get(target['specmode'], 'Calculate clean products')  # map specmode to description..
                for target in inputs.target_list                       # .. for every clean target..
            }

        result.metadata['long description'] = ' / '.join(description)

        return result

    def analyse(self, result):
        return result
    
    def _get_image_rms_as_sensitivity(self, result, target, heuristics):
        imname = result.image
        if not os.path.exists(imname):
            return None
        cqa = casatools.quanta
        cell = target['cell'][0:2] if len(target['cell']) >= 2 else (target['cell'][0], target['cell'][0])
        with casatools.ImageReader(imname) as image:
            restoringbeam = image.restoringbeam()
            csys = image.coordsys()
            chan_width = csys.increment(type='spectral', format='q')['quantity']['*1']
            csys.done()
        # effectiveBW
        if result.specmode == 'cube': # use nbin for cube and repBW
            msobj = self.inputs.context.observing_run.get_ms(name=result.vis[0])
            nbin = target['nbin'] if target['nbin'] > 0 else 1
            SCF, physicalBW_of_1chan, effectiveBW_of_1chan = heuristics.get_bw_corr_factor(msobj, result.spw, nbin)
            effectiveBW_of_image = cqa.quantity(nbin / SCF**2 * effectiveBW_of_1chan, 'Hz')
        else: #continuum mode
            effectiveBW_of_image = result.aggregate_bw

        return Sensitivity(array='undefined',
                           field=target['field'],
                           spw=result.spw,
                           bandwidth=effectiveBW_of_image,
                           bwmode=result.orig_specmode,
                           beam=restoringbeam,
                           cell=cell,
                           robust=target['robust'],
                           uvtaper=target['uvtaper'],
                           sensitivity=cqa.quantity(result.image_rms, 'Jy/beam'))

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
        parallel_wanted = mpihelpers.parse_mpi_input_parameter(self.__inputs.parallel)

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
            'weighting': inputs.weighting,
            # other vals
            'tlimit': inputs.tlimit,
            'masklimit': inputs.masklimit,
            'cleancontranges': inputs.cleancontranges,
            'calcsb': inputs.calcsb,
            'parallel': parallel,
        })

        if target['robust']:
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

        if inputs.hm_masking == '':
            if 'TARGET' in task_args['intent']:
                # For the time being the target imaging uses the
                # inner quarter. Other methods will be made available
                # later.
                #datatask_args['hm_masking'] = 'psfiter'
                task_args['hm_masking'] = 'auto'
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

        if inputs.hm_cleaning == '':
            task_args['hm_cleaning'] = 'rms'
        else:
            task_args['hm_cleaning'] = inputs.hm_cleaning

        if task_args['hm_masking'] == 'psfiter':
            task_args['maxncleans'] = inputs.maxncleans
        else:
            task_args['maxncleans'] = 1

        if target['vis']:
            task_args['vis'] = target['vis']

        if target['is_per_eb']:
            task_args['is_per_eb'] = target['is_per_eb']

        return task_args


def _get_description_map(intent):
    if intent in ('PHASE', 'BANDPASS'):
        return {
            'mfs': 'Make calibrator images'
        }
    elif intent == 'CHECK':
        return {
            'mfs': 'Make check source images'
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
