import copy
import glob
import os
import tarfile
from fnmatch import fnmatch

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.exceptions as exceptions
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry

LOG = infrastructure.get_logger(__name__)


class RestorepimsResults(basetask.Results):
    def __init__(self, mask_list=[], restore_resources=None):
        super().__init__()
        self.pipeline_casa_task = 'Restorepims'
        self.mask_list = mask_list
        self.restore_resources = restore_resources

    def merge_with_context(self, context):
        """See :method:`~pipeline.infrastructure.api.Results.merge_with_context`."""
        return

    def __repr__(self):
        return f'RestorepimsResults:\n\tmask_list={self.mask_list}'


class RestorepimsInputs(vdp.StandardInputs):

    reimaging_resources = vdp.VisDependentProperty(default='reimaging_resources.tgz')

    def __init__(self, context, vis=None, reimaging_resources=None):
        self.context = context
        self.vis = vis
        self.reimaging_resources = reimaging_resources


@task_registry.set_equivalent_casa_task('hifv_restorepims')
@task_registry.set_casa_commands_comment('Restore RFI-flagged and self-calibrated visibility for a per-image measurement set (PIMS) using reimaging resources from the single-epoch continuum imaging products.')
class Restorepims(basetask.StandardTaskTemplate):
    Inputs = RestorepimsInputs

    def prepare(self):

        LOG.info("This Restorepims class is running.")

        # flag version after hifv_checkflag and before hifv_statwt in the SEIP production workflow.
        self.flagversion = 'statwt_1'
        self.flagversion_backup = 'hifv_restorepims_initial'
        self.imagename = self.inputs.context.clean_list_pending[0]['imagename'].replace(
            'sSTAGENUMBER.', '')
        self.restore_resources = {}

        # extract all essential resources
        is_resources_available = self._check_resources()

        if not is_resources_available:
            exception_msg = []
            for k, v in self.restore_resources.items():
                exception_msg.append(f"{k:20}: {v}")
            exception_msg.insert(
                0, 'Some resources required for hifv_restorepims()/hif_makeimages() are not available, and Pipeline cannot continue.')
            raise exceptions.PipelineException(f"\n{'-'*120}\n"+'\n'.join(exception_msg)+f"\n{'-'*120}\n")

        # backup the intial flag state when needed
        self._backup_flags()

        # restore the MODEL column to the identical state used in the SE hifv_selfcal() and hifv_statwt() stages.
        # the flag state at this moment should be identical to the initial state in the SEIP worflow (before SE/hifv_checkflag)
        self._do_restoremodel()

        # restore the flag version (after SE/hifv_checkflag and before SE/hifv_statwt) from the SE flag file.
        self._do_restoreflags(versionname=self.flagversion)

        # calculate WEIGHT from DATA-MODEL, identical to the one generated in SE:hifv_statwt()
        self._do_statwt()

        # apply the selfcal table from the SE reimaging resources to get the CORRECTED column
        self._do_applycal()

        mask_list = [self.restore_resources['tier1_mask'][0][0], self.restore_resources['tier2_mask'][0][0]]
        results = RestorepimsResults(mask_list=mask_list, restore_resources=self.restore_resources)

        return results

    def analyse(self, results):
        return results

    def _backup_flags(self):
        """Backup the initial FLAS state in PIMS, in case the task gets re-run."""

        flagversion_backup_path = self.inputs.vis+'.flagversions/flags.'+self.flagversion_backup
        if not os.path.exists(flagversion_backup_path):
            # back up the initial flags, in case one wants to rerun hifv_restorepims().
            job = casa_tasks.flagmanager(vis=self.inputs.vis, mode='save',
                                         versionname=self.flagversion_backup,
                                         comment='flagversion before running hifv_restorepims() first time',
                                         merge='replace')
            self._executor.execute(job)
        else:
            LOG.warning(
                f'Found the FLAGs backup from hifv_restorepims() under the name: {self.flagversion_backup}, and will skip new backup')

    def _check_resources(self):

        is_resources_available = False

        # Create the resource request list
        # - self.imagename at this point is from SEIP_parameter.list of the SEIP products.
        # - inputs.vis should be identical to the SEIP input with the same flag state.
        # Therefore, we verify these names against the file list inside reimaging_resources.tgz

        reimaging_resources_tgz = self.inputs.reimaging_resources
        flagd_pat_key_desc = (self.inputs.vis+'.flagversions*', 'flag_dir', 'flag directory')
        flagv_pat_key_desc = (self.inputs.vis+f'.flagversions/flags.{self.flagversion}', 'flag_version', 'flag version')
        sctab_pat_key_desc = (self.inputs.vis+'.*.phase-self-cal.tbl*', 'selfcal_table', 'selfcal table')
        model_pat_key_desc = ('s*_0.'+self.imagename+'.I.iter1.model*', 'model_images', 'model image(s)')
        tier1_pat_key_desc = ('s*_0.'+self.imagename+'.QLcatmask-tier1.mask*', 'tier1_mask', 'tier1 mask')
        tier2_pat_key_desc = ('s*_0.'+self.imagename+'.combined-tier2.mask*', 'tier2_mask', 'tier2 mask')
        pkd_list = [flagd_pat_key_desc, flagv_pat_key_desc, sctab_pat_key_desc,
                    model_pat_key_desc, tier1_pat_key_desc, tier2_pat_key_desc]

        # check the reimaging resources tarball before trying unpacking.

        if os.path.isfile(reimaging_resources_tgz) and tarfile.is_tarfile(reimaging_resources_tgz):
            is_resources_available = True
        self.restore_resources['reimaging_resources'] = (reimaging_resources_tgz, is_resources_available)
        if is_resources_available:
            LOG.info(f"Found the reimaging resources tarball: {reimaging_resources_tgz}")
        else:
            LOG.error(
                f"The reimaging resources tarball {reimaging_resources_tgz} doesn't exist or is not a tarfile.")
            return is_resources_available

        # untar any files required for the restorepims operation:

        with tarfile.open(reimaging_resources_tgz, 'r:gz') as tar:

            members = []
            for member in tar.getmembers():
                is_resource = any([fnmatch(member.name, pkd[0]) for pkd in pkd_list])
                if is_resource:
                    members.append(member)
            # if the request flagversion already exists, do not override the flagversions directory to preserve
            # any recent flagversions, e.g. the hifv_restorepims backup.
            if os.path.exists(flagv_pat_key_desc[0]):
                LOG.warning(
                    f'{flagv_pat_key_desc[0]} exists and the .flagversions directory will be re-used and not extracted.')
                members = [member for member in members if not fnmatch(member.name, flagd_pat_key_desc[0])]
            for member in members:
                LOG.info(f'extracting: {member.name}')

            tar.extractall(path='.', members=members)

        # check against the resource requirement list

        for pkd in pkd_list:
            paths = glob.glob(pkd[0])
            n_paths = len(paths)
            if n_paths == 0:
                is_resources_available = False
                LOG.error(
                    f"Cannot find the request {pkd[2]} using the name pattern: {pkd[0]}")
                is_resources_available = False
                self.restore_resources[pkd[1]] = (pkd[0], False)    # (file_pattern, False)
            else:
                self.restore_resources[pkd[1]] = (paths, True)      # (file_list, True)
                LOG.info(f"Found the request {pkd[2]}: {', '.join(paths)}")

        return is_resources_available

    def _do_restoreflags(self, versionname=None):

        if versionname is None:
            versionname = self.flagversion

        task = casa_tasks.flagmanager(vis=self.inputs.vis, mode='restore', versionname=versionname)
        return self._executor.execute(task)

    def _reinitialize_pims(self):
        """Re-intialize the WEIGHTs/FLAGs of a PIMS.
        
        Note: this method is only called when hifv_restorepims() gets rerun.
        """

        LOG.warning(f'Because the FLAGs/WEIGHTs are likely already modified, to recover the PIMS initial state, we attempt to:')
        LOG.warning(f' 1. restore FLAGs from earlier backup from hifv_restorepims().')
        LOG.warning(f' 2. re-initialize WEIGHTs/SIGMAs.')

        flagversion_backup_path = self.inputs.vis+'.flagversions/flags.'+self.flagversion_backup
        if os.path.exists(flagversion_backup_path):
            task = casa_tasks.flagmanager(vis=self.inputs.vis, mode='restore', versionname=self.flagversion_backup)
            self._executor.execute(task)
        else:
            LOG.warning('Cannot find earlier FLAGS backup by hifv_restorepims, but will continue.')

        LOG.info(f'Re-initializing the weights in {self.inputs.vis}.')
        task = casa_tasks.initweights(vis=self.inputs.vis, wtmode='nyq')
        self._executor.execute(task)

    def _do_restoremodel(self):

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        with casa_tools.TableReader(ms.name) as table:
            is_modelcolumn_present = 'MODEL_DATA' in table.colnames()
        with casa_tools.TableReader(ms.name+'/SOURCE') as table:
            is_virtualmodel_present = 'SOURCE_MODEL' in table.colnames()

        # remove any (unlikely) virtual/otf model in the MS/SOURCE subtable to prevent it overriding the effect of the modelcolumn.
        # see: https://casadocs.readthedocs.io/en/latest/api/tt/casatasks.imaging.tclean.html?highlight=savemodel#savemodel
        if is_virtualmodel_present:
            job = casa_tasks.delmod(vis=self.inputs.vis, otf=True, scr=False)
            self._executor.execute(job)

        # re-initialze FLAGs/WEIGHTs if needed to ensure identical predicting (vs. SE-CONT) from model image created with normtype='flatnoise'.
        if is_modelcolumn_present:
            LOG.warning(f'MODEL_DATA column found in {ms.basename} and will be overwritten.')
            self._reinitialize_pims()
        else:
            LOG.info('Writing model data to {}'.format(ms.basename))

        imaging_parameters = self._restoremodel_imaging_parameters()

        mp_nthreads = casa_tools.casalog.ompGetNumThreads()
        mpi_parallel = imaging_parameters['parallel']
        LOG.info(f'Predicting the MODEL column with the following tclean() parallelization settings:')
        LOG.info(f'    openmp_nthreads : {mp_nthreads}')
        LOG.info(f'    mpi_parallel    : {mpi_parallel}')

        job = casa_tasks.tclean(**imaging_parameters)

        return self._executor.execute(job)

    def _restoremodel_imaging_parameters(self):
        """Create the tclean parameter set for filling the model column.
        
        Note:
            1.  to restore the model to the same state as in the SE workflow (i.e., performing the identical same model image->vis transform), 
                one has to ensure :
                - calpsf=True when .psf/.weight/.sumwt/. is missing, as in the case of reimaging_resources.tgz.
                    - calcpsf=False ony work properly if .psf/.weight/.sumwt are presents (as de-degridding needs WEIGHTs or proper initialization).
                - flags: must be identical to the initial flags after hif_transformimagedata() when calcpsf=True (as it affects the initialization of imager/de-gridder)
                - WTs: must be identical to the initial weights after hif_transformimagedata()                
                - Using the same imaging scheme (e.g. weighting, gridder, uvtaper, layout, cfcache, etc.) because it affects the initialization of imager/de-gridder.
                    - .model is not a sky model in absolute flux scale
                    - PB, mosaic-pattern, weighting, gridder choice, etc. determines the transformation of 'flatnoise' models from the image to the visibility domain jointly.
            2.  OpenMP threading must be identical, otherwise, small numerical difference would occur.
                For a byte-to-byte match (assuming the same CASA version), a hifv_restorepims() called from mpicasa will need the reimaging_resources.tgz
                from a VLASS-SE-CONT parallel run.

        """

        imaging_parameters = copy.deepcopy(self.inputs.context.clean_list_pending[0])
        for par_removed in ['nbin', 'sensitivity', 'heuristics', 'intent']:
            imaging_parameters.pop(par_removed, None)

        model_image = self.restore_resources['model_images'][0][0]
        last_idx = model_image.rfind('.model')
        imagename = model_image[:last_idx]

        # adopt the SE imaging heuristics from SE:stage5
        imaging_parameters['vis'] = self.inputs.vis
        # explictly set to default which might be changed later if we decide not re-using the SE-CONT naming.
        imaging_parameters['startmodel'] = ''
        imaging_parameters['imagename'] = imagename
        imaging_parameters['cleanmask'] = ''
        imaging_parameters['niter'] = 0
        imaging_parameters['threshold'] = '0.0mJy'
        imaging_parameters['nsigma'] = 0
        imaging_parameters['weighting'] = 'briggs'
        imaging_parameters['robust'] = -2.0
        imaging_parameters['outframe'] = 'LSRK'
        imaging_parameters['savemodel'] = 'modelcolumn'
        imaging_parameters['parallel'] = False
        imaging_parameters['pointingoffsetsigdev'] = [300, 30]
        imaging_parameters['mosweight'] = False
        imaging_parameters['rotatepastep'] = 5.0
        imaging_parameters['pbcor'] = False
        imaging_parameters['pblimit'] = 0.1
        imaging_parameters['specmode'] = 'mfs'

        imaging_parameters['calcres'] = False
        imaging_parameters['calcpsf'] = True      # required, see the comments in this method.

        # remove non-model pre-xisting images as they will be re-computed.
        images_present = glob.glob(imagename+'.*')
        for image_present in images_present:
            if not fnmatch(image_present, imagename+'.model*'):
                job = casa_tasks.rmtree(image_present)
                self._executor.execute(job)

        return imaging_parameters

    def _do_statwt(self):
        """Rerun statwt following the SE setting.

        Also see the SEIP setting in hifv_statwt()
        """
        task_args = {'vis': self.inputs.vis,
                     'fitspw': '',
                     'fitcorr': '',
                     'combine': '',
                     'minsamp': 8,
                     'field': '',
                     'spw': '',
                     'datacolumn': 'residual_data'}

        task_args['combine'] = 'field,scan,state,corr'
        task_args['minsamp'] = ''
        task_args['chanbin'] = 1
        task_args['timebin'] = '1yr'

        # the flag state has already been restored from SE/statwt_1 at this point, no need to backup again
        task_args['flagbackup'] = False

        job = casa_tasks.statwt(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self):
        """Rerun applycal using the selfcal table from the SE reimaging resources.

        Also see the SEIP setting in hifv_selfcal()
        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spwsobjlist = m.get_spectral_windows(science_windows_only=True)
        spws = [int(spw.id) for spw in spwsobjlist]
        numspws = len(m.get_spectral_windows(science_windows_only=False))
        lowestscispwid = min(spws)  # PIPE-101, PIPE-1042: spwmap parameter in applycal must be a list of integers
        gaintable = self.restore_resources['selfcal_table'][0][0]

        # VLASS mode
        applycal_task_args = {'vis': self.inputs.vis,
                              'gaintable': gaintable,
                              'interp': ['nearestPD'],
                              'spwmap': [numspws * [lowestscispwid]],
                              'parang': False,
                              'applymode': 'calonly'}

        applycal_task_args['calwt'] = False
        applycal_task_args['interp'] = ['nearest']

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)
