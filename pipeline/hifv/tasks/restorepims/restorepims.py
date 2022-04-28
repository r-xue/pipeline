import copy
import glob
import os
import re
import tarfile

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
@task_registry.set_casa_commands_comment('Restore rfi-flagged and self-calibrated visibility for a per-image measurement set (PIMS) using reimaging resources from the single-epoch continuum imaging products')
class Restorepims(basetask.StandardTaskTemplate):
    Inputs = RestorepimsInputs

    def prepare(self):

        LOG.info("This Restorepims class is running.")

        # flag version after hifv_checkflag and before hifv_statwt in the SEIP production workflow.
        self.flagversion = 'statwt_1'
        self.caltable = None
        self.imagename = self.inputs.context.clean_list_pending[0]['imagename'].replace(
            'sSTAGENUMBER.', '')
        self.restore_resources = {}

        is_resources_available = self._check_resources()

        if not is_resources_available:
            raise exceptions.PipelineException(
                "The resources required for hifv_restorepims() are not available, and Pipeline cannot continue.")

        # restore the MODEL column to the identical state used in the SE hifv_selfcal() and hifv_statwt() stages.
        # the flag state at this moment should be identical to the initial state in the SEIP worflow (before SE/hifv_checkflag)
        self._do_restoremodel()

        # restore the flag version (after SE/hifv_checkflag and before SE/hifv_statwt) from the SE flag file.
        self._do_restoreflags(versionname=self.flagversion)

        # calculate WEIGHT from DATA-MODEL, identical to the one generated in SE:hifv_statwt()
        self._do_statwt()

        # apply the selfcal table from the SE reimaging resources to get the CORRECTED column
        self._do_applycal()

        results = RestorepimsResults(mask_list=self.mask_list, restore_resources=self.restore_resources)

        return results

    def analyse(self, results):
        return results

    def _check_resources(self):

        reimaging_resources_tgz = self.inputs.reimaging_resources
        is_resources_available = False

        # check the reimaging resources tarball
        if os.path.isfile(reimaging_resources_tgz):
            if tarfile.is_tarfile(reimaging_resources_tgz):
                is_resources_available = True
        if is_resources_available:
            LOG.info(f"Found the reimaging resources file ({reimaging_resources_tgz})")
            self.restore_resources['reimaging_resources'] = (reimaging_resources_tgz, True)
        else:
            LOG.error(
                f"The reimaging resources file ({reimaging_resources_tgz}) doesn't exist or is not a tarfile.")
            self.restore_resources['reimaging_resources'] = (reimaging_resources_tgz, False)
            return is_resources_available

        # untar any files required for the restorepims operation
        with tarfile.open(reimaging_resources_tgz, 'r:gz') as tar:
            members = []
            for member in tar.getmembers():
                if member.name.startswith(self.inputs.vis + '.flagversions/'):
                    members.append(member)
                if member.name.startswith(self.inputs.vis) and '.phase-self-cal.tbl/' in member.name:
                    members.append(member)
                if re.search(r'^s\d{1,2}_0\.'+re.escape(self.imagename)+r'\.I\.iter1\.model', member.name) is not None:
                    members.append(member)
                if re.search(r'^s\d{1,2}_0\.'+re.escape(self.imagename)+r'\.QLcatmask-tier1\.mask/', member.name) is not None:
                    members.append(member)
                if re.search(r'^s\d{1,2}_0\.'+re.escape(self.imagename)+r'\.combined-tier2\.mask/', member.name) is not None:
                    members.append(member)
            tar.extractall(path='.', members=members)

        # check selfcal table from vlass-se-cont
        selfcal_tbs = glob.glob(self.inputs.vis+'.*.phase-self-cal.tbl')
        if len(selfcal_tbs) != 1:
            LOG.error("Cannot find the required selfcal table (*.phase-self-cal.tbl), or more than one selfcal tables are found!")
            is_resources_available = False
            self.restore_resources['selfcal_table'] = (self.inputs.vis+'.*.phase-self-cal.tbl', False)
        else:
            self.caltable = selfcal_tbs[0]
            self.restore_resources['selfcal_table'] = (self.caltable, True)
            LOG.info(f"The task will use the selfcal table {self.caltable}")

        # check the backup flag version from vlass-se-cont
        flag_prefix = self.inputs.vis+'.flagversions/flags.'
        flagversion_tbs = glob.glob(flag_prefix+'*')
        flagversion_names = [tb0.replace(flag_prefix, '', 1) for tb0 in flagversion_tbs]
        if self.flagversion not in flagversion_names:
            LOG.error(f"The requested flag version ({self.flagversion}) doesn't exist.")
            is_resources_available = False
            self.restore_resources['flag_table'] = (f"{self.inputs.vis}.flagversions/{self.flagversion}", False)
        else:
            LOG.info(f"Found the requested flag version '{self.flagversion}' from {self.inputs.vis}.flagversions")
            self.restore_resources['flag_table'] = (f"{self.inputs.vis}.flagversions/{self.flagversion}", True)

        tier1_mask = glob.glob('s*_0.'+self.imagename+'.QLcatmask-tier1.mask')
        tier2_mask = glob.glob('s*_0.'+self.imagename+'.combined-tier2.mask')
        n_tier1 = len(tier1_mask)
        n_tier2 = len(tier2_mask)
        if n_tier1 != 1 or n_tier2 != 1:
            LOG.error(
                f"Found {n_tier1} tier1 mask{'s'[:n_tier1^1]} and {n_tier2} tier2 mask{'s'[:n_tier2^1]}, which is unexpected.")
            self.restore_resources['tier1_mask'] = ('s*_0.'+self.imagename+'.QLcatmask-tier1.mask', False)
            self.restore_resources['tier2_mask'] = ('s*_0.'+self.imagename+'.combined-tier2.mask', False)
        else:
            self.mask_list = [tier1_mask[0], tier2_mask[0]]
            LOG.info(f"Found the requested tclean mask list: {self.mask_list}")
            self.restore_resources['tier1_mask'] = (self.mask_list[0], True)
            self.restore_resources['tier2_mask'] = (self.mask_list[1], True)

        model_images = glob.glob('s*_0.'+self.imagename+'.I.iter1.model.tt?')
        if len(model_images) == 0:
            is_resources_available = False
            LOG.error(
                f"Can't find the SE vlass_stage=1 model images s*_0.{self.imagename}.I.iter1.model* for the MODEL column prediction")
            self.restore_resources['model_image'] = ('s*_0.'+self.imagename+'.I.iter1.model.tt?', False)
        else:
            last_idx = model_images[0].rfind('.model')
            self.imagename = model_images[0][:last_idx]
            LOG.info(f"Found the requested tclean model image(s): {model_images}")
            LOG.info(f"Use tclean:imagename={self.imagename} for the MODEL column prediction.")
            self.restore_resources['model_image'] = (f'{self.imagename}'+'.tt0', True)

        return is_resources_available

    def _do_restoreflags(self, versionname=None):

        if versionname is None:
            versionname = self.flagversion
        task = casa_tasks.flagmanager(vis=self.inputs.vis, mode='restore', versionname=versionname)

        return self._executor.execute(task)

    def _do_restoremodel(self):

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames():
                LOG.info('Writing model data to {}'.format(ms.basename))
            else:
                LOG.warning('MODEL_DATA column found in {} and will be overwritten.'.format(ms.basename))

        imaging_parameters = self._restoremodel_imaging_parameters()
        job = casa_tasks.tclean(**imaging_parameters)

        return self._executor.execute(job)

    def _restoremodel_imaging_parameters(self):
        """Create the tclean parameter set for filling the model column."""

        imaging_parameters = copy.deepcopy(self.inputs.context.clean_list_pending[0])
        for par_removed in ['nbin', 'sensitivity', 'heuristics', 'intent']:
            imaging_parameters.pop(par_removed, None)

        # adopt the SE imaging heuristics from SE:stage5
        imaging_parameters['vis'] = self.inputs.vis
        imaging_parameters['imagename'] = self.imagename
        imaging_parameters['cleanmask'] = ''
        imaging_parameters['niter'] = 0
        imaging_parameters['threshold'] = '0.0mJy'
        imaging_parameters['nsigma'] = 0
        imaging_parameters['weighting'] = 'briggs'
        imaging_parameters['robust'] = -2.0
        imaging_parameters['outframe'] = 'LSRK'
        imaging_parameters['calcres'] = True
        imaging_parameters['calcpsf'] = True
        imaging_parameters['savemodel'] = 'modelcolumn'
        imaging_parameters['parallel'] = False
        imaging_parameters['pointingoffsetsigdev'] = [300, 30]
        imaging_parameters['mosweight'] = False
        imaging_parameters['rotatepastep'] = 5.0
        imaging_parameters['pbcor'] = False
        imaging_parameters['pblimit'] = 0.1
        imaging_parameters['specmode'] = 'mfs'

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

        job = casa_tasks.statwt(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self):
        """Rerun applycal using the selfcal table from the SE reimaging resources.

        also see the SEIP setting in hifv_selfcal()
        """
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spwsobjlist = m.get_spectral_windows(science_windows_only=True)
        spws = [int(spw.id) for spw in spwsobjlist]
        numspws = len(m.get_spectral_windows(science_windows_only=False))
        lowestscispwid = min(spws)  # PIPE-101, PIPE-1042: spwmap parameter in applycal must be a list of integers

        # VLASS mode
        applycal_task_args = {'vis': self.inputs.vis,
                              'gaintable': self.caltable,
                              'interp': ['nearestPD'],
                              'spwmap': [numspws * [lowestscispwid]],
                              'parang': False,
                              'applymode': 'calonly'}

        applycal_task_args['calwt'] = False
        applycal_task_args['interp'] = ['nearest']

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)
