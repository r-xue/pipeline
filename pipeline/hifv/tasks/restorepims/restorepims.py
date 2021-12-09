import os
import tarfile
import glob
import copy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry

LOG = infrastructure.get_logger(__name__)


class RestorepimsResults(basetask.Results):
    def __init__(self):
        super(RestorepimsResults, self).__init__()
        self.pipeline_casa_task = 'Restorepims'

    def merge_with_context(self, context):
        """See :method:`~pipeline.infrastructure.api.Results.merge_with_context`."""
        return

    def __repr__(self):
        #return 'RestorepimsResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'RestorepimsResults:'


class RestorepimsInputs(vdp.StandardInputs):

    reimaging_resources = vdp.VisDependentProperty(default='reimaging_resources.tgz')

    def __init__(self, context, vis=None, reimaging_resources=None):
        self.context = context
        self.vis = vis
        self.reimaging_resources = reimaging_resources


@task_registry.set_equivalent_casa_task('hifv_restorepims')
@task_registry.set_casa_commands_comment('Restore calibrated visibility for a per-image measurement set (PIMS) using the reimaging resources from single-epoch imaging products')
class Restorepims(basetask.StandardTaskTemplate):
    Inputs = RestorepimsInputs

    def prepare(self):

        LOG.info("This Restorepims class is running.")

        self.flagversion = 'statwt_1'
        self.caltable = None
        self.imagename = self.inputs.context.clean_list_pending[0]['imagename'].replace(
            'sSTAGENUMBER', 's5_0')+'.I.iter1'

        self._check_resources()

        # restore the flag version (just before SE/hifv_statwt) from the SE flag file
        self._do_restoreflags()

        # restore the model column used fro SE/selfcal
        self._do_restoremodel()

        # calculate weighting from DATA_RESIDUAL (empty corrected column at this point)
        self._do_statwt()

        # apply the selfcal table from the SE reimaging resources
        self._do_applycal()

        return RestorepimsResults()

    def analyse(self, results):
        return results

    def _check_resources(self):

        reimaging_resources_tgz = self.inputs.reimaging_resources
        is_resources_available = False
        if os.path.isfile(reimaging_resources_tgz):
            if tarfile.is_tarfile(reimaging_resources_tgz):
                is_resources_available = True

        if is_resources_available:
            LOG.info(f"Found the reimaging resources file ({reimaging_resources_tgz})")
        else:
            LOG.error(
                f"The required reimaging resources file ({reimaging_resources_tgz}) doesn't exist or is not a tar file.")

        with tarfile.open(reimaging_resources_tgz, 'r:gz') as tar:
            members = []
            for member in tar.getmembers():
                if member.name.startswith(self.inputs.vis + '.flagversions/'):
                    members.append(member)
                if member.name.startswith(self.inputs.vis) and '.phase-self-cal.tbl/' in member.name:
                    members.append(member)
                if member.name.startswith(self.imagename):
                    members.append(member)
            tar.extractall(path='.', members=members)

        is_resources_ready = True
        selfcal_tbs = glob.glob(self.inputs.vis+'.*.phase-self-cal.tbl')
        if len(selfcal_tbs) != 1:
            LOG.error(f"The required selfcal table doesn't exist or more than one selfcal tables are found!")
            is_resources_ready = False
        else:
            self.caltable = selfcal_tbs[0]

        if not os.path.isdir(self.inputs.vis+'.flagversions/flags.'+self.flagversion):
            LOG.error(f"The required flag version ({self.flagversion}) doesn't exist.")
            is_resources_ready = False

        if not (os.path.isdir(self.imagename+'.model.tt0') and os.path.isdir(self.imagename+'.model.tt0')):
            is_resources_ready = False

        return is_resources_ready

    def _do_restoreflags(self, versionname=None):

        if versionname is None:
            versionname = self.flagversion
        task = casa_tasks.flagmanager(vis=self.inputs.vis, mode='restore', versionname=versionname)

        return self._executor.execute(task)

    def _do_restoremodel(self):
        """_do_restoremodel [summary]

        [extended_summary]
        """
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames():
                LOG.info('Writing model data to {}'.format(ms.basename))
            else:
                LOG.info('MODEL_DATA column found in {} and will be overritten'.format(ms.basename))

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
        
        also see hifv_statwt()
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
        
        also see hifv_selfcal()
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
