import os
import tarfile
import glob

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
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        return

    def __repr__(self):
        #return 'RestorepimsResults:\n\t{0}'.format(
        #    '\n\t'.join([ms.name for ms in self.mses]))
        return 'RestorepimsResults:'


class RestorepimsInputs(vdp.StandardInputs):
    def __init__(self, context, vis=None):
        self.context = context
        self.vis = vis


@task_registry.set_equivalent_casa_task('hifv_restorepims')
@task_registry.set_casa_commands_comment('Add your task description for inclusion in casa_commands.log')
class Restorepims(basetask.StandardTaskTemplate):
    Inputs = RestorepimsInputs

    def prepare(self):

        LOG.info("This Restorepims class is running.")

        self.reimaging_resources = 'reimaging_resources.tgz'
        self.flagversion = 'statwt_1'
        self.caltable = None

        self._check_resources()
        self._do_restoreflags()
        self._do_restoremodel()
        self._do_statwt()
        self._do_applycal()

        return RestorepimsResults()

    def analyse(self, results):
        return results

    def _check_resources(self):

        with tarfile.open(self.reimaging_resources, 'r') as tar:
            members = []
            for member in tar.getmembers():
                if member.name.startswith(self.inputs.vis + '.flagversions'):
                    members.append(member)
                if member.name.startswith(self.inputs.vis) and 'phase-self-cal.tbl' in member.name:
                    members.append(member)
            tar.extractall(members=members)

        selfcal_tbs = glob.glob(self.inputs.vis+'*'+'phase-self-cal.tbl')
        if len(selfcal_tbs) != 1:
            LOG.error(f"The required selfcal table doesn't exist or more than one selfcal tables are found!")
        else:
            self.caltable = selfcal_tbs[0]

        if not os.path.isdir(self.inputs.vis+'.flagversions/flags.'+self.flagversion):
            LOG.error(f"The required flag version ({self.flagversion}) doesn't exist.")

        return

    def _do_restoreflags(self):

        task = casa_tasks.flagmanager(vis=self.inputs.vis, mode='restore', versionname=self.flagversion)
        self._executor.execute(task)

        return

    def _do_restoremodel(self):

        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames():
                LOG.info('Writing model data to {}'.format(ms.basename))
            else:
                LOG.info('MODEL_DATA column found in {} and will be overritten'.format(ms.basename))

        imaging_parameters = self._restoremodel_imaging_parameters()
        job = casa_tasks.tclean(**imaging_parameters)
        tclean_result = self._executor.execute(job)

    def _restoremodel_imaging_parameters(self):

        par_list = ['field', 'imagename', 'spw', 'uvrange', 'imsize', 'cell', 'phasecenter',
                    'specmode', 'reffreq', 'nchan', 'stokes', 'uvtaper', 'gridder']
        imaging_parameters = {}
        for par in par_list:
            imaging_parameters[par] = self.inputs.context.clean_list_pending[0][par]

        imaging_parameters['vis'] = self.inputs.vis
        imaging_parameters['imagename'] = imaging_parameters['imagename'].replace('STAGENUMBER', '5')+'.iter1'
        imaging_parameters['calcres'] = True
        imaging_parameters['calcpsf'] = True
        imaging_parameters['savemodel'] = 'modelcolumn'
        imaging_parameters['parallel'] = False
        imaging_parameters['cleanmask'] = ''
        imaging_parameters['niter'] = 0
        imaging_parameters['threshold'] = '0.0mJy'
        imaging_parameters['nsigma'] = -1

        return imaging_parameters

    def _do_statwt(self):

        # VLA (default mode)
        # Note if default task_args changes, then 'vlass-se' case might need to be updated (PIPE-723)
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
        """Run CASA task applycal"""

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
