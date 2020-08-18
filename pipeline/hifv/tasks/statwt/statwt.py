import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import cont_file_to_CASA
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
import pipeline.infrastructure.casatools as casatools
from pipeline.hifv.heuristics import set_add_model_column_parameters

LOG = infrastructure.get_logger(__name__)

# CALCULATE DATA WEIGHTS BASED ON ST. DEV. WITHIN EACH SPW
# use statwt


class StatwtInputs(vdp.StandardInputs):
    datacolumn = vdp.VisDependentProperty(default='corrected')
    overwrite_modelcol = vdp.VisDependentProperty(default=False)
    statwtmode = vdp.VisDependentProperty(default='VLA')

    def __init__(self, context, vis=None, datacolumn=None, overwrite_modelcol=None, statwtmode=None):
        super(StatwtInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.datacolumn = datacolumn
        self.overwrite_modelcol = overwrite_modelcol
        self.statwtmode = statwtmode


class StatwtResults(basetask.Results):
    def __init__(self, jobs=None, flag_summaries=[]):

        if jobs is None:
            jobs = []

        super(StatwtResults, self).__init__()
        self.jobs = jobs
        self.summaries = flag_summaries

    def __repr__(self):
        s = 'Statwt results:\n'
        for job in self.jobs:
            s += '%s performed. ' % str(job)
        return s 


@task_registry.set_equivalent_casa_task('hifv_statwt')
class Statwt(basetask.StandardTaskTemplate):
    Inputs = StatwtInputs

    def prepare(self):

        if self.inputs.datacolumn == 'residual_data':
            LOG.info('Checking for model column')
            self._check_for_modelcolumn()

        if self.inputs.statwtmode not in ['VLA','VLASS-SE']:
            LOG.warn('Unkown mode \'%s\' was set. Known modes are [\'VLA\',\'VLASS-SE\']. '
                     'Continuing in \'VLA\' mode.' % self.inputs.statwtmode)
            self.inputs.statwtmode = 'VLA'

        fielddict = cont_file_to_CASA(self.inputs.vis, self.inputs.context)
        fields = ','.join(str(x) for x in fielddict) if fielddict != {} else ''

        flag_summaries = []
        # flag statistics before task
        flag_summaries.append(self._do_flagsummary('before', field=fields))
        # actual statwt operation
        statwt_result = self._do_statwt(fielddict)
        # flag statistics after task
        flag_summaries.append(self._do_flagsummary('statwt', field=fields))

        return StatwtResults(jobs=[statwt_result], flag_summaries=flag_summaries)

    def analyse(self, results):
        return results

    def _do_statwt(self, fielddict):

        if fielddict != {}:
            LOG.info('cont.dat file present.  Using VLA Spectral Line Heuristics for task statwt.')

        # VLA (default mode)
        # Note if default task_args changes, then 'vlass-se' case might need to be updated (PIPE-723)
        task_args = {'vis': self.inputs.vis,
                     'fitspw': '',
                     'fitcorr': '',
                     'combine': '',
                     'minsamp': 8,
                     'field': '',
                     'spw': '',
                     'datacolumn': self.inputs.datacolumn}
        # VLASS-SE
        if self.inputs.statwtmode == 'VLASS-SE':
            task_args['combine'] = 'field,scan,state,corr'
            task_args['minsamp'] = ''
            task_args['chanbin'] = 1
            task_args['timebin'] = '1yr'
            # TODO: should it be set here or in the recipe via the task argument?
            # task_args['datacolumn'] = 'residual data'

        if fielddict == {}:
            job = casa_tasks.statwt(**task_args)
            return self._executor.execute(job)

        # cont.dat file present and need to execute by field and fitspw
        if fielddict != {}:
            for field in fielddict:
                task_args['fitspw'] = fielddict[field]
                task_args['field'] = field
                job = casa_tasks.statwt(**task_args)

                statwt_result = self._executor.execute(job)

            return statwt_result

    def _do_flagsummary(self, name, field=''):
        fielddict = cont_file_to_CASA(self.inputs.vis, self.inputs.context)
        job = casa_tasks.flagdata(name=name, vis=self.inputs.vis, field=field, mode='summary')
        return self._executor.execute(job)

    def _check_for_modelcolumn(self):
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casatools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames() or self.inputs.overwrite_modelcol:
                LOG.info('Writing model data to {}'.format(ms.basename))
                imaging_parameters = set_add_model_column_parameters(self.inputs.context)
                job = casa_tasks.tclean(**imaging_parameters)
                tclean_result = self._executor.execute(job)
            else:
                LOG.info('Using existing MODEL_DATA column found in {}'.format(ms.basename))
