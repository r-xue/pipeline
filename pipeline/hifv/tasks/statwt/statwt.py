import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import cont_file_to_CASA
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

# CALCULATE DATA WEIGHTS BASED ON ST. DEV. WITHIN EACH SPW
# use statwt


class StatwtInputs(vdp.StandardInputs):
    datacolumn = vdp.VisDependentProperty(default='corrected')

    def __init__(self, context, vis=None, datacolumn=None):
        super(StatwtInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.datacolumn = datacolumn


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

        fielddict = cont_file_to_CASA()
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

        task_args = {'vis': self.inputs.vis,
                     'fitspw': '',
                     'fitcorr': '',
                     'combine': '',
                     'minsamp': 8,
                     'field': '',
                     'spw': '',
                     'datacolumn': self.inputs.datacolumn}

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

    def _do_flagsummary(self, name, field = ''):
        fielddict = cont_file_to_CASA()
        job = casa_tasks.flagdata(name=name, vis = self.inputs.vis, field = field, mode='summary')
        return self._executor.execute(job)
