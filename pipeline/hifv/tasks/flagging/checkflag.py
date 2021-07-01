import collections
import copy
import os

import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp

from pipeline.hifv.heuristics import RflagDevHeuristic
from pipeline.hifv.heuristics import set_add_model_column_parameters
from pipeline.infrastructure import casa_tasks, casa_tools, task_registry

from .displaycheckflag import checkflagSummaryChart

LOG = infrastructure.get_logger(__name__)

# CHECKING FLAGGING OF ALL CALIBRATORS
# use rflag mode of flagdata


class CheckflagInputs(vdp.StandardInputs):
    checkflagmode = vdp.VisDependentProperty(default='')
    overwrite_modelcol = vdp.VisDependentProperty(default=False)
    growflags = vdp.VisDependentProperty(default=False)

    def __init__(self, context, vis=None, checkflagmode=None, overwrite_modelcol=None, growflags=None):
        super(CheckflagInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.checkflagmode = checkflagmode
        self.overwrite_modelcol = overwrite_modelcol
        self.growflags = growflags


class CheckflagResults(basetask.Results):
    def __init__(self, jobs=None, results=None, summaries=None):

        if jobs is None:
            jobs = []
        if results is None:
            results = []
        if summaries is None:
            summaries = []

        super(CheckflagResults, self).__init__()

        self.jobs = jobs
        self.results = results
        self.summaries = summaries
        self.plots = {}

    def __repr__(self):
        s = 'Checkflag (rflag mode) results:\n'
        for job in self.jobs:
            s += '%s performed. Statistics to follow?' % str(job)
        return s


@task_registry.set_equivalent_casa_task('hifv_checkflag')
class Checkflag(basetask.StandardTaskTemplate):
    Inputs = CheckflagInputs

    def prepare(self):

        LOG.info("Checkflag task: " + self.inputs.checkflagmode)
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        self.tint = m.get_vla_max_integration_time()
        self.corr_type_string = m.polarizations[0].corr_type_string

        timedevscale = 4.0
        freqdevscale = 4.0

        summaries = []  # QA statistics summaries for before and after targetflag

        if self.inputs.checkflagmode == 'vlass-imaging':
            LOG.info('Checking for model column')
            self._check_for_modelcolumn()

        # get the before-flagging total statistics
        # PIPE-757: skip before-flagging summary in all VLASS calibration checkflagmodes: bpd-vlass*/allcals-vlass*/target-vlass*
        # PIPE-502/995: run before-flagging summary in all other checkflagmodes, including vlass-imaging*.
        if '-vlass' not in self.inputs.checkflagmode:
            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='before')
            summarydict = self._executor.execute(job)
            summaries.append(summarydict)

        # Set up threshold multiplier values for calibrators and targets separately.
        # Xpol are used for cross-hands, Ppol are used for parallel hands. As
        # noted above, I'm still refining these values; I suppose they could be
        # input parameters for the task, if needed:

        self.rflagThreshMultiplierCalsXpol = 4.0
        self.rflagThreshMultiplierCalsPpol = 4.0
        self.rflagThreshMultiplierTargetXpol = 4.0
        self.rflagThreshMultiplierTargetPpol = 7.0
        if self.inputs.checkflagmode == 'vlass-imaging':
            self.rflagThreshMultiplierTargetPpol = 4.0
        self.tfcropThreshMultiplierCals = 3.0
        self.tfcropThreshMultiplierTarget = 3.0

        # tfcrop is run per integration, set ntime here (this should be derived
        # by the pipeline):
        # tint = 0.45

        if self.inputs.checkflagmode == 'bpd-vlass':
            extendflag_result = self.do_bpdvlass()
            return extendflag_result

        if self.inputs.checkflagmode == 'allcals-vlass':
            extendflag_result = self.do_allcalsvlass()
            return extendflag_result

        if self.inputs.checkflagmode == 'target-vlass' or self.inputs.checkflagmode == 'vlass-imaging':
            fieldsobj = m.get_fields(intent='TARGET')
            fieldids = [field.id for field in fieldsobj]
            fieldselect = ','.join([str(fieldid) for fieldid in fieldids])

            if not fieldselect:
                LOG.warning("No scans with intent=TARGET are present.  CASA task flagdata not executed.")
                return CheckflagResults(summaries=summaries)
            else:
                # PIPE-502/995: save before-flagging summary plots and plotting scale for 'vlass-imaging'
                if self.inputs.checkflagmode == 'vlass-imaging':
                    LOG.info('Estimating the amplitude range of unflagged data for summary plots')
                    amp_range = self._get_amp_range()
                    amp_d = amp_range[1]-amp_range[0]
                    summary_plotrange = [0, 0, max(0, amp_range[0]-0.1*amp_d), amp_range[1]+0.1*amp_d]
                    LOG.info('Creating before-flagging summary plots')
                    plotms_args_overrides = {'plotrange': summary_plotrange,
                                             'title': 'Amp vs. Frequency (before flagging)'}
                    summaryplot_before = self._create_summaryplots(suffix='before', plotms_args=plotms_args_overrides)
                    
                extendflag_result = self.do_targetvlass()
                
                # PIPE-502/995: attach before-flagging summary plots and plotting scale for 'vlass-imaging'
                if self.inputs.checkflagmode == 'vlass-imaging':
                    extendflag_result.plots['before'] = summaryplot_before
                    extendflag_result.plots['plotrange'] = summary_plotrange

                # PIPE-502/757/995: get after-flagging summary for 'target-vlass'/'vlass-imaging'
                job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary', name='after')
                summarydict = self._executor.execute(job)
                summaries.append(summarydict)
                extendflag_result.summaries = summaries
                return extendflag_result

        if self.inputs.checkflagmode in ('bpd-vla', 'allcals-vla', 'target-vla'):

            if self.inputs.checkflagmode == 'target-vla':
                fieldselect, scanselect, intentselect = self._select_data()
                if not fieldselect:
                    LOG.warning("No scans with intent=TARGET are present.  CASA task flagdata not executed.")
                    return CheckflagResults()

            extendflag_result = self.do_rfi_flag()

            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary')
            summarydict = self._executor.execute(job)
            summaries.append(summarydict)
            extendflag_result.summaries = summaries
            return extendflag_result


        #####################################################################################################
        #   consider to deprecate the workflow below this line, if:
        #       - the generic heuristics is verified.
        #       - old checkflagmodes are succeded by new checkflagmodes
        #####################################################################################################

        if self.inputs.checkflagmode == 'bpd':
            fieldselect = self.inputs.context.evla['msinfo'][m.name].checkflagfields
            scanselect = self.inputs.context.evla['msinfo'][m.name].testgainscans

        if self.inputs.checkflagmode == 'allcals':
            fieldselect = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string.split(',')
            scanselect = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string.split(',')

            checkflagfields = self.inputs.context.evla['msinfo'][m.name].checkflagfields.split(',')
            testgainscans = self.inputs.context.evla['msinfo'][m.name].testgainscans.split(',')

            fieldselect = ','.join([fieldid for fieldid in fieldselect if fieldid not in checkflagfields])
            scanselect = ','.join([scan for scan in scanselect if scan not in testgainscans])

        if self.inputs.checkflagmode == 'target':
            fieldsobj = m.get_fields(intent='TARGET')
            fieldids = [field.id for field in fieldsobj]
            fieldselect = ','.join([str(fieldid) for fieldid in fieldids])
            scanselect = ''
            timedevscale = 7.0
            freqdevscale = 7.0

            if not fieldselect:
                LOG.warning("No scans with intent=TARGET are present.  CASA task flagdata not executed.")
                return CheckflagResults()

        if self.inputs.checkflagmode in ('bpd', 'allcals', 'target'):
            flagbackup = True

            if ('RL' in self.corr_type_string) and ('LR' in self.corr_type_string):
                for correlation in ['ABS_RL', 'ABS_LR']:

                    method_args = {'mode': 'rflag',
                                   'field': fieldselect,
                                   'correlation': correlation,
                                   'scan': scanselect,
                                   'ntime': 'scan',
                                   'datacolumn': 'corrected',
                                   'flagbackup': flagbackup}

                    self._do_checkflag(**method_args)

                    flagbackup = False

                self._do_extendflag(field=fieldselect, scan=scanselect)

            datacolumn = 'residual'
            corrlist = ['REAL_RR', 'REAL_LL']

            if self.inputs.checkflagmode in ('allcals', 'target'):
                datacolumn = 'corrected'
                corrlist = ['ABS_RR', 'ABS_LL']

            for correlation in corrlist:
                method_args = {'mode': 'rflag',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'ntime': 'scan',
                               'datacolumn': datacolumn,
                               'flagbackup': False,
                               'timedevscale': timedevscale,
                               'freqdevscale': freqdevscale}

                self._do_checkflag(**method_args)

            self._do_extendflag(field=fieldselect, scan=scanselect)

            if ('RL' in self.corr_type_string) and ('LR' in self.corr_type_string):
                for correlation in ['ABS_LR', 'ABS_RL']:
                    method_args = {'mode': 'tfcrop',
                                   'field': fieldselect,
                                   'correlation': correlation,
                                   'scan': scanselect,
                                   'ntime': self.tint,
                                   'datacolumn': 'corrected',
                                   'flagbackup': False}

                    self._do_tfcropflag(**method_args)

                self._do_extendflag(field=fieldselect, scan=scanselect)

            for correlation in ['ABS_LL', 'ABS_RR']:
                method_args = {'mode': 'tfcrop',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'ntime': self.tint,
                               'datacolumn': 'corrected',
                               'flagbackup': False}

                self._do_tfcropflag(**method_args)

            extendflag_result = self._do_extendflag(field=fieldselect, scan=scanselect)

            # Do a second time on targets
            if self.inputs.checkflagmode == 'target':
                for correlation in ['ABS_LL', 'ABS_RR']:
                    method_args = {'mode': 'tfcrop',
                                   'field': fieldselect,
                                   'correlation': correlation,
                                   'scan': scanselect,
                                   'ntime': self.tint,
                                   'datacolumn': 'corrected',
                                   'flagbackup': False}

                    self._do_tfcropflag(**method_args)

                extendflag_result = self._do_extendflag(field=fieldselect, scan=scanselect)

            # PIPE-939: optional growflags for 'bpd' and 'allcals'
            if self.inputs.checkflagmode in ('bpd', 'allcals') and self.inputs.growflags:
                extendflag_result = self._do_extendflag(field=fieldselect, scan=scanselect,
                                                        growtime=100.0, growfreq=100.0,
                                                        growaround=True, flagneartime=True, flagnearfreq=True)

            job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary')
            summarydict = self._executor.execute(job)
            summaries.append(summarydict)
            extendflag_result.summaries = summaries
            return extendflag_result

        # Values from pipeline context
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        checkflagfields = self.inputs.context.evla['msinfo'][m.name].checkflagfields
        corrstring = m.get_vla_corrstring()
        testgainscans = self.inputs.context.evla['msinfo'][m.name].testgainscans

        method_args = {'mode': 'rflag',
                       'field': checkflagfields,
                       'correlation': 'ABS_' + corrstring,
                       'scan': testgainscans,
                       'ntime': 'scan',
                       'datacolumn': 'corrected',
                       'flagbackup': False}

        if self.inputs.checkflagmode == 'semi':
            calibrator_field_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string
            calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

            method_args = {'mode': 'rflag',
                           'field': calibrator_field_select_string,
                           'correlation': 'ABS_' + corrstring,
                           'scan': calibrator_scan_select_string,
                           'ntime': 'scan',
                           'datacolumn': 'corrected',
                           'flagbackup': False}

        self._do_checkflag(**method_args)

        # PIPE-939: optional growflags for checkflag'semi' and ''
        if self.inputs.checkflagmode in ('semi', '') and self.inputs.growflags:
            extendflag_result = self._do_extendflag(field=method_args['field'],
                                                    scan=method_args['scan'],
                                                    growtime=100.0, growfreq=100.0,
                                                    growaround=True, flagneartime=True, flagnearfreq=True)

        # get the after flag total statistics
        job = casa_tasks.flagdata(vis=self.inputs.vis, mode='summary')
        summarydict = self._executor.execute(job)
        summaries.append(summarydict)

        return CheckflagResults([job], summaries=summaries)

    def _do_checkflag(self, mode='rflag', field=None, correlation=None, scan=None, intent='',
                      ntime='scan', datacolumn='corrected', flagbackup=False, timedevscale=4.0,
                      freqdevscale=4.0, action='apply', timedev='', freqdev='', savepars=True):
        # This method can be replaced by self._do_rflag(**kwargs, calcftdev=False, extendflags=False).

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'correlation': correlation,
                     'scan': scan,
                     'intent': intent,
                     'ntime': ntime,
                     'combinescans': False,
                     'datacolumn': datacolumn,
                     'winsize': 3,
                     'timedevscale': timedevscale,
                     'freqdevscale': freqdevscale,
                     'timedev': timedev,
                     'freqdev': freqdev,
                     'action': action,
                     'display': '',
                     'extendflags': False,
                     'flagbackup': flagbackup,
                     'savepars': savepars}

        job = casa_tasks.flagdata(**task_args)
        jobresult = self._executor.execute(job)

        return jobresult

    def _do_rflag(self, mode='rflag', field=None, correlation=None, scan=None, intent='',
                  ntime='scan', datacolumn='corrected', flagbackup=False, timedevscale=4.0,
                  freqdevscale=4.0, action='apply', timedev='', freqdev='', savepars=True,
                  calcftdev=True, useheuristic=True, extendflags=False):
        """
        calcftdev: decide to run a single-pass rflag or two-pass; for two-pass, action will always be 1) "calculate" 2) "apply"
        useheuristics: use the internal ftdev threshold heuristic, or not; only matters if calcftdev=True
        extendflags: set the "extendflags" plan:
            True/False: extend flags along with the rflag call
            Dictionary: extend flags after the rflag call
        """

        if isinstance(extendflags, bool):
            extendflags_rflag = extendflags
            extend_followup = None
        else:
            extendflags_rflag = False
            extend_followup = extendflags

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'correlation': correlation,
                     'scan': scan,
                     'intent': intent,
                     'ntime': ntime,
                     'combinescans': False,
                     'datacolumn': datacolumn,
                     'winsize': 3,
                     'timedevscale': timedevscale,
                     'freqdevscale': freqdevscale,
                     'timedev': timedev,
                     'freqdev': freqdev,
                     'action': action,
                     'display': '',
                     'extendflags': extendflags_rflag,
                     'flagbackup': flagbackup,
                     'savepars': savepars}

        if calcftdev:
            task_args['action'] = 'calculate'

            job = casa_tasks.flagdata(**task_args)
            jobresult = self._executor.execute(job)

            if jobresult is None:
                LOG.warn("This is likely a dryrun test because 'None' is returned from the visibility noise calculation!")
                LOG.warn("Proceed with timedev/freqdev=''.")
                ftdev = None
            else:
                if useheuristic:
                    rflagdev = RflagDevHeuristic()
                    ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
                    ftdev = rflagdev(ms, jobresult['report0'])
                else:
                    ftdev = jobresult['report0']

            if ftdev is not None:
                task_args['timedev'] = ftdev['timedev']
                task_args['freqdev'] = ftdev['freqdev']
            task_args['action'] = 'apply'

        job = casa_tasks.flagdata(**task_args)
        jobresult = self._executor.execute(job)

        if extend_followup is not None:
            self._do_extendflag(field=field, scan=scan, intent=intent, **extend_followup)

        return jobresult

    def analyse(self, results):
        return results

    def _do_extendflag(self, mode='extend', field=None,  scan=None, intent='',
                       ntime='scan', extendpols=True, flagbackup=False,
                       growtime=100.0, growfreq=60.0, growaround=False,
                       flagneartime=False, flagnearfreq=False):

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'scan': scan,
                     'intent': intent,
                     'ntime': ntime,
                     'combinescans': False,
                     'extendpols': extendpols,
                     'growtime': growtime,
                     'growfreq': growfreq,
                     'growaround': growaround,
                     'flagneartime': flagneartime,
                     'flagnearfreq': flagnearfreq,
                     'action': 'apply',
                     'display': '',
                     'extendflags': False,
                     'flagbackup': flagbackup,
                     'savepars': False}

        job = casa_tasks.flagdata(**task_args)
        result = self._executor.execute(job)

        return CheckflagResults(jobs=[job])

    def _do_tfcropflag(self, mode='tfcrop', field=None, correlation=None, scan=None, intent='',
                       ntime=0.45, datacolumn='corrected', flagbackup=True,
                       freqcutoff=3.0, timecutoff=4.0, savepars=True, extendflags=False):

        if isinstance(extendflags, bool):
            extendflags_rflag = extendflags
            extend_followup = None
        else:
            extendflags_rflag = False
            extend_followup = extendflags

        task_args = {'vis': self.inputs.vis,
                     'mode': mode,
                     'field': field,
                     'correlation': correlation,
                     'scan': scan,
                     'intent': intent,
                     'ntime': ntime,
                     'combinescans': False,
                     'datacolumn': datacolumn,
                     'freqcutoff': freqcutoff,
                     'timecutoff': timecutoff,
                     'freqfit': 'line',
                     'flagdimension': 'freq',
                     'action': 'apply',
                     'display': '',
                     'extendflags': extendflags_rflag,
                     'flagbackup': flagbackup,
                     'savepars': savepars}

        job = casa_tasks.flagdata(**task_args)
        self._executor.execute(job)

        if extend_followup is not None:
            self._do_extendflag(field=field, scan=scan, intent=intent, **extend_followup)

        return

    def _select_data(self):

        fieldselect = ''
        scanselect = ''
        intentselect = ''

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        if self.inputs.checkflagmode in ('bpd-vla'):
            fieldselect = self.inputs.context.evla['msinfo'][m.name].checkflagfields
            scanselect = self.inputs.context.evla['msinfo'][m.name].testgainscans
            intentselect = ''

        if self.inputs.checkflagmode in ('allcals-vla'):
            fieldselect = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string.split(',')
            scanselect = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string.split(',')
            checkflagfields = self.inputs.context.evla['msinfo'][m.name].checkflagfields.split(',')
            testgainscans = self.inputs.context.evla['msinfo'][m.name].testgainscans.split(',')
            fieldselect = ','.join([fieldid for fieldid in fieldselect if fieldid not in checkflagfields])
            scanselect = ','.join([scan for scan in scanselect if scan not in testgainscans])
            intentselect = ''

        if self.inputs.checkflagmode in ('target-vla'):
            fieldids = [field.id for field in m.get_fields(intent='TARGET')]
            fieldselect = ','.join([str(fieldid) for fieldid in fieldids])
            scanselect = ''
            intentselect = '*TARGET*'

        return fieldselect, scanselect, intentselect

    def _select_rfi_standard(self):
        """Set rflag data selection and threshold-multiplier in individual rflag iterations.

        Note from BK:
        Set up threshold multiplier values for calibrators and targets separately.
        Xpol are used for cross-hands, Ppol are used for parallel hands. As
        noted above, I'm still refining these values; I suppose they could be
        input parameters for the task, if needed.  

        rflag_standard: list of tuple, with each tuple (a, b, c, d) describing the specifications of one self._do_rflag call:

                            a) data column selection
                            b) correlation selection
                            c) ftdev threshold multiplier
                            d) extendflag setting
                                Boolean (True/False): 
                                    use the default basic extendflagging scheme; see the 'extendflags' subprameter of flagdata(mode='rflag'.
                                A dictionary (e.g. {'growtime':100,'growfreq':100,'')
                                    do extendflag using seperate flagdata(mode='extend',..) call following flagdata(model='rflag',extendflags=False,..)

        tfcrop_standard: list of tuple, with each tuple (a, b, c, d) describing the specifications of one self._do_rflag call:

                            a) data column selection
                            b) correlation selection
                            c) tfcrop threshold multiplier
                            d) extendflag setting
                                Boolean (True/False): 
                                    use the default basic extendflagging scheme; see the 'extendflags' subprameter of flagdata(mode='rflag'.
                                A dictionary (e.g. {'growtime':100,'growfreq':100,'')
                                    do extendflag using seperate flagdata(mode='extend',..) call following flagdata(model='rflag',extendflags=False,..

        growflag_standrd: dictionary to specify the final optional "growflags".

        """

        rflag_standard = tfcrop_standard = growflag_standard = None

        if self.inputs.checkflagmode in ('bpd-vla'):
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('residual', 'REAL_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]
            growflag_standard = {'growtime': 100,
                                 'growfreq': 100,
                                 'growaround': True,
                                 'flagneartime': True,
                                 'flagnearfreq': True}

        if self.inputs.checkflagmode in ('allcals-vla'):
            # PIPE-987: follow the VLASS flagging scheme described in CAS-11598.
            rflag_standard = [('corrected', 'ABS_RL', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_RR', 4.0, {'growtime': 100., 'growfreq': 100.}),
                              ('corrected', 'ABS_LL', 4.0, {'growtime': 100., 'growfreq': 100.})]
            tfcrop_standard = [('corrected', 'ABS_RL', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_RR', 3.0, {'growtime': 100., 'growfreq': 100.}),
                               ('corrected', 'ABS_LL', 3.0, {'growtime': 100., 'growfreq': 100.})]
            growflag_standard = {'growtime': 100,
                                 'growfreq': 100,
                                 'growaround': True,
                                 'flagneartime': True,
                                 'flagnearfreq': True}

        if self.inputs.checkflagmode in ('target-vla'):
            # PIPE-685/CARS-540: apply three incremental 'rflag' iterations; no tfcrop follows; growflags at the end
            rflag_standard = [('corrected', '', 4.5, True),
                              ('corrected', '', 4.5, True),
                              ('corrected', '', 4.5, True)]
            tfcrop_standard = None
            growflag_standard = {'growtime': 50,
                                 'growfreq': 50,
                                 'growaround': True,
                                 'flagneartime': True,
                                 'flagnearfreq': True}

        return rflag_standard, tfcrop_standard, growflag_standard

    def do_rfi_flag(self):
        """Performance RFI flagging using multiple passes of rflag/tfcrop/extend."""

        fieldselect, scanselect, intentselect = self._select_data()
        rflag_standard, tfcrop_standard, growflag_standard = self._select_rfi_standard()

        if rflag_standard is not None:

            for datacolumn, correlation, scale, extendflags in rflag_standard:
                if correlation.split('_')[1] not in self.corr_type_string:
                    continue
                method_args = {'mode': 'rflag',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'intent': intentselect,
                               'ntime': 'scan',
                               'timedevscale': scale,
                               'freqdevscale': scale,
                               'datacolumn': datacolumn,
                               'flagbackup': False,
                               'savepars': False,
                               'extendflags': extendflags}

                self._do_rflag(**method_args)

        if tfcrop_standard is not None:

            for datacolumn, correlation, tfcropThreshMultiplier, extendflags in tfcrop_standard:
                if correlation.split('_')[1] not in self.corr_type_string:
                    continue
                method_args = {'mode': 'tfcrop',
                               'field': fieldselect,
                               'correlation': correlation,
                               'scan': scanselect,
                               'intent': intentselect,
                               'timecutoff': tfcropThreshMultiplier,
                               'freqcutoff': tfcropThreshMultiplier,
                               'ntime': self.tint,
                               'datacolumn': datacolumn,
                               'flagbackup': False,
                               'savepars': False,
                               'extendflags': extendflags}

                self._do_tfcropflag(**method_args)

        extendflag_result = CheckflagResults()

        if growflag_standard is not None:
            extendflag_result = self._do_extendflag(
                field=fieldselect, scan=scanselect, intent=intentselect, **growflag_standard)

        return extendflag_result

    def thresholds(self, inputThresholds):
        # the following command maps spws 2~9 to one baseband, and 10~17 to
        # the other; the pipeline should replace this with internally-derived
        # values

        if inputThresholds is None:
            LOG.warn("This is likely a dryrun test because 'None' is returned from the visibility noise calculation!")
            LOG.warn("Proceed with timedev/freqdev=''.")
            return {'report0': {'freqdev': '', 'timedev': ''}}

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        vlabasebands = m.get_vla_baseband_spws(science_windows_only=True)

        bbspws = [list(map(int, i.split(','))) for i in vlabasebands]

        # bbspws = [[2, 3, 4, 5, 6, 7, 8, 9], [10, 11, 12, 13, 14, 15, 16, 17]]

        outputThresholds = copy.deepcopy(inputThresholds)

        for ftdev in ['freqdev', 'timedev']:

            fields = inputThresholds['report0'][ftdev][:, 0]
            spws = inputThresholds['report0'][ftdev][:, 1]
            threshes = inputThresholds['report0'][ftdev][:, 2]

            ufields = np.unique(fields)
            for ifield in ufields:
                fldmask = np.where(fields == ifield)
                if len(fldmask[0]) == 0:
                    continue  # no data matching field
                # filter spws and threshes whose fields==ifield
                field_spws = spws[fldmask]
                field_threshes = threshes[fldmask]

                for ibbspws in bbspws:
                    spwmask = np.where(np.array([ispw in ibbspws for ispw in field_spws]) == True)
                    if len(spwmask[0]) == 0:
                        continue  # no data matching ibbspws
                    # filter threshes whose fields==ifield and spws in ibbspws
                    spw_field_threshes = field_threshes[spwmask]
                    medthresh = np.median(spw_field_threshes)
                    medmask = np.where(spw_field_threshes > medthresh)
                    outmask = fldmask[0][spwmask[0][medmask]]
                    outputThresholds['report0'][ftdev][:, 2][outmask] = medthresh
        return outputThresholds

    def do_bpdvlass(self):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        fieldselect = self.inputs.context.evla['msinfo'][m.name].checkflagfields
        scanselect = self.inputs.context.evla['msinfo'][m.name].testgainscans

        for correlation, scale, datacolumn in [('ABS_RL', self.rflagThreshMultiplierCalsXpol, 'corrected'),
                                               ('ABS_LR', self.rflagThreshMultiplierCalsXpol, 'corrected'),
                                               ('REAL_RR', self.rflagThreshMultiplierCalsPpol, 'residual'),
                                               ('REAL_LL', self.rflagThreshMultiplierCalsPpol, 'residual')]:

            method_args = {'mode': 'rflag',
                           'field': fieldselect,
                           'correlation': correlation,
                           'scan': scanselect,
                           'ntime': 'scan',
                           'timedevscale': scale,
                           'freqdevscale': scale,
                           'datacolumn': datacolumn,
                           'flagbackup': False,
                           'action': 'calculate',
                           'savepars': False}

            rflagthresholds = self._do_checkflag(**method_args)

            rflagthresholdsnew = self.thresholds(rflagthresholds)

            method_args['timedev'] = rflagthresholdsnew['report0']['timedev']
            method_args['freqdev'] = rflagthresholdsnew['report0']['freqdev']
            method_args['action'] = 'apply'

            self._do_checkflag(**method_args)

            self._do_extendflag(field=fieldselect, scan=scanselect, growtime=100.0, growfreq=100.0)

        for correlation in ['ABS_LR', 'ABS_RL', 'ABS_LL', 'ABS_RR']:
            method_args = {'mode': 'tfcrop',
                           'field': fieldselect,
                           'correlation': correlation,
                           'scan': scanselect,
                           'timecutoff': self.tfcropThreshMultiplierCals,
                           'freqcutoff': self.tfcropThreshMultiplierCals,
                           'ntime': self.tint,
                           'datacolumn': 'corrected',
                           'flagbackup': False,
                           'savepars': False}

            self._do_tfcropflag(**method_args)

            self._do_extendflag(field=fieldselect, scan=scanselect, growtime=100.0, growfreq=100.0)

        # Grow flags
        extendflag_result = self._do_extendflag(field=fieldselect, scan=scanselect,
                                                growtime=100.0, growfreq=100.0,
                                                growaround=True, flagneartime=True, flagnearfreq=True)

        return extendflag_result

    def do_allcalsvlass(self):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        fieldselect = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string.split(',')
        scanselect = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string.split(',')

        checkflagfields = self.inputs.context.evla['msinfo'][m.name].checkflagfields.split(',')
        testgainscans = self.inputs.context.evla['msinfo'][m.name].testgainscans.split(',')

        fieldselect = ','.join([fieldid for fieldid in fieldselect if fieldid not in checkflagfields])
        scanselect = ','.join([scan for scan in scanselect if scan not in testgainscans])

        for correlation, scale, datacolumn in [('ABS_RL', self.rflagThreshMultiplierCalsXpol, 'corrected'),
                                               ('ABS_LR', self.rflagThreshMultiplierCalsXpol, 'corrected'),
                                               ('ABS_RR', self.rflagThreshMultiplierCalsXpol, 'corrected'),
                                               ('ABS_LL', self.rflagThreshMultiplierCalsXpol, 'corrected')]:

            method_args = {'mode': 'rflag',
                           'field': fieldselect,
                           'correlation': correlation,
                           'scan': scanselect,
                           'ntime': 'scan',
                           'timedevscale': scale,
                           'freqdevscale': scale,
                           'datacolumn': datacolumn,
                           'flagbackup': False,
                           'action': 'calculate',
                           'savepars': False}

            rflagthresholds = self._do_checkflag(**method_args)

            rflagthresholdsnew = self.thresholds(rflagthresholds)

            method_args['timedev'] = rflagthresholdsnew['report0']['timedev']
            method_args['freqdev'] = rflagthresholdsnew['report0']['freqdev']
            method_args['action'] = 'apply'

            self._do_checkflag(**method_args)

            self._do_extendflag(field=fieldselect, scan=scanselect, growtime=100.0, growfreq=100.0)

        for correlation in ['ABS_LR', 'ABS_RL', 'ABS_LL', 'ABS_RR']:
            method_args = {'mode': 'tfcrop',
                           'field': fieldselect,
                           'correlation': correlation,
                           'scan': scanselect,
                           'timecutoff': self.tfcropThreshMultiplierCals,
                           'freqcutoff': self.tfcropThreshMultiplierCals,
                           'ntime': self.tint,
                           'datacolumn': 'corrected',
                           'flagbackup': False,
                           'savepars': False}

            self._do_tfcropflag(**method_args)

            self._do_extendflag(field=fieldselect, scan=scanselect, growtime=100.0, growfreq=100.0)

        # Grow flags
        extendflag_result = self._do_extendflag(field=fieldselect, scan=scanselect,
                                                growtime=100.0, growfreq=100.0,
                                                growaround=True, flagneartime=True, flagnearfreq=True)

        return extendflag_result

    def do_targetvlass(self):

        datacolumn = 'corrected'
        if self.inputs.checkflagmode == 'vlass-imaging':
            datacolumn = 'data'

        for correlation, scale in [('ABS_RL', self.rflagThreshMultiplierTargetXpol),
                                   ('ABS_LR', self.rflagThreshMultiplierTargetXpol),
                                   ('ABS_RR', self.rflagThreshMultiplierTargetPpol),
                                   ('ABS_LL', self.rflagThreshMultiplierTargetPpol)]:

            if self.inputs.checkflagmode == 'vlass-imaging' and correlation == 'ABS_RR':
                datacolumn = 'residual_data'
            if self.inputs.checkflagmode == 'vlass-imaging' and correlation == 'ABS_LL':
                datacolumn = 'residual_data'

            method_args = {'mode': 'rflag',
                           'field': '',
                           'correlation': correlation,
                           'scan': '',
                           'intent': '*TARGET*',
                           'ntime': 'scan',
                           'timedevscale': scale,
                           'freqdevscale': scale,
                           'datacolumn': datacolumn,
                           'flagbackup': False,
                           'action': 'calculate',
                           'savepars': False}

            rflagthresholds = self._do_checkflag(**method_args)

            rflagthresholdsnew = self.thresholds(rflagthresholds)

            method_args['timedev'] = rflagthresholdsnew['report0']['timedev']
            method_args['freqdev'] = rflagthresholdsnew['report0']['freqdev']
            method_args['action'] = 'apply'

            self._do_checkflag(**method_args)

            self._do_extendflag(field='', scan='', intent='*TARGET*', growtime=100.0, growfreq=100.0)

        datacolumn = 'corrected'
        if self.inputs.checkflagmode == 'vlass-imaging':
            datacolumn = 'data'

        for correlation in ['ABS_LR', 'ABS_RL', 'ABS_LL', 'ABS_RR']:
            method_args = {'mode': 'tfcrop',
                           'field': '',
                           'correlation': correlation,
                           'scan': '',
                           'intent': '*TARGET*',
                           'timecutoff': self.tfcropThreshMultiplierTarget,
                           'freqcutoff': self.tfcropThreshMultiplierTarget,
                           'ntime': self.tint,
                           'datacolumn': datacolumn,
                           'flagbackup': False,
                           'savepars': False}

            self._do_tfcropflag(**method_args)

            self._do_extendflag(field='', scan='', intent='*TARGET*', growtime=100.0, growfreq=100.0)

        return CheckflagResults()

    def _check_for_modelcolumn(self):
        ms = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        with casa_tools.TableReader(ms.name) as table:
            if 'MODEL_DATA' not in table.colnames() or self.inputs.overwrite_modelcol:
                LOG.info('Writing model data to {}'.format(ms.basename))
                imaging_parameters = set_add_model_column_parameters(self.inputs.context)
                job = casa_tasks.tclean(**imaging_parameters)
                tclean_result = self._executor.execute(job)
            else:
                LOG.info('Using existing MODEL_DATA column found in {}'.format(ms.basename))

    def _create_summaryplots(self, suffix='before', plotms_args={}):
        summary_plots = {}
        results_tmp = basetask.ResultsList()
        results_tmp.inputs = self.inputs.as_dict()
        results_tmp.stage_number = self.inputs.context.task_counter
        ms = os.path.basename(results_tmp.inputs['vis'])
        summary_plots[ms] = checkflagSummaryChart(
            self.inputs.context, results_tmp, suffix=suffix, plotms_args=plotms_args).plot()

        return summary_plots

    def _get_amp_range(self):
        # get amplitude min/max for the amp. vs. freq summary plots
        try:
            with casa_tools.MSReader(self.inputs.vis) as msfile:
                amp_range = msfile.range(['amplitude'])['amplitude'].tolist()
            return amp_range

        except:
            LOG.warn("Unable to obtain the range of data amps.")
            return [0., 0.]
