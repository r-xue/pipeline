from __future__ import absolute_import

import os
import numpy as np

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import getCalFlaggedSoln
from pipeline.hifv.heuristics import weakbp, do_bandpass, uvrange
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class semiFinalBPdcalsInputs(vdp.StandardInputs):
    weakbp = vdp.VisDependentProperty(default=False)
    refantignore = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, weakbp=None, refantignore=None):
        super(semiFinalBPdcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore


class semiFinalBPdcalsResults(basetask.Results):
    def __init__(self, final=None, pool=None, preceding=None, bpdgain_touse=None,
                 gtypecaltable=None, ktypecaltable=None, bpcaltable=None, flaggedSolnApplycalbandpass=None,
                 flaggedSolnApplycaldelay=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(semiFinalBPdcalsResults, self).__init__()

        # self.vis = None
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay


@task_registry.set_equivalent_casa_task('hifv_semiFinalBPdcals')
class semiFinalBPdcals(basetask.StandardTaskTemplate):
    Inputs = semiFinalBPdcalsInputs
    
    def prepare(self):

        self.parang = True
        try:
            self.setjy_results = self.inputs.context.results[0].read()[0].setjy_results
        except Exception as e:
            self.setjy_results = self.inputs.context.results[0].read().setjy_results

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_semiFinalBPdcals.s'
        
        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'semiFinaldelayinitialgain.tbl'
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'delay.tbl'
        bpcaltable    = tableprefix + str(stage_number) + '_4.' + 'BPcal.tbl'
        tablebase     = tableprefix + str(stage_number) + '_3.' + 'BPinitialgain'

        table_suffix = ['.tbl', '3.tbl', '10.tbl']
        soltimes = [1.0, 3.0, 10.0]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        soltimes = [m.get_vla_max_integration_time() * x for x in soltimes]
        solints = ['int', '3.0s', '10.0s']
        soltime = soltimes[0]
        solint = solints[0]
        
        context = self.inputs.context
        refantfield = context.evla['msinfo'][m.name].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                geometry=True, flagging=True, intent='',
                                                spw='', refantignore=self.inputs.refantignore)
        
        RefAntOutput = refantobj.calculate()
        
        self._do_gtype_delaycal(caltable=gtypecaltable, context=context, RefAntOutput=RefAntOutput)

        fracFlaggedSolns = 1.0

        critfrac = m.get_vla_critfrac()

        # Iterate and check the fraciton of Flagged solutions, each time running gaincal in 'K' mode
        flagcount = 0
        while fracFlaggedSolns > critfrac and flagcount < 4:
                
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable,
                                    context=context, RefAntOutput=RefAntOutput)
            flaggedSolnResult = getCalFlaggedSoln(ktypecaltable)
            fracFlaggedSolns = self._check_flagSolns(flaggedSolnResult, RefAntOutput)
            
            LOG.info("Fraction of flagged solutions = " + str(flaggedSolnResult['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult['antmedian']['fraction']))
            flagcount += 1

        LOG.info("Delay calibration complete")

        # Do initial gaincal on BP calibrator then semi-final BP calibration
        gain_solint1 = context.evla['msinfo'][m.name].gain_solint1
        self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                solint=gain_solint1, context=context, RefAntOutput=RefAntOutput)
        
        bpdgain_touse = tablebase + table_suffix[0]
        
        LOG.debug("WEAKBP: "+str(self.inputs.weakbp))

        if self.inputs.weakbp:
            LOG.debug("USING WEAKBP HEURISTICS")
            interp = weakbp(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                            ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False)
        else:
            LOG.debug("Using REGULAR heuristics")
            interp = ''
            do_bandpass(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                        spw='', ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                        solint='inf', append=False)

            AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
            AllCalTables.append(ktypecaltable)
            # AllCalTables.append(bpdgain_touse)
            AllCalTables.append(bpcaltable)
            ntables = len(AllCalTables)
            interp = [''] * ntables
            LOG.info("Using 'linear,linearflag' for bandpass table")
            interp[-1] = 'linear,linearflag'

        self._do_applycal(context=context, ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                                            bpcaltable=bpcaltable, interp=interp)

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return semiFinalBPdcalsResults(bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                       ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                       flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                       flaggedSolnApplycaldelay=flaggedSolnApplycaldelay)

    def analyse(self, results):
        return results
    
    def _do_gtype_delaycal(self, caltable=None, context=None, RefAntOutput=None):
        
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = context.evla['msinfo'][m.name].delay_field_select_string
        tst_delay_spw = m.get_vla_tst_delay_spw()
        delay_scan_select_string = context.evla['msinfo'][m.name].delay_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()
        
        # need to add scan?
        # ref antenna string needs to be lower case for gaincal
        delaycal_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_delay_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': delay_scan_select_string,
                              'solint': 'int',
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': False,
                              'docallib': False,
                              'gaintable': sorted(self.inputs.context.callibrary.active.get_caltable()),
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        fields = delay_field_select_string.split(',')
        for fieldidstring in fields:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            delaycal_task_args['field'] = fieldidstring
            delaycal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                delaycal_task_args['append'] = True

            job = casa_tasks.gaincal(**delaycal_task_args)

            self._executor.execute(job)

        return True
    
    def _do_ktype_delaycal(self, caltable=None, addcaltable=None, context=None, RefAntOutput=None):
        
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = context.evla['msinfo'][m.name].delay_field_select_string
        delay_scan_select_string = context.evla['msinfo'][m.name].delay_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        # need to add scan?
        # ref antenna string needs to be lower case for gaincal

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        delaycal_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': '',
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': delay_scan_select_string,
                              'solint': 'inf',
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'K',
                              'smodel': [],
                              'calmode': 'p',
                              'append': False,
                              'docallib': False,
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        for fieldidstring in delay_field_select_string.split(','):
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            delaycal_task_args['field'] = fieldidstring
            delaycal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                delaycal_task_args['append'] = True

            job = casa_tasks.gaincal(**delaycal_task_args)

            self._executor.execute(job)

        return True
    
    def _check_flagSolns(self, flaggedSolnResult, RefAntOutput):
        
        if flaggedSolnResult['all']['total'] > 0:
            fracFlaggedSolns = flaggedSolnResult['antmedian']['fraction']
        else:
            fracFlaggedSolns = 1.0

        # refant_csvstring = self.inputs.context.observing_run.measurement_sets[0].reference_antenna
        # refantlist = [x for x in refant_csvstring.split(',')]

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        # critfrac = self.inputs.context.evla['msinfo'][m.name].critfrac
        critfrac = m.get_vla_critfrac()

        if fracFlaggedSolns > critfrac:
            # RefAntOutput.pop(0)
            RefAntOutput = np.delete(RefAntOutput, 0)
            self.inputs.context.observing_run.measurement_sets[0].reference_antenna = ','.join(RefAntOutput)
            LOG.info("Not enough good solutions, trying a different reference antenna.")
            LOG.info("The pipeline start with antenna "+RefAntOutput[0]+" as the reference.")

        return fracFlaggedSolns
    
    def _do_gtype_bpdgains(self, caltable, addcaltable=None, solint='int', context=None, RefAntOutput=None):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw()
        bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        # need to add scan?
        # ref antenna string needs to be lower case for gaincal

        GainTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        GainTables.append(addcaltable)

        bpdgains_task_args = {'vis': self.inputs.vis,
                              'caltable': caltable,
                              'field': '',
                              'spw': tst_bpass_spw,
                              'intent': '',
                              'selectdata': True,
                              'uvrange': '',
                              'scan': '',
                              'solint': solint,
                              'combine': 'scan',
                              'preavg': -1.0,
                              'refant': ','.join(RefAntOutput),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': False,
                              'docallib': False,
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        bpscanslist = map(int, bandpass_scan_select_string.split(','))
        scanobjlist = m.get_scans(scan_id=bpscanslist)
        fieldidlist = []
        for scanobj in scanobjlist:
            fieldobj, = scanobj.fields
            if str(fieldobj.id) not in fieldidlist:
                fieldidlist.append(str(fieldobj.id))

        for fieldidstring in fieldidlist:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            bpdgains_task_args['field'] = fieldidstring
            bpdgains_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                bpdgains_task_args['append'] = True

            job = casa_tasks.gaincal(**bpdgains_task_args)

            self._executor.execute(job)

        return True
      
    def _do_applycal(self, context=None, ktypecaltable=None, bpdgain_touse=None, bpcaltable=None, interp=None):
        """Run CASA task applycal"""
        
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string
        
        LOG.info("Applying semi-final delay and BP calibrations to all calibrators")

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        # AllCalTables.append(bpdgain_touse)
        AllCalTables.append(bpcaltable)

        ntables=len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': '',
                              'intent': '',
                              'selectdata': True,
                              'scan': calibrator_scan_select_string,
                              'docallib': False,
                              'gaintable': AllCalTables,
                              'gainfield': [''],
                              'interp': interp,
                              'spwmap': [],
                              'calwt': [False]*ntables,
                              'parang': self.parang,
                              'applymode': 'calflagstrict',
                              'flagbackup': True}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)
