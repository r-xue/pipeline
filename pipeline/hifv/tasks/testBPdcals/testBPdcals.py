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
import pipeline.infrastructure.casatools as casatools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class testBPdcalsInputs(vdp.StandardInputs):
    weakbp = vdp.VisDependentProperty(default=False)
    refantignore = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, weakbp=None, refantignore=None):
        super(testBPdcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore
        self.gain_solint1 = 'int'
        self.gain_solint2 = 'int'


class testBPdcalsResults(basetask.Results):
    def __init__(self, final=None, pool=None, preceding=None, gain_solint1=None,
                 shortsol1=None, vis=None, bpdgain_touse=None, gtypecaltable=None,
                 ktypecaltable=None, bpcaltable=None, flaggedSolnApplycalbandpass=None,
                 flaggedSolnApplycaldelay=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(testBPdcalsResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.gain_solint1 = gain_solint1
        self.shortsol1 = shortsol1
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay


    def merge_with_context(self, context):
        m = context.observing_run.get_ms(self.vis)
        context.evla['msinfo'][m.name].gain_solint1 = self.gain_solint1
        context.evla['msinfo'][m.name].shortsol1 = self.shortsol1
    
        
@task_registry.set_equivalent_casa_task('hifv_testBPdcals')
class testBPdcals(basetask.StandardTaskTemplate):
    Inputs = testBPdcalsInputs

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

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_testBPdcals.s'

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'testdelayinitialgain.tbl'
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'testdelay.tbl'
        bpcaltable    = tableprefix + str(stage_number) + '_4.' + 'testBPcal.tbl'
        tablebase     = tableprefix + str(stage_number) + '_3.' + 'testBPdinitialgain'
        table_suffix = ['.tbl', '3.tbl', '10.tbl']
        soltimes = [1.0, 3.0, 10.0]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        soltimes = [m.get_vla_max_integration_time() * x for x in soltimes]
        solints = ['int', str(soltimes[1]) + 's', str(soltimes[2]) + 's']
        soltime = soltimes[0]
        solint = solints[0]

        context = self.inputs.context
        refantfield = context.evla['msinfo'][m.name].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                geometry=True, flagging=True, intent='',
                                                spw='', refantignore=self.inputs.refantignore)
        
        RefAntOutput = refantobj.calculate()
        
        LOG.info("RefAntOutput: {}".format(RefAntOutput))
        
        self._do_gtype_delaycal(caltable=gtypecaltable, context=context, RefAntOutput=RefAntOutput)
        
        LOG.info("Initial phase calibration on delay calibrator complete")

        fracFlaggedSolns = 1.0

        critfrac = m.get_vla_critfrac()

        # Iterate and check the fraction of Flagged solutions, each time running gaincal in 'K' mode
        flagcount=0
        while fracFlaggedSolns > critfrac and flagcount < 4:

            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable,
                                    context=context, RefAntOutput=RefAntOutput)
            flaggedSolnResult = getCalFlaggedSoln(ktypecaltable)
            (fracFlaggedSolns, RefAntOutput) = self._check_flagSolns(flaggedSolnResult, RefAntOutput)
            LOG.info("Fraction of flagged solutions = " + str(flaggedSolnResult['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult['antmedian']['fraction']))
            flagcount += 1

        # Do initial amplitude and phase gain solutions on the BPcalibrator and delay
        # calibrator; the amplitudes are used for flagging; only phase
        # calibration is applied in final BP calibration, so that solutions are
        # not normalized per spw and take out the baseband filter shape

        # Try running with solint of int_time, 3*int_time, and 10*int_time.
        # If there is still a large fraction of failed solutions with
        # solint=10*int_time the source may be too weak, and calibration via the 
        # pipeline has failed; will need to implement a mode to cope with weak 
        # calibrators (later)

        context = self.inputs.context
        
        bpdgain_touse = tablebase + table_suffix[0]
        
        self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                solint=solint, context=context, RefAntOutput=RefAntOutput)

        flaggedSolnResult1 = getCalFlaggedSoln(tablebase + table_suffix[0])
        LOG.info("For solint = " + solint + " fraction of flagged solutions = " +
                 str(flaggedSolnResult1['all']['fraction']))
        LOG.info("Median fraction of flagged solutions per antenna = " +
                 str(flaggedSolnResult1['antmedian']['fraction']))

        if flaggedSolnResult1['all']['total'] > 0:
            fracFlaggedSolns1 = flaggedSolnResult1['antmedian']['fraction']
        else:
            fracFlaggedSolns1 = 1.0

        gain_solint1 = solint
        shortsol1 = soltime

        if fracFlaggedSolns1 > 0.05:
            soltime = soltimes[1]
            solint = solints[1]

            context = self.inputs.context
            
            self._do_gtype_bpdgains(tablebase + table_suffix[1], addcaltable=ktypecaltable,
                                    solint=solint, context=context, RefAntOutput=RefAntOutput)

            flaggedSolnResult3 = getCalFlaggedSoln(tablebase + table_suffix[1])
            LOG.info("For solint = "+solint+" fraction of flagged solutions = " +
                     str(flaggedSolnResult3['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult3['antmedian']['fraction']))

            if flaggedSolnResult3['all']['total'] > 0:
                fracFlaggedSolns3 = flaggedSolnResult3['antmedian']['fraction']
            else:
                fracFlaggedSolns3 = 1.0

            if fracFlaggedSolns3 < fracFlaggedSolns1:
                gain_solint1 = solint
                shortsol1 = soltime
            
                bpdgain_touse = tablebase + table_suffix[1]
            
                if fracFlaggedSolns3 > 0.05:
                    soltime = soltimes[2]
                    solint = solints[2]

                    context = self.inputs.context
                
                    self._do_gtype_bpdgains(tablebase + table_suffix[2], addcaltable=ktypecaltable, solint=solint,
                                            context=context, RefAntOutput=RefAntOutput)
                    flaggedSolnResult10 = getCalFlaggedSoln(tablebase + table_suffix[2])
                    LOG.info("For solint = "+solint+" fraction of flagged solutions = " +
                             str(flaggedSolnResult10['all']['fraction']))
                    LOG.info("Median fraction of flagged solutions per antenna = " +
                             str(flaggedSolnResult10['antmedian']['fraction']))

                    if flaggedSolnResult10['all']['total'] > 0:
                        fracFlaggedSolns10 = flaggedSolnResult10['antmedian']['fraction']
                    else:
                        fracFlaggedSolns10 = 1.0

                    if fracFlaggedSolns10 < fracFlaggedSolns3:
                        gain_solint1 = solint
                        shortsol1 = soltime
                        bpdgain_touse = tablebase + table_suffix[2]

                        if fracFlaggedSolns10 > 0.05:
                            LOG.warn("There is a large fraction of flagged solutions, " +
                                     "there might be something wrong with your data.  " +
                                     "The fraction of flagged solutions is " + str(fracFlaggedSolns10))

        LOG.info("Test amp and phase calibration on delay and bandpass calibrators complete")
        LOG.info("Using short solint = {!s}".format(str(gain_solint1)))

        LOG.info("Doing test bandpass calibration")

        if self.inputs.weakbp:
            # LOG.info("USING WEAKBP HEURISTICS")
            interp = weakbp(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                            ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False)
        else:
            # LOG.info("Using REGULAR heuristics")
            interp = ''
            do_bandpass(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                        spw='', ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                        solint='inf', append=False)

            AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
            AllCalTables.append(ktypecaltable)
            AllCalTables.append(bpdgain_touse)
            AllCalTables.append(bpcaltable)
            ntables = len(AllCalTables)
            interp = [''] * ntables
            LOG.info("Using 'linear,linearflag' for bandpass table")
            interp[-1] = 'linear,linearflag'

        LOG.info("Test bandpass calibration complete")
        LOG.info("Fraction of flagged solutions = {!s}".format(str(flaggedSolnResult['all']['fraction'])))
        LOG.info("Median fraction of flagged solutions per antenna = "+str(flaggedSolnResult['antmedian']['fraction']))

        LOG.info("Executing flagdata in clip mode.")
        self._do_clipflag(bpcaltable)

        LOG.info("Applying test calibrations to BP and delay calibrators")

        self._do_applycal(context=context, ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                                            bpcaltable=bpcaltable, interp=interp)

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return testBPdcalsResults(gain_solint1=gain_solint1, shortsol1=shortsol1, vis=self.inputs.vis,
                                  bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
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

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        critfrac = m.get_vla_critfrac()

        if fracFlaggedSolns > critfrac:
            RefAntOutput = np.delete(RefAntOutput, 0)
            self.inputs.context.observing_run.measurement_sets[0].reference_antenna = ','.join(RefAntOutput)
            LOG.info("Not enough good solutions, trying a different reference antenna.")
            LOG.info("The pipeline will start with antenna "+RefAntOutput[0].lower()+" as the reference.")

        return fracFlaggedSolns, RefAntOutput

    def _do_gtype_bpdgains(self, caltable, addcaltable=None, solint='int', context=None, RefAntOutput=None):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw()
        delay_scan_select_string = context.evla['msinfo'][m.name].delay_scan_select_string
        bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        if delay_scan_select_string == bandpass_scan_select_string:
            testgainscans = bandpass_scan_select_string
        else:
            testgainscans = bandpass_scan_select_string + ',' + delay_scan_select_string

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
                              'minsnr': 5.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'ap',
                              'append': False,
                              'docallib': False,
                              'gaintable': GainTables,
                              'gainfield': [''],
                              'interp': [''],
                              'spwmap': [],
                              'parang': self.parang}

        testgainscanslist = map(int, testgainscans.split(','))
        scanobjlist = m.get_scans(scan_id=testgainscanslist)
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

    def _do_clipflag(self, bpcaltable):

        task_args = {'vis': bpcaltable,
                     'mode': 'clip',
                     'datacolumn': 'CPARAM',
                     'clipminmax': [0.0, 2.0],
                     'correlation': 'ABS_ALL',
                     'clipoutside': True,
                     'flagbackup': False,
                     'savepars': False,
                     'action': 'apply'}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self, context=None, ktypecaltable=None, bpdgain_touse=None, bpcaltable=None, interp=None):
        """Run CASA task applycal"""
        
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        testgainscans = context.evla['msinfo'][m.name].testgainscans

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpdgain_touse)
        AllCalTables.append(bpcaltable)

        ntables = len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': '',
                              'intent': '',
                              'selectdata': True,
                              'scan': testgainscans,
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
