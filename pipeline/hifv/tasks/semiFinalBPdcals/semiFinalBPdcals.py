import os
import collections

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

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = m.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        gtypecaltable = {}
        ktypecaltable = {}
        bpcaltable = {}
        bpdgain_touse = {}
        flaggedSolnApplycalbandpass = {}
        flaggedSolnApplycaldelay = {}

        for band, spwlist in band2spw.items():

            bpdgain_tousename, gtypecaltablename, ktypecaltablename, bpcaltablename, \
            flaggedSolnApplycalbandpassperband, flaggedSolnApplycaldelayperband = self._do_semifinal(band, spwlist)

            gtypecaltable[band] = gtypecaltablename
            ktypecaltable[band] = ktypecaltablename
            bpcaltable[band] = bpcaltablename
            bpdgain_touse[band] = bpdgain_tousename
            flaggedSolnApplycalbandpass[band] = flaggedSolnApplycalbandpassperband
            flaggedSolnApplycaldelay[band] = flaggedSolnApplycaldelayperband

        return semiFinalBPdcalsResults(bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                       ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                       flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                       flaggedSolnApplycaldelay=flaggedSolnApplycaldelay)

    def analyse(self, results):
        return results

    def _do_semifinal(self, band, spwlist):

        LOG.info("Executing for band {!s}  spws: {!s}".format(band, ','.join(spwlist)))
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

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'semiFinaldelayinitialgain_{!s}.tbl'.format(band)
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'delay_{!s}.tbl'.format(band)
        bpcaltable = tableprefix + str(stage_number) + '_4.' + 'BPcal_{!s}.tbl'.format(band)
        tablebase = tableprefix + str(stage_number) + '_3.' + 'BPinitialgain'

        table_suffix = ['_{!s}.tbl'.format(band), '3_{!s}.tbl'.format(band), '10_{!s}.tbl'.format(band)]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        self.ignorerefant = self.inputs.context.evla['msinfo'][m.name].ignorerefant

        context = self.inputs.context
        refantignore = self.inputs.refantignore + ','.join(self.ignorerefant)
        refantfield = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                geometry=True, flagging=True, intent='',
                                                spw='', refantignore=refantignore)

        RefAntOutput = refantobj.calculate()

        self._do_gtype_delaycal(caltable=gtypecaltable, context=context, RefAntOutput=RefAntOutput, spwlist=spwlist)

        fracFlaggedSolns = 1.0

        critfrac = m.get_vla_critfrac()

        # Iterate and check the fraciton of Flagged solutions, each time running gaincal in 'K' mode
        flagcount = 0
        while fracFlaggedSolns > critfrac and flagcount < 4:
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable,
                                    context=context, RefAntOutput=RefAntOutput, spw=','.join(spwlist))
            flaggedSolnResult = getCalFlaggedSoln(ktypecaltable)
            fracFlaggedSolns = self._check_flagSolns(flaggedSolnResult, RefAntOutput)

            LOG.info("Fraction of flagged solutions = " + str(flaggedSolnResult['all']['fraction']))
            LOG.info("Median fraction of flagged solutions per antenna = " +
                     str(flaggedSolnResult['antmedian']['fraction']))
            flagcount += 1

        LOG.info("Delay calibration complete for band {!s}".format(band))

        # Do initial gaincal on BP calibrator then semi-final BP calibration
        gain_solint1 = context.evla['msinfo'][m.name].gain_solint1[band]
        self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                solint=gain_solint1, context=context, RefAntOutput=RefAntOutput, spwlist=spwlist)

        bpdgain_touse = tablebase + table_suffix[0]

        LOG.debug("WEAKBP: " + str(self.inputs.weakbp))

        if self.inputs.weakbp:
            LOG.debug("USING WEAKBP HEURISTICS")
            interp = weakbp(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                            ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False,
                            executor=self._executor, spw=','.join(spwlist))
        else:
            LOG.debug("Using REGULAR heuristics")
            do_bandpass(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                        spw=','.join(spwlist), ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                        solint='inf', append=False, executor=self._executor)

            AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
            AllCalTables.append(ktypecaltable)
            # AllCalTables.append(bpdgain_touse)
            AllCalTables.append(bpcaltable)
            ntables = len(AllCalTables)
            interp = [''] * ntables
            LOG.info("Using 'linear,linearflag' for bandpass table")
            interp[-1] = 'linear,linearflag'

        self._do_applycal(context=context, ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse,
                          bpcaltable=bpcaltable, interp=interp, spw=','.join(spwlist))

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, flaggedSolnApplycalbandpass, \
               flaggedSolnApplycaldelay

    def _do_gtype_delaycal(self, caltable=None, context=None, RefAntOutput=None, spwlist=[]):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = context.evla['msinfo'][m.name].delay_field_select_string
        tst_delay_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
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

    def _do_ktype_delaycal(self, caltable=None, addcaltable=None, context=None, RefAntOutput=None, spw=''):

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
                              'spw': spw,
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
        critfrac = m.get_vla_critfrac()

        if fracFlaggedSolns > critfrac:
            RefAntOutput = np.delete(RefAntOutput, 0)
            self.inputs.context.observing_run.measurement_sets[0].reference_antenna = ','.join(RefAntOutput)
            LOG.info("Not enough good solutions, trying a different reference antenna.")
            LOG.info("The pipeline start with antenna "+RefAntOutput[0]+" as the reference.")

        return fracFlaggedSolns

    def _do_gtype_bpdgains(self, caltable, addcaltable=None, solint='int', context=None, RefAntOutput=None, spwlist=[]):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
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
                              'scan': bandpass_scan_select_string,
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

        bpscanslist = list(map(int, bandpass_scan_select_string.split(',')))
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

    def _do_applycal(self, context=None, ktypecaltable=None, bpdgain_touse=None, bpcaltable=None, interp=None, spw=''):
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
                              'spw': spw,
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
