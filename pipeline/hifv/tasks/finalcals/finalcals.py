import math
import os
import shutil
import collections

import numpy as np

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.hifv.heuristics import getCalFlaggedSoln
from pipeline.hifv.heuristics import find_EVLA_band
from pipeline.hifv.heuristics import standard as standard
from pipeline.hifv.heuristics import weakbp, do_bandpass, uvrange
from pipeline.hifv.tasks.setmodel.vlasetjy import standard_sources
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class FinalcalsInputs(vdp.StandardInputs):
    weakbp = vdp.VisDependentProperty(default=False)
    refantignore = vdp.VisDependentProperty(default='')

    def __init__(self, context, vis=None, weakbp=None, refantignore=None):
        super(FinalcalsInputs, self).__init__()
        self.context = context
        self.vis = vis
        self._weakbp = weakbp
        self.refantignore = refantignore


class FinalcalsResults(basetask.Results):
    def __init__(self, final=None, pool=None, preceding=None, vis=None, bpdgain_touse=None,
                 gtypecaltable=None, ktypecaltable=None, bpcaltable=None,
                 phaseshortgaincaltable=None, finalampgaincaltable=None,
                 finalphasegaincaltable=None, flaggedSolnApplycalbandpass=None,
                 flaggedSolnApplycaldelay=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(FinalcalsResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.bpdgain_touse = bpdgain_touse
        self.gtypecaltable = gtypecaltable
        self.ktypecaltable = ktypecaltable
        self.bpcaltable = bpcaltable
        self.phaseshortgaincaltable = phaseshortgaincaltable
        self.finalampgaincaltable = finalampgaincaltable
        self.finalphasegaincaltable = finalphasegaincaltable
        self.flaggedSolnApplycalbandpass = flaggedSolnApplycalbandpass
        self.flaggedSolnApplycaldelay = flaggedSolnApplycaldelay

    def merge_with_context(self, context):
        if not self.final:
            LOG.error('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)


@task_registry.set_equivalent_casa_task('hifv_finalcals')
class Finalcals(basetask.StandardTaskTemplate):
    Inputs = FinalcalsInputs

    def prepare(self):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()
        band2spw = collections.defaultdict(list)
        spwobjlist = m.get_spectral_windows(science_windows_only=True)
        listspws = [spw.id for spw in spwobjlist]
        for spw, band in spw2band.items():
            if spw in listspws:  # Science intents only
                band2spw[band].append(str(spw))

        self.pool = []
        self.final = []

        bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, phaseshortgaincaltable, \
        finalampgaincaltable, finalphasegaincaltable, \
        flaggedSolnApplycalbandpass, flaggedSolnApplycaldelay = self._do_finalscals(band2spw)

        return FinalcalsResults(vis=self.inputs.vis, pool=self.pool, final=self.final,
                                bpdgain_touse=bpdgain_touse, gtypecaltable=gtypecaltable,
                                ktypecaltable=ktypecaltable, bpcaltable=bpcaltable,
                                phaseshortgaincaltable=phaseshortgaincaltable,
                                finalampgaincaltable=finalampgaincaltable,
                                finalphasegaincaltable=finalphasegaincaltable,
                                flaggedSolnApplycalbandpass=flaggedSolnApplycalbandpass,
                                flaggedSolnApplycaldelay=flaggedSolnApplycaldelay)

    def _do_finalscals(self, band2spw):

        self.parang = True
        try:
            self.setjy_results = self.inputs.context.results[0].read()[0].setjy_results
        except Exception as e:
            self.setjy_results = self.inputs.context.results[0].read().setjy_results

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_finalcals.s'

        gtypecaltable = tableprefix + str(stage_number) + '_1.' + 'finaldelayinitialgain.tbl'
        ktypecaltable = tableprefix + str(stage_number) + '_2.' + 'finaldelay.tbl'
        bpcaltable = tableprefix + str(stage_number) + '_4.' + 'finalBPcal.tbl'
        tablebase = tableprefix + str(stage_number) + '_3.' + 'finalBPinitialgain'
        table_suffix = ['.tbl', '3.tbl', '10.tbl']
        soltimes = [1.0, 3.0, 10.0]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        soltimes = [m.get_vla_max_integration_time() * x for x in soltimes]
        solints = ['int', '3.0s', '10.0s']
        soltime = soltimes[0]
        solint = solints[0]
        self.ignorerefant = self.inputs.context.evla['msinfo'][m.name].ignorerefant

        context = self.inputs.context
        refantignore = self.inputs.refantignore + ','.join(self.ignorerefant)
        refantfield = context.evla['msinfo'][m.name].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                geometry=True, flagging=True, intent='',
                                                spw='', refantignore=refantignore)

        RefAntOutput = refantobj.calculate()

        refAnt = ','.join(RefAntOutput)

        LOG.info("The pipeline will use antenna(s) " + refAnt + " as the reference")

        for band, spwlist in band2spw.items():
            LOG.info("EXECUTING G-TYPE DELAYCAL FOR BAND {!s}  spws: {!s}".format(band, ','.join(spwlist)))
            self._do_gtype_delaycal(caltable=gtypecaltable, context=context, refAnt=refAnt, spwlist=spwlist)

        for band, spwlist in band2spw.items():
            LOG.info("EXECUTING K-TYPE DELAYCAL FOR BAND {!s}  spws: {!s}".format(band, ','.join(spwlist)))
            self._do_ktype_delaycal(caltable=ktypecaltable, addcaltable=gtypecaltable, context=context, refAnt=refAnt,
                                    spw=','.join(spwlist))

        LOG.info("Delay calibration complete")

        # Do initial gaincal on BP calibrator then semi-final BP calibration
        for band, spwlist in band2spw.items():
            gain_solint1 = context.evla['msinfo'][m.name].gain_solint1[band]
            self._do_gtype_bpdgains(tablebase + table_suffix[0], addcaltable=ktypecaltable,
                                    solint=gain_solint1, context=context, refAnt=refAnt, spwlist=spwlist)

        bpdgain_touse = tablebase + table_suffix[0]
        LOG.info("Initial BP gain calibration complete")

        for band, spwlist in band2spw.items():
            append = False
            isdir = os.path.isdir(bpcaltable)
            if isdir:
                append = True
                LOG.info("Appending to existing table: {!s}".format(bpcaltable))

            if self.inputs.weakbp:
                LOG.debug("USING WEAKBP HEURISTICS")

                interp = weakbp(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                                ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=append,
                                executor=self._executor, spw=','.join(spwlist))
            else:
                LOG.debug("Using REGULAR heuristics")
                interp = ''
                do_bandpass(self.inputs.vis, bpcaltable, context=context, RefAntOutput=RefAntOutput,
                            spw=','.join(spwlist), ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=append,
                            executor=self._executor)

        LOG.info("Bandpass calibration complete")

        # Derive an average phase solution for the bandpass calibrator to apply
        # to all data to make QA plots easier to interpret.

        refantmode = 'flex'
        intents = list(m.intents)
        if [intent for intent in intents if 'POL' in intent]:
            # set to strict
            refantmode = 'strict'

        avgpgain = tableprefix + str(stage_number) + '_5.' + 'averagephasegain.tbl'

        for band, spwlist in band2spw.items():
            self._do_avgphasegaincal(avgpgain, context, refAnt,
                                     ktypecaltable=ktypecaltable, bpcaltable=bpcaltable, refantmode=refantmode,
                                     spw=','.join(spwlist))

        # In case any antenna is flagged by this process, unflag all solutions
        # in this gain table (if an antenna does exist or has bad solutions from
        # other steps, it will be flagged by those gain tables).

        self._do_unflag(avgpgain)
        self._do_applycal(context=context, ktypecaltable=ktypecaltable,
                          bpcaltable=bpcaltable, avgphasegaincaltable=avgpgain, interp=interp)

        # ---------------------------------------------------

        calMs = 'finalcalibrators.ms'
        isdir = os.path.isdir(calMs)
        if isdir:
            shutil.rmtree(calMs)
        split_result = self._do_split(calMs, '')  #, ','.join(spwlist))

        field_spws = m.get_vla_field_spws()

        # Run setjy execution per band
        all_sejy_result = self._doall_setjy(calMs, field_spws)

        LOG.info("Using power-law fits results from fluxscale.")
        for fs_result in self.inputs.context.evla['msinfo'][m.name].fluxscale_result:
            powerfit_setjy = self._do_powerfitsetjy(calMs, fs_result)

        phaseshortgaincaltable = tableprefix + str(stage_number) + '_6.' + 'phaseshortgaincal.tbl'
        finalampgaincaltable = tableprefix + str(stage_number) + '_7.' + 'finalampgaincal.tbl'
        finalphasegaincaltable = tableprefix + str(stage_number) + '_8.' + 'finalphasegaincal.tbl'

        for band, spwlist in band2spw.items():
            new_gain_solint1 = context.evla['msinfo'][m.name].new_gain_solint1[band]
            phaseshortgaincal_results = self._do_calibratorgaincal(calMs, phaseshortgaincaltable,
                                                                   new_gain_solint1, 3.0, 'p', [''], refAnt,
                                                                   refantmode=refantmode, spw=','.join(spwlist))

            gain_solint2 = context.evla['msinfo'][m.name].gain_solint2[band]
            finalampgaincal_results = self._do_calibratorgaincal(calMs, finalampgaincaltable, gain_solint2, 5.0,
                                                                 'ap', [phaseshortgaincaltable], refAnt,
                                                                 refantmode=refantmode, spw=','.join(spwlist))

            finalphasegaincal_results = self._do_calibratorgaincal(calMs, finalphasegaincaltable, gain_solint2,
                                                                   3.0, 'p', [finalampgaincaltable], refAnt,
                                                                   refantmode=refantmode, spw=','.join(spwlist))

        tablesToAdd = [(ktypecaltable, '', ''), (bpcaltable, 'linear,linearflag', ''),
                       (avgpgain, '', ''), (finalampgaincaltable, '', ''),
                       (finalphasegaincaltable, '', '')]
        # tablesToAdd = [(table, interp, gainfield) for table, interp, gainfield in tablesToAdd]

        callist = []
        for addcaltable, interp, gainfield in tablesToAdd:
            LOG.info("Finalcals stage:  Adding " + addcaltable + " to callibrary.")
            calto = callibrary.CalTo(self.inputs.vis)
            calfrom = callibrary.CalFrom(gaintable=addcaltable, interp=interp, calwt=False,
                                         caltype='finalcal', gainfield=gainfield)
            calapp = callibrary.CalApplication(calto, calfrom)
            callist.append(calapp)
            self.pool.append(calapp)
            self.final.append(calapp)

        flaggedSolnApplycalbandpass = getCalFlaggedSoln(bpdgain_touse)
        flaggedSolnApplycaldelay = getCalFlaggedSoln(ktypecaltable)

        return bpdgain_touse, gtypecaltable, ktypecaltable, bpcaltable, phaseshortgaincaltable,\
               finalampgaincaltable, finalphasegaincaltable, flaggedSolnApplycalbandpass, flaggedSolnApplycaldelay

    def analyse(self, results):
        return results

    def _do_gtype_delaycal(self, caltable=None, context=None, refAnt=None, spwlist=[]):

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = context.evla['msinfo'][m.name].delay_field_select_string
        tst_delay_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
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
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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

    def _do_ktype_delaycal(self, caltable=None, addcaltable=None, context=None, refAnt=None, spw=''):

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        delay_field_select_string = context.evla['msinfo'][m.name].delay_field_select_string
        delay_scan_select_string = context.evla['msinfo'][m.name].delay_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

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
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'K',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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

    def _do_gtype_bpdgains(self, caltable, addcaltable=None, solint='int', context=None, refAnt=None, spwlist=[]):

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        tst_bpass_spw = m.get_vla_tst_bpass_spw(spwlist=spwlist)
        bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

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
                              'refant': refAnt.lower(),
                              'minblperant': minBL_for_cal,
                              'minsnr': 3.0,
                              'solnorm': False,
                              'gaintype': 'G',
                              'smodel': [],
                              'calmode': 'p',
                              'append': append,
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

    def _do_avgphasegaincal(self, caltable, context, refAnt, ktypecaltable=None, bpcaltable=None,
                            refantmode='flex', spw=''):

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s}".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        bandpass_field_select_string = context.evla['msinfo'][m.name].bandpass_field_select_string
        bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
        minBL_for_cal = m.vla_minbaselineforcal()

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpcaltable)

        avgphasegaincal_task_args = {'vis': self.inputs.vis,
                                     'caltable': caltable,
                                     'field': '',
                                     'spw': spw,
                                     'selectdata': True,
                                     'uvrange': '',
                                     'scan': '',
                                     'solint': 'inf',
                                     'combine': 'scan',
                                     'preavg': -1.0,
                                     'refant': refAnt.lower(),
                                     'minblperant': minBL_for_cal,
                                     'minsnr': 1.0,
                                     'solnorm': False,
                                     'gaintype': 'G',
                                     'smodel': [],
                                     'calmode': 'p',
                                     'append': append,
                                     'docallib': False,
                                     'gaintable': AllCalTables,
                                     'gainfield': [''],
                                     'interp': [''],
                                     'spwmap': [],
                                     'parang': self.parang,
                                     'refantmode': refantmode}

        bpscanslist = list(map(int, bandpass_scan_select_string.split(',')))
        scanobjlist = m.get_scans(scan_id=bpscanslist)
        allfieldidlist = []
        for scanobj in scanobjlist:
            fieldobj, = scanobj.fields
            if str(fieldobj.id) not in allfieldidlist:
                allfieldidlist.append(str(fieldobj.id))

        # See vlascanheuristics - only use the first bandpass calibrator
        fieldidlist = [fieldid for fieldid in allfieldidlist if fieldid in bandpass_field_select_string]

        for fieldidstring in fieldidlist:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            avgphasegaincal_task_args['field'] = fieldidstring
            avgphasegaincal_task_args['uvrange'] = uvrangestring
            if os.path.exists(caltable):
                avgphasegaincal_task_args['append'] = True

            job = casa_tasks.gaincal(**avgphasegaincal_task_args)

            self._executor.execute(job)

        return True

    def _do_unflag(self, gaintable):

        task_args = {'vis': gaintable,
                     'mode': 'unflag',
                     'action': 'apply',
                     'display': '',
                     'flagbackup': False,
                     'savepars': False}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)

    def _do_applycal(self, context=None, ktypecaltable=None, bpcaltable=None,
                     avgphasegaincaltable=None, interp=None, spw=''):
        """Run CASA task applycal"""

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = context.evla['msinfo'][m.name].calibrator_scan_select_string

        AllCalTables = sorted(self.inputs.context.callibrary.active.get_caltable())
        AllCalTables.append(ktypecaltable)
        AllCalTables.append(bpcaltable)
        AllCalTables.append(avgphasegaincaltable)

        ntables = len(AllCalTables)

        applycal_task_args = {'vis': self.inputs.vis,
                              'field': '',
                              'spw': spw,
                              'intent': '',
                              'selectdata': True,
                              'scan': calibrator_scan_select_string,
                              'docallib': False,
                              'gaintable': AllCalTables,
                              'gainfield': [''],
                              'interp': [interp],
                              'spwmap': [],
                              'calwt': [False] * ntables,
                              'parang': self.parang,
                              'applymode': 'calflagstrict',
                              'flagbackup': True}

        job = casa_tasks.applycal(**applycal_task_args)

        return self._executor.execute(job)

    def _do_split(self, calMs, spw=''):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        channels = m.get_vla_numchan()
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        task_args = {'vis': m.name,
                     'outputvis': calMs,
                     'datacolumn': 'corrected',
                     'keepmms': True,
                     'field': '',
                     'spw': spw,
                     # 'width': int(max(channels)),
                     'width': 1,
                     'antenna': '',
                     'timebin': '0s',
                     'timerange': '',
                     'scan': calibrator_scan_select_string,
                     'intent': '',
                     'array': '',
                     'uvrange': '',
                     'correlation': '',
                     'observation': '',
                     'keepflags': False}

        job = casa_tasks.split(**task_args)

        return self._executor.execute(job)

    def _doall_setjy(self, calMs, field_spws):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        spw2band = m.get_vla_spw2band()

        standard_source_names, standard_source_fields = standard_sources(calMs)

        # Look in spectral window domain object as this information already exists!
        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            channels = table.getcol('NUM_CHAN')
            originalBBClist = table.getcol('BBC_NO')
            spw_bandwidths = table.getcol('TOTAL_BANDWIDTH')
            reference_frequencies = table.getcol('REF_FREQUENCY')

        center_frequencies = [rf + spwbw / 2 for rf, spwbw in zip(reference_frequencies, spw_bandwidths)]

        for i, fields in enumerate(standard_source_fields):
            for myfield in fields:
                domainfield = m.get_fields(myfield)[0]
                if 'AMPLITUDE' in domainfield.intents:
                    jobs = []
                    VLAspws = field_spws[myfield]
                    strlistVLAspws = ','.join(str(spw) for spw in VLAspws)
                    spws = [spw for spw in m.get_spectral_windows(strlistVLAspws)]

                    for spw in spws:
                        reference_frequency = center_frequencies[spw.id]
                        try:
                            EVLA_band = spw2band[spw.id]
                        except:
                            LOG.info('Unable to get band from spw id - using reference frequency instead')
                            EVLA_band = find_EVLA_band(reference_frequency)

                        LOG.info("Center freq for spw " + str(spw.id) + " = " + str(
                            reference_frequency) + ", observing band = " + EVLA_band)

                        model_image = standard_source_names[i] + '_' + EVLA_band + '.im'

                        LOG.info(
                            "Setting model for field " + str(myfield) + " spw " + str(spw.id) + " using " + model_image)

                        # Double check, but the fluxdensity=-1 should not matter since
                        #  the model image take precedence
                        try:
                            job = self._do_setjy(calMs, str(myfield), str(spw.id), model_image, -1)
                            jobs.append(job)
                            # result.measurements.update(setjy_result.measurements)
                        except Exception:
                            # something has gone wrong, return an empty result
                            LOG.warn(
                                "SetJy issue with field id=" + str(job.kw['field']) + " and spw=" + str(job.kw['spw']))

                    LOG.info("Merging flux scaling operation for setjy jobs for " + self.inputs.vis)
                    jobs_and_components = utils.merge_jobs(jobs, casa_tasks.setjy, merge=('spw',))
                    for job, _ in jobs_and_components:
                        try:
                            self._executor.execute(job)
                        except Exception:
                            LOG.warn(
                                "SetJy issue with field id=" + str(job.kw['field']) + " and spw=" + str(job.kw['spw']))

        return True

    def _do_setjy(self, calMs, field, spw, model_image, fluxdensity):

        try:
            task_args = {'vis': calMs,
                         'field': field,
                         'spw': spw,
                         'selectdata': False,
                         'model': model_image,
                         'listmodels': False,
                         'scalebychan': True,
                         'fluxdensity': -1,
                         'standard': standard.Standard()(field),
                         'usescratch': True}

            job = casa_tasks.setjy(**task_args)

            return job
        except Exception as e:
            LOG.info(str(e))
            return None

    def _do_powerfit(self, field_spws):

        context = self.inputs.context

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        sources = context.evla['msinfo'][m.name].fluxscale_sources
        flux_densities = context.evla['msinfo'][m.name].fluxscale_flux_densities
        spws = context.evla['msinfo'][m.name].fluxscale_spws
        fluxscale_result = context.evla['msinfo'][m.name].fluxscale_result
        spw2band = m.get_vla_spw2band()
        bands = list(spw2band.values())

        # Look in spectral window domain object as this information already exists!
        with casa_tools.TableReader(self.inputs.vis + '/SPECTRAL_WINDOW') as table:
            spw_bandwidths = table.getcol('TOTAL_BANDWIDTH')
            reference_frequencies = table.getcol('REF_FREQUENCY')

        center_frequencies = [rf + spwbw / 2 for rf, spwbw in zip(reference_frequencies, spw_bandwidths)]

        unique_sources = list(np.unique(sources))
        results = []
        for source in unique_sources:
            indices = []
            for ii in range(len(sources)):
                if sources[ii] == source:
                    indices.append(ii)
            bands_from_spw = []

            if bands == []:
                for ii in range(len(indices)):
                    bands.append(find_EVLA_band(center_frequencies[spws[indices[ii]]]))
            else:
                for ii in range(len(indices)):
                    bands_from_spw.append(spw2band[spws[indices[ii]]])
                bands = bands_from_spw

            unique_bands = list(np.unique(bands))

            fieldobject = m.get_fields(source)
            fieldid = str([str(f.id) for f in fieldobject if str(f.id) in fluxscale_result][0])

            for band in unique_bands:
                lfreqs = []
                lfds = []
                lerrs = []
                uspws = []

                # Use spw id to band mappings if available
                if list(spw2band.values()):
                    for ii in range(len(indices)):
                        if spw2band[spws[indices[ii]]] == band:
                            lfreqs.append(math.log10(center_frequencies[spws[indices[ii]]]))
                            lfds.append(math.log10(flux_densities[indices[ii]][0]))
                            lerrs.append((flux_densities[indices[ii]][1]) / (flux_densities[indices[ii]][0]) / 2.303)
                            uspws.append(spws[indices[ii]])

                # Use frequencies for band mappings if no spwid-to-band mapping is available
                if not list(spw2band.values()):
                    for ii in range(len(indices)):
                        if find_EVLA_band(center_frequencies[spws[indices[ii]]]) == band:
                            lfreqs.append(math.log10(center_frequencies[spws[indices[ii]]]))
                            lfds.append(math.log10(flux_densities[indices[ii]][0]))
                            lerrs.append((flux_densities[indices[ii]][1]) / (flux_densities[indices[ii]][0]) / 2.303)
                            uspws.append(spws[indices[ii]])

                if len(lfds) < 2:
                    fitcoeff = [lfds[0], 0.0, 0.0, 0.0, 0.0]
                else:
                    fitcoeff = fluxscale_result[fieldid]['spidx']

                freqs = fluxscale_result['freq']
                fitflx = fluxscale_result[fieldid]['fitFluxd']
                fitreff = fluxscale_result[fieldid]['fitRefFreq']
                spidx = fluxscale_result[fieldid]['spidx']
                reffreq = fitreff / 1.e9
                spix = fluxscale_result[fieldid]['spidx'][1]
                spixerr = fluxscale_result[fieldid]['spidxerr'][1]

                freqs = np.array(sorted(freqs[uspws]))

                logfittedfluxd = np.zeros(len(freqs))
                for i in range(len(spidx)):
                    logfittedfluxd += spidx[i] * (np.log10(freqs / fitreff)) ** i

                fittedfluxd = 10.0 ** logfittedfluxd

                results.append([source, uspws, fitflx, spix, reffreq])
                LOG.info(source + ' ' + band + ' fitted spectral index = ' + str(spix))
                LOG.info("Frequency, data, and fitted data:")

                for ii in range(len(freqs)):
                    SS = fittedfluxd[ii]
                    freq = freqs[ii] / 1.e9
                    LOG.info('    ' + str(freq) + '  ' + str(10.0 ** lfds[ii]) + '  ' + str(SS))

        return results

    def _do_powerfitsetjy(self, calMs, fluxscale_result):

        LOG.info("Setting power-law fit in the model column")

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        # fluxscale_result = self.inputs.context.evla['msinfo'][m.name].fluxscale_result
        dictkeys = list(fluxscale_result.keys())
        keys_to_remove = ['freq', 'spwName', 'spwID']
        dictkeys = [field_id for field_id in dictkeys if field_id not in keys_to_remove]

        for fieldid in dictkeys:
            jobs_calMs = []

            spws = list(fluxscale_result['spwID'])
            scispws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]
            newspws = [str(spwint) for spwint in list(set(scispws) & set(spws))]

            try:
                LOG.info('Running setjy for field ' + str(fieldid) + ': ' + str(fluxscale_result[fieldid]['fieldName']))
                task_args = {'vis': calMs,
                             'field': fluxscale_result[fieldid]['fieldName'],
                             'spw': ','.join(newspws),
                             'selectdata': False,
                             'model': '',
                             'listmodels': False,
                             'scalebychan': True,
                             'fluxdensity': [fluxscale_result[fieldid]['fitFluxd'], 0, 0, 0],
                             'spix': list(fluxscale_result[fieldid]['spidx'][1:3]),
                             'reffreq': str(fluxscale_result[fieldid]['fitRefFreq']) + 'Hz',
                             'standard': 'manual',
                             'usescratch': True}

                # job = casa_tasks.setjy(**task_args)
                jobs_calMs.append(casa_tasks.setjy(**task_args))

            except Exception as e:
                LOG.info(e)

            # merge identical jobs into one job with a multi-spw argument
            LOG.info("Merging setjy jobs for finalcalibrators.ms")
            jobs_and_components_calMs = utils.merge_jobs(jobs_calMs, casa_tasks.setjy, merge=('spw',))
            for job, _ in jobs_and_components_calMs:
                self._executor.execute(job)

        return True

    def _do_calibratorgaincal(self, calMs, caltable, solint, minsnr, calmode, gaintablelist, refAnt, refantmode='flex', spw=''):

        append = False
        isdir = os.path.isdir(caltable)
        if isdir:
            append = True
            LOG.info("Appending to existing table: {!s} ".format(caltable))

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        calibrator_scan_select_string = self.inputs.context.evla['msinfo'][m.name].calibrator_scan_select_string

        scanlist = [int(scan) for scan in calibrator_scan_select_string.split(',')]
        scanids_perband = ','.join([str(scan.id) for scan in m.get_scans(scan_id=scanlist, spw=spw)])

        minBL_for_cal = m.vla_minbaselineforcal()

        task_args = {'vis': calMs,
                     'caltable': caltable,
                     'field': '',
                     'spw': spw,
                     'intent': '',
                     'selectdata': False,
                     'solint': solint,
                     'combine': 'scan',
                     'preavg': -1.0,
                     'refant': refAnt.lower(),
                     'minblperant': minBL_for_cal,
                     'minsnr': minsnr,
                     'solnorm': False,
                     'gaintype': 'G',
                     'smodel': [],
                     'calmode': calmode,
                     'append': append,
                     'gaintable': gaintablelist,
                     'gainfield': [''],
                     'interp': [''],
                     'spwmap': [],
                     'parang': self.parang,
                     'uvrange': '',
                     'refantmode': refantmode}

        calscanslist = list(map(int, scanids_perband.split(',')))
        scanobjlist = m.get_scans(scan_id=calscanslist,
                                  scan_intent=['AMPLITUDE', 'BANDPASS', 'POLLEAKAGE', 'POLANGLE',
                                               'PHASE', 'POLARIZATION', 'CHECK'])
        fieldidlist = []
        for scanobj in scanobjlist:
            fieldobj, = scanobj.fields
            if str(fieldobj.id) not in fieldidlist:
                fieldidlist.append(str(fieldobj.id))

        for fieldidstring in fieldidlist:
            fieldid = int(fieldidstring)
            uvrangestring = uvrange(self.setjy_results, fieldid)
            task_args['field'] = fieldidstring
            task_args['uvrange'] = uvrangestring
            task_args['selectdata'] = True
            if os.path.exists(caltable):
                task_args['append'] = True

            job = casa_tasks.gaincal(**task_args)

            self._executor.execute(job)

        return True


