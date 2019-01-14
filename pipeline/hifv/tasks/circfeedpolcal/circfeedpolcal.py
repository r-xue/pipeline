from __future__ import absolute_import

import os
import math
import ast
import collections

import pipeline.hif.heuristics.findrefant as findrefant
import pipeline.hif.tasks.gaincal as gaincal
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks.polarization import polarization
from pipeline.hifv.tasks.setmodel.vlasetjy import standard_sources
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class CircfeedpolcalResults(polarization.PolarizationResults):
    def __init__(self, final=None, pool=None, preceding=None, vis=None,
                 refant=None, calstrategy=None, caldictionary=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []
        if refant is None:
            refant = ''
        if calstrategy is None:
            calstrategy = ''
        if caldictionary is None:
            caldictionary = {}

        super(CircfeedpolcalResults, self).__init__()
        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.refant = refant
        self.calstrategy = calstrategy
        self.caldictionary = caldictionary

    def merge_with_context(self, context):
        """
        See :method:`~pipeline.infrastructure.api.Results.merge_with_context`
        """
        if not self.final:
            LOG.warn('No circfeedpolcal results to add to the callibrary')
            return

        for calapp in self.final:
            LOG.debug('Adding pol calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        # return 'CircfeedpolcalResults:\n\t{0}'.format(
        #     '\n\t'.join([ms.name for ms in self.mses]))
        return 'CircfeedpolcalResults:'


class CircfeedpolcalInputs(vdp.StandardInputs):
    Dterm_solint = vdp.VisDependentProperty(default='2MHz')
    refantignore = vdp.VisDependentProperty(default='')
    leakage_poltype = vdp.VisDependentProperty(default='')
    mbdkcross = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def clipminmax(self):
        return [0.0, 0.25]

    def __init__(self, context, vis=None, Dterm_solint=None, refantignore=None, leakage_poltype=None,
                 mbdkcross=None, clipminmax=None):
        super(CircfeedpolcalInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.Dterm_solint = Dterm_solint
        self.refantignore = refantignore
        self.leakage_poltype = leakage_poltype
        self.mbdkcross = mbdkcross
        self.clipminmax = clipminmax


@task_registry.set_equivalent_casa_task('hifv_circfeedpolcal')
class Circfeedpolcal(polarization.Polarization):
    Inputs = CircfeedpolcalInputs

    def prepare(self):

        self.callist = []

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        intents = list(m.intents)

        self.RefAntOutput = ['']
        self.calstrategy = ''
        self.caldictionary = {}

        if [intent for intent in intents if 'POL' in intent]:
            self.do_prepare()

        return CircfeedpolcalResults(vis=self.inputs.vis, pool=self.callist, final=self.callist,
                                     refant=self.RefAntOutput[0].lower(), calstrategy=self.calstrategy,
                                     caldictionary=self.caldictionary)

    def analyse(self, results):
        return results

    def do_prepare(self):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        refantfield = self.inputs.context.evla['msinfo'][m.name].calibrator_field_select_string
        refantobj = findrefant.RefAntHeuristics(vis=self.inputs.vis, field=refantfield,
                                                geometry=True, flagging=True, intent='', spw='',
                                                refantignore=self.inputs.refantignore)
        self.RefAntOutput = refantobj.calculate()

        # setjy for amplitude/flux calibrator (VLASS 3C286 or 3C48)
        fluxcalfieldname, fluxcalfieldid, fluxcal = self._do_setjy()

        try:
            stage_number = self.inputs.context.results[-1].read()[0].stage_number + 1
        except Exception as e:
            stage_number = self.inputs.context.results[-1].read().stage_number + 1

        tableprefix = os.path.basename(self.inputs.vis) + '.' + 'hifv_circfeedpolcal.s'

        tablesToAdd = [[tableprefix + str(stage_number) + '_1.' + 'kcross.tbl', 'kcross', []],
                       [tableprefix + str(stage_number) + '_2.' + 'D2.tbl', 'polarization', []],
                       [tableprefix + str(stage_number) + '_3.' + 'X1.tbl', 'polarization', []]]

        # D-terms   - do we need this?
        # self.do_polcal(self.inputs.vis+'.D1', 'D+QU',field='',
        #               intent='CALIBRATE_POL_LEAKAGE#UNSPECIFIED',
        #               gainfield=[''], spwmap=[],
        #               solint='inf')

        # First pass R-L delay

        tablesToAdd[0][2] = []  # Default for KCROSS table
        if self.inputs.mbdkcross:
            # baseband_spws = [spw.id for spw in m.get_spectral_windows(science_windows_only=True)]
            baseband_spws = self.vla_basebands(science_windows_only=True)
            addcallib = False
            if len(baseband_spws) == 1:
                addcallib = True
            for spws in baseband_spws:
                LOG.info("Executing gaincal on baseband with spws={!s}".format(spws))
                self.do_gaincal(tablesToAdd[0][0], field=fluxcalfieldname, spw=spws,
                                combine='scan,spw', addcallib=addcallib)
                tablesToAdd[0][2] = self.do_spwmap()
        else:
            spwsobj = m.get_spectral_windows(science_windows_only=True)
            spwslist = [str(spw.id) for spw in spwsobj]
            spws = ','.join(spwslist)
            self.do_gaincal(tablesToAdd[0][0], field=fluxcalfieldname, spw=spws, addcallib=True)

        # Determine number of scans with POLLEAKGE intent and use the first POLLEAKAGE FIELD
        polleakagefield = ''
        polleakagefields = m.get_fields(intent='POLLEAKAGE')
        try:
            polleakagefield = polleakagefields[0].name
        except Exception as ex:
            # If no POLLEAKAGE intent is associated with a field, then use the flux calibrator
            polleakagefield = fluxcalfieldname
            LOG.warning("Exception: No POLLEAKGE intents found. {!s}".format(str(ex)))
        if len(polleakagefields) > 1:
            # Use the first pol leakage field
            polleakagefield = polleakagefields[0].name
            LOG.info("More than one field with intent of POLLEAKGE.  Using field {!s}".format(polleakagefield))

        polleakagescans = []
        poltype = 'Df+QU'  # Default
        for scan in m.get_scans(field=polleakagefield):
            if 'POLLEAKAGE' in scan.intents:
                polleakagescans.append((scan.id, scan.intents))

        # Calibration Strategies
        LOG.info("Number of POL_LEAKAGE scans: {!s}".format(len(polleakagescans)))
        self.calstrategy = ''
        if len(polleakagescans) >= 3:
            poltype = 'Df+QU'   # C4
            self.calstrategy = "Using Calibration Strategy C4: 3 or more slices CALIBRATE_POL_LEAKAGE, KCROSS, Df+QU, Xf."
        if len(polleakagescans) < 3:
            poltype = 'Df'      # C1
            self.calstrategy = "Using Calibration Strategy C1: Less than 3 slices CALIBRATE_POL_LEAKAGE, KCROSS, Df, Xf."
        LOG.info(self.calstrategy)

        if self.inputs.leakage_poltype:
            poltype = self.inputs.leakage_poltype
            self.calstrategy = "Calibration Strategy OVERRIDE: User-defined leakage_poltype of " + str(poltype)
            LOG.warn(self.calstrategy)

        # Determine the first POLANGLE FIELD
        polanglefield = ''
        polanglefields = m.get_fields(intent='POLANGLE')
        try:
            polanglefield = polanglefields[0].name
        except Exception as ex:
            # If no POLANGLE intent is associated with a field, then use the flux calibrator
            polanglefield = fluxcalfieldname
            LOG.warning("Exception: No POLANGLE intents found. {!s}".format(str(ex)))
        if len(polanglefields) > 1:
            # Use the first pol angle field
            polanglefield = polanglefields[0].name
            LOG.info("More than one field with intent of POLANGLE.  Using field {!s}".format(polanglefield))

        # D-terms in 2MHz pieces, minsnr of 5.0
        LOG.info("Polcal D-terms using solint=\'inf,{!s}\'".format(self.inputs.Dterm_solint))

        self.do_polcal(tablesToAdd[1][0], kcrosstable=tablesToAdd[0][0], poltype=poltype, field=polleakagefield,
                       intent='CALIBRATE_POL_LEAKAGE#UNSPECIFIED',
                       gainfield=[''], kcrossspwmap=tablesToAdd[0][2],
                       solint='inf,{!s}'.format(self.inputs.Dterm_solint),
                       minsnr=5.0)

        # Clip flagging
        self._do_clipflag(tablesToAdd[1][0])

        # 2MHz pieces, minsnr of 3.0
        self.do_polcal(tablesToAdd[2][0], kcrosstable=tablesToAdd[0][0], poltype='Xf', field=polanglefield,
                       intent='CALIBRATE_POL_ANGLE#UNSPECIFIED',
                       gainfield=[''], kcrossspwmap=tablesToAdd[0][2], solint='inf,2MHz',
                       minsnr=3.0)

        for (addcaltable, caltype, spwmap) in tablesToAdd:
            calto = callibrary.CalTo(self.inputs.vis)
            calfrom = callibrary.CalFrom(gaintable=addcaltable, interp='', calwt=False,
                                         caltype=caltype, spwmap=spwmap)
            calapp = callibrary.CalApplication(calto, calfrom)
            self.callist.append(calapp)

        self.caldictionary = {'fluxcalfieldname': fluxcalfieldname,
                              'fluxcalfieldid': fluxcalfieldid,
                              'fluxcal': fluxcal,
                              'polanglefield': polanglefield,
                              'polleakagefield': polleakagefield}

    def _modifyGainTables(self, GainTables):
        '''

        Args:
            GainTables: Python List of tables from the calibrary

        Returns: replaces the finalphasegaincal name with the phaseshortgaincal table from hifv_finalcals

        '''

        idx = -1  # Should be last element
        newtable = ''
        for i, table in enumerate(GainTables):
            if 'finalphasegaincal' in table:
                idx = i
                try:
                    finalcals_result = self.inputs.context.results[-1].read()[0]
                except Exception as e:
                    finalcals_result = self.inputs.context.results[-1].read()
                newtable = finalcals_result.phaseshortgaincaltable
        GainTables[idx] = newtable

        return GainTables

    def do_gaincal(self, caltable, field='', spw='', combine='scan', addcallib=False):

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        minBL_for_cal = m.vla_minbaselineforcal()

        append = False
        if os.path.exists(caltable):
            append = True
            addcallib = True
            LOG.info("{!s} exists.  Appending to caltable.".format(caltable))

        GainTables = []
        interp = []

        calto = callibrary.CalTo(self.inputs.vis)
        calstate = self.inputs.context.callibrary.get_calstate(calto)
        merged = calstate.merged()
        for calto, calfroms in merged.iteritems():
            calapp = callibrary.CalApplication(calto, calfroms)
            GainTables.append(calapp.gaintable)
            interp.append(calapp.interp)

        GainTables = GainTables[0]
        interp = interp[0]

        GainTables = self._modifyGainTables(GainTables)

        task_inputs = gaincal.GTypeGaincal.Inputs(self.inputs.context,
                                                  output_dir='',
                                                  vis=self.inputs.vis,
                                                  caltable=caltable,
                                                  field=field,
                                                  intent='',
                                                  scan='',
                                                  spw=spw,
                                                  solint='inf',
                                                  gaintype='KCROSS',
                                                  combine=combine,
                                                  refant=self.RefAntOutput,
                                                  minblperant=minBL_for_cal,
                                                  parang=True,
                                                  append=append)

        # gaincal_task = gaincal.GTypeGaincal(task_inputs)
        # result = self._executor.execute(gaincal_task, merge=True)

        casa_task_args = {'vis': self.inputs.vis,
                          'caltable': caltable,
                          'field': field,
                          'intent': 'CALIBRATE_FLUX#UNSPECIFIED,CALIBRATE_AMPLI#UNSPECIFIED,CALIBRATE_PHASE#UNSPECIFIED,CALIBRATE_BANDPASS#UNSPECIFIED',
                          'scan': '',
                          'spw': spw,
                          'solint': 'inf',
                          'gaintype': 'KCROSS',
                          'combine': combine,
                          'refant': ','.join(self.RefAntOutput),
                          'gaintable': GainTables,
                          'interp': interp,
                          'minblperant': minBL_for_cal,
                          'parang': True,
                          'append': append}

        job = casa_tasks.gaincal(**casa_task_args)

        self._executor.execute(job)

        if addcallib:
            LOG.info("Adding " + str(caltable) + " to callibrary.")
            calfrom = callibrary.CalFrom(gaintable=caltable, interp='', calwt=False, caltype='kcross')
            calto = callibrary.CalTo(self.inputs.vis)
            calapp = callibrary.CalApplication(calto, calfrom)
            self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        # return result
        return True

    def do_polcal(self, caltable, kcrosstable='', poltype='', field='', intent='',
                  gainfield=[''], kcrossspwmap=[], solint='inf', minsnr=5.0):

        GainTables = []

        calto = callibrary.CalTo(self.inputs.vis)
        calstate = self.inputs.context.callibrary.get_calstate(calto)
        merged = calstate.merged()
        for calto, calfroms in merged.iteritems():
            calapp = callibrary.CalApplication(calto, calfroms)
            GainTables.append(calapp.gaintable)

        GainTables = GainTables[0]
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)
        minBL_for_cal = m.vla_minbaselineforcal()

        spwmap = []
        for gaintable in GainTables:
            if gaintable in kcrosstable:
                spwmap.append(kcrossspwmap)
            else:
                spwmap.append([])

        GainTables = self._modifyGainTables(GainTables)

        task_args = {'vis': self.inputs.vis,
                     'caltable': caltable,
                     'field': field,
                     'intent': intent,
                     'refant': ','.join(self.RefAntOutput),
                     'gaintable': GainTables,
                     'poltype': poltype,
                     'gainfield': gainfield,
                     'minsnr': minsnr,
                     'minblperant': minBL_for_cal,
                     'combine': 'scan',
                     'spwmap': spwmap,
                     'solint': solint}

        task = casa_tasks.polcal(**task_args)

        result = self._executor.execute(task)

        calfrom = callibrary.CalFrom(gaintable=caltable, interp='', calwt=False, caltype='polarization')
        calto = callibrary.CalTo(self.inputs.vis)
        calapp = callibrary.CalApplication(calto, calfrom)
        self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        return result

    def _do_setjy(self):
        """
        The code in this private class method are (for now) specific to VLASS
        requirements and heuristics.

        Returns: string name of the amplitude flux calibrator

        """

        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        # fluxfields = m.get_fields(intent='AMPLITUDE')
        # fluxfieldnames = [amp.name for amp in fluxfields]

        standard_source_names, standard_source_fields = standard_sources(self.inputs.vis)

        fluxcal = ''
        fluxcalfieldid = None
        fluxcalfieldname = ''
        for i, fields in enumerate(standard_source_fields):
            for myfield in fields:
                if standard_source_names[i] in ('3C48', '3C286') \
                        and 'POLANGLE' in m.get_fields(field_id=myfield)[0].intents:
                    fluxcalfieldid = myfield
                    fluxcalfieldname = m.get_fields(field_id=myfield)[0].name
                    fluxcal = standard_source_names[i]
                elif standard_source_names[i] in ('3C48', '3C286') \
                        and 'AMPLITUDE' in m.get_fields(field_id=myfield)[0].intents:
                    fluxcalfieldid = myfield
                    fluxcalfieldname = m.get_fields(field_id=myfield)[0].name
                    fluxcal = standard_source_names[i]


        try:
            task_args = {}
            if fluxcal in ('3C286', '1331+3030', '"1331+305=3C286"', 'J1331+3030'):
                d0 = 33.0 * math.pi / 180.0
                task_args = {'vis': self.inputs.vis,
                             'field': fluxcalfieldname,
                             'standard': 'manual',
                             'spw': '',
                             'fluxdensity': [9.97326, 0, 0, 0],
                             'spix': [-0.582142, -0.154655],
                             'reffreq': '3000.0MHz',
                             'polindex': [0.107943, 0.01184, -0.0055, 0.0224, -0.0312],
                             'polangle': [d0, 0],
                             'rotmeas': 0,
                             'scalebychan': True,
                             'usescratch': True}

            elif fluxcal in ('3C48', 'J0137+3309', '0137+3309', '"0137+331=3C48"'):
                task_args = {'vis': self.inputs.vis,
                             'field': fluxcalfieldname,
                             'spw': '',
                             'selectdata': False,
                             'timerange': '',
                             'scan': '',
                             'intent': '',
                             'observation': '',
                             'scalebychan': True,
                             'standard': 'manual',
                             'model': '',
                             'modimage': '',
                             'listmodels': False,
                             'fluxdensity': [6.4861, -0.132, 0.0417, 0],
                             'spix': [-0.934677, -0.125579],
                             'reffreq': '3000.0MHz',
                             'polindex': [0.02143, 0.0392, 0.002349, -0.0230],
                             'polangle': [-1.7233, 1.569, -2.282, 1.49],
                             'rotmeas': 0,  # inside polangle
                             'fluxdict': {},
                             'useephemdir': False,
                             'interpolation': 'nearest',
                             'usescratch': True}
            else:
                LOG.error("No known flux calibrator found - please check the data.")

            job = casa_tasks.setjy(**task_args)

            self._executor.execute(job)
        except Exception as ex:
            LOG.warn("Exception: Problem with circfeedpolcal setjy. {!s}".format(str(ex)))
            return None

        return fluxcalfieldname, fluxcalfieldid, fluxcal

    def vla_basebands(self, science_windows_only=True):

        vlabasebands = []
        m = self.inputs.context.observing_run.get_ms(self.inputs.vis)

        banddict = collections.defaultdict(lambda: collections.defaultdict(list))

        for spw in m.get_spectral_windows(science_windows_only=science_windows_only):
            try:
                band = spw.name.split('#')[0].split('_')[1]
                baseband = spw.name.split('#')[1]
                banddict[band][baseband].append({str(spw.id): (spw.min_frequency, spw.max_frequency)})
            except Exception as ex:
                LOG.warn("Exception: Baseband name cannot be parsed. {!s}".format(str(ex)))

        for band in banddict:
            for baseband in banddict[band]:
                spws = []
                for spwitem in banddict[band][baseband]:
                    # TODO: review if this relies on order of keys.
                    spws.append(spwitem.keys()[0])
                vlabasebands.append(','.join(spws))

        return vlabasebands

    def do_spwmap(self):
        """
        Returns: spwmap for use with gaintable in callibrary (polcal and applycal)
        """

        vlabasebands = self.vla_basebands(science_windows_only=False)

        spwmap = []

        for spwstr in vlabasebands:
            spwlist = [int(spw) for spw in spwstr.split(',')]
            basebandmap = [spwlist[0]] * len(spwlist)
            spwmap.extend(basebandmap)

        return spwmap

    def _do_clipflag(self, dcaltable):

        clipminmax = self.inputs.clipminmax
        if type(self.inputs.clipminmax) is str:
            clipminmax = ast.literal_eval(self.inputs.clipminmax)

        task_args = {'vis': dcaltable,
                     'mode': 'clip',
                     'datacolumn': 'CPARAM',
                     'clipminmax': clipminmax,
                     'correlation': 'ABS_ALL',
                     'clipoutside': True,
                     'flagbackup': False,
                     'savepars': False,
                     'action': 'apply'}

        job = casa_tasks.flagdata(**task_args)

        return self._executor.execute(job)


