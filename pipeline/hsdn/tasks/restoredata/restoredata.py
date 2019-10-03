import os

import pipeline.h.tasks.restoredata.restoredata as restoredata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from pipeline.hsd.tasks.applycal import applycal
from pipeline.hsd.tasks.k2jycal import k2jycal
from ..importdata import importdata as importdata

LOG = infrastructure.get_logger(__name__)


class NRORestoreDataInputs(restoredata.RestoreDataInputs):
    scalefile = vdp.VisDependentProperty(default='nroscalefile.csv')

    def __init__(self, context, infiles=None, caltable=None, scalefile=None, 
                 copytoraw=None, products_dir=None, rawdata_dir=None, output_dir=None, 
                 vis=None):
        super(NRORestoreDataInputs, self).__init__(context, products_dir=None, rawdata_dir=rawdata_dir,
                                                   output_dir=output_dir, vis=vis)


class NRORestoreDataResults(restoredata.RestoreDataResults):
    def __init__(self, importdata_results=None, applycal_results=None):
        """
        Initialise the results objects.
        """
        super(NRORestoreDataResults, self).__init__(importdata_results, applycal_results)

    def merge_with_context(self, context):
        super(NRORestoreDataResults, self).merge_with_context(context)

        # set k2jy factor to ms domain objects
        if isinstance(self.applycal_results, basetask.ResultsList):
            for result in self.applycal_results:
                self._merge_k2jycal(context, result)
        else:
            self._merge_k2jycal(context, self.applycal_results)

    def _merge_k2jycal(self, context, applycal_results):
        for calapp in applycal_results.applied:
            msobj = context.observing_run.get_ms(name=os.path.basename(calapp.vis))
            if not hasattr(msobj, 'k2jy_factor'):
                for _calfrom in calapp.calfrom:
                    if _calfrom.caltype == 'amp' or _calfrom.caltype == 'gaincal':
                        LOG.debug('Adding k2jy factor to {0}'.format(msobj.basename))
                        # k2jy gaincal table
                        k2jytable = _calfrom.gaintable
                        k2jy_factor = {}
                        with casatools.TableReader(k2jytable) as tb:
                            spws = tb.getcol('SPECTRAL_WINDOW_ID')
                            antennas = tb.getcol('ANTENNA1')
                            params = tb.getcol('CPARAM').real
                            nrow = tb.nrows()
                        for irow in range(nrow):
                            spwid = spws[irow]
                            antenna = antennas[irow]
                            param = params[:, 0, irow]
                            npol = param.shape[0]
                            antname = msobj.get_antenna(antenna)[0].name
                            dd = msobj.get_data_description(spw=int(spwid))
                            if dd is None:
                                continue
                            for ipol in range(npol):
                                polname = dd.get_polarization_label(ipol)
                                k2jy_factor[(spwid, antname, polname)] = 1.0 / (param[ipol] * param[ipol])
                        msobj.k2jy_factor = k2jy_factor
            LOG.debug('msobj.k2jy_factor = {0}'.format(getattr(msobj, 'k2jy_factor', 'N/A')))


@task_registry.set_equivalent_casa_task('hsdn_restoredata')
class NRORestoreData(restoredata.RestoreData):
    Inputs = NRORestoreDataInputs

    def prepare(self):
        # run prepare method in the parent class
        results = super(NRORestoreData, self).prepare()

        # apply baseline table and produce baseline-subtracted MSs

        # apply final flags for baseline-subtracted MSs

        sdresults = NRORestoreDataResults(results.importdata_results, 
                                          results.applycal_results)

        return sdresults

    def _do_importasdm(self, sessionlist, vislist):
        inputs = self.inputs
        # NROImportDataInputs operate in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.
        LOG.debug('inputs = {0}'.format(inputs))
        container = vdp.InputsContainer(importdata.NROImportData, inputs.context, vis=vislist, 
                                        output_dir=None, overwrite=False, nocopy=False, 
                                        createmms=None)
        importdata_task = importdata.NROImportData(container)
        return self._executor.execute(importdata_task, merge=True)

    def _do_applycal(self):
        inputs = self.inputs

        # Sensitively correction using scalefile and k2kycal. This is unique operation
        # only for Nobeyama mesurement set data. 
        LOG.debug('inputs = {0}'.format(inputs))
        container = vdp.InputsContainer(k2jycal.SDK2JyCal, inputs.context, reffile=inputs.scalefile)
        k2jycal_task = k2jycal.SDK2JyCal(container)
        LOG.debug('k2jycal container = {0}'.format(container))
        self._executor.execute(k2jycal_task, merge=True)

        # SDApplyCalInputs operates in the scope of a single measurement set.
        # To operate in the scope of multiple MSes we must use an
        # InputsContainer.
        container = vdp.InputsContainer(applycal.SDApplycal, inputs.context)
        applycal_task = applycal.SDApplycal(container)
        LOG.debug('_do_applycal container = {0}'.format(container))
        return self._executor.execute(applycal_task, merge=True)
