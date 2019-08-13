from __future__ import absolute_import

import os
import pipeline.h.tasks.importdata.fluxes as fluxes
import pipeline.h.tasks.importdata.importdata as importdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
import urllib2
import ssl
from pipeline.infrastructure import task_registry
from . import dbfluxes

__all__ = [
    'ALMAImportData',
    'ALMAImportDataInputs'
]

LOG = infrastructure.get_logger(__name__)

try:
    FLUX_SERVICE_URL = os.environ['FLUX_SERVICE_URL']
except Exception as e:
    FLUX_SERVICE_URL = ''
    # FLUX_SERVICE_URL = 'https://2019jul.asa-test.alma.cl/sc/flux'

try:
    FLUX_SERVICE_URL_BACKUP = os.environ['FLUX_SERVICE_URL_BACKUP']
    # 'https://2019jul.asa-test.alma.cl/sc/flux'
except Exception as e:
    FLUX_SERVICE_URL_BACKUP = ''


class ALMAImportDataInputs(importdata.ImportDataInputs):
    asis = vdp.VisDependentProperty(default='Antenna CalAtmosphere CalPointing CalWVR ExecBlock Receiver SBSummary Source Station')
    dbservice = vdp.VisDependentProperty(default=False)
    createmms = vdp.VisDependentProperty(default='false')

    def __init__(self, context, vis=None, output_dir=None, asis=None, process_caldevice=None, session=None,
                 overwrite=None, nocopy=None, bdfflags=None, lazy=None, save_flagonline=None, dbservice=None,
                 createmms=None, ocorr_mode=None, asimaging=None):
        super(ALMAImportDataInputs, self).__init__(context, vis=vis, output_dir=output_dir, asis=asis,
                                                   process_caldevice=process_caldevice, session=session,
                                                   overwrite=overwrite, nocopy=nocopy, bdfflags=bdfflags, lazy=lazy,
                                                   save_flagonline=save_flagonline, createmms=createmms,
                                                   ocorr_mode=ocorr_mode, asimaging=asimaging)
        self.dbservice = dbservice


@task_registry.set_equivalent_casa_task('hifa_importdata')
@task_registry.set_casa_commands_comment('If required, ASDMs are converted to MeasurementSets.')
class ALMAImportData(importdata.ImportData):
    Inputs = ALMAImportDataInputs

    def _get_fluxes(self, context, observing_run):
        # get the flux measurements from Source.xml for each MS

        if self.inputs.dbservice:
            testquery = '?DATE=27-March-2013&FREQUENCY=86837309056.169219970703125&WEIGHTED=true&RESULT=0&NAME=J1427-4206'
            # Test for service response
            baseurl = FLUX_SERVICE_URL
            url = baseurl + testquery
            if baseurl == '':
                url = ''

            try:
                # ignore HTTPS certificate
                ssl_context = ssl._create_unverified_context()
                response = urllib2.urlopen(url, context=ssl_context, timeout=60.0)
                xml_results = dbfluxes.get_setjy_results(observing_run.measurement_sets)
                fluxservice = 'FIRSTURL'
            except Exception as e:
                try:
                    LOG.warn('Unable to execute initial test query with primary flux service.')
                    # ignore HTTPS certificate
                    ssl_context = ssl._create_unverified_context()
                    baseurl = FLUX_SERVICE_URL_BACKUP
                    url = baseurl + testquery
                    if baseurl == '':
                        url = ''
                    response = urllib2.urlopen(url, context=ssl_context, timeout=60.0)
                    xml_results = dbfluxes.get_setjy_results(observing_run.measurement_sets)
                    fluxservice='BACKUPURL'
                except Exception as e2:
                    if url == '':
                        msg = 'Backup URL not defined for test query...'
                    else:
                        msg = 'Unable to execute backup test query with flux service.'
                    LOG.warn(msg+'\nProceeding without using the online flux catalog service.')
                    xml_results = fluxes.get_setjy_results(observing_run.measurement_sets)
                    fluxservice = 'FAIL'
        else:
            xml_results = fluxes.get_setjy_results(observing_run.measurement_sets)
            fluxservice = None
        # write/append them to flux.csv

        # Cycle 1 hack for exporting the field intents to the CSV file:
        # export_flux_from_result queries the context, so we pseudo-register
        # the mses with the context by replacing the original observing run
        orig_observing_run = context.observing_run
        context.observing_run = observing_run
        try:
            fluxes.export_flux_from_result(xml_results, context)
        finally:
            context.observing_run = orig_observing_run

        # re-read from flux.csv, which will include any user-coded values
        combined_results = fluxes.import_flux(context.output_dir, observing_run)

        return fluxservice, combined_results
