"""
Created on Dec 4, 2017

@author: kana
"""
from __future__ import absolute_import

import os

import pipeline.h.tasks.importdata.importdata as importdata
import pipeline.hsd.tasks.importdata.importdata as sd_importdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class NROImportDataInputs(importdata.ImportDataInputs):
    def __init__(self, context=None, vis=None, output_dir=None, session=None,
                 overwrite=None, nocopy=None, createmms=None):
        #no-op parameters for MS
        asis=''
        process_caldevice=False
        bdfflags=False
        lazy=False
        ocorr_mode='ao'
        save_flagonline=False

        super(NROImportDataInputs, self).__init__(context, vis=vis, output_dir=output_dir, asis=asis,
                                                  process_caldevice=process_caldevice, session=session,
                                                  overwrite=overwrite, nocopy=nocopy, save_flagonline=save_flagonline,
                                                  bdfflags=bdfflags, lazy=lazy, createmms=createmms,
                                                  ocorr_mode=ocorr_mode)


class NROImportDataResults(sd_importdata.SDImportDataResults):
    """
    NROImportDataResults is an equivalent class with ImportDataResults. 
    Purpose of NROImportDataResults is to replace QA scoring associated 
    with ImportDataResults with single dish specific QA scoring, which 
    is associated with this class.

    ImportDataResults holds the results of the ImportData task. It contains
    the resulting MeasurementSet domain objects and optionally the additional 
    SetJy results generated from flux entries in Source.xml.
    """

    def __init__(self, mses=None, reduction_group_list=None, datatable_prefix=None, setjy_results=None):
        super(NROImportDataResults, self).__init__(mses=mses, reduction_group_list=reduction_group_list,
                                                   datatable_prefix=datatable_prefix, setjy_results=setjy_results)

    def merge_with_context(self, context):
        super(NROImportDataResults, self).merge_with_context(context)
        # Set observatory information
        for ms in self.mses:
            if ms.antenna_array.name in ('NRO',):
                context.project_summary.telescope = 'NRO'
                context.project_summary.observatory = 'Nobeyama Radio Observatory'
                break


@task_registry.set_equivalent_casa_task('hsdn_importdata')
@task_registry.set_casa_commands_comment('Import Nobeyama MeasurementSets.')
class NROImportData(sd_importdata.SDImportData):
    Inputs = NROImportDataInputs 

    def prepare(self, **parameters):
        # the input data should be MSes
        # TODO: check data type
        # get results object by running super.prepare()
        results = super(NROImportData, self).prepare()
        myresults = NROImportDataResults(mses=results.mses, reduction_group_list=results.reduction_group_list,
                                         datatable_prefix=results.datatable_prefix, setjy_results=results.setjy_results)
        myresults.origin = results.origin
        return myresults

    def analyse(self, result):
        # get version information for merge2
        for ms in result.mses:
            with casatools.TableReader(os.path.join(ms.name, 'OBSERVATION')) as tb:
                col = 'RELEASE_DATE'
                release_dates = tb.getcol(col)
                unit = tb.getcolkeywords(col)['QuantumUnits'][0]
            if len(release_dates) > 0:
                release_date = release_dates[0]
                qa = casatools.quanta
                merge2_version = qa.time(qa.quantity(release_date, unit), form='ymd')
                if isinstance(merge2_version, list):
                    merge2_version = merge2_version[0]
            else:
                merge2_version = 'N/A'
            ms.merge2_version = merge2_version
        return result
