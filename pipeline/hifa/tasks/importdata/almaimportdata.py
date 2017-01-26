from __future__ import absolute_import

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask

import pipeline.h.tasks.importdata.importdata as importdata

LOG = infrastructure.get_logger(__name__)

class ALMAImportDataInputs(importdata.ImportDataInputs):

    @basetask.log_equivalent_CASA_call
    def __init__(self, context, vis=None, output_dir=None, asis=None,
                 process_caldevice=None, session=None, overwrite=None,
                 bdfflags=None, lazy=None, save_flagonline=None, dbservice=None,
                 createmms=None, ocorr_mode=None, clearcals=None):
        self._init_properties(vars())

    asis = basetask.property_with_default('asis', 'SBSummary ExecBlock Antenna Station Receiver Source CalAtmosphere CalWVR')
    dbservice = basetask.property_with_default('dbservice', True)


class ALMAImportData(importdata.ImportData):
    Inputs = ALMAImportDataInputs


