from __future__ import absolute_import
import os
import re
import contextlib
import tarfile
import string
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.basetask as basetask
import pipeline.domain as domain
from pipeline.infrastructure import tablereader
from ... import heuristics
from .. import common
from ..common import utils

import pipeline.hif.tasks.importdata.importdata as importdata
import pipeline.hifa.tasks.importdata.almaimportdata as almaimportdata

LOG = infrastructure.get_logger(__name__)

class SDImportDataInputs(almaimportdata.ALMAImportDataInputs):
    @basetask.log_equivalent_CASA_call
    def __init__(self, context=None, vis=None, output_dir=None,
                 asis=None, process_caldevice=None, session=None, overwrite=None, 
                 bdfflags=None, save_flagonline=None, lazy=None, dbservice=None,
                 with_pointing_correction=None, createmms=None):
        self._init_properties(vars())

    asis = basetask.property_with_default('asis', 'Antenna Station Receiver CalAtmosphere CalWVR')
    with_pointing_correction = basetask.property_with_default('with_pointing_correction', True)

class SDImportDataResults(basetask.Results):
    '''
    SDImportDataResults is an equivalent class with ImportDataResults. 
    Purpose of SDImportDataResults is to replace QA scoring associated 
    with ImportDataResults with single dish specific QA scoring, which 
    is associated with this class.
    
    ImportDataResults holds the results of the ImportData task. It contains
    the resulting MeasurementSet domain objects and optionally the additional 
    SetJy results generated from flux entries in Source.xml.
    '''
    
    def __init__(self, mses=None, setjy_results=None):
        super(SDImportDataResults, self).__init__()
        self.mses = [] if mses is None else mses
        self.setjy_results = setjy_results
        self.origin = {}
        self.results = importdata.ImportDataResults(mses=mses, setjy_results=setjy_results)
        
    def merge_with_context(self, context):
        if not isinstance(context.observing_run, domain.ScantableList):
            context.observing_run = domain.ScantableList()
        self.results.merge_with_context(context)
           
    def __repr__(self):
        return 'SDImportDataResults:\n\t{0}'.format(
                '\n\t'.join([ms.name for ms in self.mses]))

class SDImportData(importdata.ImportData):
    Inputs = SDImportDataInputs 
    
    def prepare(self, **parameters):
        results = super(SDImportData, self).prepare()
        myresults = SDImportDataResults(mses=results.mses, setjy_results=results.setjy_results)
        myresults.origin = results.origin
        return myresults
