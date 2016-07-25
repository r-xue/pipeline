"""
The exportdata for SD module provides base classes for preparing data products
on disk for upload to the archive. 

To test these classes, register some data with the pipeline using ImportData,
then execute:

import pipeline
# Create a pipeline context and register some data
context = pipeline.Pipeline().context
output_dir = "."
products_dir = "./products"
inputs = pipeline.tasks.singledish.SDExportData.Inputs(context,output_dir,products_dir)
task = pipeline.tasks.singledish.SDExportData (inputs)
results = task.execute (dry_run = True)
results = task.execute (dry_run = False)

"""
from __future__ import absolute_import
import os
import re
import copy
import errno
import StringIO
import tarfile
import fnmatch
import shutil
import string
import collections
import pipeline.infrastructure.basetask as basetask
from pipeline.infrastructure import casa_tasks
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.sdfilenamer as filenamer
import pipeline.hif.tasks.exportdata.exportdata as hif_exportdata
from . import manifest

# the logger for this module
LOG = infrastructure.get_logger(__name__)


# Inputs class must be separated per task class even if it's effectively the same
class SDMSExportDataInputs(hif_exportdata.ExportDataInputs):
    pass


class SDMSExportData(hif_exportdata.ExportData):
    """
    SDMSExportData is the base class for exporting data to the products
    subdirectory. It performs the following operations:
    
    - Saves the pipeline processing request in an XML file
    - Saves the images in FITS cubes one per target and spectral window
    - Saves the final flags and bl coefficient per ASDM in a compressed / tarred CASA flag
      versions file
    - Saves the final web log in a compressed / tarred file
    - Saves the text formatted list of contents of products directory
    """
    Inputs = SDMSExportDataInputs
    
    def prepare(self):
        results = super(SDMSExportData, self).prepare()
        manifest = os.path.join(self.inputs.context.products_dir, results.manifest)
        LOG.debug('manifest file is \'%s\''%(manifest))
        
        jyperk = self._export_jyperk(self.inputs.context, self.inputs.context.products_dir)
        if jyperk is not None:
            self._add_jyperk_to_manifest(manifest, jyperk)
        
        return results
     
    def _export_final_calfiles(self, context, oussid, session, vislist, products_dir):
        """
        Save the final calibration tables in a tarfile one file
        per session.
        
        This method is an exact copy of same method in superclass except 
        for handling baseline caltables.
        """

        # Save the current working directory and move to the pipeline
        # working directory. This is required for tarfile IO
        cwd = os.getcwd()
        try:
            os.chdir(context.output_dir)

            # Define the name of the output tarfile
            tarfilename = '{}.{}.caltables.tgz'.format(oussid, session)
            LOG.info('Saving final caltables for %s in %s', session, tarfilename)

            # Create the tar file
            if self._executor._dry_run:
                return tarfilename

            caltables = set()
            
            bl_caltables = set()

            for visfile in vislist:
                LOG.info('Collecting final caltables for %s in %s',
                         os.path.basename(visfile), tarfilename)

                # Create the list of applied caltables for that vis
                try:
                    calto = callibrary.CalTo(vis=visfile)
                    calstate = context.callibrary.applied.trimmed(context, calto)
                    caltables.update(calstate.get_caltable())
                except:
                    LOG.info('No caltables for MS %s' % os.path.basename(visfile))
                    
                # Create the list of baseline caltable for that vis
                namer = filenamer.CalibrationTable()
                namer.asdm(os.path.basename(visfile))
                namer.bl_cal()
                name = namer.get_filename()
                LOG.debug('bl cal table for %s is %s'%(visfile, name))
                if os.path.exists(name):
                    bl_caltables.add(name)

            if not caltables:
                return 'Undefined'

            with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:
                # Tar the session list.
                for table in caltables:
                    tar.add(table)
                    
                for table in bl_caltables:
                    tar.add(table)

            return tarfilename

        finally:
            # Restore the original current working directory
            os.chdir(cwd)

    def _export_jyperk(self, context, products_dir):
        reffile_list = set(self.__get_reffile(context.results))
        
        if len(reffile_list) == 0:
            # if no reffile is found, return None
            LOG.debug('No K2Jy factor file found.')
            return None
        if len(reffile_list) > 1:
            raise RuntimeError("K2Jy conversion file must be only one. %s found."%(len(reffile_list)))
        
        jyperk = reffile_list.pop()
        
        if not os.path.exists(jyperk):
            # if reffile doesn't exist, return None
            LOG.debug('K2Jy file \'%s\' not found'%(jyperk))
            return None
        
        shutil.copy(jyperk, products_dir)
        return jyperk
    
    def __get_reffile(self, results):
        for proxy in results:
            result = proxy.read()
            if not isinstance(result, basetask.ResultsList):
                result = [result]
            for r in result:
                if str(r).find('SDK2JyCalResults') != -1:
                    if hasattr(r, 'reffile'):
                        reffile = r.reffile
                        if reffile is not None and os.path.exists(reffile):
                            yield reffile
                            
    def _add_jyperk_to_manifest(self, manifest_file, jyperk):
        pipemanifest = manifest.SingleDishPipelineManifest('')
        pipemanifest.import_xml(manifest_file)
        ous = pipemanifest.get_ous()
        pipemanifest.add_jyperk(ous, os.path.basename(jyperk))
        pipemanifest.write(manifest_file)
