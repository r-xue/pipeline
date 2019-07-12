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

import collections
import glob
import itertools
import os
import shutil
import string
import tarfile

import pipeline.h.tasks.exportdata.exportdata as exportdata
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.h.tasks.exportdata.aqua import export_to_disk as aqua_export_to_disk
from pipeline.infrastructure import task_registry
from . import almasdaqua

# the logger for this module
LOG = infrastructure.get_logger(__name__)


# Inputs class must be separated per task class even if it's effectively the same
class SDExportDataInputs(exportdata.ExportDataInputs):
    pass


@task_registry.set_equivalent_casa_task('hsd_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class SDExportData(exportdata.ExportData):
    """
    SDExportData is the base class for exporting data to the products
    subdirectory. It performs the following operations:

    - Saves the pipeline processing request in an XML file
    - Saves the images in FITS cubes one per target and spectral window
    - Saves the final flags and bl coefficient per ASDM in a compressed / tarred CASA flag
      versions file
    - Saves the final web log in a compressed / tarred file
    - Saves the text formatted list of contents of products directory
    """
    Inputs = SDExportDataInputs

    # This is almost equivalent to ALMAExportData.prepare()
    # only difference is to use self._make_lists instead of ExportData._make_lists
    def prepare(self):

        results = super(SDExportData, self).prepare()

        oussid = self.get_oussid(self.inputs.context)

        # Make the imaging vislist and the sessions lists.
        session_list, session_names, session_vislists, vislist = self._make_lists(
            self.inputs.context, self.inputs.session, self.inputs.vis, imaging_only_mses=True)

        LOG.info('vislist={}'.format(vislist))

        if vislist:
            # Export the auxiliary caltables if any
            #    These are currently the uvcontinuum fit tables.
            auxcaltables = self._do_aux_session_products(self.inputs.context, oussid, session_names, session_vislists,
                                                         self.inputs.products_dir)

            # Export the auxiliary cal apply files if any
            #    These are currently the uvcontinuum fit tables.
            auxcalapplys = self._do_aux_ms_products(self.inputs.context, vislist, self.inputs.products_dir)
        else:
            auxcaltables = None
            auxcalapplys = None

        # Export the auxiliary file products into a single tar file
        #    These are optional for reprocessing but informative to the user
        #    The calibrator source fluxes file
        #    The antenna positions file
        #    The continuum regions file
        #    The target flagging file
        recipe_name = self.get_recipename(self.inputs.context)
        if not recipe_name:
            prefix = oussid
        else:
            prefix = oussid + '.' + recipe_name
        auxfproducts = self._do_auxiliary_products(self.inputs.context, oussid, self.inputs.output_dir,
                                                   self.inputs.products_dir)

        # Export the AQUA report
        aquareport_name = 'pipeline_aquareport.xml'
        pipe_aqua_reportfile = self._export_aqua_report(self.inputs.context, prefix, aquareport_name,
                                                        self.inputs.products_dir)

        # Update the manifest
        if auxfproducts is not None or pipe_aqua_reportfile is not None:
            manifest_file = os.path.join(self.inputs.context.products_dir, results.manifest)
            self._add_to_manifest(manifest_file, auxfproducts, auxcaltables, auxcalapplys, pipe_aqua_reportfile)

        return results

    def _make_lists(self, context, session, vis, imaging_only_mses=False):
        """
        Create the vis and sessions lists
        """
        LOG.info('Single dish specific _make_lists')
        # Force inputs.vis to be a list.
        vislist = vis
        if isinstance(vislist, str):
            vislist = [vislist, ]
        # in single dish pipeline, all mses are non-imaging ones but they need to be
        # returned even when imaging is False so no filtering is done

        # Get the session list and the visibility files associated with
        # each session.
        session_list, session_names, session_vislists = self._get_sessions(context, session, vislist)

        return session_list, session_names, session_vislists, vislist

    def _do_aux_session_products(self, context, oussid, session_names, session_vislists, products_dir):

        # Make the standard sessions dictionary and export per session products
        #    Currently these are compressed tar files of per session calibration tables
        # Export tar files of the calibration tables one per session
        LOG.info('_do_aux_session_products')
        caltable_file_list = []
        for i in range(len(session_names)):
            caltable_file = self._export_final_baseline_calfiles(
                context, oussid, session_names[i], session_vislists[i], products_dir)
            caltable_file_list.append(caltable_file)

        # Create the ordered session dictionary
        #    The keys are the session names
        #    The values are a tuple containing the vislist and the caltables
        sessiondict = collections.OrderedDict()
        for i in range(len(session_names)):
            sessiondict[session_names[i]] = ([os.path.basename(visfile) for visfile in session_vislists[i]],
                                             os.path.basename(caltable_file_list[i]))

        return sessiondict

    def __get_last_baseline_table(self, vis):
        basename = os.path.basename(vis.rstrip('/'))
        bl_tables = glob.glob('{}.*hsd_baseline*.bl.tbl'.format(basename))
        if len(bl_tables) > 0:
            bl_tables.sort()
            name = bl_tables[-1]
            LOG.debug('bl cal table for {} is {}'.format(vis, name))
            return name
        else:
            return None


    def _export_final_baseline_calfiles(self, context, oussid, session, vislist, products_dir):
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
            tarfilename = self.NameBuilder.caltables(ousstatus_entity_id=oussid,
                                                     session_name=session,
                                                     aux_product=True)
            #tarfilename = '{}.{}.auxcaltables.tgz'.format(oussid, session)
            #tarfilename = '{}.{}.caltables.tgz'.format(oussid, session)
            LOG.info('Saving final caltables for %s in %s', session, tarfilename)

            # Create the tar file
            if self._executor._dry_run:
                return tarfilename

#             caltables = set()

            bl_caltables = set()

            for visfile in vislist:
                LOG.info('Collecting final caltables for %s in %s',
                         os.path.basename(visfile), tarfilename)

                # Create the list of baseline caltable for that vis
                name = self.__get_last_baseline_table(visfile)
                if name is not None:
                    bl_caltables.add(name)

                with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:
                    # Tar the session list.
                    # for table in caltables:
                    #     tar.add(table, arcname=os.path.basename(table))

                    for table in bl_caltables:
                        tar.add(table, arcname=os.path.basename(table))

            return tarfilename

        finally:
            # Restore the original current working directory
            os.chdir(cwd)

    def _do_aux_ms_products(self, context, vislist, products_dir):

        # Loop over the measurements sets in the working directory, and
        # create the calibration apply file(s) in the products directory.
        apply_file_list = []
        for visfile in vislist:
            apply_file = self._export_final_baseline_applylist(context, visfile, products_dir)
            apply_file_list.append(apply_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are a tuple containing the flags and applycal files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                os.path.basename(apply_file_list[i])

        return visdict

    def _export_final_baseline_applylist(self, context, vis, products_dir):
        """
        Save the final calibration list to a file. For now this is
        a text file. Eventually it will be the CASA callibrary file.
        """

        applyfile_name = self.NameBuilder.calapply_list(os.path.basename(vis),
                                                        aux_product=True)
        #applyfile_name = os.path.basename(vis) + '.auxcalapply.txt'
        LOG.info('Storing calibration apply list for %s in  %s',
                 os.path.basename(vis), applyfile_name)

        if self._executor._dry_run:
            return applyfile_name

        try:
            # Log the list in human readable form. Better way to do this ?
            cmd = string.Template("sdbaseline(infile='${infile}', datacolumn='corrected', spw='${spw}', blmode='apply',"
                                  " bltable='${bltable}', blfunc='poly', outfile='${outfile}', overwrite=True)")

            # Create the list of baseline caltable for that vis
            name = self.__get_last_baseline_table(vis)
            ms = context.observing_run.get_ms(vis)
            science_spws = ms.get_spectral_windows(science_windows_only=True)
            spw = ','.join(itertools.imap(lambda s: str(s.id), science_spws))
            if name is not None:
                applied_calstate = cmd.safe_substitute(infile=vis,
                                                       bltable=name,
                                                       spw=spw,
                                                       outfile=vis.rstrip('/') + '_bl')

                # Open the file.
                with open(os.path.join(products_dir, applyfile_name), "w") as applyfile:
                    applyfile.write('# Apply file for %s\n' % (os.path.basename(vis)))
                    applyfile.write(applied_calstate)
        except:
            applyfile_name = 'Undefined'
            LOG.info('No calibrations for MS %s' % os.path.basename(vis))

        return applyfile_name

    def _detect_jyperk(self, context):
        reffile_list = set(self.__get_reffile(context.results))

        if len(reffile_list) == 0:
            # if no reffile is found, return None
            LOG.debug('No K2Jy factor file found.')
            return None
        if len(reffile_list) > 1:
            raise RuntimeError("K2Jy conversion file must be only one. %s found." % (len(reffile_list)))

        jyperk = reffile_list.pop()

        if not os.path.exists(jyperk):
            # if reffile doesn't exist, return None
            LOG.debug('K2Jy file \'%s\' not found' % jyperk)
            return None

        LOG.info('Exporting {0} as a product'.format(jyperk))
        return os.path.abspath(jyperk)

    @staticmethod
    def __get_reffile(results):
        for proxy in results:
            result = proxy.read()
            if not isinstance(result, basetask.ResultsList):
                result = [result]
            for r in result:
                if str(r).find('SDK2JyCalResults') != -1 and hasattr(r, 'reffile'):
                    reffile = r.reffile
                    if reffile is not None and os.path.exists(reffile):
                        yield reffile

    def _do_auxiliary_products(self, context, oussid, output_dir, products_dir):
        # Track whether any auxiliary products exist to be exported.
        aux_prod_exists = False

        # Get the jyperk file, check whether it exists.
        jyperk = self._detect_jyperk(context)
        if jyperk and os.path.exists(jyperk):
            aux_prod_exists = True

        # Export the general and target source template flagging files
        #    The general template flagging files are not required for the restore but are
        #    informative to the user.
        #    Whether or not the target template files should be exported to the archive depends
        #    on the final place of the target flagging step in the work flow and
        #    how flags will or will not be stored back into the ASDM.
        flags_file_list = [os.path.join(output_dir, fname) for fname in glob.glob('*.flag*template.txt')]
        if flags_file_list:
            aux_prod_exists = True

        # If no auxiliary product was found, skip creation of tarfile and return early.
        if not aux_prod_exists:
            return None

        # Create the tarfile.
        cwd = os.getcwd()
        try:
            os.chdir(output_dir)

            # Define the name of the output tarfile.
            tarfilename = self.NameBuilder.auxiliary_products('auxproducts.tgz',
                                                              ousstatus_entity_id=oussid)
            #tarfilename = '{}.auxproducts.tgz'.format(oussid)
            LOG.info('Saving auxiliary data products in %s', tarfilename)

            # Open tarfile.
            with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:

                # Save flux file.
                if jyperk and os.path.exists(jyperk):
                    tar.add(jyperk, arcname=os.path.basename(jyperk))
                    LOG.info('Saving auxiliary data product {} in {}'.format(os.path.basename(jyperk), tarfilename))
                elif isinstance(jyperk, str):
                    LOG.info('Auxiliary data product {} does not exist'.format(os.path.basename(jyperk)))

                # Save flag files.
                for flags_file in flags_file_list:
                    if os.path.exists(flags_file):
                        tar.add(flags_file, arcname=os.path.basename(flags_file))
                        LOG.info('Saving auxiliary data product {} in {}'.format(os.path.basename(flags_file),
                                                                                 tarfilename))
                    else:
                        LOG.info('Auxiliary data product {} does not exist'.format(os.path.basename(flags_file)))

                tar.close()
        finally:
            # Restore the original current working directory.
            os.chdir(cwd)

        return tarfilename

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        """
        Save the CASA restore scropt.
        """

        # Generate the file list

        # Get the output file name
        ps = context.project_structure
        script_file = os.path.join(context.report_dir, script_name)
        out_script_file = self.NameBuilder.casa_script(script_name,
                                                       project_structure=ps,
                                                       ousstatus_entity_id=oussid,
                                                       output_dir=products_dir)
        # if ps is None or ps.ousstatus_entity_id == 'unknown':
        #     script_file = os.path.join(context.report_dir, script_name)
        #     out_script_file = os.path.join(products_dir, script_name)
        # else:
        #     script_file = os.path.join(context.report_dir, script_name)
        #     out_script_file = os.path.join(products_dir, oussid + '.' + script_name)

        LOG.info('Creating casa restore script %s' % script_file)

        # This is hardcoded.
        tmpvislist = []

        # ALMA TP default
        ocorr_mode = 'ao'

        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, filext = os.path.splitext(filename)
            tmpvislist.append(filename)
        task_string = "    hsd_restoredata(vis=%s, session=%s, ocorr_mode='%s')" % (tmpvislist, session_list,
                                                                                    ocorr_mode)

        template = '''__rethrow_casa_exceptions = True
h_init()
try:
%s
finally:
    h_save()
''' % task_string

        with open(script_file, 'w') as casa_restore_file:
            casa_restore_file.write(template)

        LOG.info('Copying casa restore script %s to %s' % (script_file, out_script_file))
        if not self._executor._dry_run:
            shutil.copy(script_file, out_script_file)

        return os.path.basename(out_script_file)

    def _export_aqua_report(self, context, oussid, aquareport_name, products_dir):
        """
        Save the AQUA report.
        Note the method is mostly a duplicate of the conterpart
             in hifa/tasks/exportdata/almaexportdata
        """
        aqua_file = os.path.join(context.output_dir, aquareport_name)

        report_generator = almasdaqua.AlmaAquaXmlGenerator()
        LOG.info('Generating pipeline AQUA report')
        try:
            report_xml = report_generator.get_report_xml(context)
            aqua_export_to_disk(report_xml, aqua_file)
        except Exception, e:
            LOG.error('Error generating the pipeline AQUA report')
            LOG.info('{}'.format(e))
            return 'Undefined'

        ps = context.project_structure
        out_aqua_file = self.NameBuilder.aqua_report(aquareport_name,
                                                     project_structure=ps,
                                                     ousstatus_entity_id=oussid,
                                                     output_dir=products_dir)
        # if ps is None or ps.ousstatus_entity_id == 'unknown':
        #     out_aqua_file = os.path.join(products_dir, aquareport_name)
        # else:
        #     out_aqua_file = os.path.join(products_dir, oussid + '.' + aquareport_name)

        LOG.info('Copying AQUA report %s to %s' % (aqua_file, out_aqua_file))
        shutil.copy(aqua_file, out_aqua_file)
        return os.path.basename(out_aqua_file)
