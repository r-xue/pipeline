"""
The exportdata module provides base classes for preparing data products
on disk for upload to the archive. 

To test these classes, register some data with the pipeline using ImportData,
then execute:

import pipeline
vis = [ '<MS name>' ]

# Create a pipeline context and register some data
context = pipeline.Pipeline().context
inputs = pipeline.tasks.ImportData.Inputs(context, vis=vis)
task = pipeline.tasks.ImportData(inputs)
results = task.execute(dry_run=False)
results.accept(context)

# Run some other pipeline tasks, e.g flagging, calibration,
# and imaging in a similar manner

# Execute the export data task. The details of
# what gets exported depends on what tasks were run
# previously but may include the following
# TBD
inputs = pipeline.tasks.exportdata.Exportdata.Inputs(context,
      vis, output_dir, sessions, pprfile, products_dir)
task = pipeline.tasks.exportdata.ExportData(inputs)
  results = task.execute(dry_run = True)
"""
import collections
import copy
import errno
import fnmatch
import glob
import io
import os
import shutil
import sys
import tarfile

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.imagelibrary as imagelibrary
import pipeline.infrastructure.vdp as vdp
from pipeline import environment
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.filenamer import fitsname
from ..common import manifest

# the logger for this module
LOG = infrastructure.get_logger(__name__)


StdFileProducts = collections.namedtuple('StdFileProducts', 'ppr_file weblog_file casa_commands_file casa_pipescript casa_restore_script')


# product name utility 
class PipelineProductNameBuiler(object):
    @classmethod
    def __build(self, *args, **kwargs):
        if 'separator' in kwargs:
            separator = kwargs['separator']
        else:
            separator = '.'
        return separator.join(map(str, args))

    @classmethod
    def _join_dir(self, name, output_dir=None):
        if output_dir is not None:
            name = os.path.join(output_dir, name)
        return name

    @classmethod
    def _build_from_oussid(self, basename, ousstatus_entity_id=None, output_dir=None):
        if ousstatus_entity_id is None:
            name = basename
        else:
            name = self.__build(ousstatus_entity_id, basename)
        return self._join_dir(name, output_dir)

    @classmethod
    def _build_from_ps_oussid(self, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        if project_structure is None:
            name = basename
        elif project_structure.ousstatus_entity_id == 'unknown':
            name = basename
        else:
            name = self._build_from_oussid(basename, ousstatus_entity_id=ousstatus_entity_id)
        return self._join_dir(name, output_dir)

    @classmethod
    def _build_from_oussid_session(self, basename, ousstatus_entity_id=None, session_name=None, output_dir=None):
        name = self.__build(ousstatus_entity_id, session_name, basename)
        return self._join_dir(name, output_dir)

    @classmethod
    def _build_calproduct_name(self, basename, aux_product=False, output_dir=None):
        if aux_product:
            prefix='auxcal'
        else:
            prefix='cal'
        name = self.__build(prefix, basename, separator='')
        return self._join_dir(name, output_dir)

    @classmethod
    def _build_from_vis(self, basename, vis, output_dir=None):
        name = self.__build(os.path.basename(vis), basename)
        return self._join_dir(name, output_dir)

    @classmethod
    def weblog(self, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return self._build_from_ps_oussid('weblog.tgz', 
                                          project_structure=project_structure, 
                                          ousstatus_entity_id=ousstatus_entity_id,
                                          output_dir=output_dir)

    @classmethod
    def casa_script(self, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return self._build_from_ps_oussid(basename, 
                                          project_structure=project_structure, 
                                          ousstatus_entity_id=ousstatus_entity_id,
                                          output_dir=output_dir)

    @classmethod
    def manifest(self, basename, ousstatus_entity_id, output_dir=None):
        return self._build_from_oussid(basename,
                                       ousstatus_entity_id=ousstatus_entity_id,
                                       output_dir=output_dir)

    @classmethod
    def calapply_list(self, vis, aux_product=False, output_dir=None):
        basename = self._build_calproduct_name('apply.txt', aux_product=aux_product)
        return self._build_from_vis(basename, vis, output_dir=output_dir)

    @classmethod
    def caltables(self, ousstatus_entity_id=None, session_name=None, aux_product=False, output_dir=None):
        basename = self._build_calproduct_name('tables.tgz', aux_product=aux_product)
        return self._build_from_oussid_session(basename=basename,
                                               ousstatus_entity_id=ousstatus_entity_id,
                                               session_name=session_name,
                                               output_dir=None)

    @classmethod
    def aqua_report(self, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return self._build_from_ps_oussid(basename, 
                                          project_structure=project_structure, 
                                          ousstatus_entity_id=ousstatus_entity_id,
                                          output_dir=output_dir)

    @classmethod
    def auxiliary_products(self, basename, ousstatus_entity_id=None, output_dir=None):
        return self._build_from_oussid(basename,
                                       ousstatus_entity_id=ousstatus_entity_id,
                                       output_dir=output_dir)


class ExportDataInputs(vdp.StandardInputs):
    """
    ExportDataInputs manages the inputs for the ExportData task.

    .. py:attribute:: context

    the (:class:`~pipeline.infrastructure.launcher.Context`) holding all
    pipeline state

    .. py:attribute:: output_dir

    the directory containing the output of the pipeline

    .. py:attribute:: session

    a string or list of strings containing the sessions(s) associated
    with each vis. Default to a single session containing all vis.
    Vis without a matching session are assigned to the last session
    in the list.

    .. py:attribute:: vis

    a string or list of strings containing the MS name(s) on which to
    operate

    .. py:attribute:: pprfile

    the pipeline processing request. 

    .. py:attribute:: calintents

    the list of calintents defining the calibrator source images to be
    saved.  Defaults to all calibrator intents.

    .. py:attribute:: calimages

    the list of calibrator source images to be saved.  Defaults to all
    calibrator images matching calintents. If defined overrides
    calintents and the calibrator images in the context.

    .. py:attribute:: targetimages

    the list of target source images to be saved.  Defaults to all
    target images. If defined overrides the list of target images in
    the context.

    .. py:attribute:: products_dir

    the directory where the data productions will be written
    """

    calimages = vdp.VisDependentProperty(default=[])
    calintents = vdp.VisDependentProperty(default='')
    exportmses = vdp.VisDependentProperty(default=False)
    pprfile = vdp.VisDependentProperty(default='')
    session = vdp.VisDependentProperty(default=[])
    targetimages = vdp.VisDependentProperty(default=[])
    imaging_products_only = vdp.VisDependentProperty(default=False)

    @vdp.VisDependentProperty
    def products_dir(self):
        if self.context.products_dir is None:
            return os.path.abspath('./')
        else:
            return self.context.products_dir

    @vdp.VisDependentProperty
    def exportcalprods(self):
        return not (self.imaging_products_only or self.exportmses)

    def __init__(self, context, output_dir=None, session=None, vis=None, exportmses=None,
                 pprfile=None, calintents=None, calimages=None, targetimages=None,
                 products_dir=None, imaging_products_only=None):
        """
        Initialise the Inputs, initialising any property values to those given
        here.

        :param context: the pipeline Context state object
        :type context: :class:`~pipeline.infrastructure.launcher.Context`
        :param output_dir: the working directory for pipeline data
        :type output_dir: string
        :param session: the  sessions for which data are to be exported
        :type session: a string or list of strings
        :param vis: the measurement set(s) for which products are to be exported
        :type vis: a string or list of strings
        :param pprfile: the pipeline processing request
        :type pprfile: a string
        :param calimages: the list of calibrator images to be saved
        :type calimages: a list
        :param targetimages: the list of target images to be saved
        :type targetimages: a list
        :param products_dir: the data products directory for pipeline data
        :type products_dir: string
        """
        super(ExportDataInputs, self).__init__()
        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.session = session
        self.exportmses = exportmses
        self.pprfile = pprfile
        self.calintents = calintents
        self.calimages = calimages
        self.targetimages = targetimages
        self.products_dir = products_dir
        self.imaging_products_only = imaging_products_only


class ExportDataResults(basetask.Results):
    def __init__(self, pprequest='', sessiondict=None, msvisdict=None, calvisdict=None, calimages=None, targetimages=None, weblog='',
                 pipescript='', restorescript='', commandslog='', manifest=''):
        """
        Initialise the results object with the given list of JobRequests.
        """
        super(ExportDataResults, self).__init__()

        if sessiondict is None:
            sessiondict = collections.OrderedDict()
        if msvisdict is None:
            msvisdict = collections.OrderedDict()
        if calvisdict is None:
            calvisdict = collections.OrderedDict()
        if calimages is None:
            calimages = []
        if targetimages is None:
            targetimages = []

        self.pprequest = pprequest
        self.sessiondict = sessiondict
        self.msvisdict = msvisdict
        self.calvisdict = msvisdict
        self.calimages = calimages
        self.targetimages = targetimages
        self.weblog = weblog
        self.pipescript = pipescript
        self.restorescript = restorescript
        self.commandslog = commandslog
        self.manifest = manifest

    def __repr__(self):
        s = 'ExportData results:\n'
        return s


@task_registry.set_equivalent_casa_task('h_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class ExportData(basetask.StandardTaskTemplate):
    """
    ExportData is the base class for exporting data to the products
    subdirectory. It performs the following operations:

    - Saves the pipeline processing request in an XML file
    - Saves the final flags per ASDM in a compressed / tarred CASA flag
      versions file
    - Saves the final calibration apply list per ASDM in a text file
    - Saves the final set of caltables per session in a compressed /
      tarred file containing CASA tables
    - Saves the final web log in a compressed / tarred file
    - Saves the final CASA command log in a text file
    - Saves the final pipeline script in a Python file
    - Saves the final pipeline restore script in a Python file
    - Saves the images in FITS cubes one per target and spectral window
    """

    # link the accompanying inputs to this task
    Inputs = ExportDataInputs

    # Override the default behavior for multi-vis tasks
    is_multi_vis_task = True

    # name builder
    NameBuilder = PipelineProductNameBuiler

    def prepare(self):
        """
        Prepare and execute an export data job appropriate to the
        task inputs.
        """
        # Create a local alias for inputs, so we're not saying
        # 'self.inputs' everywhere
        inputs = self.inputs

        try:
            LOG.trace('Creating products directory: %s', inputs.products_dir)
            os.makedirs(inputs.products_dir)
        except OSError as exc:
            if exc.errno != errno.EEXIST:
                raise

        # Initialize the standard ous is string.
        oussid = self.get_oussid(inputs.context)

        # Define the results object
        result = ExportDataResults()

        # Make the standard vislist and the sessions lists. 
        #    These lists are constructed for the calibration mses only no matter the value of
        #    inputs.imaging_products_only
        session_list, session_names, session_vislists, vislist = self._make_lists(inputs.context, inputs.session,
                                                                                  inputs.vis, imaging_only_mses=False)

        # Export the standard per OUS file products
        #    The pipeline processing request
        #    A compressed tarfile of the weblog
        #    The pipeline processing script
        #    The pipeline restore script (if exportporting calibartion products)
        #    The CASA commands log
        recipe_name = self.get_recipename(inputs.context)
        if not recipe_name:
            prefix = oussid
        else:
            prefix = oussid + '.' + recipe_name
        stdfproducts = self._do_standard_ous_products(
            inputs.context, inputs.exportcalprods, prefix, inputs.pprfile, session_list, vislist, inputs.output_dir,
            inputs.products_dir)
        if stdfproducts.ppr_file:
            result.pprequest = os.path.basename(stdfproducts.ppr_file)
        result.weblog = os.path.basename(stdfproducts.weblog_file)
        result.pipescript = os.path.basename(stdfproducts.casa_pipescript)
        if not inputs.exportcalprods:
            result.restorescript = 'Undefined'
        else:
            result.restorescript = os.path.basename(stdfproducts.casa_restore_script)
        result.commandslog = os.path.basename(stdfproducts.casa_commands_file)

        # Make the standard ms dictionary and export per ms products
        #    Currently these are compressed tar files of per MS flagging tables and per MS text files of calibration
        #    apply instructions
        msvisdict = collections.OrderedDict()
        calvisdict = collections.OrderedDict()
        if not inputs.imaging_products_only:
            if inputs.exportmses:
                msvisdict = self._do_ms_products(inputs.context, vislist, inputs.products_dir)
            if inputs.exportcalprods:
                calvisdict = self._do_standard_ms_products(inputs.context, vislist, inputs.products_dir)
        result.msvisdict = msvisdict
        result.calvisdict = calvisdict

        # Make the standard sessions dictionary and export per session products
        #    Currently these are compressed tar files of per session calibration tables
        sessiondict = collections.OrderedDict()
        if not inputs.imaging_products_only:
            if inputs.exportcalprods:
                sessiondict = self._do_standard_session_products(inputs.context, oussid, session_names, session_vislists,
                                                             inputs.products_dir)
            elif inputs.exportmses:
                # still needs sessiondict
                for i in range(len(session_names)):
                    sessiondict[session_names[i]] = \
                    ([os.path.basename(visfile) for visfile in session_vislists[i]], )
        result.sessiondict = sessiondict

        # Export calibrator images to FITS
        calimages_list, calimages_fitslist, calimages_fitskeywords = self._export_images(inputs.context, True, inputs.calintents,
                                                                                         inputs.calimages, inputs.products_dir)
        result.calimages=(calimages_list, calimages_fitslist)

        # Export science target images to FITS
        targetimages_list, targetimages_fitslist, targetimages_fitskeywords = self._export_images(inputs.context, False, 'TARGET',
                                                                                                  inputs.targetimages, inputs.products_dir)
        result.targetimages=(targetimages_list, targetimages_fitslist)

        # Export the pipeline manifest file
        # 
        pipemanifest = self._make_pipe_manifest(inputs.context, oussid, stdfproducts, sessiondict, msvisdict,
                                                inputs.exportmses, calvisdict, inputs.exportcalprods,
                                                [os.path.basename(image) for image in calimages_fitslist], calimages_fitskeywords,
                                                [os.path.basename(image) for image in targetimages_fitslist], targetimages_fitskeywords)
        casa_pipe_manifest = self._export_pipe_manifest(prefix, 'pipeline_manifest.xml', inputs.products_dir,
                                                        pipemanifest)
        result.manifest = os.path.basename(casa_pipe_manifest)

        # Return the results object, which will be used for the weblog
        return result

    def analyse(self, results):
        """
        Analyse the results of the export data operation.

        This method does not perform any analysis, so the results object is
        returned exactly as-is, with no data massaging or results items
        added.

        :rtype: :class:~`ExportDataResults`
        """
        return results

    def get_oussid(self, context):
        """
        Determine the ous prefix
        """

        # Get the parent ous ousstatus name. This is the sanitized ous
        # status uid
        ps = context.project_structure
        if ps is None or ps.ousstatus_entity_id == 'unknown':
            oussid = 'unknown'
        else:
            oussid = ps.ousstatus_entity_id.translate(str.maketrans(':/', '__'))

        return oussid

    def get_recipename(self, context):
        """
        Get the recipe name
        """

        # Get the parent ous ousstatus name. This is the sanitized ous
        # status uid
        ps = context.project_structure
        if ps is None or ps.recipe_name == 'Undefined':
            recipe_name = ''
        else:
            recipe_name = ps.recipe_name

        return recipe_name

    def _make_lists(self, context, session, vis, imaging_only_mses=False):
        """
        Create the vis and sessions lists
        """

        # Force inputs.vis to be a list.
        vislist = vis
        if isinstance(vislist, str):
            vislist = [vislist]
        if imaging_only_mses:
            vislist = [vis for vis in vislist if context.observing_run.get_ms(name=vis).is_imaging_ms]
        else:
            vislist = [vis for vis in vislist if not context.observing_run.get_ms(name=vis).is_imaging_ms]

        # Get the session list and the visibility files associated with
        # each session.
        session_list, session_names, session_vislists= self._get_sessions( \
            context, session, vislist)

        return session_list, session_names, session_vislists, vislist

    def _do_standard_ous_products(self, context, exportcalprods, oussid, pprfile, session_list, vislist, output_dir,
                                  products_dir):
        """
        Generate the per ous standard products
        """

        # Locate and copy the pipeline processing request.
        #     There should normally be at most one pipeline processing request.
        #     In interactive mode there is no PPR.
        ppr_files = self._export_pprfile(context, output_dir, products_dir, oussid, pprfile)
        if ppr_files:
            ppr_file = os.path.basename(ppr_files[0])
        else:
            ppr_file = None

        # Export a tar file of the web log
        weblog_file = self._export_weblog(context, products_dir, oussid)

        # Export the processing log independently of the web log
        casa_commands_file = self._export_casa_commands_log(context, context.logs['casa_commands'], products_dir,
                                                            oussid)

        # Export the processing script independently of the web log
        casa_pipescript = self._export_casa_script(context, context.logs['pipeline_script'], products_dir, oussid)

        # Export the restore script independently of the web log
        if not exportcalprods:
            casa_restore_script = 'Undefined'
        else:
            casa_restore_script = self._export_casa_restore_script(context, context.logs['pipeline_restore_script'],
                                                                   products_dir, oussid, vislist, session_list)

        return StdFileProducts(ppr_file, weblog_file, casa_commands_file, casa_pipescript, casa_restore_script)

    def _do_ms_products(self, context, vislist, products_dir):
        """
        Tar up the final calibrated mses and put them in the products
        directory.
        Used for reprocessing applications
        """

        # Loop over the measurements sets in the working directory and tar
        # them up.
        mslist = []
        for visfile in vislist:
            ms_file = self._export_final_ms( context, visfile, products_dir)
            mslist.append(ms_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are the ms files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                 os.path.basename(mslist[i])

        return visdict

    def _do_standard_ms_products(self, context, vislist, products_dir):
        """
        Generate the per ms standard products
        """

        # Loop over the measurements sets in the working directory and
        # save the final flags using the flag manager.
        flag_version_name = 'Pipeline_Final'
        for visfile in vislist:
            self._save_final_flagversion(visfile, flag_version_name)

        # Copy the final flag versions to the data products directory
        # and tar them up.
        flag_version_list = []
        for visfile in vislist:
            flag_version_file = self._export_final_flagversion(visfile, flag_version_name, products_dir)
            flag_version_list.append(flag_version_file)

        # Loop over the measurements sets in the working directory, and
        # create the calibration apply file(s) in the products directory.
        apply_file_list = []
        for visfile in vislist:
            apply_file =  self._export_final_applylist(context, \
                visfile, products_dir)
            apply_file_list.append(apply_file)

        # Create the ordered vis dictionary
        #    The keys are the base vis names
        #    The values are a tuple containing the flags and applycal files
        visdict = collections.OrderedDict()
        for i in range(len(vislist)):
            visdict[os.path.basename(vislist[i])] = \
                (os.path.basename(flag_version_list[i]), \
                 os.path.basename(apply_file_list[i]))

        return visdict

    def _do_standard_session_products(self, context, oussid, session_names, session_vislists, products_dir,
                                      imaging=False):
        """
        Generate the per ms standard products
        """

        # Export tar files of the calibration tables one per session
        caltable_file_list = []
        for i in range(len(session_names)):
            caltable_file = self._export_final_calfiles(context, oussid,
                session_names[i], session_vislists[i], products_dir, imaging=imaging)
            caltable_file_list.append(caltable_file)

        # Create the ordered session dictionary
        #    The keys are the session names
        #    The values are a tuple containing the vislist and the caltables
        sessiondict = collections.OrderedDict()
        for i in range(len(session_names)):
            sessiondict[session_names[i]] = \
               ([os.path.basename(visfile) for visfile in session_vislists[i]], \
                 os.path.basename(caltable_file_list[i]))

        return sessiondict

    def _do_if_auxiliary_products(self, oussid, output_dir, products_dir, vislist, imaging_products_only):
        """
        Generate the auxiliary products
        """

        if imaging_products_only:
            contfile_name = 'cont.dat'
            fluxfile_name = 'Undefined'
            antposfile_name = 'Undefined'
        else:
            fluxfile_name = 'flux.csv'
            antposfile_name = 'antennapos.csv'
            contfile_name = 'cont.dat'
        empty = True

        # Get the flux, antenna position, and continuum subtraction
        # files and test to see if at least one of them exists
        flux_file = os.path.join(output_dir, fluxfile_name)
        antpos_file = os.path.join(output_dir, antposfile_name)
        cont_file = os.path.join(output_dir, contfile_name)
        if os.path.exists(flux_file) or os.path.exists(antpos_file) or os.path.exists(cont_file):
            empty = False

        # Export the general and target source template flagging files
        #    The general template flagging files are not required for the restore but are
        #    informative to the user.
        #    Whether or not the target template files should be exported to the archive depends
        #    on the final place of the target flagging step in the work flow and
        #    how flags will or will not be stored back into the ASDM.

        targetflags_filelist = []
        if self.inputs.imaging_products_only:
            flags_file_list = glob.glob('*.flagtargetstemplate.txt')
        elif not vislist:
            flags_file_list = glob.glob('*.flagtemplate.txt')
            flags_file_list.extend(glob.glob('*.flagtsystemplate.txt'))
        else:
            flags_file_list = glob.glob('*.flag*template.txt')
        for file_name in flags_file_list:
            flags_file = os.path.join(output_dir, file_name)
            if os.path.exists(flags_file):
                empty = False
                targetflags_filelist.append(flags_file)
            else:
                targetflags_filelist.append('Undefined')

        if empty:
            return None

        # Define the name of the output tarfile
        tarfilename = f'{oussid}.auxproducts.tgz'
        LOG.info('Saving auxiliary data products in %s', tarfilename)

        # Open tarfile
        with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:

            # Save flux file
            if os.path.exists(flux_file):
                tar.add(flux_file, arcname=os.path.basename(flux_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(flux_file), tarfilename)
            else:
                LOG.info('Auxiliary data product flux.csv does not exist')

            # Save antenna positions file
            if os.path.exists(antpos_file):
                tar.add(antpos_file, arcname=os.path.basename(antpos_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(antpos_file), tarfilename)
            else:
                LOG.info('Auxiliary data product antennapos.csv does not exist')

            # Save continuum regions file
            if os.path.exists(cont_file):
                tar.add(cont_file, arcname=os.path.basename(cont_file))
                LOG.info('Saving auxiliary data product %s in %s', os.path.basename(cont_file), tarfilename)
            else:
                LOG.info('Auxiliary data product cont.dat does not exist')

            # Save target flag files
            for flags_file in targetflags_filelist:
                if os.path.exists(flags_file):
                    tar.add(flags_file, arcname=os.path.basename(flags_file))
                    LOG.info('Saving auxiliary data product %s in %s', os.path.basename(flags_file), tarfilename)
                else:
                    LOG.info('Auxiliary data product flagging target templates file does not exist')

            tar.close()

        return tarfilename

    def _make_pipe_manifest(self, context, oussid, stdfproducts, sessiondict, msvisdict, exportmses, calvisdict,
                            exportcalprods, calimages, calimages_fitskeywords, targetimages, targetimages_fitskeywords):
        """
        Generate the manifest file
        """

        # Separate the calibrator images into per ous and per ms images
        # based on the image values of prefix.
        per_ous_calimages = []
        per_ous_calimages_keywords = []
        per_ms_calimages = []
        per_ms_calimages_keywords = []
        for i, image in enumerate(calimages):
            if image.startswith(oussid) or image.startswith('oussid') or image.startswith('unknown'):
                per_ous_calimages.append(image)
                per_ous_calimages_keywords.append(calimages_fitskeywords[i])
            else:
                per_ms_calimages.append(image)
                per_ms_calimages_keywords.append(calimages_fitskeywords[i])

        # Initialize the manifest document and the top level ous status.
        pipemanifest = self._init_pipemanifest(oussid)
        ouss = pipemanifest.set_ous(oussid)
        pipemanifest.add_casa_version(ouss, environment.casa_version_string)
        pipemanifest.add_pipeline_version(ouss, environment.pipeline_revision)
        pipemanifest.add_procedure_name(ouss, context.project_structure.recipe_name)
        pipemanifest.add_environment_info(ouss)

        if stdfproducts.ppr_file:
            pipemanifest.add_pprfile(ouss, os.path.basename(stdfproducts.ppr_file))

        # Add the flagging and calibration products
        for session_name in sessiondict:
            session = pipemanifest.set_session(ouss, session_name)
            if exportcalprods:
                pipemanifest.add_caltables(session, sessiondict[session_name][1])
            for vis_name in sessiondict[session_name][0]:
                immatchlist = [imname for imname in per_ms_calimages if imname.startswith(vis_name)]
                (ms_file, flags_file, calapply_file) = (None, None, None)
                if exportmses:
                    ms_file = msvisdict[vis_name]
                if exportcalprods:
                    (flags_file, calapply_file) = calvisdict[vis_name]
                pipemanifest.add_asdm_imlist(session, vis_name, ms_file, flags_file, calapply_file, immatchlist,
                                             'calibrator')

        # Add a tar file of the web log
        pipemanifest.add_weblog(ouss, os.path.basename(stdfproducts.weblog_file))

        # Add the processing log independently of the web log
        pipemanifest.add_casa_cmdlog(ouss, os.path.basename(stdfproducts.casa_commands_file))

        # Add the processing script independently of the web log
        pipemanifest.add_pipescript(ouss, os.path.basename(stdfproducts.casa_pipescript))

        # Add the restore script independently of the web log
        if stdfproducts.casa_restore_script != 'Undefined':
            pipemanifest.add_restorescript(ouss, os.path.basename(stdfproducts.casa_restore_script))

        # Add the calibrator images
        pipemanifest.add_images(ouss, per_ous_calimages, 'calibrator', per_ous_calimages_keywords)

        # Add the target images
        pipemanifest.add_images(ouss, targetimages, 'target', targetimages_fitskeywords)

        return pipemanifest

    def _init_pipemanifest(self, oussid):
        """
        Initialize the pipeline manifest
        """
        return manifest.PipelineManifest(oussid)

    def _export_pprfile(self, context, output_dir, products_dir, oussid, pprfile):
        # Prepare the search template for the pipeline processing request file.
        #    Was a template in the past
        #    Forced to one file now but keep the template structure for the moment
        if pprfile == '':
            ps = context.project_structure
            if ps is None or ps.ppr_file == '':
                pprtemplate = None
            else:
                pprtemplate = os.path.basename(ps.ppr_file)
        else:
            pprtemplate = os.path.basename(pprfile)

        # Locate the pipeline processing request(s) and  generate a list
        # to be copied to the data products directory. Normally there
        # should be only one match but if there are more copy them all.
        pprmatches = []
        if pprtemplate is not None:
            for file in os.listdir(os.path.abspath(output_dir)): # the file list will be names without path
                if fnmatch.fnmatch(file, pprtemplate):
                    LOG.debug('Located pipeline processing request %s', file)
                    pprmatches.append(os.path.join(output_dir, file))

        # Copy the pipeline processing request files.
        pprmatchesout = []
        for file in pprmatches:
            if oussid:
                outfile = os.path.join(products_dir, oussid + '.pprequest.xml')
            else:
                outfile = file
            pprmatchesout.append(outfile)
            LOG.info('Copying pipeline processing file %s to %s', os.path.basename(file), os.path.basename(outfile))
            if not self._executor._dry_run:
                shutil.copy(file, outfile)

        return pprmatchesout

    def _export_final_ms(self, context, vis, products_dir):
        """
        Save the ms to a compressed tarfile in products.
        """
        # Define the name of the output tarfile
        visname = os.path.basename(vis)
        tarfilename = visname + '.tgz'
        LOG.info('Storing final ms %s in %s', visname, tarfilename)

        # Create the tar file
        if self._executor._dry_run:
            return tarfilename

        tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")
        tar.add(visname)
        tar.close()

        return tarfilename

    def _save_final_flagversion(self, vis, flag_version_name):
        """
        Save the final flags to a final flag version.
        """

        LOG.info('Saving final flags for %s in flag version %s', os.path.basename(vis), flag_version_name)
        if not self._executor._dry_run:
            task = casa_tasks.flagmanager(vis=vis, mode='save', versionname=flag_version_name)
            self._executor.execute(task)

    def _export_final_flagversion(self, vis, flag_version_name, products_dir):
        """
        Save the final flags version to a compressed tarfile in products.
        """
        # Define the name of the output tarfile
        visname = os.path.basename(vis)
        tarfilename = visname + '.flagversions.tgz'
        LOG.info('Storing final flags for %s in %s', visname, tarfilename)

        # Define the directory to be saved, and where to store in tar archive.
        flagsname = os.path.join(vis + '.flagversions', 'flags.' + flag_version_name)
        flagsarcname = os.path.join(visname + '.flagversions', 'flags.' + flag_version_name)
        LOG.info('Saving flag version %s', flag_version_name)

        # Define the versions list file to be saved
        flag_version_list = os.path.join(visname + '.flagversions', 'FLAG_VERSION_LIST')
        ti = tarfile.TarInfo(flag_version_list)
        line = "{} : Final pipeline flags\n".format(flag_version_name).encode(sys.stdout.encoding)
        ti.size = len(line)
        LOG.info('Saving flag version list')

        # Create the tar file
        if not self._executor._dry_run:
            tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")
            tar.add(flagsname, arcname=flagsarcname)
            tar.addfile(ti, io.BytesIO(line))
            tar.close()

        return tarfilename

    def _export_final_applylist(self, context, vis, products_dir, imaging=False):
        """
        Save the final calibration list to a file. For now this is
        a text file. Eventually it will be the CASA callibrary file.
        """
        applyfile_name = self.NameBuilder.calapply_list(vis=vis, aux_product=imaging)
        LOG.info('Storing calibration apply list for %s in  %s', os.path.basename(vis), applyfile_name)

        if self._executor._dry_run:
            return applyfile_name

        try:
            calto = callibrary.CalTo(vis=vis)
            applied_calstate = context.callibrary.applied.trimmed(context, calto)

            # Log the list in human readable form. Better way to do this ?
            nitems = 0
            for calto, calfrom in applied_calstate.merged().items():
                LOG.info('Apply to:  Field: %s  Spw: %s  Antenna: %s',
                         calto.field, calto.spw, calto.antenna)
                nitems = nitems + 1
                for item in calfrom:
                    LOG.info('    Gaintable: %s  Caltype: %s  Gainfield: %s  Spwmap: %s  Interp: %s',
                              os.path.basename(item.gaintable),
                              item.caltype,
                              item.gainfield,
                              item.spwmap,
                              item.interp)

            # Open the file
            if nitems > 0:
                with open(os.path.join(products_dir, applyfile_name), "w") as applyfile:
                    applyfile.write('# Apply file for %s\n' % (os.path.basename(vis)))
                    applyfile.write(applied_calstate.as_applycal())
            else:
                applyfile_name = 'Undefined'
                LOG.info('No calibrations for MS %s', os.path.basename(vis))
        except:
            applyfile_name = 'Undefined'
            LOG.info('No calibrations for MS %s', os.path.basename(vis))

        return applyfile_name

    def _get_sessions(self, context, sessions, vis):
        """
        Return a list of sessions where each element of the list contains
        the vis files associated with that session. In future this routine
        will be driven by the context but for now use the user defined sessions
        """

        # If the input session list is empty put all the visibility files
        # in the same session.
        if len(sessions) == 0:
            wksessions = []
            for visname in vis:
                session = context.observing_run.get_ms(name=visname).session
                wksessions.append(session)
        else:
            wksessions = sessions

        # Determine the number of unique sessions.
        session_seqno = 0; session_dict = {}
        for i in range(len(wksessions)):
            if wksessions[i] not in session_dict:
                session_dict[wksessions[i]] = session_seqno
                session_seqno = session_seqno + 1

        # Initialize the output session names and visibility file lists
        session_names = []
        session_vis_list = []
        for key, value in sorted(session_dict.items(), key=lambda k_v: (k_v[1], k_v[0])):
            session_names.append(key)
            session_vis_list.append([])

        # Assign the visibility files to the correct session
        for j in range(len(vis)):
            # Match the session names if possible
            if j < len(wksessions):
                for i in range(len(session_names)):
                    if wksessions[j] == session_names[i]:
                        session_vis_list[i].append(vis[j])
            # Assign to the last session
            else:
                session_vis_list[len(session_names)-1].append(vis[j])

        # Log the sessions
        for i in range(len(session_vis_list)):
            LOG.info('Visibility list for session %s is %s', session_names[i], session_vis_list[i])

        return wksessions, session_names, session_vis_list

    def _export_final_calfiles(self, context, oussid, session, vislist, products_dir, imaging=False):
        """
        Save the final calibration tables in a tarfile one file
        per session.
        """
        # Define the name of the output tarfile
        tarfilename = self.NameBuilder.caltables(ousstatus_entity_id=oussid,
                                                 session_name=session,
                                                 aux_product=imaging)
        LOG.info('Saving final caltables for %s in %s', session, tarfilename)

        # Create the tar file
        if self._executor._dry_run:
            return tarfilename

        caltables = set()

        for visfile in vislist:
            LOG.info('Collecting final caltables for %s in %s', os.path.basename(visfile), tarfilename)

            # Create the list of applied caltables for that vis
            try:
                calto = callibrary.CalTo(vis=visfile)
                calstate = context.callibrary.applied.trimmed(context, calto)
                caltables.update(calstate.get_caltable())
            except:
                LOG.info('No caltables for MS %s', os.path.basename(visfile))

        if not caltables:
            return 'Undefined'

        with tarfile.open(os.path.join(products_dir, tarfilename), 'w:gz') as tar:
            # Tar the session list.
            for table in caltables:
                tar.add(table, arcname=os.path.basename(table))

        return tarfilename

    def _export_weblog(self, context, products_dir, oussid):
        """
        Save the processing web log to a tarfile
        """
        # Define the name of the output tarfile
        ps = context.project_structure
        tarfilename = self.NameBuilder.weblog(project_structure=ps,
                                              ousstatus_entity_id=oussid)

        LOG.info('Saving final weblog in %s', tarfilename)

        # Create the tar file
        if not self._executor._dry_run:
            tar = tarfile.open(os.path.join(products_dir, tarfilename), "w:gz")
            tar.add(os.path.join(os.path.basename(os.path.dirname(context.report_dir)), 'html'))
            tar.close()

        return tarfilename

    def _export_casa_commands_log(self, context, casalog_name, products_dir, oussid):
        """
        Save the CASA commands file.
        """
        casalog_file = os.path.join(context.report_dir, casalog_name)

        ps = context.project_structure
        out_casalog_file = self.NameBuilder.casa_script(casalog_name, 
                                                        project_structure=ps, 
                                                        ousstatus_entity_id=oussid,
                                                        output_dir=products_dir)

        LOG.info('Copying casa commands log %s to %s', casalog_file, out_casalog_file)
        if not self._executor._dry_run:
            shutil.copy(casalog_file, out_casalog_file)

        return os.path.basename(out_casalog_file)

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        """
        Save the CASA restore scropt.
        """
        script_file = os.path.join(context.report_dir, script_name)

        # Get the output file name
        ps = context.project_structure
        out_script_file = self.NameBuilder.casa_script(script_name, 
                                                       project_structure=ps, 
                                                       ousstatus_entity_id=oussid,
                                                       output_dir=products_dir)

        LOG.info('Creating casa restore script %s', script_file)

        # This is hardcoded.
        tmpvislist = []

        #ALMA default
        ocorr_mode = 'ca'

        for vis in vislist:
            filename = os.path.basename(vis)
            if filename.endswith('.ms'):
                filename, filext = os.path.splitext(filename)
            tmpvislist.append(filename)
        task_string = "    hif_restoredata(vis=%s, session=%s, ocorr_mode='%s')" % (tmpvislist, session_list, ocorr_mode)

        template = '''h_init()
try:
%s
finally:
    h_save()
''' % task_string

        with open(script_file, 'w') as casa_restore_file:
            casa_restore_file.write(template)

        LOG.info('Copying casa restore script %s to %s', script_file, out_script_file)
        if not self._executor._dry_run:
            shutil.copy(script_file, out_script_file)

        return os.path.basename(out_script_file)

    def _export_casa_script(self, context, casascript_name, products_dir, oussid):
        """
        Save the CASA script.
        """

        ps = context.project_structure
        casascript_file = os.path.join(context.report_dir, casascript_name)
        out_casascript_file = self.NameBuilder.casa_script(casascript_name, 
                                                           project_structure=ps, 
                                                           ousstatus_entity_id=oussid,
                                                           output_dir=products_dir)

        LOG.info('Copying casa script file %s to %s', casascript_file, out_casascript_file)
        if not self._executor._dry_run:
            shutil.copy(casascript_file, out_casascript_file)

        return os.path.basename(out_casascript_file)

    def _export_pipe_manifest(self, oussid, manifest_name, products_dir, pipemanifest):
        """
        Save the manifest file.
        """
        out_manifest_file = self.NameBuilder.manifest(manifest_name, 
                                                      ousstatus_entity_id=oussid,
                                                      output_dir=products_dir)
        LOG.info('Creating manifest file %s', out_manifest_file)
        if not self._executor._dry_run:
            pipemanifest.write(out_manifest_file)

        return out_manifest_file

    def _export_images(self, context, calimages, calintents, images, products_dir):
        """
        Export the images to FITS files.
        """

        try:
            import astropy.io.fits as apfits
        except ImportError as e:
            LOG.debug('Import error: {!s}'.format(e))
            raise Exception(
                "Astropy is not installed, which is required to run h*_exportdata when images/cubes exist.")

        # Create the image list
        images_list = []
        if len(images) == 0:
            # Get the image library
            if calimages:
                LOG.info('Exporting calibrator source images')
                if calintents == '':
                    intents = ['PHASE', 'BANDPASS', 'CHECK', 'AMPLITUDE']
                else:
                    intents = calintents.split(',')
                cleanlist = context.calimlist.get_imlist()
            else:
                LOG.info('Exporting target source images')
                intents = ['TARGET']
                cleanlist = context.sciimlist.get_imlist()

            for image_number, image in enumerate(cleanlist):
                # We need to store the image
                cleanlist[image_number]['fitsfiles'] = []
                cleanlist[image_number]['auxfitsfiles'] = []
                version = image.get('version', 1)
                # Image name probably includes path
                if image['sourcetype'] in intents:
                    if image['multiterm']:
                        for nt in range(image['multiterm']):
                            imagename = image['imagename'].replace('.image', '.image.tt%d' % (nt))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        if (image['imagename'].find('.pbcor') != -1):
                            imagename = image['imagename'].replace('.image.pbcor', '.alpha')
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                            imagename = '%s.error' % (image['imagename'].replace('.image.pbcor', '.alpha'))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            imagename = image['imagename'].replace('.image', '.alpha')
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                            imagename = '%s.error' % (image['imagename'].replace('.image', '.alpha'))
                            images_list.append((imagename, version))
                            cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                    elif (image['imagename'].find('image.sd') != -1): # single dish
                        imagename = image['imagename']
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                        imagename = image['imagename'].replace('image.sd', 'image.sd.weight')
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))
                    else:
                        imagename = image['imagename']
                        images_list.append((imagename, version))
                        cleanlist[image_number]['fitsfiles'].append(fitsname(products_dir, imagename, version))

                    # Add PBs for interferometry
                    if (image['imagename'].find('.image') != -1) and (image['imagename'].find('.image.sd') == -1):
                        if (image['imagename'].find('.pbcor') != -1):
                            if (image['multiterm']):
                                imagename = image['imagename'].replace('.image.pbcor', '.pb.tt0')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                            else:
                                imagename = image['imagename'].replace('.image.pbcor', '.pb')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            if (image['multiterm']):
                                imagename = image['imagename'].replace('.image', '.pb.tt0')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                            else:
                                imagename = image['imagename'].replace('.image', '.pb')
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))

                    # Add auto-boxing masks for interferometry
                    if (image['imagename'].find('.image') != -1) and (image['imagename'].find('.image.sd') == -1):
                        if (image['imagename'].find('.pbcor') != -1):
                            imagename = image['imagename'].replace('.image.pbcor', '.mask')
                            imagename2 = image['imagename'].replace('.image.pbcor', '.cleanmask')
                            if os.path.exists(imagename) and not os.path.exists(imagename2):
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
                        else:
                            imagename = image['imagename'].replace('.image', '.mask')
                            imagename2 = image['imagename'].replace('.image', '.cleanmask')
                            if os.path.exists(imagename) and not os.path.exists(imagename2):
                                images_list.append((imagename, version))
                                cleanlist[image_number]['auxfitsfiles'].append(fitsname(products_dir, imagename, version))
        else:
            # Assume only the root image name was given.
            cleanlib = imagelibrary.ImageLibrary()
            for image in images:
                if calimages:
                    imageitem = imagelibrary.ImageItem(imagename=image,
                                                       sourcename='UNKNOWN',
                                                       spwlist='UNKNOWN',
                                                       sourcetype='CALIBRATOR')
                else:
                    imageitem = imagelibrary.ImageItem(imagename=image,
                                                       sourcename='UNKNOWN',
                                                       spwlist='UNKNOWN',
                                                       sourcetype='TARGET')
                cleanlib.add_item(imageitem)
                if os.path.basename(image) == '':
                    images_list.append((os.path.join(context.output_dir, image), 1))
                else:
                    images_list.append((image, 1))
            cleanlist = cleanlib.get_imlist()
            # Need to add the FITS names
            for i in range(len(cleanlist)):
                cleanlist[i]['fitsfiles'] = [fitsname(products_dir, images_list[i][0])]
                cleanlist[i]['auxfitsfiles'] = []

        # Convert to FITS.
        fits_list = []
        fits_keywords_list = []
        for image_ver in images_list:
            image, version = image_ver
            fitsfile = fitsname(products_dir, image, version)
            # skip if image doesn't exist
            if not os.path.exists(image):
                LOG.info('Skipping unexisting image %s', os.path.basename(image))
                continue
            LOG.info('Saving final image %s to FITS file %s', os.path.basename(image), os.path.basename(fitsfile))

            # PIPE-325: abbreviate 'spw' for FITS header when spw string is "too long"
            with casa_tools.ImageReader(image) as img:
                info = img.miscinfo()
                if ('spw' in info) and (len(info['spw']) >= 68):
                    spw_sorted = sorted([int(x) for x in info['spw'].split(',')])
                    info['spw'] = '{},...,{}'.format(spw_sorted[0], spw_sorted[-1])
                    img.setmiscinfo(info)

            if not self._executor._dry_run:
                task = casa_tasks.exportfits(imagename=image, fitsimage=fitsfile, velocity=False, optical=False,
                                             bitpix=-32, minpix=0, maxpix=-1, overwrite=True, dropstokes=False,
                                             stokeslast=True)
                self._executor.execute(task)
                fits_list.append(fitsfile)
                # Fetch header keywords for manifest
                try:
                    ff = apfits.open(fitsfile)
                    fits_keywords = dict()
                    for key in ['object', 'obsra', 'obsdec', 'intent', 'specmode',
                                'naxis1', 'ctype1', 'cunit1', 'crpix1', 'crval1', 'cdelt1',
                                'naxis2', 'ctype2', 'cunit2', 'crpix2', 'crval2', 'cdelt2',
                                'naxis3', 'ctype3', 'cunit3', 'crpix3', 'crval3', 'cdelt3',
                                'naxis4', 'ctype4', 'cunit4', 'crpix4', 'crval4', 'cdelt4',
                                'bmaj', 'bmin', 'bpa', 'robust', 'weight']:
                        try:
                            fits_keywords[key] = '{}'.format(str(ff[0].header[key]))
                        except:
                            # Some images do not have beam, robust or weight keywords
                            fits_keywords[key] = 'N/A'
                    if 'spw' in ff[0].header:
                        fits_keywords['virtspw'] = '{}'.format(str(ff[0].header['spw']))
                    if 'nspwnam' in ff[0].header:
                        nspwnam = ff[0].header['nspwnam']
                        fits_keywords['nspwnam'] = '{}'.format(str(nspwnam))
                        for i in range(1, nspwnam+1):
                            key = 'spwnam{:02d}'.format(i)
                            try:
                                fits_keywords[key] = '{}'.format(str(ff[0].header[key]))
                            except:
                                fits_keywords[key] = 'N/A'
                    ff.close()
                except Exception as e:
                    LOG.info('Fetching FITS keywords for {} failed: {}'.format(fitsfile, e))
                    fits_keywords = {}
                fits_keywords_list.append(fits_keywords)

        new_cleanlist = copy.deepcopy(cleanlist)

        return new_cleanlist, fits_list, fits_keywords_list

    @staticmethod
    def _add_to_manifest(manifest_file, aux_fproducts, aux_caltablesdict, aux_calapplysdict, aqua_report):

        pipemanifest = manifest.PipelineManifest('')
        pipemanifest.import_xml(manifest_file)
        ouss = pipemanifest.get_ous()

        if aqua_report:
            pipemanifest.add_aqua_report(ouss, os.path.basename(aqua_report))

        if aux_fproducts:
            # Add auxiliary data products file
            pipemanifest.add_aux_products_file(ouss, os.path.basename(aux_fproducts))

        # Add the auxiliary caltables
        if aux_caltablesdict:
            for session_name in aux_caltablesdict:
                session = pipemanifest.get_session(ouss, session_name)
                if session is None:
                    session = pipemanifest.set_session(ouss, session_name)
                pipemanifest.add_auxcaltables(session, aux_caltablesdict[session_name][1])
                for vis_name in aux_caltablesdict[session_name][0]:
                    pipemanifest.add_auxasdm(session, vis_name, aux_calapplysdict[vis_name])

        pipemanifest.write(manifest_file)

