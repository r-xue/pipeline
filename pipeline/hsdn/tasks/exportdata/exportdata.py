"""
Single-Dish Exportdata task dedicated to NRO data.

Please see hsd/tasks/exportdata/exportdata.py for generic
description on how Exportdata task works.
"""
from __future__ import absolute_import

import pipeline.h.tasks.exportdata.exportdata as exportdata
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import task_registry
import pipeline.hsd.tasks.exportdata.exportdata as sdexportdata
from . import manifest
from . import nrotemplategenerator

import collections
import os

# the logger for this module
LOG = infrastructure.get_logger(__name__)


class NROPipelineNameBuilder(exportdata.PipelineProductNameBuiler):
    @classmethod
    def _build_from_oussid(self, basename, ousstatus_entity_id=None, output_dir=None):
        return self._join_dir(basename, output_dir)

    @classmethod
    def _build_from_ps_oussid(self, basename, project_structure=None, ousstatus_entity_id=None, output_dir=None):
        return self._join_dir(basename, output_dir)

    @classmethod
    def _build_from_oussid_session(self, basename, ousstatus_entity_id=None, session_name=None, output_dir=None):
        return self._join_dir(basename, output_dir)


# Inputs class must be separated per task class even if it's effectively the same
class NROExportDataInputs(sdexportdata.SDExportDataInputs):
    pass


@task_registry.set_equivalent_casa_task('hsdn_exportdata')
@task_registry.set_casa_commands_comment('The output data products are computed.')
class NROExportData(sdexportdata.SDExportData):
    """
    NROExportData is the base class for exporting data to the products
    subdirectory. It performs the following operations:

    - Saves the pipeline processing request in an XML file
    - Saves the images in FITS cubes one per target and spectral window
    - Saves the final flags and bl coefficient per ASDM in a compressed / tarred CASA flag
      versions file
    - Saves the final web log in a compressed / tarred file
    - Saves the text formatted list of contents of products directory
    """
    Inputs = NROExportDataInputs

    NameBuilder = NROPipelineNameBuilder

    def prepare(self):
        results = super(NROExportData, self).prepare()

        # manifest file
        manifest_file = os.path.join(self.inputs.context.products_dir, results.manifest)

        # export NRO data reduction template
        template_script = self._export_reduction_template(self.inputs.products_dir)

        if template_script is not None:
            self._update_manifest(manifest_file, script=template_script)

        # export NRO scaling file template
        template_file = self._export_nroscalefile_template(self.inputs.products_dir)

        if template_file is not None:
            self._update_manifest(manifest_file, scalefile=template_file)

        return results

    def _export_reduction_template(self, products_dir):
        script_name = 'rebase_and_image.py'
        config_name = 'rebase_and_image_config.py'
        script_path = os.path.join(products_dir, script_name)
        config_path = os.path.join(products_dir, config_name)

        status = nrotemplategenerator.generate_script(self.inputs.context, script_path, config_path)
        return script_name if status is True else None

    def _export_nroscalefile_template(self, products_dir):
        datafile_name = 'nroscalefile.csv'
        datafile_path = os.path.join(products_dir, datafile_name)

        status = nrotemplategenerator.generate_csv(self.inputs.context, datafile_path)
        return datafile_name if status is True else None

    def _update_manifest(self, manifest_file, script=None, scalefile=None):
        pipemanifest = manifest.NROPipelineManifest('')
        pipemanifest.import_xml(manifest_file)
        ouss = pipemanifest.get_ous()

        if script:
            pipemanifest.add_reduction_script(ouss, script)
            pipemanifest.write(manifest_file)

        if scalefile:
            pipemanifest.add_scalefile(ouss, scalefile)
            pipemanifest.write(manifest_file)

    def _export_casa_restore_script(self, context, script_name, products_dir, oussid, vislist, session_list):
        tmpvislist = list(map(os.path.basename, vislist))
        restore_task_name = 'hsdn_restoredata'
        args = collections.OrderedDict(vis=tmpvislist, reffile='./nroscalefile.csv')
        return self._export_casa_restore_script_template(context, script_name, products_dir, oussid,
                                                         restore_task_name, args)
