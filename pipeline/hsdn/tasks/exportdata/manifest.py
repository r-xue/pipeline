"""The NRO pipeline data product manifest class."""

import xml.etree.cElementTree as eltree

import pipeline.h.tasks.common.manifest as manifest


class NROPipelineManifest(manifest.PipelineManifest):
    """Class for creating the NRO pipeline data product manifest."""

    @staticmethod
    def add_reduction_script(ous, script):
        """Add the template reduction script for restoredata workflow."""
        eltree.SubElement(ous, "reduction_script", name=script)

    @staticmethod
    def add_scalefile(ous, filename):
        """Add the template scale file for restoredata workflow."""
        eltree.SubElement(ous, "scale_file", name=filename)
