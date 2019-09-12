import xml.etree.cElementTree as eltree

import pipeline.h.tasks.common.manifest as manifest


class NROPipelineManifest(manifest.PipelineManifest):
    @staticmethod
    def add_reduction_script(ous, script):
        """
        Add the template reduction script for restoredata workflow
        """
        eltree.SubElement(ous, "reduction_script", name=script)
