import pipeline.infrastructure as infrastructure
#import pipeline.infrastructure.api as api
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.project as project
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics import checkproductsize
from pipeline.infrastructure import task_registry
from .resultobjects import CheckProductSizeResult

LOG = infrastructure.get_logger(__name__)


class CheckProductSizeInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    parallel = vdp.VisDependentProperty(default='automatic')

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxcubelimit(self):
        return project.PerformanceParameters().max_cube_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxcubesize(self):
        return project.PerformanceParameters().max_cube_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maxproductsize(self):
        return project.PerformanceParameters().max_product_size

    @vdp.VisDependentProperty(null_input=[None, '', -1, -1.0])
    def maximsize(self):
        return -1

    def __init__(self, context, output_dir=None, vis=None, maxcubesize=None, maxcubelimit=None, maxproductsize=None,
                 calcsb=None, parallel=None, maximsize=None):
        super(CheckProductSizeInputs, self).__init__()

        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.maxcubesize = maxcubesize
        self.maxcubelimit = maxcubelimit
        self.maxproductsize = maxproductsize
        self.maximsize = maximsize
        self.calcsb = calcsb
        self.parallel = parallel


# tell the infrastructure to give us mstransformed data when possible by
# registering our preference for imaging measurement sets
#api.ImagingMeasurementSetsPreferred.register(CheckProductSizeInputs)


@task_registry.set_equivalent_casa_task('hif_checkproductsize')
class CheckProductSize(basetask.StandardTaskTemplate):
    Inputs = CheckProductSizeInputs

    is_multi_vis_task = True

    def prepare(self):
        # Check parameter settings
        if (self.inputs.maxcubesize == -1) and \
           (self.inputs.maxcubelimit == -1) and \
           (self.inputs.maxproductsize == -1) and (self.inputs.maximsize == -1):
            LOG.info('No size limits given.')
            result = CheckProductSizeResult(self.inputs.maxcubesize, \
                                            self.inputs.maxcubelimit, \
                                            self.inputs.maxproductsize, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            self.inputs.maximsize, \
                                            -1, \
                                            -1, \
                                            {}, \
                                            'OK', \
                                            {'longmsg': 'No size limits given', 'shortmsg': 'No size limits'}, \
                                            None)
            # Log summary information
            LOG.info(str(result))
            return result

        # Mitigate either product byte size or image pixel count.
        if ((self.inputs.maxcubesize != -1) or (self.inputs.maxcubelimit != -1) or (self.inputs.maxproductsize != -1)) \
                and (self.inputs.maximsize != -1):
            result = CheckProductSizeResult(self.inputs.maxcubesize, \
                                            self.inputs.maxcubelimit, \
                                            self.inputs.maxproductsize, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            self.inputs.maximsize, \
                                            -1, \
                                            -1, \
                                            {}, \
                                            'ERROR', \
                                            {'longmsg': 'Parameter error: cannot mitigate product byte size and image pixel count at the same time.',\
                                             'shortmsg': 'Parameter error'},\
                                            None)
            # Log summary information
            LOG.info(str(result))
            return result

        if (self.inputs.maxcubesize != -1) and \
           (self.inputs.maxcubelimit != -1) and \
           (self.inputs.maxcubesize > self.inputs.maxcubelimit):
            result = CheckProductSizeResult(self.inputs.maxcubesize, \
                                            self.inputs.maxcubelimit, \
                                            self.inputs.maxproductsize, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            self.inputs.maximsize, \
                                            -1, \
                                            -1, \
                                            {}, \
                                            'ERROR', \
                                            {'longmsg': 'Parameter error: maxcubelimit must be >= maxcubesize', 'shortmsg': 'Parameter error'}, \
                                            None)
            # Log summary information
            LOG.info(str(result))
            return result

        if (self.inputs.maxcubesize != -1) and \
           (self.inputs.maxproductsize != -1) and \
           (self.inputs.maxcubesize >= self.inputs.maxproductsize):
            result = CheckProductSizeResult(self.inputs.maxcubesize, \
                                            self.inputs.maxcubelimit, \
                                            self.inputs.maxproductsize, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            self.inputs.maximsize, \
                                            -1, \
                                            -1, \
                                            {}, \
                                            'ERROR', \
                                            {'longmsg': 'Parameter error: maxproductsize must be > maxcubesize', 'shortmsg': 'Parameter error'}, \
                                            None)
            # Log summary information
            LOG.info(str(result))
            return result

        if (self.inputs.maxcubelimit != -1) and \
           (self.inputs.maxproductsize != -1) and \
           (self.inputs.maxcubelimit >= self.inputs.maxproductsize):
            result = CheckProductSizeResult(self.inputs.maxcubesize, \
                                            self.inputs.maxcubelimit, \
                                            self.inputs.maxproductsize, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            -1, \
                                            self.inputs.maximsize, \
                                            -1, \
                                            {}, \
                                            'ERROR', \
                                            {'longmsg': 'Parameter error: maxproductsize must be > maxcubelimit', 'shortmsg': 'Parameter error'}, \
                                            None)
            # Log summary information
            LOG.info(str(result))
            return result

        checkproductsize_heuristics = checkproductsize.CheckProductSizeHeuristics(self.inputs)

        # Clear any previous size mitigation parameters
        self.inputs.context.size_mitigation_parameters = {}

        # Mitigate image pixel count (currently used for VLA, see PIPE-676)
        if self.inputs.maximsize != -1:
            size_mitigation_parameters, \
            original_maxcubesize, original_productsize, \
            cube_mitigated_productsize, \
            maxcubesize, productsize, \
            original_imsize, mitigated_imsize, \
            error, reason, \
            known_synthesized_beams = \
                checkproductsize_heuristics.mitigate_imsize()
        # Mitigate data product byte size (currently used for ALMA)
        else:
            size_mitigation_parameters, \
            original_maxcubesize, original_productsize, \
            cube_mitigated_productsize, \
            maxcubesize, productsize, \
            original_imsize, mitigated_imsize, \
            error, reason, \
            known_synthesized_beams = \
                checkproductsize_heuristics.mitigate_sizes()

        if error:
            status = 'ERROR'
        elif size_mitigation_parameters != {}:
            status = 'MITIGATED'
        else:
            status = 'OK'

        size_mitigation_parameters['status'] = status

        result = CheckProductSizeResult(self.inputs.maxcubesize,
                                        self.inputs.maxcubelimit,
                                        self.inputs.maxproductsize,
                                        original_maxcubesize,
                                        original_productsize,
                                        cube_mitigated_productsize,
                                        maxcubesize,
                                        productsize,
                                        self.inputs.maximsize, \
                                        original_imsize, \
                                        mitigated_imsize, \
                                        size_mitigation_parameters,
                                        status,
                                        reason,
                                        known_synthesized_beams)

        # Log summary information
        LOG.info(str(result))

        return result

    def analyse(self, result):
        return result
