import os

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.casa_tasks as casa_tasks
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import fieldnames
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.utils import relative_path
from .. import common
from ..k2jycal import k2jycal

LOG = logging.get_logger(__name__)


class SDATMCorrectionInputs(vdp.StandardInputs):
    atmtype = vdp.VisDependentProperty(default=1)
    dtem_dh = vdp.VisDependentProperty(default=-5.6)
    h0 = vdp.VisDependentProperty(default=2.0)
    intent = vdp.VisDependentProperty(default='TARGET')

    @atmtype.convert
    def atmtype(self, value):
        if isinstance(value, str):
            value = int(value)
        return value

    @vdp.VisDependentProperty
    def infiles(self):
        return self.vis

    @infiles.convert
    def infiles(self, value):
        self.vis = value
        return value

    @vdp.VisDependentProperty
    def antenna(self):
        return ''

    @antenna.convert
    def antenna(self, value):
        antennas = self.ms.get_antenna(value)
        # if all antennas are selected, return ''
        if len(antennas) == len(self.ms.antennas):
            return ''
        return utils.find_ranges([a.id for a in antennas])

    @vdp.VisDependentProperty
    def field(self):
        # this will give something like '0542+3243,0343+242'
        field_finder = fieldnames.IntentFieldnames()
        intent_fields = field_finder.calculate(self.ms, self.intent)

        # run the answer through a set, just in case there are duplicates
        fields = set()
        fields.update(utils.safe_split(intent_fields))

        return ','.join(fields)

    @vdp.VisDependentProperty
    def spw(self):
        science_spws = self.ms.get_spectral_windows(with_channels=True)
        return ','.join([str(spw.id) for spw in science_spws])

    @vdp.VisDependentProperty
    def pol(self):
        # filters polarization by self.spw
        selected_spwids = [int(spwobj.id) for spwobj in self.ms.get_spectral_windows(self.spw, with_channels=True)]
        pols = set()
        for idx in selected_spwids:
            pols.update(self.ms.get_data_description(spw=idx).corr_axis)

        return ','.join(pols)

    def __init__(self, context, atmtype=None, dtem_dh=None, h0=None,
                 infiles=None, antenna=None, field=None, spw=None, pol=None):
        super().__init__()

        self.context = context
        self.atmtype = atmtype
        self.dtem_dh = dtem_dh
        self.h0 = h0
        self.infiles = infiles
        self.antenna = antenna
        self.field = field
        self.spw = spw
        self.pol = pol

    def _identify_datacolumn(self, vis):
        datacolumn = ''
        with casa_tools.TableReader(vis) as tb:
            colnames = tb.colnames()

        names = (('CORRECTED_DATA', 'corrected'),
                 ('FLOAT_DATA', 'float_data'),
                 ('DATA', 'data'))
        for name, value in names:
            if name in colnames:
                datacolumn = value
                break

        if len(datacolumn) == 0:
            raise Exception('No datacolumn is found.')

        return datacolumn

    def get_k2jycal_result(self):
        results = self.context.results
        result = None
        for r in map(lambda x: x.read(), results):
            if isinstance(r, k2jycal.SDK2JyCalResults):
                result = r
                break
            elif isinstance(r, basetask.ResultsList) and isinstance(r[0], k2jycal.SDK2JyCalResults):
                result = r[0]
                break
        return result

    def get_gainfactor(self):
        result = self.get_k2jycal_result()
        gainfactor = 1.0
        if result is not None:
            final = result.final
            if len(final) > 0:
                gainfactor = final[0].gaintable
        return gainfactor

    def to_casa_args(self):
        args = super().to_casa_args()

        # infile
        args.pop('infiles', None)
        infile = args.pop('vis')
        args['infile'] = infile

        # datacolumn
        args['datacolumn'] = self._identify_datacolumn(infile)

        # outfile
        if 'outfile' not in args:
            basename = os.path.basename(infile.rstrip('/'))
            suffix = '.atmcor.atmtype{}'.format(args['atmtype'])
            outfile = basename + suffix
            args['outfile'] = relative_path(os.path.join(self.output_dir, outfile))

        # ganfactor
        args['gainfactor'] = self.get_gainfactor()

        # overwrite is always True
        args['overwrite'] = True

        # spw -> outputspw
        args['outputspw'] = args.pop('spw', '')

        # pol -> correlation
        args['correlation'] = args.pop('pol', '')

        return args


class SDATMCorrectionResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super().__init__(task, success, outcome)
        # outcome is the name of output file from sdatmcor
        self.atmcor_ms_name = outcome

    def merge_with_context(self, context):
        super().merge_with_context(context)

        # TODO: register MS after sdatmcor to the context

    def _outcome_name(self):
        return os.path.basename(self.atmcor_ms_name)


@task_registry.set_equivalent_casa_task('hsd_atmcor')
@task_registry.set_casa_commands_comment(
    'Apply offline correction of atmospheric transmission model.'
)
class SerialSDATMCorrection(basetask.StandardTaskTemplate):
    Inputs = SDATMCorrectionInputs

    def prepare(self):
        args = self.inputs.to_casa_args()
        LOG.info('Processing parameter for sdatmcor: %s', args)
        job = casa_tasks.sdatmcor(**args)
        self._executor.execute(job)

        if not os.path.exists(args['outfile']):
            raise Exception('Output MS does not exist. It seems sdatmcor failed.')

        results = SDATMCorrectionResults(
            task=self.__class__,
            success=True,
            outcome=args['outfile']
        )

        return results

    def analyse(self, result):
        return result


### Tier-0 parallelization
class HpcSDATMCorrectionInputs(SDATMCorrectionInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, atmtype=None,
                 infiles=None, antenna=None, field=None, spw=None, pol=None,
                 parallel=None):
        super().__init__(context, atmtype, infiles, antenna, field, spw, atmtype)
        self.parallel = parallel


# @task_registry.set_equivalent_casa_task('hsd_atmcor')
# @task_registry.set_casa_commands_comment(
#     'Apply offline correction of atmospheric transmission model.'
# )
class HpcSDATMCorrection(sessionutils.ParallelTemplate):
    Inputs = HpcSDATMCorrectionInputs
    Task = SerialSDATMCorrection

    def __init__(self, inputs):
        super().__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis, exception):
        LOG.error('Error operating target flag for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)
