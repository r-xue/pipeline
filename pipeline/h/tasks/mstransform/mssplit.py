import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType, MeasurementSet
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'MsSplitInputs',
    'MsSplit',
    'MsSplitResults'
]


# Define the minimum set of parameters required to split out
# the requested data (defined by field, spw, intent) from the
# original MS and optionally average in time and channel.  
#
# If replace is True replace the parameter MS with the transformed
# one on disk and in the context

class MsSplitInputs(vdp.StandardInputs):
    chanbin = vdp.VisDependentProperty(default=1)
    datacolumn = vdp.VisDependentProperty(default='data')
    field = vdp.VisDependentProperty(default='')
    intent = vdp.VisDependentProperty(default='')
    replace = vdp.VisDependentProperty(default=True)
    spw = vdp.VisDependentProperty(default='')
    timebin = vdp.VisDependentProperty(default='0s')

    @vdp.VisDependentProperty
    def outputvis(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_split.ms'

    def __init__(self, context, vis=None, output_dir=None, outputvis=None, field=None, intent=None, spw=None,
                 datacolumn=None, chanbin=None, timebin=None, replace=None):
        super(MsSplitInputs, self).__init__()

        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        self.outputvis = outputvis
        self.field = field
        self.intent = intent
        self.spw = spw
        self.datacolumn = datacolumn
        self.chanbin = chanbin
        self.timebin = timebin
        self.replace = replace

    def to_casa_args(self):
        d = super(MsSplitInputs, self).to_casa_args()

        if d['chanbin'] > 1:
            d['chanaverage'] = True
        if d['timebin'] != '0s':
            d['timeaverage'] = True

        # Filter out unwanted parameters
        del d['replace']

        return d


@task_registry.set_equivalent_casa_task('h_mssplit')
@task_registry.set_casa_commands_comment(
    'The parent MS is split by field, intent, or spw and/or averaged by channel and time.'
)
class MsSplit(basetask.StandardTaskTemplate):
    Inputs = MsSplitInputs

    def prepare(self):

        inputs = self.inputs

        # Test whether or not a split has been requested
        if inputs.field == '' and inputs.spw == '' and inputs.intent == '' and \
            inputs.chanbin == 1 and inputs.timebin == '0s':
            result = MsSplitResults(vis=inputs.vis, outputvis=inputs.outputvis)
            LOG.warning('Output MS equals input MS %s' % (os.path.basename(inputs.vis)))
            return

        # Split is required so create the results structure
        result = MsSplitResults(vis=inputs.vis, outputvis=inputs.outputvis)

        # Run CASA task
        #    Does this need a try / except block 

        mstransform_args = inputs.to_casa_args()
        mstransform_job = casa_tasks.mstransform(**mstransform_args)
        self._executor.execute(mstransform_job)

        return result

    def analyse(self, result):
        # Check for existence of the output vis. 
        if not os.path.exists(result.outputvis):
            return result

        inputs = self.inputs

        # There seems to be a rerendering issue with replace. Fir now just
        # remove the old file.
        if inputs.replace:
            shutil.rmtree(result.vis)

        # Import the new MS
        to_import = os.path.abspath(result.outputvis)
        observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

        # Adopt same session as source measurement set
        for ms in observing_run.measurement_sets:
            LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
            ms.session = self.inputs.ms.session
            ms.origin_ms = self.inputs.ms.origin_ms
            self._set_data_column_to_ms(ms)

        # Note there will be only 1 MS in the temporary observing run structure
        result.ms = observing_run.measurement_sets[0]

        return result

    def _set_data_column_to_ms(self, msobj: MeasurementSet):
        """
        Set data_column to input MeasurementSet domain object.

        This method sets data_column information of output MS depending on
        intent and datacolumn selection of the Inputs class.

        Args:
            msobj: MS domain object to set data_column information.
        """
        datacolumn = self.inputs.datacolumn
        in_column = datacolumn.upper() if datacolumn != 'corrected' else 'CORRECTED_DATA'
        LOG.debug('in_column = %s' % in_column)
        #if self.inputs.replace and not os.path.exists(msobj.origin_ms):
        #    # Replace RAW column if original MS was replaced.
        #    data_type = DataType.RAW
        #el

        data_type = None
        if self.inputs.intent == 'TARGET':
            data_type = DataType.REGCAL_CONTLINE_SCIENCE
        else:
            for t, c in self.inputs.ms.data_column.items():
                if c == in_column:
                    data_type = t
                    LOG.debug(f'Identified data type {data_type}')
                    break

        if data_type is None:
            data_type = DataType.RAW
            LOG.warning(
                f'The datatype of the requested datacolumn is unknown, and a fallback value of {data_type} is used.')

        out_column = in_column if datacolumn != 'corrected' else 'DATA'
        LOG.info(f'Setting {data_type} to {out_column}')
        msobj.set_data_column(data_type, out_column)

class MsSplitResults(basetask.Results):
    def __init__(self, vis, outputvis):
        super(MsSplitResults, self).__init__()
        self.vis = vis
        self.outputvis = outputvis
        self.ms = None

    def merge_with_context(self, context):
        # Check for an output vis
        if not self.ms:
            LOG.error('No h_mssplit results to merge')
            return

        target = context.observing_run
        parentms = None
        # The parent MS has been removed.
        if not os.path.exists(self.vis):
            for index, ms in enumerate(target.get_measurement_sets()):
                if ms.name == self.vis:
                    parentms = index
                    break

        if self.ms:
            if parentms is not None:
                LOG.info('Replace {} in context'.format(self.ms.name))
                del target.measurement_sets[parentms]
                target.add_measurement_set(self.ms)

            else:
                LOG.info('Adding {} to context'.format(self.ms.name))
                target.add_measurement_set(self.ms)

    def __str__(self):
        # Format the MsSplit results.
        s = 'MsSplitResults:\n'
        s += '\tOriginal MS {vis} transformed to {outputvis}\n'.format(
            vis=os.path.basename(self.vis),
            outputvis=os.path.basename(self.outputvis))

        return s

    def __repr__(self):
        return 'MsSplitResults({}, {})'.format(os.path.basename(self.vis), os.path.basename(self.outputvis))
