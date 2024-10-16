import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.h.tasks.mstransform import mssplit
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class TransformimagedataResults(basetask.Results):
    def __init__(self, vis, outputvis):
        super(TransformimagedataResults, self).__init__()
        self.vis = vis
        self.outputvis = outputvis
        self.ms = None

    def merge_with_context(self, context):
        # Check for an output vis
        if not self.ms:
            LOG.error('No hif_transformimagedata results to merge')
            return

        target = context.observing_run
        parentms = None
        #if self.vis == self.outputvis:
        # The parent MS has been removed.
        if not os.path.exists(self.vis):
            for index, ms in enumerate(target.get_measurement_sets()):
                #if ms.name == self.outputvis:
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

        # Remove original measurement set from context
        context.observing_run.measurement_sets.pop(0)

        for i in range(0, len(context.clean_list_pending)):
            outvisname = os.path.join(context.output_dir, os.path.basename(self.outputvis))
            context.clean_list_pending[i]['heuristics'].observing_run.measurement_sets[0].name = outvisname
            newvislist = [self.outputvis]
            context.clean_list_pending[i]['heuristics'].vislist = newvislist

    def __str__(self):
        # Format the MsSplit results.
        s = 'Transformimagedata:\n'
        s += '\tOriginal MS {vis} transformed to {outputvis}\n'.format(
            vis=os.path.basename(self.vis),
            outputvis=os.path.basename(self.outputvis))

        return s

    def __repr__(self):
        return 'Transformimagedata({}, {})'.format(os.path.basename(self.vis), os.path.basename(self.outputvis))


class TransformimagedataInputs(mssplit.MsSplitInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    clear_pointing = vdp.VisDependentProperty(default=True)
    modify_weights = vdp.VisDependentProperty(default=False)
    wtmode = vdp.VisDependentProperty(default='')
    replace = vdp.VisDependentProperty(default=False)
    datacolumn = vdp.VisDependentProperty(default='corrected')

    @vdp.VisDependentProperty
    def outputvis(self):

        output_dir = self.context.output_dir
        if isinstance(self._outputvis, vdp.NullMarker):
            # Need this to be in the working directory
            # vis_root = os.path.splitext(self.vis)[0]
            vis_root = os.path.splitext(os.path.basename(self.vis))[0]
            return os.path.join(output_dir, vis_root + '_split.ms')
        else:
            return os.path.join(output_dir, os.path.basename(self.outputvis))

    @outputvis.convert
    def outputvis(self, value=''):
        return value

    def __init__(self, context, vis=None, output_dir=None,
                 outputvis=None, field=None, intent=None, spw=None,
                 datacolumn=None, chanbin=None, timebin=None, replace=None,
                 clear_pointing=None, modify_weights=None, wtmode=None):

        # super(TransformimagedataInputs, self).__init__()

        # set the properties to the values given as input arguments
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

        if clear_pointing is not False:
            clear_pointing = True
        self.clear_pointing = clear_pointing

        if modify_weights is not True:
            modify_weights = False
        self.modify_weights = modify_weights

        self.wtmode = wtmode


@task_registry.set_equivalent_casa_task('hif_transformimagedata')
class Transformimagedata(mssplit.MsSplit):
    Inputs = TransformimagedataInputs

    def prepare(self):

        inputs = self.inputs

        # Test whether or not a split has been requested
        """
        if inputs.field == '' and inputs.spw == '' and inputs.intent == '' and \
            inputs.chanbin == 1 and inputs.timebin == '0s':
            result = TransformimagedataResults(vis=inputs.vis, outputvis=inputs.outputvis)
            LOG.warning('Output MS equals input MS %s' % (os.path.basename(inputs.vis)))
            return
        """

        # Split is required so create the results structure
        result = TransformimagedataResults(vis=inputs.vis, outputvis=inputs.outputvis)

        # Run CASA task
        #    Does this need a try / except block

        visfields = []
        visspws = []
        for imageparam in inputs.context.clean_list_pending:
            visfields.extend(imageparam['field'].split(','))
            visspws.extend(imageparam['spw'].split(','))

        visfields = set(visfields)
        visfields = list(visfields)
        visfields = ','.join(visfields)

        visspws = set(visspws)
        visspws = sorted(visspws)
        visspws = ','.join(visspws)

        mstransform_args = inputs.to_casa_args()
        mstransform_args['field'] = visfields
        mstransform_args['reindex'] = False
        mstransform_args['spw'] = visspws

        for dictkey in ('clear_pointing', 'modify_weights', 'wtmode'):
            try:
                del mstransform_args[dictkey]
            except KeyError:
                pass

        mstransform_job = casa_tasks.mstransform(**mstransform_args)

        self._executor.execute(mstransform_job)

        return result

    def analyse(self, result):
        # Check for existence of the output vis.
        if not os.path.exists(result.outputvis):
            return result

        inputs = self.inputs

        # There seems to be a rerendering issue with replace. For now just
        # remove the old file.
        if inputs.replace:
            shutil.rmtree(result.vis)
            #shutil.move (result.outputvis, result.vis)
            #result.outputvis = result.vis

        # Import the new MS
        rel_to_import = result.outputvis
        observing_run = tablereader.ObservingRunReader.get_observing_run(rel_to_import)

        # Adopt same session as source measurement set
        for ms in observing_run.measurement_sets:
            LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
            ms.session = self.inputs.ms.session
            ms.origin_ms = self.inputs.ms.origin_ms
            self._set_data_column_to_ms(ms)

        # Note there will be only 1 MS in the temporary observing run structure
        result.ms = observing_run.measurement_sets[0]

        if inputs.clear_pointing:
            LOG.info('Removing POINTING table from ' + ms.name)
            with casa_tools.TableReader(ms.name + '/POINTING', nomodify=False) as table:
                rows = table.rownumbers()
                table.removerows(rows)

        if inputs.modify_weights:
            LOG.info('Re-initializing the weights in ' + ms.name)
            if inputs.wtmode:
                task = casa_tasks.initweights(vis=ms.name, wtmode=inputs.wtmode)
            else:
                task = casa_tasks.initweights(vis=ms.name)
            self._executor.execute(task)

        return result
