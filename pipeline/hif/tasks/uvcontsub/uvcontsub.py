import os
import shutil
from collections import namedtuple

import pipeline.h.tasks.applycal.applycal as applycal
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.domain import DataType
from pipeline.infrastructure import task_registry

from pipeline.hif.tasks.makeimlist import makeimlist


LOG = infrastructure.get_logger(__name__)


class UVcontSubInputs(vdp.StandardInputs):
    # Search order of input vis
    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE, DataType.REGCAL_CONTLINE_ALL, DataType.RAW]

    fitorder = vdp.VisDependentProperty(default={})
    intent = vdp.VisDependentProperty(default='TARGET')

    def __init__(self, context, output_dir=None, vis=None, field=None,
                 spw=None, intent=None, fitorder=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        self.field = field
        self.spw = spw
        self.intent = intent
        self.fitorder = fitorder


@task_registry.set_equivalent_casa_task('hif_uvcontsub')
class UVcontSub(applycal.Applycal):
    Inputs = UVcontSubInputs

    def prepare(self):
        inputs = self.inputs

        # Define minimal entity object to be able to use some
        # of the imaging heuristics methods to convert
        # frequency ranges to TOPO.
        MinimalTcleanHeuristicsInputsGenerator = namedtuple('MinimalTcleanHeuristicsInputs', 'vis field intent phasecenter spw spwsel_lsrk specmode')

        # Check for size mitigation errors.
        if 'status' in inputs.context.size_mitigation_parameters:
            if inputs.context.size_mitigation_parameters['status'] == 'ERROR':
                result = UVcontSubResults()
                result.mitigation_error = True
                return result

        # If no field and no spw is specified, use all TARGET intents except
        # mitigated sources.
        # If field or spw is specified, work on that selection

        # Determine intent(s) to work on
        allowed_intents = ('TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE')
        if inputs.intent not in (None, ''):
            if all(i.strip() in allowed_intents for i in inputs.intent.split(',')):
                intent = inputs.intent.replace(' ', '')
            else:
                result = UVcontSubResults()
                result.error = True
                result.error_msg = f'"intent" must be in {allowed_intents}'
                LOG.error(result.error_msg)
                return result
        else:
            intent = 'TARGET'

        # Determine field IDs to work on
        if inputs.field not in (None, ''):
            field = inputs.field
        else:
            field = ''

        # Determine spw IDs to work on
        if inputs.spw not in (None, ''):
            spw = inputs.spw
        else:
            spw = ''

        known_synthesized_beams = inputs.context.synthesized_beams

        # Get list of fields and spw to work on from makeimlist call
        # which automatically handles mitigated sources. spw mitigation
        # shall not be considered, hence the specmode is mfs.

        # Create makeimlist inputs
        makeimlist_inputs = makeimlist.MakeImListInputs(inputs.context, vis=[inputs.vis])
        # TODO: Set field, intent and spw based on input if given
        makeimlist_inputs.field = field
        makeimlist_inputs.intent = intent
        makeimlist_inputs.spw = spw
        makeimlist_inputs.specmode = 'mfs'
        makeimlist_inputs.clearlist = True

        # Create makeimlist task for size calculations
        makeimlist_task = makeimlist.MakeImList(makeimlist_inputs)

        # Get default target setup
        makeimlist_inputs.known_synthesized_beams = known_synthesized_beams
        makeimlist_result = makeimlist_task.prepare()
        known_synthesized_beams = makeimlist_result.synthesized_beams
        imlist = makeimlist_result.targets
        fitspec = {}
        for imaging_target in imlist:
            minimal_tclean_inputs = MinimalTcleanHeuristicsInputsGenerator(imaging_target['vis'],
                                                                           imaging_target['field'],
                                                                           imaging_target['intent'],
                                                                           imaging_target['phasecenter'],
                                                                           imaging_target['spw'],
                                                                           imaging_target['spwsel_lsrk'],
                                                                           imaging_target['specmode'])
            (_, _, _, spw_topo_chan_param_dict, _, _, _) = imaging_target['heuristics'].calc_topo_ranges(minimal_tclean_inputs)
            field_ids = imaging_target['heuristics'].field(imaging_target['intent'], imaging_target['field'])[0]
            # TODO: Use automatics dict hierarchy
            if field_ids not in fitspec:
                fitspec[field_ids] = {}
            if minimal_tclean_inputs.spw not in fitspec[field_ids]:
                fitspec[field_ids][minimal_tclean_inputs.spw] = {}
            fitspec[field_ids][minimal_tclean_inputs.spw]['chan'] = spw_topo_chan_param_dict[minimal_tclean_inputs.vis[0]][minimal_tclean_inputs.spw]

        #contfile = inputs.context.contfile if inputs.context.contfile is not None else 'cont.dat'

        result = UVcontSubResults()

        outputvis = inputs.vis.replace('_targets', '_targets_line')
        # Check if it already exists and remove it
        if os.path.exists(outputvis):
            LOG.info('Removing {} from disk'.format(outputvis))
            shutil.rmtree(outputvis)

        # Copy across requisite XML files.
        self._copy_xml_files(inputs.vis, outputvis)

        result.vis = inputs.vis
        result.outputvis = outputvis

        return result

    def analyse(self, result):

        if not result.mitigation_error:
            # Check for existence of the output vis.
            if not os.path.exists(result.outputvis):
                LOG.debug('Error creating science targets line MS %s' % (os.path.basename(result.outputvis)))
                return result

            # Import the new measurement set.
            to_import = os.path.relpath(result.outputvis)
            observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

            # Adopt same session as source measurement set
            for ms in observing_run.measurement_sets:
                LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
                ms.session = self.inputs.ms.session
                ms.set_data_column(DataType.REGCAL_LINE_SCIENCE, 'DATA')
            result.line_mses.extend(observing_run.measurement_sets)

        return result

    @staticmethod
    def _copy_xml_files(vis, outputvis):
        for xml_filename in ['SpectralWindow.xml', 'DataDescription.xml']:
            vis_source = os.path.join(vis, xml_filename)
            outputvis_target_line = os.path.join(outputvis, xml_filename)
            if os.path.exists(vis_source) and os.path.exists(outputvis):
                LOG.info('Copying %s from original MS to science targets line MS', xml_filename)
                LOG.trace('Copying %s: %s to %s', xml_filename, vis_source, outputvis_target_line)
                shutil.copyfile(vis_source, outputvis_target_line)


class UVcontSubResults(basetask.Results):
    """
    UVcontSubResults is the results class for the pipeline UVcontSub task.
    """

    def __init__(self, applied=[]):
        super(UVcontSubResults, self).__init__()
        self.mitigation_error = False
        self.vis = None
        # TODO: outputvis needed?
        self.outputvis = None
        self.line_mses = []
        self.error = False
        self.error_msg = ''

    def merge_with_context(self, context):
        # Check for an output vis
        if not self.line_mses:
            LOG.error('No hif_uvcontsub results to merge')
            return

        target = context.observing_run

        # Adding line mses to context
        for ms in self.line_mses:
            # Check if MS with the same name had already been registered and remove it
            try:
                index = [existing_ms.basename for existing_ms in target.measurement_sets].index(ms.basename)
                LOG.info('Removing {} from context'.format(ms.name))
                target.measurement_sets.pop(index)
            except:
                # Exception happens if name is not found. No special handling needed.
                pass
            LOG.info('Adding {} to context'.format(ms.name))
            target.add_measurement_set(ms)

        # Create targets flagging template file if it does not already exist
        for ms in self.line_mses:
            template_flagsfile = os.path.join(
                self.inputs['output_dir'], os.path.splitext(os.path.basename(self.vis))[0] + '.flagtargetstemplate.txt')
            self._make_template_flagfile(template_flagsfile, 'User flagging commands file for the imaging pipeline')

        # Initialize callibrary
        for ms in self.line_mses:
            # TODO: Check for existing entries for the line MS and remove them.
            #       This is probably only the case for future selfcal use cases.
            calto = callibrary.CalTo(vis=ms.name)
            LOG.info('Registering {} with callibrary'.format(ms.name))
            context.callibrary.add(calto, [])

    def _make_template_flagfile(self, outfile, titlestr):
        # Create a new file if overwrite is true and the file
        # does not already exist.
        if not os.path.exists(outfile):
            template_text = FLAGGING_TEMPLATE_HEADER.replace('___TITLESTR___', titlestr)
            with open(outfile, 'w') as f:
                f.writelines(template_text)

    def __repr__(self):
        s = 'UVcontSubResults:\n'
        return s

FLAGGING_TEMPLATE_HEADER = '''#
# ___TITLESTR___
#
# Examples
# Note: Do not put spaces inside the reason string !
#
# mode='manual' correlation='YY' antenna='DV01;DV08;DA43;DA48&DV23' spw='21:1920~2880' autocorr=False reason='bad_channels'
# mode='manual' spw='25:0~3;122~127' reason='stage8_2'
# mode='manual' antenna='DV07' timerange='2013/01/31/08:09:55.248~2013/01/31/08:10:01.296' reason='quack'
#
'''
