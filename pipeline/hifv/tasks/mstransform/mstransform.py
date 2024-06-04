import os
import shutil
import traceback

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry
from pipeline.hif.tasks.mstransform import mstransform as mst

LOG = infrastructure.get_logger(__name__)


class VlaMstransformInputs(mst.MstransformInputs):

    # New a second MS with a new data type is needed for VLA continuum imaging
    @vdp.VisDependentProperty
    def outputvis(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_targets_cont.ms'

    @vdp.VisDependentProperty
    def outputvis_for_line(self):
        vis_root = os.path.splitext(self.vis)[0]
        return vis_root + '_targets.ms'
    
    # TODO: conversion for spw_line
    # Find all the spws with TARGET intent. These may be a subset of the
    # science spws which include calibration spws.
    @vdp.VisDependentProperty
    def spw_line(self):
        science_target_intents = set(self.intent.split(','))
        science_target_spws = []

        science_spws = [spw for spw in self.ms.get_spectral_windows(science_windows_only=True)]
        for spw in science_spws:
            if spw.intents.intersection(science_target_intents):
                science_target_spws.append(spw)

        return ','.join([str(spw.id) for spw in science_target_spws])

    @spw_line.convert
    def spw_line(self, value):
        science_target_intents = set(self.intent.split(','))
        science_target_spws = []

        science_spws = [spw for spw in self.ms.get_spectral_windows(task_arg=value, science_windows_only=True)]
        for spw in science_spws:
            if spw.intents.intersection(science_target_intents):
                science_target_spws.append(spw)

        return ','.join([str(spw.id) for spw in science_target_spws])

    def __init__(self, context, output_dir=None, vis=None, outputvis=None, field=None, intent=None, spw=None,
                 spw_line=None, chanbin=None, timebin=None, outputvis_for_line=None):

        super().__init__(context, output_dir, vis, outputvis, field, intent, spw, chanbin, timebin)
        self.spw_line = spw_line
        self.outputvis = outputvis
        self.outputvis_for_line = outputvis_for_line


@task_registry.set_equivalent_casa_task('hifv_mstransform')
class VlaMstransform(mst.Mstransform):
    Inputs = VlaMstransformInputs

    def prepare(self):
        inputs = self.inputs

        # Create the results structure
        result = VlaMstransformResults(vis=inputs.vis, outputvis=inputs.outputvis,
                                       outputvis_for_line=outputvis_for_line)

        # Run CASA task to create the output MS for continuum data
        mstransform_args = inputs.to_casa_args()
        # Remove input member variables that don't belong as input to the mstransform task
        # TODO: Handle this in a better way.
        mstransform_args.pop('outputvis_for_line', None)
        mstransform_args.pop('spw_line', None)
        mstransform_job = casa_tasks.mstransform(**mstransform_args)

        try:
            self._executor.execute(mstransform_job)
        except OSError as ee:
            LOG.warning(f"Caught mstransform exception: {ee}")

        # Copy across requisite XML files.
        mst.Mstransform._copy_xml_files(inputs.vis, inputs.outputvis)

        # Split off non-RFI flagged target data
        # The main goal is to get an MS with the same shape as the _target.ms to
        # get the flags for non-RFI flagged data

        # Restore flags from before RFI flagging was applied
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname='before_rflag_statwt')
        self._executor.execute(task)

        # Run CASA task to create the output MS for the line data
        mstransform_args['outputvis'] = inputs.outputvis_for_line

        # TODO: add ability to also split off pre-identified SPWs here, in addition or instead of 
        # ones specified directly as task input
        if inputs.spw_line:
            mstransform_args['spw'] = inputs.spw_line
        mstransform_job = casa_tasks.mstransform(**mstransform_args)

        try:
            self._executor.execute(mstransform_job)
        except OSError as ee:
            LOG.warning(f"Caught mstransform exception: {ee}")

        # Save flags from line MS without rfi flagging
        task = casa_tasks.flagmanager(vis=outputvis_for_line, mode='save', versionname='before_rflag_statwt')
        self._executor.execute(task)

        # Copy across requisite XML files.
        mst.Mstransform._copy_xml_files(inputs.vis, outputvis_for_line)

        # Restore RFI flags to main MS
        #task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname='rfi_flagged_statwt')
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname='statwt_1')
        self._executor.execute(task)

        return result

    def analyse(self, result):

        # Check for existence of the output vis.
        if not os.path.exists(result.outputvis):
            LOG.debug('Error creating science targets cont+line MS for continuum %s' % (os.path.basename(result.outputvis)))
            return result

        # Check for existence of the output vis for line processing.
        if not os.path.exists(result.outputvis_for_line):
            LOG.debug('Error creating science targets cont+line MS for line %s' % (os.path.basename(result.outputvis_for_line)))
            return result

        # TODO: probably move this function
        def _import_new_ms(to_import, datatype):
            observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)

            # Adopt same session as source measurement set
            for ms in observing_run.measurement_sets:
                LOG.debug('Setting session to %s for %s', self.inputs.ms.session, ms.basename)
                ms.session = self.inputs.ms.session
                LOG.debug('Setting data_column and origin_ms.')
                ms.origin_ms = self.inputs.ms.origin_ms
                ms.set_data_column(datatype, 'DATA')

            result.mses.extend(observing_run.measurement_sets)

        # Import the new measurement sets.
        try:
            to_import = os.path.relpath(result.outputvis)
            _import_new_ms(to_import, datatype=DataType.REGCAL_CONT_SCIENCE)
            to_import_for_line = os.path.relpath(result.outputvis_for_line)
            _import_new_ms(to_import_for_line, datatype=DataType.REGCAL_CONTLINE_SCIENCE)
        except Exception:
            traceback.print_exec()

        return result


class VlaMstransformResults(mst.MstransformResults):
    def __init__(self, vis, outputvis, outputvis_for_line):
        super().__init__(vis, outputvis)
        self.outputvis_for_line = outputvis_for_line

    def __str__(self):
        # Format the Mstransform results.
        s = 'VlaMstransformResults:\n'
        s += '\tOriginal MS {vis} transformed to {outputvis} and {outputvis_for_line} \n'.format(
            vis=os.path.basename(self.vis),
            outputvis=os.path.basename(self.outputvis),
            outputvis_for_line=os.path.basename(self.outputvis_for_line))
        return s

    def __repr__(self):
        return 'VlaMstranformResults({}, {} + {})'.format(os.path.basename(self.vis), os.path.basename(self.outputvis),
                                                          os.path.basename(self.outputvis_for_line))


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
