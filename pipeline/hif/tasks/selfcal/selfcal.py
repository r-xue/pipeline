import os
import shutil
import traceback

import numpy as np

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import DataType
from pipeline.hif.heuristics.auto_selfcal import auto_selfcal
from pipeline.hif.tasks.applycal import IFApplycal
from pipeline.hif.tasks.makeimlist import MakeImList
from pipeline.infrastructure import callibrary, casa_tasks, casa_tools, utils, task_registry
from pipeline.infrastructure.contfilehandler import contfile_to_chansel
from pipeline.infrastructure.mpihelpers import TaskQueue

LOG = infrastructure.get_logger(__name__)


class SelfcalResults(basetask.Results):
    def __init__(self, targets, applycal_result_contline=None, applycal_result_line=None):
        super().__init__()
        self.pipeline_casa_task = 'Selfcal'
        self.targets = targets
        self.applycal_result_contline = applycal_result_contline
        self.applycal_result_line = applycal_result_line

    def merge_with_context(self, context):
        """See :method:`~pipeline.infrastructure.api.Results.merge_with_context`."""

        # save selfcal results into the Pipeline context
        if not hasattr(context, 'scal_targets'):
            context.scal_targets = self.targets

        if self.applycal_result_contline is not None:
            self._register_datatype(context, self.applycal_result_contline, DataType.SELFCAL_CONTLINE_SCIENCE)
        if self.applycal_result_line is not None:
            self._register_datatype(context, self.applycal_result_line, DataType.SELFCAL_LINE_SCIENCE)

    def _register_datatype(self, context, appycal_result, dtype):

        calto_list = []
        for r in appycal_result:
            for calapp in r.applied:
                calto_list.append(calapp.calto)

        vislist = []
        for target in self.targets:
            vislist.extend(target['sc_vislist'])
        vislist = list(set(vislist))

        # register the selfcal results to the observing run
        for calto in calto_list:

            vis = calto.vis
            field_sel = calto.field
            spw_sel = calto.spw

            with casa_tools.TableReader(vis) as tb:
                # check for the existance of CORRECTED_DATA first
                if 'CORRECTED_DATA' not in tb.colnames():
                    LOG.warning(f'No CORRECTED_DATA column in {vis}, skip {dtype} registration')
                    continue
                LOG.info(f'Register the CORRECTED_DATA column as {dtype} for {vis} {field_sel} {spw_sel}')
                ms = context.observing_run.get_ms(vis)
                ms.set_data_column(dtype, 'CORRECTED_DATA', source=field_sel, spw=spw_sel)

    def __repr__(self):
        return 'SelfcalResults:'


class SelfcalInputs(vdp.StandardInputs):

    # restrict input vis to be of type REGCAL_CONTLINE_SCIENCE
    # potentially we could allow REGCAL_CONTLINE_ALL here (e.g. tmp ms splitted from 'corrected' data column),
    # but there is no space for applying final selfcal solutions to the data.

    processing_data_type = [DataType.REGCAL_CONTLINE_SCIENCE]

    # simple properties with no logic
    field = vdp.VisDependentProperty(default='')
    spw = vdp.VisDependentProperty(default='')
    contfile = vdp.VisDependentProperty(default='cont.dat')
    apply = vdp.VisDependentProperty(default=False)
    parallel = vdp.VisDependentProperty(default='automatic')
    recal = vdp.VisDependentProperty(default=False)

    def __init__(self, context, vis=None, field=None, spw=None, contfile=None, apply=None, parallel=None, recal=None):
        super().__init__()
        self.context = context
        self.vis = vis
        self.field = field
        self.spw = spw
        self.contfile = contfile
        self.apply = apply
        self.parallel = parallel
        self.recal = recal


@task_registry.set_equivalent_casa_task('hif_selfcal')
@task_registry.set_casa_commands_comment('Run self-calibration using the science target visibilities.')
class Selfcal(basetask.StandardTaskTemplate):
    Inputs = SelfcalInputs
    is_multi_vis_task = True

    def __init__(self, inputs):
        super().__init__(inputs)

        self.spectral_average = True
        self.do_amp_selfcal = True
        self.inf_EB_gaincal_combine = 'scan'
        self.inf_EB_gaintype = 'G'
        self.inf_EB_override = False
        self.gaincal_minsnr = 2.0
        self.minsnr_to_proceed = 3.0
        self.delta_beam_thresh = 0.05

        self.apply_cal_mode_default = 'calflag'
        self.rel_thresh_scaling = 'log10'  # can set to linear, log10, or loge (natural log)
        self.dividing_factor = -99.0    # number that the peak SNR is divided by to determine first clean threshold -99.0 uses default
        # default is 40 for <8ghz and 15.0 for all other frequencies
        self.check_all_spws = False   # generate per-spw images to check phase transfer did not go poorly for narrow windows
        self.apply_to_target_ms = False  # apply final selfcal solutions back to the input _target.ms files

        # if 'VLA' in self.telescope:
        #    self.check_all_spws = False

    def prepare(self):

        inputs = self.inputs
        if inputs.vis is None or inputs.vis == [] or inputs.vis == '':
            raise ValueError(
                f'No input visibilities specified matching required DataType {inputs.processing_data_type}, please review in the DataType information in Imported MS(es).')

        if not isinstance(inputs.vis, list):
            inputs.vis = [inputs.vis]

        if not hasattr(self.inputs.context, 'scal_targets') or self.inputs.recal:
            # skip the "selfcal-solver" executation if the selfcal results are already in the context and recal is False.
            LOG.info('No selfcal results found in the context. Proceed to execute the selfcal solver.')
            scal_targets = self._solve_selfcal()
        else:
            LOG.info('Found selfcal results in the context. Skip the selfcal solver.')
            scal_targets = self.inputs.context.scal_targets

        obs_run = self.inputs.context.observing_run

        mses_regcal_contline = obs_run.get_measurement_sets_of_type([DataType.REGCAL_CONTLINE_SCIENCE], msonly=True)
        mses_selfcal_contline = obs_run.get_measurement_sets_of_type([DataType.SELFCAL_CONTLINE_SCIENCE], msonly=True)

        applycal_result_contline = applycal_result_line = None
        if mses_regcal_contline:
            if not mses_selfcal_contline:
                LOG.info('No DataType:SELFCAL_CONTLINE_SCIENCE found.')
                LOG.info('Attempt to apply any selfcal solutions to the REGCAL_CONTLINE_SCIENCE MS(es):')
                for ms in mses_regcal_contline:
                    LOG.debug(f'  {ms.basename}: {ms.data_column}')
                applycal_result_contline = self._apply_scal(scal_targets, mses_regcal_contline)
            else:
                LOG.info('Found DataType:SELFCAL_CONTLINE_SCIENCE.')
                for ms in mses_selfcal_contline:
                    LOG.debug(f'  {ms.basename}: {ms.data_column}')
                LOG.info('Skip applying selfcal solutions to the REGCAL_CONTLINE_SCIENCE MS(es).')

        mses_regcal_line = obs_run.get_measurement_sets_of_type([DataType.REGCAL_LINE_SCIENCE], msonly=True)
        mses_selfcal_line = obs_run.get_measurement_sets_of_type([DataType.SELFCAL_LINE_SCIENCE], msonly=True)

        if mses_regcal_line:
            if not mses_selfcal_line:
                LOG.info('No DataType:SELFCAL_LINE_SCIENCE found.')
                LOG.info('Attempt to apply any selfcal solutions to the REGCAL_LINE_SCIENCE MS(es).')
                for ms in mses_regcal_line:
                    LOG.debug(f'  {ms.basename}: {ms.data_column}')
                applycal_result_line = self._apply_scal(scal_targets,  mses_regcal_line)
            else:
                LOG.info('Found DataType:SELFCAL_LINE_SCIENCE.')
                for ms in mses_selfcal_line:
                    LOG.debug(f'  {ms.basename}: {ms.data_column}')
                LOG.info('Skip applying selfcal solutions to the REGCAL_LINE_SCIENCE MS(es).')

        return SelfcalResults(scal_targets, applycal_result_contline, applycal_result_line)

    def _solve_selfcal(self):

        # collect the target list
        scal_targets = self._get_scaltargets(scal=True)

        # split percleantarget MSes with spectral line flagged
        self._flag_lines()
        self._split_scaltargets(scal_targets)
        self._restore_flags()

        # # register the percleantarget MSes
        # self._register_percleantarget_ms(vislist)

        # # start the selfcal sequence.

        tclean_parallel_request = mpihelpers.parse_mpi_input_parameter(self.inputs.parallel)
        taskqueue_parallel_request = len(scal_targets) > 1
        with TaskQueue(parallel=taskqueue_parallel_request) as tq:
            for target in scal_targets:
                target['sc_parallel'] = (tclean_parallel_request and not tq.is_async())
                tq.add_functioncall(self._run_selfcal_sequence, target, self._executor.copy(exclude_context=True))
        tq_results = tq.get_results()

        for idx, target in enumerate(scal_targets):
            scal_library, solints, bands = tq_results[idx]
            if scal_library is None:
                raise ValueError('auto_selfcal heuristics failed for target {0} spw {1} from {2}'.format(
                    target['field'], target['spw'], target['sc_workdir']))
            target['sc_band'] = bands[0]
            target['sc_solints'] = solints[bands[0]]
            # note scal_library is keyed by field name without quotes at this moment.
            # see. https://casadocs.readthedocs.io/en/stable/notebooks/visibility_data_selection.html#The-field-Parameter
            #       utils.fieldname_for_casa() and
            #       utils.dequote_fieldname_for_casa()
            field_name = utils.dequote(target['field'])
            target['sc_lib'] = scal_library[field_name][target['sc_band']]
            target['field_name'] = field_name
            target['sc_rms_scale'] = target['sc_lib']['RMS_final'] / target['sc_lib']['theoretical_sensitivity']
            target['sc_success'] = target['sc_lib']['SC_success']

        return scal_targets

    @staticmethod
    def _run_selfcal_sequence(scal_target, executor):

        workdir = os.path.abspath('./')
        selfcal_library, solints, bands = None, None, None

        try:
            os.chdir(scal_target['sc_workdir'])
            LOG.info('')
            LOG.info('Running auto_selfcal heuristics on target {0} spw {1} from {2}'.format(
                scal_target['field'], scal_target['spw'], scal_target['sc_workdir']))
            LOG.info('')
            selfcal_heuristics = auto_selfcal.SelfcalHeuristics(scal_target, executor=executor)
            # import pickle
            # with open('selfcal_heuristics.pickle', 'wb') as handle:
            #     pickle.dump(selfcal_library, handle, protocol=pickle.HIGHEST_PROTOCOL)
            selfcal_library, solints, bands = selfcal_heuristics()
        except Exception as err:
            LOG.error('Exception from hif.heuristics.auto_selfcal.SelfcalHeuristics:')
            LOG.error(str(err))
            LOG.error(traceback.format_exc())
        finally:
            os.chdir(workdir)
            if scal_target['sc_parallel']:
                # sc_parallel=True indicats that we are certainly running tclean(parallel=true) in a sequential TaskQueue.
                # A side effect of doing this while changing cwd is that the working directory of MPIServers will be "stuck"
                # to the one where tclean(paralllel=True) started.
                # As a workaround, we send the chdir command to the MPIServers exeplicitly.
                mpihelpers.mpiclient.push_command_request(f'os.chdir({workdir!r})', block=True, target_server=mpihelpers.mpi_server_list)

        return selfcal_library, solints, bands

    def _apply_scal(self, sc_targets, mses):

        calapps = []
        vislist = []

        # collect a name list of MSes which we could apply the selfcal solutions to.
        vislist_calto = [ms.name for ms in mses]

        for cleantarget in sc_targets:

            sc_lib = cleantarget['sc_lib']
            sc_workdir = cleantarget['sc_workdir']
            if not sc_lib['SC_success']:
                continue

            for vis in sc_lib['vislist']:
                for idx, gaintable in enumerate(sc_lib[vis]['gaintable_final']):
                    for vis_calto in vislist_calto:
                        if vis_calto.startswith(os.path.splitext(os.path.basename(vis))[0]):
                            # workaround a potential issue from heuristics.auto_selfcal when gaintable has only one element, when it's not a list of list.
                            spwmap_final = sc_lib[vis]['spwmap_final']

                            if any(not isinstance(spwmap, list) for spwmap in spwmap_final) or not spwmap_final:
                                spwmap_final = [spwmap_final]
                            gaintable = os.path.join(sc_workdir, sc_lib[vis]['gaintable_final'][idx])
                            calfrom = callibrary.CalFrom(gaintable=gaintable,
                                                         interp=sc_lib[vis]['applycal_interpolate_final'][idx], calwt=True,
                                                         spwmap=spwmap_final[idx], caltype='gaincal')
                            calto = callibrary.CalTo(vis=vis_calto, field=cleantarget['field'], spw=cleantarget['spw_real'][vis])
                            # applymode=sc_lib[vis]['applycal_mode_final']
                            calapps.append(callibrary.CalApplication(calto, calfrom))
                            vislist.append(vis_calto)

        for calapp in calapps:
            self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        vislist = sorted(set(vislist))
        taskqueue_parallel_request = len(vislist) > 1
        with TaskQueue(parallel=taskqueue_parallel_request, executor=self._executor) as tq:
            for vis in vislist:
                task_args = {'vis': vis, 'applymode': 'calflag'}
                tq.add_pipelinetask(IFApplycal, task_args, self.inputs.context)

        tq_results = tq.get_results()

        return tq_results

    def analyse(self, results):
        return results

    def _register_percleantarget_ms(self, vislist):
        """Register the per cleantarget MSes in the context and add the selfcal heuristics to the targets.
        
        note: not used in the current implementation.
        """

        # modify context and target_list to include the new measurement sets and selfcal heuristics.
        for vis in vislist:

            # add tmp ms to tmp context
            observing_run = tablereader.ObservingRunReader.get_observing_run(vis)
            for ms in observing_run.measurement_sets:
                ms.set_data_column(DataType.REGCAL_CONTLINE_SCIENCE, 'DATA')
                self.inputs.context.observing_run.add_measurement_set(ms)

        return

    def _get_scaltargets(self, scal=True):
        """Get the cleantarget list from the context.
        
        This essenially runs MakeImList and go through all nesscary steps to get the target list.
        However, it will pick up the selfcal heuristics from imageparams_factory,ImageParamsHeuristicsFactory
        """

        telescope = self.inputs.context.project_summary.telescope
        if telescope == 'ALMA':
            repr_ms = self.inputs.ms[0]
            diameter = min([a.diameter for a in repr_ms.antennas])
            if diameter == 7.0:
                telescope = 'ACA'
            else:
                telescope = 'ALMA'

        makeimlist_inputs = MakeImList.Inputs(self.inputs.context,
                                              vis=None,
                                              intent='TARGET',
                                              specmode='cont',
                                              clearlist=True,
                                              scal=scal, contfile=self.inputs.contfile,
                                              field=self.inputs.field,
                                              spw=self.inputs.spw)
        makeimlist_task = MakeImList(makeimlist_inputs)
        makeimlist_results = makeimlist_task.execute(dry_run=False)

        scal_targets = makeimlist_results.targets
        for scal_target in scal_targets:
            scal_target['sc_telescope'] = telescope

        return scal_targets

    def _remove_ms(self, vis):

        vis_dirs = [vis, vis+'.flagversions']
        for vis_dir in vis_dirs:
            if os.path.isdir(vis_dir):
                LOG.debug(f'removing {vis_dir}')
                self._executable.rmtree(vis_dir)

    def _split_scaltargets(self, scal_targets):
        """Split the input MSes into smaller MSes per cleantargets effeciently."""

        outputvis_list = []
        parallel = mpihelpers.parse_mpi_input_parameter(self.inputs.parallel)

        taskqueue_parallel_request = len(scal_targets) > 1 and parallel
        with utils.ignore_pointing(self.inputs.vis):
            with TaskQueue(parallel=taskqueue_parallel_request) as tq:

                for target in scal_targets:

                    vislist = []

                    spw_str = target['spw'].replace(',', '_')
                    sc_workdir = filenamer.sanitize(f'sc_workdir_{target["field"]}_spw{spw_str}')

                    if os.path.isdir(sc_workdir):
                        shutil.rmtree(sc_workdir)
                    os.mkdir(sc_workdir)

                    spw_real = {}
                    field = target['field']
                    uvrange = target['uvrange']
                    for vis in target['vis']:

                        # we use virtualspw here for the naming convention (similar to the imaging naming convention).
                        real_spwsel = self.inputs.context.observing_run.get_real_spwsel([target['spw']], [vis])[0]
                        spw_real[vis] = real_spwsel
                        outputvis = os.path.join(sc_workdir, os.path.basename(vis))
                        self._remove_ms(outputvis)

                        ms = self.inputs.context.observing_run.get_ms(vis)
                        spws = ms.get_spectral_windows(real_spwsel)

                        mean_freq = np.mean([float(spw.mean_frequency.to_units(measures.FrequencyUnits.HERTZ)) for spw in spws])
                        bwarray = np.array([float(spw.bandwidth.to_units(measures.FrequencyUnits.HERTZ)) for spw in spws])
                        chanarray = np.array([spw.num_channels for spw in spws])
                        chanwidth_desired_hz = self.get_desired_width(mean_freq)
                        chanbin = self.get_spw_chanbin(bwarray, chanarray, chanwidth_desired_hz)

                        task_args = {'vis': vis, 'outputvis': outputvis, 'field': field, 'spw': real_spwsel, 'uvrange': uvrange,
                                     'chanaverage': True, 'chanbin': chanbin, 'usewtspectrum': True,
                                     'datacolumn': 'data', 'reindex': False, 'keepflags': False}
                        outputvis_list.append((vis, outputvis))

                        tq.add_jobrequest(casa_tasks.mstransform, task_args, executor=self._executor)
                        vislist.append(os.path.basename(outputvis))

                    target['sc_workdir'] = sc_workdir
                    target['spw_real'] = spw_real
                    target['sc_vislist'] = vislist

        for outputvis in outputvis_list:
            # Copy across requisite XML files.
            self._copy_xml_files(outputvis[0], outputvis[1])

        return scal_targets

    @staticmethod
    def _copy_xml_files(vis, outputvis):
        for xml_filename in ['SpectralWindow.xml', 'DataDescription.xml']:
            vis_source = os.path.join(vis, xml_filename)
            outputvis_targets_contline = os.path.join(outputvis, xml_filename)
            if os.path.exists(vis_source) and os.path.exists(outputvis):
                LOG.info('Copying %s from original MS to science targets cont+line MS', xml_filename)
                LOG.trace('Copying %s: %s to %s', xml_filename, vis_source, outputvis_targets_contline)
                shutil.copyfile(vis_source, outputvis_targets_contline)

    @staticmethod
    def get_desired_width(meanfreq):
        """Get the desired channel width for the given mean frequency."""
        if meanfreq >= 50.0e9:
            chanwidth = 15.625e6
        elif (meanfreq < 50.0e9) and (meanfreq >= 40.0e9):
            chanwidth = 16.0e6
        elif (meanfreq < 40.0e9) and (meanfreq >= 26.0e9):
            chanwidth = 8.0e6
        elif (meanfreq < 26.0e9) and (meanfreq >= 18.0e9):
            chanwidth = 16.0e6
        elif (meanfreq < 18.0e9) and (meanfreq >= 8.0e9):
            chanwidth = 8.0e6
        elif (meanfreq < 8.0e9) and (meanfreq >= 4.0e9):
            chanwidth = 4.0e6
        elif (meanfreq < 4.0e9) and (meanfreq >= 2.0e9):
            chanwidth = 4.0e6
        elif (meanfreq < 4.0e9):
            chanwidth = 2.0e6
        return chanwidth

    @staticmethod
    def get_spw_chanbin(bwarray, chanarray, chanwidth=15.625e6):
        """Calculate the number of channels to average over for each spw.
        
        note: mstransform only accept chanbin as integer.
        """
        avgarray = [1]*len(bwarray)
        for idx, bw in enumerate(bwarray):
            nchan = bw/chanwidth
            nchan = np.round(nchan)
            avgarray[idx] = int(chanarray[idx]/nchan)
            if avgarray[idx] < 1.0:
                avgarray[idx] = 1
        return avgarray

    def _restore_flags(self):
        """Restore the before lineflagging flag state, after splitting per_cleantarget tmp MS."""
        for vis in self.inputs.vis:
            # restore to the starting flags
            # self._executable.initweights(vis=vis, wtmode='delwtsp')  # remove channelized weights
            if os.path.exists(vis+".flagversions/flags.before_hif_selfcal"):
                self._executable.flagmanager(vis=vis, mode='restore', versionname='before_hif_selfcal',
                                             comment='Flag states before hif_selfcal')

    def _flag_lines(self):
        """Flag the lines when cont.dat is present, before splitting per_cleantarget tmp MS."""

        for vis in self.inputs.vis:
            # self._executable.initweights(vis=vis, wtmode='weight', dowtsp=True)  # initialize channelized weights
            # save starting flags or restore to the starting flags
            if os.path.exists(vis+".flagversions/flags.before_hif_selfcal"):
                self._executable.flagmanager(vis=vis, mode='restore', versionname='before_hif_selfcal',
                                             comment='Flag states before hif_selfcal')
            else:
                self._executable.flagmanager(vis=vis, mode='save', versionname='before_hif_selfcal')

            # note that contfile_to_chansel will do the virtual2real spw translation automatically.
            lines_sel_dict = contfile_to_chansel(vis, self.inputs.context, contfile='cont.dat', excludechans=True)

            for field, lines_sel in lines_sel_dict.items():
                LOG.info("Flagging lines in field {} with the spw selection {}".format(field, lines_sel))
                self._executable.flagdata(vis=vis, field=field, mode='manual', spw=lines_sel, flagbackup=False, action='apply')
