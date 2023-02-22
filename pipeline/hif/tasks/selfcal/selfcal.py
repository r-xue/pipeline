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
from pipeline.infrastructure import callibrary, casa_tasks, task_registry, utils
from pipeline.infrastructure.contfilehandler import contfile_to_chansel
from pipeline.infrastructure.mpihelpers import TaskQueue

LOG = infrastructure.get_logger(__name__)


class SelfcalResults(basetask.Results):
    def __init__(self, targets):
        super().__init__()
        self.pipeline_casa_task = 'Selfcal'
        self.targets = targets

    def merge_with_context(self, context):
        """See :method:`~pipeline.infrastructure.api.Results.merge_with_context`."""
        self._register_datatypes(context)

        return

    def _register_datatypes(self, context):
        vislist = []
        for target in self.targets:
            vislist.extend(target['sc_vislist'])
        vislist = list(set(vislist))

        for vis in vislist:
            ms = context.observing_run.get_ms(vis)
            ms.set_data_column(DataType.SELFCAL_CONTLINE_SCIENCE, 'CORRECTED_DATA')

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

    def __init__(self, context, vis=None, field=None, spw=None, contfile=None, apply=None, parallel=None):
        super().__init__()
        self.context = context
        self.vis = vis
        self.field = field
        self.spw = spw
        self.contfile = contfile
        self.apply = apply
        self.parallel = parallel


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

        self.telescope = self.inputs.context.project_summary.telescope
        # if self.telescope == 'ALMA':
        #     repr_ms = self.inputs.ms[0]
        #     diameter = min([a.diameter for a in repr_ms.antennas])
        #     if diameter == 7.0:
        #         self.telescope = 'ACA'
        #     else:
        #         self.telescope = 'ALMA'

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

        # collect the target list
        cleantargets_sc = self._get_cleantargets(self.inputs.context, scal=True)

        # split percleantarget MSes with spectral line flagged
        self._flag_lines()
        self._split_cleantargets(cleantargets_sc)
        self._restore_flags()
        # import pprint as pp
        # pp.pprint('*'*120)
        # pp.pprint(cleantargets_sc)
        # pp.pprint('*'*120)

        # # register the percleantarget MSes
        # self._register_percleantarget_ms(vislist)

        # # collect the target list based on the percleantarget MSes
        # cleantargets_scal = self._get_cleantargets(self.inputs.context, vislist=vislist, scal=True)

        # # start the selfcal sequence.

        tclean_parallel_request = mpihelpers.parse_mpi_input_parameter(self.inputs.parallel)
        taskqueue_parallel_request = len(cleantargets_sc) > 1

        with TaskQueue(parallel=taskqueue_parallel_request) as tq:
            for target in cleantargets_sc:
                target['parallel'] = (tclean_parallel_request and not tq.is_async())
                tq.add_functioncall(self._run_selfcal_sequence, target)
        tq_results = tq.get_results()

        for idx, target in enumerate(cleantargets_sc):
            scal_library, solints, bands = tq_results[idx]
            if scal_library is None:
                raise ValueError(f'auto_selfcal failed for target {target["field"]}.')
            target['sc_band'] = bands[0]
            target['sc_solints'] = solints[bands[0]]
            # note scal_library is keyed by field name without quotes at this moment.
            # see. https://casadocs.readthedocs.io/en/stable/notebooks/visibility_data_selection.html#The-field-Parameter
            #       utils.fieldname_for_casa() and 
            #       utils.dequote_fieldname_for_casa()
            field_name = utils.dequote(target['field'])
            target['sc_lib'] = scal_library[field_name][target['sc_band']]
            target['field_name'] = field_name

        self._apply_scal(cleantargets_sc)

        return SelfcalResults(cleantargets_sc)

    @staticmethod
    def _run_selfcal_sequence(cleantarget):

        workdir = os.path.abspath('./')
        selfcal_library, solints, bands = None, None, None

        try:
            os.chdir(cleantarget['sc_workdir'])
            LOG.info('Running auto_selfcal heuristics on target {0} spw {1} from {2}/'.format(
                cleantarget['field'], cleantarget['spw'], cleantarget['sc_workdir']))
            selfcal_library, solints, bands = auto_selfcal.selfcal_workflow(cleantarget)
        except Exception as e:
            LOG.error('Exception from hif.heuristics.auto_selfcal.')
            LOG.error(str(e))
            LOG.debug(traceback.format_exc())
        finally:
            os.chdir(workdir)
            if cleantarget['parallel']:
                # parallel=True sugguests that we are running tclean(parallel=true in a sequential TaskQueue.
                # A side effect of this is that the working directory of MPIServers will be "stuck" to the one where tclean(paralllel=True) started.
                # As a workaround, we send the chdir command to the MPIServers exeplicitly.
                mpihelpers.mpiclient.push_command_request(f'os.chdir({workdir!r})', block=True, target_server=mpihelpers.mpi_server_list)
                
        return selfcal_library, solints, bands

    def _apply_scal_old(self, sc_targets):

        with open('applycal_to_orig_MSes.py', 'w') as applyCalOut:

            for cleantarget in sc_targets:

                cleantarget['spw_real'] = cleantarget['spw']
                sc_lib = cleantarget['sc_lib']
                vislist = sc_lib['vislist']

                calapps = []
                if sc_lib['SC_success']:
                    for vis in vislist:
                        solint = sc_lib['final_solint']
                        iteration = sc_lib[vis][solint]['iteration']
                        line = 'applycal(vis="' + vis.replace('.selfcal', '') + '",gaintable=' + str(
                            sc_lib[vis]['gaintable_final']) + ',interp=' + str(
                            sc_lib[vis]['applycal_interpolate_final']) + ', calwt=True,spwmap=' + str(
                            sc_lib[vis]['spwmap_final']) + ', applymode="' + sc_lib[vis]['applycal_mode_final'] + '",field="' + cleantarget['field'] + '",spw="' + cleantarget['spw_real'] + '")\n'
                        applyCalOut.writelines(line)

    def _apply_scal(self, sc_targets):

        calapps = []
        vislist = []
        for cleantarget in sc_targets:

            cleantarget['spw_real'] = cleantarget['spw']
            sc_lib = cleantarget['sc_lib']
            sc_workdir = cleantarget['sc_workdir']
            sc_vislist = sc_lib['vislist']
            vislist.extend(sc_vislist)

            if sc_lib['SC_success']:
                for vis in sc_vislist:
                    # workaround a potential issue from heuristics.auto_selfcal when gaintable has only one element, when it's not a list of list.
                    spwmap_final = sc_lib[vis]['spwmap_final']
                    if any(not isinstance(spwmap, list) for spwmap in spwmap_final) or not spwmap_final:
                        spwmap_final = [spwmap_final]
                    for idx, gaintable in enumerate(sc_lib[vis]['gaintable_final']):
                        gaintable = os.path.join(sc_workdir, sc_lib[vis]['gaintable_final'][idx])
                        calfrom = callibrary.CalFrom(gaintable=gaintable,
                                                     interp=sc_lib[vis]['applycal_interpolate_final'][idx], calwt=True,
                                                     spwmap=spwmap_final[idx], caltype='gaincal')
                        calto = callibrary.CalTo(vis=vis, field=cleantarget['field'], spw=cleantarget['spw_real'])
                        # applymode=sc_lib[vis]['applycal_mode_final']
                        calapps.append(callibrary.CalApplication(calto, calfrom))

        for calapp in calapps:
            self.inputs.context.callibrary.add(calapp.calto, calapp.calfrom)

        with TaskQueue(executor=self._executor) as tq:

            for vis in set(vislist):
                task_args = {'vis': vis, 'applymode': 'calflag'}
                tq.add_pipelinetask(IFApplycal, task_args, self.inputs.context)

        tq_results = tq.get_results()

        return tq_results

    def analyse(self, results):
        return results

    def _register_percleantarget_ms(self, vislist):
        """Register the per cleantarget MSes in the context and add the selfcal heuristics to the targets."""

        # modify context and target_list to include the new measurement sets and selfcal heuristics.
        for vis in vislist:

            # add tmp ms to tmp context
            observing_run = tablereader.ObservingRunReader.get_observing_run(vis)
            for ms in observing_run.measurement_sets:
                ms.set_data_column(DataType.REGCAL_CONTLINE_SCIENCE, 'DATA')
                self.inputs.context.observing_run.add_measurement_set(ms)

        return

    def _get_cleantargets(self, context=None, vislist=None, scal=True):
        """Get the cleantarget list from the context.
        
        This essenially runs MakeImList and go through all nesscary steps to get the target list.
        However, it will pick up the selfcal heuristics from imageparams_factory,ImageParamsHeuristicsFactory
        """
        makeimlist_inputs = MakeImList.Inputs(self.inputs.context,
                                              vis=vislist,
                                              intent='TARGET',
                                              specmode='cont',
                                              clearlist=True,
                                              scal=scal, contfile=self.inputs.contfile,
                                              field=self.inputs.field,
                                              spw=self.inputs.spw)
        makeimlist_task = MakeImList(makeimlist_inputs)
        makeimlist_results = makeimlist_task.execute(dry_run=False)

        # pp.pprint('*'*120)
        # pp.pprint(makeimlist_results.targets)
        # pp.pprint('*'*120)
        # pp.pprint(makeimlist_results.clean_list_info)
        # pp.pprint('*'*120)

        return makeimlist_results.targets

    def _remove_ms(self, vis):

        vis_dirs = [vis, vis+'.flagversions']
        for vis_dir in vis_dirs:
            if os.path.isdir(vis_dir):
                LOG.debug(f'removing {vis_dir}')
                self._executable.rmtree(vis_dir)

    def _split_cleantargets(self, targets):
        """Split the input MSes into smaller MSes per cleantargets effeciently."""

        vislist = []
        parallel = mpihelpers.parse_mpi_input_parameter(self.inputs.parallel)

        with utils.ignore_pointing(self.inputs.vis):
            with TaskQueue(parallel=parallel) as tq:

                for target in targets:

                    vislist = []

                    spw_str = target['spw'].replace(',', '_')
                    sc_workdir = filenamer.sanitize(f'sc_workdir_{target["field"]}_spw{spw_str}')

                    if os.path.isdir(sc_workdir):
                        shutil.rmtree(sc_workdir)
                    os.mkdir(sc_workdir)

                    for vis in target['vis']:

                        field = target['field']
                        # we use virtualspw here for the naming convention (similar to the imaging naming convention).
                        real_spwsel = self.inputs.context.observing_run.get_real_spwsel([target['spw']], [vis])[0]
                        outputvis = os.path.join(sc_workdir, os.path.basename(vis))
                        self._remove_ms(outputvis)

                        ms = self.inputs.context.observing_run.get_ms(vis)
                        spws = ms.get_spectral_windows(real_spwsel)

                        mean_freq = np.mean([float(spw.mean_frequency.to_units(measures.FrequencyUnits.HERTZ)) for spw in spws])
                        bwarray = np.array([float(spw.bandwidth.to_units(measures.FrequencyUnits.HERTZ)) for spw in spws])
                        chanarray = np.array([spw.num_channels for spw in spws])
                        chanwidth_desired_hz = self.get_desired_width(mean_freq)
                        chanbin = self.get_spw_chanbin(bwarray, chanarray, chanwidth_desired_hz)

                        task_args = {'vis': vis, 'outputvis': outputvis, 'field': field, 'spw': real_spwsel,
                                     'chanaverage': True, 'chanbin': chanbin, 'usewtspectrum': True,
                                     'datacolumn': 'data', 'reindex': False, 'keepflags': False}

                        tq.add_jobrequest(casa_tasks.mstransform, task_args, executor=self._executor)
                        vislist.append(os.path.basename(outputvis))
                    target['sc_workdir'] = sc_workdir
                    target['sc_vislist'] = vislist

        return targets

    @staticmethod
    def get_desired_width(meanfreq):
        if meanfreq >= 50.0e9:
            desiredWidth = 15.625e6
        elif (meanfreq < 50.0e9) and (meanfreq >= 40.0e9):
            desiredWidth = 16.0e6
        elif (meanfreq < 40.0e9) and (meanfreq >= 26.0e9):
            desiredWidth = 8.0e6
        elif (meanfreq < 26.0e9) and (meanfreq >= 18.0e9):
            desiredWidth = 16.0e6
        elif (meanfreq < 18.0e9) and (meanfreq >= 8.0e9):
            desiredWidth = 8.0e6
        elif (meanfreq < 8.0e9) and (meanfreq >= 4.0e9):
            desiredWidth = 4.0e6
        elif (meanfreq < 4.0e9) and (meanfreq >= 2.0e9):
            desiredWidth = 4.0e6
        elif (meanfreq < 4.0e9):
            desiredWidth = 2.0e6
        return desiredWidth

    @staticmethod
    def get_spw_chanbin(bwarray, chanarray, desiredWidth=15.625e6):
        """Calculate the number of channels to average over for each spw.
        
        note: mstransform only accept chanbin as integer.
        """
        avgarray = [1]*len(bwarray)
        for i in range(len(bwarray)):
            nchan = bwarray[i]/desiredWidth
            nchan = np.round(nchan)
            avgarray[i] = int(chanarray[i]/nchan)
            if avgarray[i] < 1.0:
                avgarray[i] = 1
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
