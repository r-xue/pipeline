import collections
import copy
import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
from pipeline.infrastructure.utils import relative_path
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry
from ..common import SingleDishResults

LOG = infrastructure.get_logger(__name__)


class SDSkyCalInputs(vdp.StandardInputs):
    calmode = vdp.VisDependentProperty(default='auto')
    elongated = vdp.VisDependentProperty(default=False)
    field = vdp.VisDependentProperty(default='')
    fraction = vdp.VisDependentProperty(default='10%')
    noff = vdp.VisDependentProperty(default=-1)
    outfile = vdp.VisDependentProperty(default='')
    scan = vdp.VisDependentProperty(default='')
    spw = vdp.VisDependentProperty(default='')
    width = vdp.VisDependentProperty(default=0.5)

    @vdp.VisDependentProperty
    def infiles(self):
        return self.vis

    @infiles.convert
    def infiles(self, value):
        self.vis = value
        return value

    def __init__(self, context, calmode=None, fraction=None, noff=None, width=None, elongated=None, output_dir=None,
                 infiles=None, outfile=None, field=None, spw=None, scan=None):
        super(SDSkyCalInputs, self).__init__()

        # context and vis must be set first so that properties that require
        # domain objects can be function
        self.context = context
        self.infiles = infiles
        self.output_dir = output_dir
        self.outfile = outfile

        self.calmode = calmode
        self.fraction = fraction
        self.noff = noff
        self.width = width
        self.elongated = elongated

        self.field = field
        self.spw = spw
        self.scan = scan

    def to_casa_args(self):
        args = super(SDSkyCalInputs, self).to_casa_args()

        # overwrite is always True
        args['overwrite'] = True

        # parameter name for input data is 'infile'
        args['infile'] = args.pop('infiles')

        # vis is not necessary
        del args['vis']

        return args


class SDSkyCalResults(SingleDishResults):
    """
    """
    def __init__(self, task=None, success=None, outcome=None):
        super(SDSkyCalResults, self).__init__(task, success, outcome)
        self.final = self.outcome

    def merge_with_context(self, context):
        super(SDSkyCalResults, self).merge_with_context(context)

        if self.outcome is None:
            return

        for calapp in self.outcome:
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def _outcome_name(self):
        return str(self.outcome)


#@task_registry.set_equivalent_casa_task('hsd_skycal')
#@task_registry.set_casa_commands_comment('Generates sky calibration table according to calibration strategy.')
class SerialSDSkyCal(basetask.StandardTaskTemplate):
    Inputs = SDSkyCalInputs
    ElevationDifferenceThreshold = 3.0  # deg

    def prepare(self):
        args = self.inputs.to_casa_args()
        LOG.trace('args: {}'.format(args))

        # retrieve ms domain object
        ms = self.inputs.ms
        calibration_strategy = ms.calibration_strategy
        default_field_strategy = calibration_strategy['field_strategy']

        # take calmode from calibration strategy if it is set to 'auto'
        if args['calmode'] is None or args['calmode'].lower() == 'auto':
            args['calmode'] = calibration_strategy['calmode']

        assert args['calmode'] in ['ps', 'otfraster', 'otf']

        # spw selection ---> task.prepare
        if args['spw'] is None or len(args['spw']) == 0:
            spw_list = ms.get_spectral_windows(science_windows_only=True)
            args['spw'] = ','.join(map(str, [spw.id for spw in spw_list]))

        # field selection ---> task.prepare
        if args['field'] is None or len(args['field']) == 0:
            field_strategy = default_field_strategy
        else:
            field_strategy = {}
            field_ids = casa_tools.ms.msseltoindex(vis=ms.name, field=args['field'])
            for field_id in field_ids:
                for target_id, reference_id in default_field_strategy.items():
                    if field_id == target_id:
                        field_strategy[field_id] = default_field_strategy[field_id]
                        continue
                    elif field_id == reference_id:
                        field_strategy[target_id] = field_id
                        continue

        # scan selection 
        if args['scan'] is None:
            args['scan'] = ''

        # intent
        if args['calmode'] in ['otf', 'otfraster']:
            args['intent'] = 'OBSERVE_TARGET#ON_SOURCE'

        calapps = []
        for target_id, reference_id in field_strategy.items():
            myargs = copy.deepcopy(args)

            # output file
            reference_field_name = ms.get_fields(reference_id)[0].clean_name
            if myargs['outfile'] is None or len(myargs['outfile']) == 0:
                namer = caltable_heuristic.SDSkyCaltable()
                # filenamer requires field name instead of id
                myargs['field'] = reference_field_name
                try:
                    # we temporarily need 'vis'
                    myargs['vis'] = myargs['infile']
                    myargs['outfile'] = relative_path(namer.calculate(output_dir=self.inputs.output_dir,
                                                                      stage=self.inputs.context.stage,
                                                                      **myargs))
                finally:
                    del myargs['vis']
            else:
                myargs['outfile'] = myargs['outfile'] + '.{}'.format(reference_field_name)

            # field
            myargs['field'] = str(reference_id)

            LOG.debug('args for sdcal: {}'.format(myargs))

            # create job
            job = casa_tasks.sdcal(**myargs)

            # execute job
            LOG.debug('Table cache before sdcal: {}'.format(casa_tools.table.showcache()))
            try:
                self._executor.execute(job)
            finally:
                LOG.debug('Table cache after sdcal: {}'.format(casa_tools.table.showcache()))

            # check if caltable is empty
            with casa_tools.TableReader(myargs['outfile']) as tb:
                is_caltable_empty = tb.nrows() == 0
            if is_caltable_empty:
                continue

            # make a note of the current inputs state before we start fiddling
            # with it. This origin will be attached to the final CalApplication.
            origin = callibrary.CalAppOrigin(task=SerialSDSkyCal,
                                             inputs=args)

            calto = callibrary.CalTo(vis=myargs['infile'],
                                     spw=myargs['spw'],
                                     field=str(target_id),
                                     intent='TARGET')

            # create SDCalFrom object
            calfrom = callibrary.CalFrom(gaintable=myargs['outfile'],
                                         gainfield=str(reference_id),
                                         interp='linear,linear',
                                         caltype=myargs['calmode'])

            # create CalApplication object
            calapp = callibrary.CalApplication(calto, calfrom, origin)
            calapps.append(calapp)

        results = SDSkyCalResults(task=self.__class__,
                                  success=True,
                                  outcome=calapps)
        return results

    def analyse(self, result):

        # compute elevation difference between ON and OFF and 
        # warn if it exceeds elevation threshold
        threshold = self.ElevationDifferenceThreshold
        context = self.inputs.context
        resultdict = compute_elevation_difference(context, result)
        ms = self.inputs.ms

        spectralspecs = ms.get_spectral_specs()
        spectralspecs_spwids = {}
        for ss in spectralspecs:
            spw_ss = ms.get_spectral_windows(science_windows_only=True, spectralspecs = [ss])
            list_spwids = [x.id for x in spw_ss]
            if len(list_spwids) == 0:
                continue
            else:
                spectralspecs_spwids[ss] = list_spwids

        for list_spwids in spectralspecs_spwids.values():
            spwid0 = list_spwids[0]
            for field_id, eldfield in resultdict.items():
                for antenna_id, eldant in eldfield.items():
                    assert spwid0 in eldant
                    eldiff0 = eldant[spwid0].eldiff0
                    eldiff1 = eldant[spwid0].eldiff1
                    eldiff = numpy.append(eldiff0, eldiff1)
                    if len(eldiff) > 0:
                        eldmax = numpy.max(numpy.abs(eldiff))
                        if eldmax >= threshold:
                            field_name = ms.fields[field_id].name
                            antenna_name = ms.antennas[antenna_id].name
                            LOG.warning('Elevation difference between ON and OFF for {} field {} antenna {} spw {} was {}deg'
                                        ' exceeding the threshold of {}deg'
                                        ''.format(ms.basename, field_name, antenna_name, list_spwids, eldmax, threshold))

        return result


class HpcSDSkyCalInputs(SDSkyCalInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, calmode=None, fraction=None, noff=None, width=None, elongated=None, output_dir=None,
                 infiles=None, outfile=None, field=None, spw=None, scan=None, parallel=None):
        super(HpcSDSkyCalInputs, self).__init__(context,
                                                calmode=calmode,
                                                fraction=fraction,
                                                noff=noff,
                                                width=width,
                                                elongated=elongated,
                                                output_dir=output_dir,
                                                infiles=infiles,
                                                outfile=outfile,
                                                field=field,
                                                spw=spw,
                                                scan=scan)
        self.parallel = parallel


# @task_registry.set_equivalent_casa_task('hpc_hsd_skycal')
@task_registry.set_equivalent_casa_task('hsd_skycal')
@task_registry.set_casa_commands_comment('Generates sky calibration table according to calibration strategy.')
class HpcSDSkyCal(sessionutils.ParallelTemplate):
    Inputs = HpcSDSkyCalInputs
    Task = SerialSDSkyCal

    def __init__(self, inputs):
        super(HpcSDSkyCal, self).__init__(inputs)

    @basetask.result_finaliser
    def get_result_for_exception(self, vis, exception):
        LOG.error('Error operating sky calibration for {!s}'.format(os.path.basename(vis)))
        LOG.error('{0}({1})'.format(exception.__class__.__name__, str(exception)))
        import traceback
        tb = traceback.format_exc()
        if tb.startswith('None'):
            tb = '{0}({1})'.format(exception.__class__.__name__, str(exception))
        return basetask.FailedTaskResults(self.__class__, exception, tb)


def compute_elevation_difference(context, results):
    """
    Compute elevation difference 

    Returns:
        dictionary[field_id][antenna_id][spw_id]

        Value of the dictionary should be ElevationDifference and the value should 
        contain the result from one MS (given that SDSkyCal is per-MS task)
    """
    ElevationDifference = collections.namedtuple('ElevationDifference', 
                                                 ['timeon', 'elon', 'timecal', 'elcal', 
                                                  'time0', 'eldiff0', 'time1', 'eldiff1'])

    if not isinstance(results, SDSkyCalResults):
        raise TypeError('Results type should be SDSkyCalResults')

    calapps = results.outcome

    resultdict = {}

    for calapp in calapps:
        calto = calapp.calto
        vis = calto.vis
        ms = context.observing_run.get_ms(vis)
        target_field = calto.field
        if target_field.isdigit():
            field_id_on = int(target_field)
        else:
            fields = ms.get_fields(name=target_field)
            assert len(fields) > 0
            field_id_on = fields[0].id

        #if ms.basename not in resultdict:
        #    resultdict[ms.basename] = {}

        antenna_ids = [ant.id for ant in ms.antennas]

        # representative spw
        science_spw = ms.get_spectral_windows(science_windows_only=True)
#         # choose representative spw based on representative frequency if it is available
#         if hasattr(ms, 'representative_target') and ms.representative_target[1] is not None:
#             qa = casa_tools.quanta
#             rep_freq = ms.representative_target[1]
#             centre_freqs = [qa.quantity(spw.centre_frequency.str_to_precision(16)) for spw in science_spw]
#             freq_diffs = [abs(qa.sub(cf, rep_freq).convert('Hz')['value']) for cf in centre_freqs]
#             spw_id = science_spw[numpy.argmin(freq_diffs)].id
#         else:
#             spw_id = science_spw[0].id

        calfroms = calapp.calfrom

        for calfrom in calfroms:
            caltable = calfrom.gaintable

            # FIELD_ID
            gainfield = calfrom.gainfield
            if gainfield.isdigit():
                field_id_off = int(gainfield)
            else:
                fields = ms.get_fields(name=gainfield)
                assert len(fields) > 0
                field_id_off = fields[0].id
            LOG.info('Computing elevation difference for "{}" Field ID {} (ON) and {} (OFF)'
                     ''.format(ms.basename, field_id_on, field_id_off))

            resultfield = {}

            for antenna_id in antenna_ids:

                resultant = {}

                for spw in science_spw:
                    spw_id = spw.id

                    # get timestamp from caltable
                    with casa_tools.TableReader(caltable) as tb:
                        selected = tb.query('SPECTRAL_WINDOW_ID=={}&&ANTENNA1=={}'.format(spw_id, antenna_id))
                        timecal = selected.getcol('TIME') / 86400.0  # sec -> day
                        selected.close()

                    # access DataTable to get elevation 
                    ro_datatable_name = os.path.join(context.observing_run.ms_datatable_name,
                                                     os.path.basename(ms.origin_ms), 'RO')
                    with casa_tools.TableReader(ro_datatable_name) as tb:
                        selected = tb.query('IF=={}&&ANTENNA=={}&&FIELD_ID=={}&&SRCTYPE==0'
                                            ''.format(spw_id, antenna_id, field_id_on))
                        timeon = selected.getcol('TIME')
                        elon = selected.getcol('EL')
                        selected.close()
                        selected = tb.query('IF=={}&&ANTENNA=={}&&FIELD_ID=={}&&SRCTYPE!=0'
                                            ''.format(spw_id, antenna_id, field_id_off))
                        timeoff = selected.getcol('TIME')
                        eloff = selected.getcol('EL')
                        selected.close()

                    elcal = eloff[[numpy.argmin(numpy.abs(timeoff - t)) for t in timecal]]

                    del timeoff, eloff

                    eldiff0 = []
                    eldiff1 = []
                    time0 = []
                    time1 = []
                    for t, el in zip(timeon, elon):
                        dt = timecal - t
                        idx0 = numpy.where(dt < 0)[0]
                        if len(idx0) > 0:
                            i = numpy.argmax(timecal[idx0])
                            time0.append(t)
                            eldiff0.append(el - elcal[idx0[i]])
                        idx1 = numpy.where(dt >= 0)[0]
                        if len(idx1) > 0:
                            i = numpy.argmin(timecal[idx1])
                            time1.append(t)
                            eldiff1.append(el - elcal[idx1[i]])
                    eldiff0 = numpy.asarray(eldiff0)
                    eldiff1 = numpy.asarray(eldiff1)
                    time0 = numpy.asarray(time0)
                    time1 = numpy.asarray(time1)

                    result = ElevationDifference(timeon=timeon, elon=elon, 
                                                 timecal=timecal, elcal=elcal,
                                                 time0=time0, eldiff0=eldiff0,
                                                 time1=time1, eldiff1=eldiff1)

                    resultant[spw_id] = result

                resultfield[antenna_id] = resultant

            resultdict[field_id_on] = resultfield

    return resultdict
