from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.infrastructure import task_registry
from . import csvfilereader
from . import worker  # instead of "from pipeline.hsd.tasks.k2jycal import worker"
from pipeline.hsd.tasks.k2jycal import jyperkdbaccess

LOG = infrastructure.get_logger(__name__)


class SDAmpCalInputs(vdp.StandardInputs):

    reffile = vdp.VisDependentProperty(default='')

    @vdp.VisDependentProperty
    def infiles(self):
        return self.vis

    @infiles.convert
    def infiles(self, value):
        self.vis = value
        return value

    @vdp.VisDependentProperty
    def caltable(self):
        """
        Get the caltable argument for these inputs.

        If set to a table-naming heuristic, this should give a sensible name
        considering the current CASA task arguments.
        """
        namer = caltable_heuristic.AmpCaltable()
        # ignore caltable to avoid circular reference
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir,
                               stage=self.context.stage, **casa_args)

    def __init__(self, context, output_dir=None, infiles=None, caltable=None,
                 reffile=None, dbservice=None, endpoint=None):
        super(SDAmpCalInputs, self).__init__()

        # context and vis/infiles must be set first so that properties that require
        # domain objects can be function
        self.context = context
        self.infiles = infiles
        self.output_dir = output_dir

        # set the properties to the values given as input arguments
        self.caltable = caltable
        self.reffile = reffile
        self.dbservice = dbservice
        self.endpoint = endpoint


class SDAmpCalResults(basetask.Results):
    def __init__(self, vis=None, final=[], pool=[], reffile=None, factors={},
                 all_ok=False, dbstatus=None):
        super(SDAmpCalResults, self).__init__()

        self.vis = vis
        self.pool = pool[:]
        self.final = final[:]
        self.error = set()
        self.reffile = reffile
        self.factors = factors
        self.all_ok = all_ok
        self.dbstatus = dbstatus

    def merge_with_context(self, context):
        if not self.final:
            LOG.warn('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)
        # merge k2jy factor to context assing the value as an attribute of MS
        for vis, valid_k2jy in self.factors.iteritems():
            msobj = context.observing_run.get_ms(name=vis)
            msobj.k2jy_factor = {}
            for spwid, spw_k2jy in valid_k2jy.iteritems():
                for ant, ant_k2jy in spw_k2jy.iteritems():
                    for pol, pol_k2jy in ant_k2jy.iteritems():
                        msobj.k2jy_factor[(spwid, ant, pol)] = pol_k2jy

    def __repr__(self):
        # Format the Tsyscal results.
        s = 'SDAmpCalResults:\n'
        for calapplication in self.final:
            s += '\tBest caltable for spw #{spw} in {vis} is {name}\n'.format(
                spw=calapplication.spw, vis=os.path.basename(calapplication.vis),
                name=calapplication.gaintable)
        return s


@task_registry.set_equivalent_casa_task('hsd_k2jycal')
@task_registry.set_casa_commands_comment('The calibration tables of relative amplitude between beams are generated.')
class SDAmpCal(basetask.StandardTaskTemplate):
    Inputs = SDAmpCalInputs

    def prepare(self):
        inputs = self.inputs

        # obtain Jy/K factors
        factors_list = []
        reffile = None
        # dbstatus represents the response from the DB as well as whether or not
        # the task attempted to access the DB
        #
        #     dbstatus = None  -- not attempted to access (dbservice=False)
        #     dbstatus = True  -- the DB returned a factor (could be incomplete)
        #     dbstatus = False -- the DB didn't return a factor
        dbstatus = None
        if inputs.dbservice is True:
            # Try accessing Jy/K DB if dbservice is True
            reffile = 'jyperk_query.csv'
            factors_list = self._query_factors()
            if len(factors_list) > 0:
                dbstatus = True
                # export factors for future reference
                export_jyperk(reffile, factors_list)
            else:
                dbstatus = False

        if (inputs.dbservice is False) or (len(factors_list) == 0):
            # Read amplitude factor file
            reffile = os.path.abspath(os.path.expandvars(os.path.expanduser(inputs.reffile)))
            factors_list = self._read_factors(reffile)

        LOG.debug('factors_list=%s' % factors_list)
        if len(factors_list) == 0:
            LOG.warn('No factor file available')
            return SDAmpCalResults(vis=os.path.basename(inputs.vis), pool=[])
#            return SDK2JyCalResults(vis=os.path.basename(inputs.vis), pool=[])

        # generate scaling factor dictionary
        factors = rearrange_factors_list(factors_list)

        callist = []
        valid_factors = {}
        all_factors_ok = True
        # Loop over MS and generate a caltable per MS
        ampcal_inputs = worker.SDAmpCalWorker.Inputs(inputs.context, inputs.output_dir, inputs.vis,
                                                       inputs.caltable, factors)
        ampcal_task = worker.SDAmpCalWorker(ampcal_inputs)
        ampcal_result = self._executor.execute(ampcal_task)
        if ampcal_result.calapp is not None:
            callist.append(ampcal_result.calapp)
        valid_factors[ampcal_result.vis] = ampcal_result.ms_factors
        all_factors_ok &= ampcal_result.factors_ok

        return SDAmpCalResults(vis=ampcal_result.vis, pool=callist, reffile=reffile,
                                factors=valid_factors, all_ok=all_factors_ok,
                                dbstatus=dbstatus)

    def analyse(self, result):
        # With no best caltable to find, our task is simply to set the one
        # caltable as the best result

        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.pool
                   if ca.exists() or self._executor._dry_run]
        result.final[:] = on_disk

        missing = [ca for ca in result.pool
                   if ca not in on_disk and not self._executor._dry_run]
        result.error.clear()
        result.error.update(missing)

        return result

    def _read_factors(self, reffile):
        inputs = self.inputs
        if not os.path.exists(inputs.reffile):
            return []
        # read scaling factor list
        factors_list = csvfilereader.read(inputs.context, reffile)
#        factors_list = jyperkreader.read(inputs.context, reffile)
        return factors_list

    def _query_factors(self):
        vis = os.path.basename(self.inputs.vis)

        # switch implementation class according to endpoint parameter
        endpoint = self.inputs.endpoint
        if endpoint == 'asdm':
            impl = jyperkdbaccess.JyPerKAsdmEndPoint
        elif endpoint == 'model-fit':
            impl = jyperkdbaccess.JyPerKModelFitEndPoint
        elif endpoint == 'interpolation':
            impl = jyperkdbaccess.JyPerKInterpolationEndPoint
        else:
            raise RuntimeError('Invalid endpoint: {}'.format(endpoint))
        query = impl(self.inputs.context)
        try:
            factors_list = query.getJyPerK(vis)

            # warn if result is empty
            if len(factors_list) == 0:
                LOG.warn('{}: Query to Jy/K DB returned empty result. Will fallback to reading CSV file.'.format(vis))
        except Exception as e:
            LOG.warn('{}: Query to Jy/K DB was failed due to the following error. Will fallback to reading CSV file.'.format(vis))
            LOG.warn(str(e))
            factors_list = []
        return factors_list


def rearrange_factors_list(factors_list):
    """
    Rearrange scaling factor list to dictionary which looks like
    {'MS': {'spw': {'Ant': {'pol': factor}}}}
    """
    factors = {}
    for (vis, ant, spw, pol, _factor) in factors_list:
        spwid = int(spw)
        factor = float(_factor)
        if vis in factors:
            if spwid in factors[vis]:
                if ant in factors[vis][spwid]:
                    if pol in factors[vis][spwid][ant]:
                        LOG.info('There are duplicate rows in reffile, use %s instead of %s for (%s,%s,%s,%s)' %
                                 (factors[vis][spwid][ant][pol], factor, vis, spwid, ant, pol))
                        factors[vis][spwid][ant][pol] = factor
                    else:
                        factors[vis][spwid][ant][pol] = factor
                else:
                    factors[vis][spwid][ant] = {pol: factor}
            else:
                factors[vis][spwid] = {ant: {pol: factor}}
        else:
            factors[vis] = {spwid: {ant: {pol: factor}}}

    return factors


def export_jyperk(outfile, factors):
    if not os.path.exists(outfile):
        # create file with header information
        with open(outfile, 'w') as f:
            f.write('MS,Beam,Spwid,Polarization,Factor\n')

    with open(outfile, 'a') as f:
        for row in factors:
            f.write('{}\n'.format(','.join(row)))
