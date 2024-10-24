from casatasks.private import tec_maps

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as caltable_heuristic
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)


class TecMapsInputs(vdp.StandardInputs):
    show_tec_maps = vdp.VisDependentProperty(default=True)
    apply_tec_correction = vdp.VisDependentProperty(default=False)

    @vdp.VisDependentProperty
    def caltable(self):
        namer = caltable_heuristic.TecMapstable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)

    @vdp.VisDependentProperty
    def parameter(self):
        return []

    def __init__(self, context, output_dir=None, vis=None, show_tec_maps=None, apply_tec_correction=None,
                 caltable=None, caltype=None, parameter=None):
        super(TecMapsInputs, self).__init__()
        self.context = context
        self.output_dir = output_dir
        self.vis = vis
        self.show_tec_maps = show_tec_maps
        self.apply_tec_correction = apply_tec_correction
        self.parameter = parameter
        self.caltable = caltable
        self.caltype = caltype

    def to_casa_args(self):
        args = super(TecMapsInputs, self).to_casa_args()
        args['caltype'] = 'tecim'
        return args


class TecMapsResults(basetask.Results):
    def __init__(self, final=None, pool=None, preceding=None, tec_image=None, tec_rms_image=None,
                 tec_plotfile=None):

        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(TecMapsResults, self).__init__()

        self.vis = None
        self.pool = pool[:]
        self.final = final[:]
        self.preceding = preceding[:]
        self.error = set()
        self.tec_image = tec_image
        self.tec_rms_image = tec_rms_image
        self.tec_plotfile = tec_plotfile

    def merge_with_context(self, context):
        if not self.final:
            LOG.error('No results to merge')
            return

        for calapp in self.final:
            LOG.debug('Adding calibration to callibrary:\n'
                      '%s\n%s' % (calapp.calto, calapp.calfrom))
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        # Format the GainCurve results.
        s = 'TecMapsResults:\n'
        for calapplication in self.final:
            s += '\tTecMaps caltable written to {name}\n'.format(
                name=calapplication.gaintable)
        return s


@task_registry.set_equivalent_casa_task('hifv_tecmaps')
class TecMaps(basetask.StandardTaskTemplate):
    Inputs = TecMapsInputs

    def prepare(self):
        inputs = self.inputs

        tec_image = None
        tec_rms_image = None

        if self.inputs.show_tec_maps or self.inputs.apply_tec_correction:

            callist = []
            try:
                tec_image, tec_rms_image, tec_plotfile = tec_maps.create(vis=inputs.vis, doplot=True, imname='iono')
            except UnboundLocalError as e:
                LOG.warning("TEC information or retrieval service is unavailable")
                LOG.warning("    CASA error: {!s}".format(e))

                return TecMapsResults(pool=callist, final=callist, tec_image=None,
                                      tec_rms_image=None, tec_plotfile=None)

            if self.inputs.apply_tec_correction:
                gencal_args = inputs.to_casa_args()
                gencal_args.pop('show_tec_maps')
                gencal_args.pop('apply_tec_correction')
                gencal_args['infile'] = tec_image
                gencal_job = casa_tasks.gencal(**gencal_args)
                self._executor.execute(gencal_job)

                calto = callibrary.CalTo(vis=inputs.vis)
                calfrom = callibrary.CalFrom(gencal_args['caltable'], caltype='tecim', interp='', calwt=False)
                calapp = callibrary.CalApplication(calto, calfrom)
                callist.append(calapp)

            return TecMapsResults(pool=callist, final=callist, tec_image=tec_image, tec_rms_image=tec_rms_image,
                                  tec_plotfile=tec_plotfile)
        else:
            return None

    def analyse(self, result):
        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.pool if ca.exists()]
        result.final[:] = on_disk

        missing = [ca for ca in result.pool if ca not in on_disk]
        result.error.clear()
        result.error.update(missing)

        return result

    def _do_tecmaps(self):
        # Private class method for reference only
        # tec_image, tec_rms_image = tec_maps.create('vlass3C48.ms')
        try:
            tec_maps.create(vis=self.vis, doplot=True, imname='iono')
        except UnboundLocalError as e:
            LOG.warning("TEC Maps error returned. CASA {!s}".format(e))
            return None
        # gencal_job = casa_tasks.gencal(**gencal_args)
        gencal_job = casa_tasks.gencal(vis=self.vis, caltable='file.tec', caltype='tecim', infile='iono.IGS_TEC.im')
        self._executor.execute(gencal_job)
