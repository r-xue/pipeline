import collections
import os

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.renderer.logger as logger
from ..common import utils
from ..common import compress
from ..common import atmutil
from ..common import display
from ..common.display import sd_polmap

_LOG = infrastructure.get_logger(__name__)
LOG = utils.OnDemandStringParseLogger(_LOG)


class PlotterPool(object):
    def __init__(self):
        self.pool = {}
        self.figure_id = display.SparseMapAxesManager.MATPLOTLIB_FIGURE_ID()

    def create_plotter(self, num_ra, num_dec, num_plane, refpix, refval, increment,
                       direction_reference=None, brightnessunit='Jy/beam'):
#         key = (num_ra, num_dec)
#         if key in self.pool:
#             LOG.info('Reuse existing plotter: (nra, ndec) = {}', key)
#             plotter = self.pool[key]
#         else:
#             LOG.info('Create plotter for (nra, ndec) = {}', key)
#             fignums = pl.get_fignums()
#             while self.figure_id in fignums:
#                 self.figure_id += 1
#             plotter = display.SDSparseMapPlotter(nh=num_ra, nv=num_dec,
#                                                    step=1, brightnessunit='Jy/beam',
#                                                    figure_id=self.figure_id)
#             self.pool[key] = plotter
#         plotter.setup_labels(refpix, refval, increment)
        plotter = display.SDSparseMapPlotter(nh=num_ra, nv=num_dec,
                                             step=1, brightnessunit=brightnessunit,
                                             figure_id=self.figure_id)
        plotter.direction_reference = direction_reference
        plotter.setup_labels(refpix, refval, increment)
        return plotter

    def done(self):
        for plotter in self.pool.itervalues():
            plotter.done()


class PlotDataStorage(object):
    def __init__(self):
        self.map_data_storage = numpy.zeros((0), dtype=float)
        self.integrated_data_storage = numpy.zeros((0), dtype=float)
        self.map_mask_storage = numpy.zeros((0), dtype=bool)
        self.integrated_mask_storage = numpy.zeros((0), dtype=bool)
        self.map_data = self.map_data_storage
        self.integrated_data = self.integrated_data_storage
        self.map_mask = self.map_mask_storage
        self.integrated_mask = self.integrated_mask_storage

    def resize_storage(self, num_ra, num_dec, num_pol, num_chan):
        num_integrated = num_pol * num_chan
        num_map = num_ra * num_dec * num_integrated
        if len(self.map_data_storage) < num_map:
            self.map_data_storage = numpy.resize(self.map_data_storage, num_map)
        self.map_data = numpy.reshape(self.map_data_storage[:num_map], (num_ra, num_dec, num_pol, num_chan))

        if len(self.map_mask_storage) < num_map:
            self.map_mask_storage = numpy.resize(self.map_mask_storage, num_map)
        self.map_mask = numpy.reshape(self.map_mask_storage[:num_map], (num_ra, num_dec, num_pol, num_chan))

        if len(self.integrated_data_storage) < num_integrated:
            self.integrated_data_storage = numpy.resize(self.integrated_data_storage, num_integrated)
        self.integrated_data = numpy.reshape(self.integrated_data_storage[:num_integrated], (num_pol, num_chan))


class BaselineSubtractionPlotManager(object):
    @staticmethod
    def _generate_plot_meta_table(spw_id, polarization_ids, grid_table):
        for row in grid_table:
            if row[0] == spw_id and row[1] in polarization_ids:
                new_row_entry = row[2:6]
                yield new_row_entry

    @staticmethod
    def generate_plot_meta_table(spw_id, polarization_ids, grid_table):
        new_table = list(BaselineSubtractionPlotManager._generate_plot_meta_table(spw_id,
                                                                                  polarization_ids,
                                                                                  grid_table))
        return new_table

    @staticmethod
    def _generate_plot_rowlist(ms_id, antenna_id, spw_id, polarization_ids, grid_table):
        for row in grid_table:
            if row[0] == spw_id and row[1] in polarization_ids:
                new_row_entry = numpy.fromiter((r[3] for r in row[6] if r[-1] == ms_id and r[-2] == antenna_id),
                                               dtype=int)
                yield new_row_entry

    @staticmethod
    def generate_plot_rowlist(ms_id, antenna_id, spw_id, polarization_ids, grid_table):
        new_table = list(BaselineSubtractionPlotManager._generate_plot_rowlist(ms_id,
                                                                               antenna_id,
                                                                               spw_id,
                                                                               polarization_ids,
                                                                               grid_table))
        return new_table

    def __init__(self, context, datatable):
        self.context = context
        self.datatable = datatable
        stage_number = self.context.task_counter
        self.stage_dir = os.path.join(self.context.report_dir, "stage%d" % stage_number)

        if basetask.DISABLE_WEBLOG:
            self.pool = None
            self.prefit_storage = None
            self.postfit_storage = None
        else:
            if not os.path.exists(self.stage_dir):
                os.makedirs(self.stage_dir)

            self.pool = PlotterPool()
            self.prefit_storage = PlotDataStorage()
            self.postfit_storage = PlotDataStorage()

    def initialize(self, ms, blvis):
        if basetask.DISABLE_WEBLOG:
            return True

        self.ms = ms

        self.rowmap = utils.make_row_map(ms, blvis)
        self.prefit_data = ms.name
        self.postfit_data = blvis

        return True

    def finalize(self):
        if self.pool is not None:
            self.pool.done()

    def resize_storage(self, num_ra, num_dec, num_pol, num_chan):
        self.prefit_storage.resize_storage(num_ra, num_dec, num_pol, num_chan)
        self.postfit_storage.resize_storage(num_ra, num_dec, num_pol, num_chan)

    def plot_spectra_with_fit(self, field_id, antenna_id, spw_id, org_direction,
                              grid_table=None, deviation_mask=None, channelmap_range=None,
                              showatm=True):
        """
        NB: spw_id is the real spw id.
        """
        if basetask.DISABLE_WEBLOG:
            return []

        if grid_table is None:
            return []

        # convert channelmap_range to plotter-aware format
        if channelmap_range is None:
            line_range = None
        else:
            line_range = [[r[0] - 0.5 * r[1], r[0] + 0.5 * r[1]] for r in channelmap_range if r[2] is True]
            if len(line_range) == 0:
                line_range = None

        self.field_id = field_id
        self.antenna_id = antenna_id
        self.spw_id = spw_id
        self.virtual_spw_id = self.context.observing_run.real2virtual_spw_id(self.spw_id, self.ms)
        data_desc = self.ms.get_data_description(spw=spw_id)
        num_pol = data_desc.num_polarizations
        self.pol_list = numpy.arange(num_pol, dtype=int)

        source_name = self.ms.fields[self.field_id].source.name.replace(' ', '_').replace('/', '_')
        LOG.debug('Generating plots for source {} ant {} spw {}',
                  source_name, self.antenna_id, self.spw_id)

        outprefix_template = lambda x: 'spectral_plot_%s_subtraction_%s_%s_ant%s_spw%s'%(x,
                                                                                         '.'.join(self.ms.basename.split('.')[:-1]),
                                                                                         source_name,
                                                                                         self.antenna_id,
                                                                                         self.virtual_spw_id)
        prefit_prefix = os.path.join(self.stage_dir, outprefix_template('before'))
        postfit_prefix = os.path.join(self.stage_dir, outprefix_template('after'))
        LOG.debug('prefit_prefix=\'{}\'', os.path.basename(prefit_prefix))
        LOG.debug('postfit_prefix=\'{}\'', os.path.basename(postfit_prefix))

        if showatm is True:
            atm_freq, atm_transmission = atmutil.get_transmission(vis=self.ms.name, antenna_id=self.antenna_id,
                                                                  spw_id=self.spw_id, doplot=False)
        else:
            atm_transmission = None
            atm_freq = None
        plot_list = self.plot_profile_map_with_fit(prefit_prefix, postfit_prefix, grid_table,
                                                   deviation_mask, line_range,
                                                   org_direction,
                                                   atm_transmission, atm_freq)
        ret = []
        for (plot_type, plots) in plot_list.iteritems():
            if plot_type == 'pre_fit':
                ptype = 'sd_sparse_map_before_subtraction_raw'
                data = self.prefit_data
            elif plot_type == 'post_fit':
                ptype = 'sd_sparse_map_after_subtraction_raw'
                data = self.postfit_data
            else:
                ptype = 'sd_sparse_map_before_sutraction_avg'
                data = self.prefit_data
            for (pol, figfile) in plots.iteritems():
                if os.path.exists(figfile):
                    parameters = {'intent': 'TARGET',
                                  'spw': self.virtual_spw_id, # parameter for plots are virtual spw id
                                  'pol': sd_polmap[pol],
                                  'ant': self.ms.antennas[self.antenna_id].name,
                                  'vis': self.ms.basename,
                                  'type': ptype,
                                  'file': data}
                    plot = logger.Plot(figfile,
                                       x_axis='Frequency',
                                       y_axis='Intensity',
                                       field=source_name,
                                       parameters=parameters)
                    ret.append(compress.CompressedObj(plot))
                    del plot
        return ret

    def plot_profile_map_with_fit(self, prefit_figfile_prefix, postfit_figfile_prefix, grid_table,
                                  deviation_mask, line_range,
                                  org_direction, atm_transmission, atm_frequency):
        """
        plot_table format:
        [[0, 0, RA0, DEC0, [IDX00, IDX01, ...]],
         [0, 1, RA0, DEC1, [IDX10, IDX11, ...]],
         ...]
        """
        ms = self.ms
        ms_id = self.context.observing_run.measurement_sets.index(ms)
        antid = self.antenna_id
        spwid = self.spw_id
        virtual_spwid = self.virtual_spw_id
        polids = self.pol_list
        prefit_data = self.prefit_data
        postfit_data = self.postfit_data
        rowmap = self.rowmap

        dtrows = self.datatable.getcol('ROW')

        # get brightnessunit from MS
        # default is Jy/beam
        bunit = utils.get_brightness_unit(ms.basename, defaultunit='Jy/beam')

        # grid_table is baseed on virtual spw id
        num_ra, num_dec, num_plane, refpix, refval, increment, rowlist = analyze_plot_table(ms,
                                                                                            ms_id,
                                                                                            antid,
                                                                                            virtual_spwid,
                                                                                            polids,
                                                                                            grid_table,
                                                                                            org_direction)

        plotter = self.pool.create_plotter(num_ra, num_dec, num_plane, refpix, refval, increment,
                                           direction_reference=self.datatable.direction_ref,
                                           brightnessunit=bunit)
        LOG.debug('vis {} ant {} spw {} plotter figure id {} has {} axes',
                  ms.basename, antid, spwid, plotter.axes.figure_id, len(plotter.axes.figure.axes))
#         LOG.info('axes list: {}', [x.__hash__()  for x in plotter.axes.figure.axes])
        spw = ms.spectral_windows[spwid]
        nchan = spw.num_channels
        data_desc = ms.get_data_description(spw=spw)
        npol = data_desc.num_polarizations
        LOG.debug('nchan={}', nchan)

        self.resize_storage(num_ra, num_dec, npol, nchan)

        frequency = numpy.fromiter((spw.channels.chan_freqs[i] * 1.0e-9 for i in xrange(nchan)),
                                   dtype=numpy.float64)  # unit in GHz
        LOG.debug('frequency={}~{} (nchan={})',
                  frequency[0], frequency[-1], len(frequency))

        if rowmap is None:
            rowmap = utils.make_row_map(ms, postfit_data)
        postfit_integrated_data, postfit_map_data = get_data(postfit_data, dtrows,
                                                             num_ra, num_dec, nchan, npol,
                                                             rowlist, rowmap=rowmap,
                                                             integrated_data_storage=self.postfit_storage.integrated_data,
                                                             map_data_storage=self.postfit_storage.map_data,
                                                             map_mask_storage=self.postfit_storage.map_mask)
        if line_range is not None:
            lines_map = get_lines(self.datatable, num_ra, npol, rowlist)
        else:
            lines_map = None

        plot_list = {}

        # plot post-fit spectra
        plot_list['post_fit'] = {}
        plotter.setup_reference_level(0.0)
        plotter.set_deviation_mask(deviation_mask)
        plotter.set_atm_transmission(atm_transmission, atm_frequency)
        plotter.set_global_scaling()
        if utils.is_nro(self.context):
            plotter.set_channel_axis()
        for ipol in xrange(npol):
            postfit_figfile = postfit_figfile_prefix + '_pol%s.png' % ipol
            #LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
            if lines_map is not None:
                plotter.setup_lines(line_range, lines_map[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(postfit_map_data[:, :, ipol, :],
                         postfit_integrated_data[ipol],
                         frequency, figfile=postfit_figfile)
            #LOG.info('#TIMING# End SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
            if os.path.exists(postfit_figfile):
                plot_list['post_fit'][ipol] = postfit_figfile

        del postfit_integrated_data

        prefit_integrated_data, prefit_map_data = get_data(prefit_data, dtrows,
                                                           num_ra, num_dec,
                                                           nchan, npol, rowlist,
                                                           integrated_data_storage=self.prefit_storage.integrated_data,
                                                           map_data_storage=self.prefit_storage.map_data,
                                                           map_mask_storage=self.prefit_storage.map_mask)

        # fit_result shares its storage with postfit_map_data to reduce memory usage
        fit_result = postfit_map_data
        for x in xrange(num_ra):
            for y in xrange(num_dec):
                prefit = prefit_map_data[x][y]
                if not numpy.all(prefit == display.NoDataThreshold):
                    postfit = postfit_map_data[x][y]
                    fit_result[x, y] = prefit - postfit
                else:
                    fit_result[x, y, ::] = display.NoDataThreshold


        # plot pre-fit spectra
        plot_list['pre_fit'] = {}
        plotter.setup_reference_level(None)
        plotter.unset_global_scaling()
        for ipol in xrange(npol):
            prefit_figfile = prefit_figfile_prefix + '_pol%s.png'%(ipol)
            #LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
            if lines_map is not None:
                plotter.setup_lines(line_range, lines_map[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(prefit_map_data[:, :, ipol, :],
                         prefit_integrated_data[ipol],
                         frequency, fit_result=fit_result[:, :, ipol, :], figfile=prefit_figfile)
            #LOG.info('#TIMING# End SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
            if os.path.exists(prefit_figfile):
                plot_list['pre_fit'][ipol] = prefit_figfile

        del prefit_map_data, postfit_map_data, fit_result


        prefit_averaged_data = get_averaged_data(prefit_data, dtrows,
                                                 num_ra, num_dec,
                                                 nchan, npol, rowlist,
                                                 map_data_storage=self.prefit_storage.map_data,
                                                 map_mask_storage=self.prefit_storage.map_mask)


        if line_range is not None:
            lines_map_avg = get_lines2(prefit_data, self.datatable, num_ra, rowlist, polids)
        else:
            lines_map_avg = None
        # plot pre-fit averaged spectra
        plot_list['pre_fit_avg'] = {}
        plotter.setup_reference_level(None)
        plotter.unset_global_scaling()
        for ipol in xrange(npol):
            prefit_avg_figfile = prefit_figfile_prefix + '_avg_pol{}.png'.format(ipol)
            if lines_map_avg is not None:
                plotter.setup_lines(line_range, lines_map_avg[ipol])
            else:
                plotter.setup_lines(line_range)
            plotter.plot(prefit_averaged_data[:, :, ipol, :],
                         prefit_integrated_data[ipol],
                         frequency, fit_result=None, figfile=prefit_avg_figfile)

            if os.path.exists(prefit_avg_figfile):
                plot_list['pre_fit_avg'][ipol] = prefit_avg_figfile

        del prefit_integrated_data, prefit_averaged_data


        plotter.done()

        return plot_list


def generate_grid_panel_map(ngrid, npanel, num_plane=1):
    ng = ngrid // npanel
    mg = ngrid % npanel
    ng_per_panel = [ng * num_plane for i in range(npanel)]
    for i in range(mg):
        ng_per_panel[i] += num_plane

    s = 0
    e = 0
    for i in range(npanel):
        s = e
        e = s + ng_per_panel[i]
        yield range(s, e)

def configure_1d_panel(nx, ny, num_plane=1):
    max_panels = 50
    num_panels = nx * ny

    nnx = nx
    nny = ny
    if num_panels > max_panels:
        div = lambda x: x // 2 + x % 2
        LOG.debug('original: {} x {} = {}'.format(nx, ny, num_panels))
        ndiv = 0
        nnp = num_panels
        while nnp > max_panels:
            nnx = div(nnx)
            nny = div(nny)
            nnp = nnx * nny
            ndiv += 1
            LOG.trace('div {}: {} x {} = {}'.format(ndiv, nnx, nny, nnp))
        LOG.debug('processed: {} x {} = {}'.format(nnx, nny, nnp))

    xpanel = list(generate_grid_panel_map(nx, nnx, num_plane))
    ypanel = list(generate_grid_panel_map(ny, nny, num_plane))

    return xpanel, ypanel

def configure_2d_panel(xpanel, ypanel, ngridx, ngridy, num_plane=3):
    xypanel = []
    for ygrid in ypanel:
        for xgrid in xpanel:
            p = []
            for y in ygrid:
                for x in xgrid:
                    a = ngridx * num_plane * y + num_plane * x
                    p.extend(range(a, a + num_plane))
            xypanel.append(p)
    return xypanel

#@utils.profiler
def analyze_plot_table(ms, ms_id, antid, virtual_spwid, polids, grid_table, org_direction):
    # plot table is separated into two parts: meta data part and row list part
    # plot_table layout: [RA_ID, DEC_ID, RA_DIR, DEC_DIR]
    # [[0, 0, RA0, DEC0], <- plane 0
    #  [0, 0, RA0, DEC0], <- plane 1
    #  [0, 0, RA0, DEC0], <- plane 2
    #  [1, 0, RA1, DEC0], <- plane 0
    #   ...
    #  [M, 0, RAM, DEC0], <- plane 2
    #  [0, 1, RA0, DEC1], <- plane 0
    #  ...
    #  [M, N, RAM, DECN]] <- plane 2
    plot_table = BaselineSubtractionPlotManager.generate_plot_meta_table(virtual_spwid,
                                                                         polids,
                                                                         grid_table)
    grid_rowlist = BaselineSubtractionPlotManager._generate_plot_rowlist(ms_id,
                                                                         antid,
                                                                         virtual_spwid,
                                                                         polids,
                                                                         grid_table)
    num_grid_rows = len(plot_table)  # num_plane * num_grid_ra * num_grid_dec
    assert num_grid_rows > 0
    num_grid_dec = plot_table[-1][1] + 1
    num_grid_ra = plot_table[-1][0] + 1
    num_plane = num_grid_rows // (num_grid_dec * num_grid_ra)
    LOG.debug('num_grid_ra={}, num_grid_dec={}, num_plane={}, num_grid_rows={}',
              num_grid_ra, num_grid_dec, num_plane, num_grid_rows)
    #each_grid = (range(i*num_plane, (i+1)*num_plane) for i in xrange(num_grid_dec * num_grid_ra))
    #rowlist = [{} for i in xrange(num_grid_dec * num_grid_ra)]
    xpanel, ypanel = configure_1d_panel(num_grid_ra, num_grid_dec)
    num_ra = len(xpanel)
    num_dec = len(ypanel)
    each_grid = configure_2d_panel(xpanel, ypanel, num_grid_ra, num_grid_dec, num_plane)
    rowlist = [{} for i in xrange(num_dec * num_ra)]

    # qa = casatools.quanta
    # if org_direction is None:
    #     ra_offset = 0
    #     dec_offset = 0
    # else:
    #     ra_offset  = qa.convert( org_direction['m0'], 'deg' )['value']
    #     dec_offset = qa.convert( org_direction['m1'], 'deg' )['value']

    for row_index, each_plane in enumerate(each_grid):
        def g():
            for plot_table_rowid in each_plane:
                plot_table_row = grid_rowlist.next()
                LOG.debug('Process row {}: ra={}, dec={}',
                          plot_table_rowid, plot_table[plot_table_rowid][2],
                          plot_table[plot_table_rowid][3])
                for i in plot_table_row:
                    # MS stores multiple polarization components in one cell
                    # so it is not necessary to check polarization id
                    LOG.trace('Adding {} to dataids', i)
                    yield i
        dataids = numpy.fromiter(g(), dtype=numpy.int64)
        #raid = plot_table[each_plane[0]][0]
        #decid = plot_table[each_plane[0]][1]
        raid = row_index % num_ra
        decid = row_index // num_ra
        ralist = [plot_table[i][2] for i in each_plane]
        declist = [plot_table[i][3] for i in each_plane]
        #ra = plot_table[each_plane[0]][2]
        #dec = plot_table[each_plane[0]][3]
        ra = numpy.mean(ralist)
        dec = numpy.mean(declist)
        if org_direction is not None:
            ra, dec = direction_recover( ra, dec, org_direction )

        rowlist[row_index].update(
                {"RAID": raid, "DECID": decid, "RA": ra, "DEC": dec,
                 "IDS": dataids})
        LOG.trace('RA {} DEC {}: dataids={}',
                  raid, decid, dataids)

    refpix_list = [0, 0]
    refval_list = [rowlist[num_ra - 1]['RA'], rowlist[0]['DEC']]#plot_table[num_ra * num_plane -1][2:4]
    # each panel contains several grid pixels
    # note that number of grid pixels per panel may not be identical
    xgrid_per_panel = len(xpanel[0])
    ygrid_per_panel = len(ypanel[0])

    # pick ra0, ra1, dec0, dec1 to calculate increments
    # note that ra/dec values in plot_table[][] are before direction_recover!
    if org_direction is None:
        ra0  = plot_table[0][2]
        if num_ra > 1:
            ra1  = plot_table[num_plane][2]
    else:
        ra0, dummy   = direction_recover( plot_table[0][2],
                                          plot_table[0][3],
                                          org_direction )
        if num_ra > 1:
            ra1, dummy  = direction_recover( plot_table[num_plane][2],
                                             plot_table[0][3],
                                             org_direction ) 

    if org_direction is None:
        dec0 = plot_table[0][3]
        if num_dec > 1:
            dec1 = plot_table[num_plane*num_grid_ra][3]
    else:
        dummy, dec0 = direction_recover( plot_table[0][2],
                                         plot_table[0][3],
                                         org_direction )
        if num_dec > 1:
            dummy, dec1 = direction_recover( plot_table[0][2],
                                             plot_table[num_plane*num_grid_ra][3],
                                             org_direction )

    # calculate increment_ra/dec
    if num_ra > 1:
        increment_ra = ( ra1 - ra0 ) * xgrid_per_panel
    else:
        dec_corr = numpy.cos(dec0 * casatools.quanta.constants('pi')['value'] / 180.0)
        if num_dec > 1:
            increment_ra = ((dec1 - dec0) / dec_corr) * xgrid_per_panel
        else:
            reference_data = ms
            beam_size = casatools.quanta.convert(reference_data.beam_sizes[antid][virtual_spwid], outunit='deg')['value']
            increment_ra = (beam_size / dec_corr) * xgrid_per_panel
    if num_dec > 1:
        LOG.trace('num_dec > 1 ({})', num_dec)
        increment_dec = (dec1 - dec0) * ygrid_per_panel
    else:
        # assuming square grid, increment for dec is estimated from the one for ra
        LOG.trace('num_dec is 1')
        dec_corr = numpy.cos(dec0 * casatools.quanta.constants('pi')['value'] / 180.0)
        LOG.trace('declination correction factor is {}', dec_corr)
        increment_dec = increment_ra * dec_corr * ygrid_per_panel
    increment_list = [-increment_ra, increment_dec]
    LOG.debug('refpix_list={}', refpix_list)
    LOG.debug('refval_list={}', refval_list)
    LOG.debug('increment_list={}', increment_list)

    return num_ra, num_dec, num_plane, refpix_list, refval_list, increment_list, rowlist

def direction_recover( ra, dec, org_direction ):
    me = casatools.measures
    qa = casatools.quanta

    direction = me.direction( org_direction['refer'],
                              str(ra)+'deg', str(dec)+'deg' )
    zero_direction  = me.direction( org_direction['refer'], '0deg', '0deg' )
    offset = me.separation( zero_direction, direction )
    posang = me.posangle( zero_direction, direction )
    new_direction = me.shift( org_direction, offset=offset, pa=posang )
    new_ra  = qa.convert( new_direction['m0'], 'deg' )['value']
    new_dec = qa.convert( new_direction['m1'], 'deg' )['value']

    return new_ra, new_dec


# #@utils.profiler
# def create_plotter(num_ra, num_dec, num_plane, refpix, refval, increment):
#     plotter = display.SDSparseMapPlotter(nh=num_ra, nv=num_dec, step=1, brightnessunit='Jy/beam')
#     plotter.setup_labels(refpix, refval, increment)
#     return plotter
#


#@utils.profiler
def get_data(infile, dtrows, num_ra, num_dec, num_chan, num_pol, rowlist, rowmap=None,
             integrated_data_storage=None, integrated_mask_storage=None,
             map_data_storage=None, map_mask_storage=None):
    # default rowmap is EchoDictionary
    if rowmap is None:
        rowmap = utils.EchoDictionary()

    integrated_shape = (num_pol, num_chan)
    map_shape = (num_ra, num_dec, num_pol, num_chan)
    if integrated_data_storage is not None:
        assert integrated_data_storage.shape == integrated_shape
        assert integrated_data_storage.dtype == float
        integrated_data = integrated_data_storage
        integrated_data[:] = 0.0
    else:
        integrated_data = numpy.zeros((num_pol, num_chan), dtype=float)

    num_accumulated = numpy.zeros((num_pol, num_chan), dtype=int)

    if map_data_storage is not None:
        assert map_data_storage.shape == map_shape
        assert map_data_storage.dtype == float
        map_data = map_data_storage
        map_data[:] = display.NoDataThreshold
    else:
        map_data = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=float) + display.NoDataThreshold
    if map_mask_storage is not None:
        assert map_mask_storage.shape == map_shape
        assert map_mask_storage.dtype == bool
        map_mask = map_mask_storage
        map_mask[:] = False
    else:
        map_mask = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=bool)

    # column name for spectral data
    with casatools.TableReader(infile) as tb:
        colnames = ['CORRECTED_DATA', 'DATA', 'FLOAT_DATA']
        colname = None
        for name in colnames:
            if name in tb.colnames():
                colname = name
                break
        assert colname is not None

        for d in rowlist:
            ix = num_ra - 1 - d['RAID']
            iy = d['DECID']
            idxs = d['IDS']
            if len(idxs) > 0:
                # to access MS rows in sorted order (avoid jumping distant row, accessing back and forth)
                rows = dtrows[idxs].copy()
                sorted_index = numpy.argsort(rows)
                idxperpol = [[], [], [], []]
                for isort in sorted_index:
                    row = rows[isort]
                    mapped_row = rowmap[row]
                    LOG.debug('row {}: mapped_row {}', row, mapped_row)
                    this_data = tb.getcell(colname, mapped_row)
                    this_mask = tb.getcell('FLAG', mapped_row)
                    LOG.trace('this_mask.shape={}', this_mask.shape)
                    for ipol in xrange(num_pol):
                        pmask = this_mask[ipol]
                        allflagged = numpy.all(pmask == True)
                        LOG.trace('all(this_mask==True) = {}', allflagged)
                        if allflagged == False:
                            idxperpol[ipol].append(idxs[isort])
                        else:
                            LOG.debug('spectrum for pol {0} is completely flagged at {1}, {2} (row {3})',
                                      ipol, ix, iy, mapped_row)
                    binary_mask = numpy.asarray(numpy.logical_not(this_mask), dtype=int)
                    integrated_data += this_data.real * binary_mask
                    num_accumulated += binary_mask
                midxperpol = []
                for ipol in xrange(num_pol):
                    pidxs = idxperpol[ipol]
                    if len(pidxs) > 0:
                        midx = median_index(pidxs)
                        median_row = dtrows[pidxs[midx]]
                        mapped_row = rowmap[median_row]
                        LOG.debug('median row for ({},{}) with pol {} is {} (mapped to {})',
                                  ix, iy, ipol, median_row, mapped_row)
                        this_data = tb.getcell(colname, mapped_row)
                        this_mask = tb.getcell('FLAG', mapped_row)
                        map_data[ix, iy, ipol] = this_data[ipol].real
                        map_mask[ix, iy, ipol] = this_mask[ipol]
                        midxperpol.append(midx)
                    else:
                        midxperpol.append(None)
            else:
                LOG.debug('no data is available for ({},{})', ix, iy)
                midxperpol = [None for ipol in xrange(num_pol)]
            d['MEDIAN_INDEX'] = midxperpol
            LOG.debug('MEDIAN_INDEX for {0}, {1} is {2}', ix, iy, midxperpol)
    integrated_data_masked = numpy.ma.masked_array(integrated_data, num_accumulated == 0)
    integrated_data_masked /= num_accumulated
    map_data_masked = numpy.ma.masked_array(map_data, map_mask)
    LOG.trace('integrated_data={}', integrated_data)
    LOG.trace('num_accumulated={}', num_accumulated)
    LOG.trace('map_data.shape={}', map_data.shape)

    return integrated_data_masked, map_data_masked

def get_averaged_data(infile, dtrows, num_ra, num_dec, num_chan, num_pol, rowlist, rowmap=None,
             map_data_storage=None, map_mask_storage=None):
    # default rowmap is EchoDictionary
    if rowmap is None:
        rowmap = utils.EchoDictionary()

    map_shape = (num_ra, num_dec, num_pol, num_chan)

    num_accumulated = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=int)

    if map_data_storage is not None:
        assert map_data_storage.shape == map_shape
        assert map_data_storage.dtype == float
        map_data = map_data_storage
        map_data[:] = 0.0
    else:
        map_data = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=float)
    if map_mask_storage is not None:
        assert map_mask_storage.shape == map_shape
        assert map_mask_storage.dtype == bool
        map_mask = map_mask_storage
        map_mask[:] = False
    else:
        map_mask = numpy.zeros((num_ra, num_dec, num_pol, num_chan), dtype=bool)

    # column name for spectral data
    with casatools.TableReader(infile) as tb:
        colnames = ['CORRECTED_DATA', 'DATA', 'FLOAT_DATA']
        colname = None
        for name in colnames:
            if name in tb.colnames():
                colname = name
                break
        assert colname is not None

        for d in rowlist:
            ix = num_ra - 1 - d['RAID']
            iy = d['DECID']
            idxs = d['IDS']
            if len(idxs) > 0:
                # to access MS rows in sorted order (avoid jumping distant row, accessing back and forth)
                rows = dtrows[idxs].copy()
                sorted_index = numpy.argsort(rows)
#                 idxperpol = [[], [], [], []]
                for isort in sorted_index:
                    row = rows[isort]
                    mapped_row = rowmap[row]
                    LOG.debug('row {}: mapped_row {}', row, mapped_row)
                    this_data = tb.getcell(colname, mapped_row)
                    this_mask = tb.getcell('FLAG', mapped_row)
                    LOG.trace('this_mask.shape={}', this_mask.shape)
                    binary_mask = numpy.asarray(numpy.logical_not(this_mask), dtype=int)
                    map_data[ix, iy] += this_data.real * binary_mask
                    num_accumulated[ix, iy] += binary_mask
            else:
                LOG.debug('no data is available for ({},{})', ix, iy)
    map_mask[:] = num_accumulated == 0
    map_data[map_mask] = display.NoDataThreshold
    map_data_masked = numpy.ma.masked_array(map_data, map_mask)
    map_data_masked /= num_accumulated
#     LOG.trace('integrated_data={}', integrated_data)
    LOG.trace('num_accumulated={}', num_accumulated)
    LOG.trace('map_data.shape={}', map_data.shape)

    return map_data_masked

def get_lines(datatable, num_ra, num_pol, rowlist):
    lines_map = [collections.defaultdict(dict)] * num_pol
    #with casatools.TableReader(rwtablename) as tb:
    for d in rowlist:
        ix = num_ra - 1 - d['RAID']
        iy = d['DECID']
        ids = d['IDS']
        midx = d['MEDIAN_INDEX']
        for ipol in xrange(len(midx)):
            if midx is not None:
                if midx[ipol] is not None:
                    masklist = datatable.getcell('MASKLIST', ids[midx[ipol]])
                    lines_map[ipol][ix][iy] = None if (len(masklist) == 0 or numpy.all(masklist == -1)) else masklist
                else:
                    lines_map[ipol][ix][iy] = None
            else:
                lines_map[ipol][ix][iy] = None
    return lines_map

def get_lines2(infile, datatable, num_ra, rowlist, polids, rowmap=None):
    if rowmap is None:
        rowmap = utils.EchoDictionary()

    num_pol = len(polids)
    lines_map = [collections.defaultdict(dict)] * num_pol
#     plot_table = BaselineSubtractionPlotManager.generate_plot_meta_table(spwid,
#                                                                          polids,
#                                                                          grid_table)
#     num_rows = len(plot_table)  # num_plane * num_ra * num_dec
#     num_dec = plot_table[-1][1] + 1
#     num_ra = plot_table[-1][0] + 1
#     num_plane = num_rows / (num_dec * num_ra)
#     LOG.debug('num_ra={}, num_dec={}, num_plane={}, num_rows={}',
#               num_ra, num_dec, num_plane, num_rows)
    with casatools.TableReader(infile) as tb:
        for d in rowlist:
            ix = num_ra - 1 - d['RAID']
            iy = d['DECID']
            ids = d['IDS']
            ref_ra = d['RA']
            ref_dec = d['DEC']
            rep_ids = [-1 for i in xrange(num_pol)]
            min_distance = [1e30 for i in xrange(num_pol)]
            for dt_id in ids:
                row = rowmap[datatable.getcell('ROW', dt_id)]
                flag = tb.getcell('FLAG', row)
#                ra = datatable.getcell('RA', dt_id)
#                dec = datatable.getcell('DEC', dt_id)
#                ra = datatable.getcell('SHIFT_RA', dt_id)
#                dec = datatable.getcell('SHIFT_DEC', dt_id)
                ra = datatable.getcell('OFS_RA', dt_id)
                dec = datatable.getcell('OFS_DEC', dt_id)
                sqdist = (ra - ref_ra) * (ra - ref_ra) + (dec - ref_dec) * (dec - ref_dec)
                for ipol in xrange(num_pol):
                    if numpy.all(flag[ipol] == True):
                        #LOG.info('TN: ({}, {}) row {} pol {} is all flagged'.format(ix, iy, row, ipol))
                        continue

                    if sqdist <= min_distance[ipol]:
                        rep_ids[ipol] = dt_id

            #LOG.info('TN: rep_ids for ({}, {}) is {}'.format(ix, iy, rep_ids))
            for ipol in xrange(num_pol):
                if rep_ids[ipol] >= 0:
                    masklist = datatable.getcell('MASKLIST', rep_ids[ipol])
                    lines_map[ipol][ix][iy] = None if (len(masklist) == 0 or numpy.all(masklist == -1)) else masklist
                else:
                    lines_map[ipol][ix][iy] = None

    return lines_map



# @utils.profiler
# def plot_profile_map_with_fit(context, ms, antid, spwid, plot_table, prefit_data, postfit_data, prefit_figfile_prefix, postfit_figfile_prefix, deviation_mask, line_range,
#                               rowmap=None):
#     """
#     plot_table format:
#     [[0, 0, RA0, DEC0, [IDX00, IDX01, ...]],
#      [0, 1, RA0, DEC1, [IDX10, IDX11, ...]],
#      ...]
#     """
#     #datatable = DataTable(context.observing_run.ms_datatable_name)
#     rotablename = DataTable.get_rotable_name(context.observing_run.ms_datatable_name)
#     rwtablename = DataTable.get_rwtable_name(context.observing_run.ms_datatable_name)
#     with casatools.TableReader(rotablename) as tb:
#         dtrows = tb.getcol('ROW')
#
#     num_ra, num_dec, num_plane, refpix, refval, increment, rowlist = analyze_plot_table(context, dtrows, ms, antid, spwid, plot_table)
#
#     plotter = create_plotter(num_ra, num_dec, num_plane, refpix, refval, increment)
#
#     spw = ms.spectral_windows[spwid]
#     nchan = spw.num_channels
#     data_desc = ms.get_data_description(spw=spw)
#     npol = data_desc.num_polarizations
#     LOG.debug('nchan={}', nchan)
#
#     frequency = numpy.fromiter((spw.channels.chan_freqs[i] * 1.0e-9 for i in xrange(nchan)), dtype=numpy.float64) # unit in GHz
#     LOG.debug('frequency={}~{} (nchan={})',
#               frequency[0], frequency[-1], len(frequency))
#
#     if rowmap is None:
#         rowmap = utils.make_row_map(ms, postfit_data)
#     postfit_integrated_data, postfit_map_data = get_data(postfit_data, dtrows,
#                                                          num_ra, num_dec, nchan, npol,
#                                                          rowlist, rowmap=rowmap)
#     lines_map = get_lines(rwtablename, num_ra, rowlist)
#
#     plot_list = {}
#
#     # plot post-fit spectra
#     plot_list['post_fit'] = {}
#     plotter.setup_lines(line_range, lines_map)
#     plotter.setup_reference_level(0.0)
#     plotter.set_deviation_mask(deviation_mask)
#     plotter.set_global_scaling()
#     for ipol in xrange(npol):
#         postfit_figfile = postfit_figfile_prefix + '_pol%s.png'%(ipol)
#         LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
#         plotter.plot(postfit_map_data[:,:,ipol,:],
#                      postfit_integrated_data[ipol],
#                      frequency, figfile=postfit_figfile)
#         LOG.info('#TIMING# End SDSparseMapPlotter.plot(postfit,pol%s)'%(ipol))
#         if os.path.exists(postfit_figfile):
#             plot_list['post_fit'][ipol] = postfit_figfile
#
#     del postfit_integrated_data
#
#     prefit_integrated_data, prefit_map_data = get_data(prefit_data, dtrows,
#                                                        num_ra, num_dec,
#                                                        nchan, npol, rowlist)
#
#     # fit_result shares its storage with postfit_map_data to reduce memory usage
#     fit_result = postfit_map_data
#     for x in xrange(num_ra):
#         for y in xrange(num_dec):
#             prefit = prefit_map_data[x][y]
#             if not numpy.all(prefit == display.NoDataThreshold):
#                 postfit = postfit_map_data[x][y]
#                 fit_result[x,y] = prefit - postfit
#             else:
#                 fit_result[x,y,::] = display.NoDataThreshold
#
#
#     # plot pre-fit spectra
#     plot_list['pre_fit'] = {}
#     plotter.setup_reference_level(None)
#     plotter.unset_global_scaling()
#     for ipol in xrange(npol):
#         prefit_figfile = prefit_figfile_prefix + '_pol%s.png'%(ipol)
#         LOG.info('#TIMING# Begin SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
#         plotter.plot(prefit_map_data[:,:,ipol,:],
#                      prefit_integrated_data[ipol],
#                      frequency, fit_result=fit_result[:,:,ipol,:], figfile=prefit_figfile)
#         LOG.info('#TIMING# End SDSparseMapPlotter.plot(prefit,pol%s)'%(ipol))
#         if os.path.exists(prefit_figfile):
#             plot_list['pre_fit'][ipol] = prefit_figfile
#
#     plotter.done()
#
#     del prefit_integrated_data, prefit_map_data, postfit_map_data, fit_result
#
#     return plot_list

def median_index(arr):
    if not numpy.iterable(arr) or len(arr) == 0:
        return numpy.nan
    else:
        sorted_index = numpy.argsort(arr)
        if len(arr) < 3:
            return sorted_index[0]
        else:
            return sorted_index[len(arr) // 2]
