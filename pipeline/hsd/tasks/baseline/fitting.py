from __future__ import absolute_import

import os
import time
import re
import numpy

import asap as sd

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casatools as casatools
import pipeline.infrastructure.sdfilenamer as filenamer
from pipeline.hsd.heuristics import fitorder, fragmentation
from .. import common
from pipeline.hsd.tasks.common import utils as sdutils

# for plot routine
from . import plotter
from pipeline.infrastructure.displays.singledish.utils import sd_polmap
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)

class FittingFactory(object):
    @staticmethod
    def get_fitting_class(fitfunc='spline'):
        if re.match('^C?SPLINE$', fitfunc.upper()):
            return CubicSplineFitting
#         elif re.match('^POLYNOMIAL$', fitfunc.upper()):
#             return PolynomialFitting
        else:
            return None
        
class FittingInputs(common.SingleDishInputs):
    def __init__(self, context, antennaid, spwid, pollist, iteration, 
                 fit_order=None, edge=None, outfile=None, 
                 grid_table=None, channelmap_range=None, stage_dir=None):
        self._init_properties(vars())
        self._bltable = None
        LOG.debug('pollist=%s'%(pollist))
    
    @property
    def edge(self):
        return (0,0) if self._edge is None else self._edge
    
    @edge.setter
    def edge(self, value):
        self._edge = value
        
    @property
    def fit_order(self):
        return 'automatic' if self._fit_order is None else self._fit_order
    
    @fit_order.setter
    def fit_order(self, value):
        self._fit_order = value
        
    #@property
    def data_object(self):
        return self.context.observing_run[self.antennaid]
        
    @property
    def infile(self):
        #return self.data_object.name
        return self.data_object().baseline_source
    
    @property
    def outfile(self):
        return self._outfile
    
    @outfile.setter
    def outfile(self, value):
        self._outfile = value

    @property
    def bltable(self):
        if self._bltable is None:
            namer = filenamer.BaselineSubtractedTable()
            namer.spectral_window(self.spwid)
            st = self.data_object()
            asdm = common.asdm_name(st)
            namer.asdm(asdm)
            namer.antenna_name(st.antenna.name)
            self._bltable = namer.get_filename()
        return self._bltable
    
    @property
    def srctype(self):
        return self.data_object().calibration_strategy['srctype']
    
    @property
    def nchan(self):
        return self.data_object().spectral_window[self.spwid].nchan
                
class FittingResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(FittingResults, self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(FittingResults, self).merge_with_context(context)
        
    def _outcome_name(self):
        return self.outcome


class FittingBase(common.SingleDishTaskTemplate):
    Inputs = FittingInputs

    ApplicableDuration = 'raster' # 'raster' | 'subscan'
    MaxPolynomialOrder = 'none' # 'none', 0, 1, 2,...
    PolynomialOrder = 'automatic' # 'automatic', 0, 1, 2, ...
    ClipCycle = 1

    def __init__(self, inputs):
        super(FittingBase, self).__init__(inputs)
        self.fragmentation_heuristic = fragmentation.FragmentationHeuristics()
        
        # fitting order
        fit_order = self.inputs.fit_order
        if fit_order == 'automatic':
            # fit order heuristics
            LOG.info('Baseline-Fitting order was automatically determined')
            self.fitorder_heuristic = fitorder.FitOrderHeuristics()
        else:
            LOG.info('Baseline-Fitting order was fixed to be %d'%(fit_order))
            self.fitorder_heuristic = lambda *args, **kwargs: self.inputs.fit_order


    @common.datatable_setter
    def prepare(self):
        """
        """
        datatable = self.datatable
        filename_in = self.inputs.infile
        filename_out = self.inputs.outfile
        iteration = self.inputs.iteration
        bltable_name = self.inputs.bltable
            
        if not filename_out or len(filename_out) == 0:
            self.outfile = self.data_object().baselined_name
            LOG.debug("Using default output scantable name, %s" % self.outfile)
        
        if not os.path.exists(filename_out):
            raise RuntimeError, "Output scantable '%s' does not exist. It should be exist before you run this method." % filename_out

        antennaid = self.inputs.antennaid
        spwid = self.inputs.spwid
        pollist = self.inputs.pollist
        LOG.debug('pollist=%s'%(pollist))
        nchan = self.inputs.nchan
        srctype = self.inputs.srctype
        edge = common.parseEdge(self.inputs.edge)
        fit_order = self.inputs.fit_order

        if self.ApplicableDuration == 'subscan':
            timetable_index = 1
        else:
            timetable_index = 0
            
        index_list_total = []
        row_list_total = []
        blinfo = []

        for pol in pollist:
            time_table = datatable.get_timetable(antennaid, spwid, pol)
            member_list = time_table[timetable_index]

            # working with spectral data in scantable
            nrow_total = sum([len(x[0]) for x in member_list])
                
            LOG.info('Calculating Baseline Fitting Parameter...')
            LOG.info('Baseline Fit: background subtraction...')
            LOG.info('Processing %d spectra...'%(nrow_total))
    
            mask_array = numpy.ones(nchan, dtype=int)
            mask_array[:edge[0]] = 0
            mask_array[nchan-edge[1]:] = 0
    
            for y in xrange(len(member_list)):
                rows = member_list[y][0]
                idxs = member_list[y][1]

#                 with casatools.TableReader(filename_in) as tb:
#                     spectra = numpy.array([tb.getcell('SPECTRA',row)
#                                            for row in rows])
                with casatools.TableReader(filename_in) as tb:
                    spectra = numpy.array([tb.getcell('SPECTRA',row)
                                           for row in rows])
                    flaglist = [self._mask_to_masklist([ -fchan+1 for fchan in sdutils.get_mask_from_flagtra(tb.getcell('FLAGTRA', row)) ])
                                for row in rows]
  
                LOG.debug("Flag Mask = %s" % str(flaglist))

                spectra[:,:edge[0]] = 0.0
                spectra[:,nchan-edge[1]:] = 0.0 
                masklist = [datatable.tb2.getcell('MASKLIST',idx)
                            for idx in idxs]
#                 masklist = [datatable.tb2.getcell('MASKLIST',idxs[i]) + flaglist[i]
#                             for i in range(len(idxs))]
    
                # fit order determination
                polyorder = self.fitorder_heuristic(spectra, [ list(masklist[i]) + flaglist[i] for i in range(len(idxs))], edge)
                if fit_order == 'automatic' and self.MaxPolynomialOrder != 'none':
                    polyorder = min(polyorder, self.MaxPolynomialOrder)
                LOG.info('time group %d: fitting order=%s'%(y,polyorder))
    
                # calculate fragmentation
                (fragment, nwindow, win_polyorder) = self.fragmentation_heuristic(polyorder, nchan, edge)
    
                nrow = len(rows)
                LOG.debug('nrow = %s'%(nrow))
                LOG.debug('len(idxs) = %s'%(len(idxs)))
                
                #index_list = []
                #row_list = []

                for i in xrange(nrow):
                    row = rows[i]
                    idx = idxs[i]
                    LOG.trace('===== Processing at row = %s ====='%(row))
                    nochange = datatable.tb2.getcell('NOCHANGE',idx)
                    LOG.trace('row = %s, Flag = %s'%(row, nochange))
        
                    # mask lines
                    maxwidth = 1
#                     _masklist = masklist[i] 
                    _masklist = list(masklist[i]) + flaglist[i]
                    for [chan0, chan1] in _masklist:
                        if chan1 - chan0 >= maxwidth:
                            maxwidth = int((chan1 - chan0 + 1) / 1.4)
                            # allowance in Process3 is 1/5:
                            #    (1 + 1/5 + 1/5)^(-1) = (5/7)^(-1)
                            #                         = 7/5 = 1.4
                    max_polyorder = int((nchan - sum(edge)) / maxwidth + 1)
                    LOG.trace('Masked Region from previous processes = %s'%(_masklist))
                    LOG.trace('edge parameters= (%s,%s)'%(edge))
                    LOG.trace('Polynomial order = %d  Max Polynomial order = %d'%(polyorder, max_polyorder))
    
                    # fitting
                    polyorder = min(polyorder, max_polyorder)
                    mask_array[edge[0]:nchan-edge[1]] = 1
                    #irow = len(row_list_total)+len(row_list)
                    irow = len(row_list_total) + i
                    param = self._calc_baseline_param(irow, polyorder, nchan, 0, edge, _masklist, win_polyorder, fragment, nwindow, mask_array)
                    # defintion of masklist differs in pipeline and ASAP (masklist = [a, b+1] in pipeline masks a channel range a ~ b-1)
                    param['masklist'] = [ [start, end-1] for [start, end] in param['masklist'] ]
                    blinfo.append(param)
                    #index_list.append(idx)
                    #row_list.append(row)

                # 2015/07/17 TN
                # In the current implementation, all the rows in timetable are processed regardless of 
                # the value of NOCHANGE column in DataTable. Thus, we don't need to construct index_list 
                # nor row_list since they should be equivalent to idxs and rows
                #index_list_total.extend(index_list)
                #row_list_total.extend(row_list)
                index_list_total.extend(idxs)
                row_list_total.extend(rows)

#         f = open(bltable_name+'.in.txt', 'w')
#         f.write("row_idx = %s\n" % str(row_list_total))
#         f.write("blinfo = %s" % str(blinfo))
#         f.close()
        # subtract baseline
        storage_save = sd.rcParams['scantable.storage']
        sd.rcParams['scantable.storage'] = 'disk'

        LOG.info('Baseline Fit: background subtraction...')
        LOG.info('Processing %d spectra...'%(len(row_list_total)))
        LOG.info('rows = %s' % str(row_list_total))
        st_out = sd.scantable(filename_out, average=False)
        LOG.info('number of rows in scantable = %d' % st_out.nrow())
        st_out.set_selection(rows=row_list_total)
        LOG.info('number of rows in selected = %d' % st_out.nrow())
        st_out.sub_baseline(insitu=True, retfitres=False, blinfo=blinfo, bltable=bltable_name, overwrite=True)
        st_out.set_selection()
        st_out.save(filename_out, format='ASAP', overwrite=True)
        
        sd.rcParams['scantable.storage'] = storage_save
        
        # plotting
        grid_table = self.inputs.grid_table
        channelmap_range = self.inputs.channelmap_range
        plot_list = []
        if grid_table is not None:
            # mkdir stage_dir if it doesn't exist
            stage_dir = self.inputs.stage_dir
            if stage_dir is None:
                stage_number = self.inputs.context.task_counter
                stage_dir = os.path.join(self.inputs.context.report_dir,"stage%d" % stage_number)
                if not os.path.exists(stage_dir):
                    os.makedirs(stage_dir)
                
            st = self.inputs.context.observing_run[antennaid]
            # TODO: use proper source name when we can handle multiple source 
            source_name = ''
            for (source_id,source) in st.source.items():
                if 'TARGET' in source.intents:
                    source_name = source.name.replace(' ', '_').replace('/','_')
#             prefix = 'spectral_plot_before_subtraction_%s_%s_ant%s_spw%s'%('.'.join(st.basename.split('.')[:-1]),source_name,antennaid,spwid)
#             plot_list.extend(self.plot_spectra(source_name, antennaid, spwid, pollist, self.inputs.grid_table, 
#                                                filename_in, stage_dir, prefix, channelmap_range))
#             prefix = prefix.replace('before', 'after')
#             plot_list.extend(self.plot_spectra(source_name, antennaid, spwid, pollist, grid_table, filename_out, stage_dir, prefix, channelmap_range))
        
            plot_list.extend(list(self.plot_spectra_with_fit(source_name, antennaid, spwid, pollist, grid_table, filename_in, filename_out, stage_dir, channelmap_range)))
        
        outcome = {'bltable': bltable_name,
                   'index_list': index_list_total,
                   'outtable': filename_out,
                   'plot_list': plot_list}
        result = FittingResults(task=self.__class__,
                                success=True,
                                outcome=outcome)
                
        if self.inputs.context.subtask_counter is 0: 
            result.stage_number = self.inputs.context.task_counter - 1
        else:
            result.stage_number = self.inputs.context.task_counter 
                
        return result

                
    def analyse(self, result):
        bltable = result.outcome['bltable']
        index_list = result.outcome.pop('index_list')
        stname = self.inputs.infile
#         stname = self.inputs.data_object.name
        spwid = self.inputs.spwid
        iteration = self.inputs.iteration
        edge = common.parseEdge(self.inputs.edge)
        datatable = self.datatable
        nchan = self.inputs.nchan
        FittingSummary.summary(bltable, stname, spwid, iteration, edge, datatable, index_list, nchan)
        return result

    def _calc_baseline_param(self, row_idx, polyorder, nchan, modification, edge, masklist, win_polyorder, fragment, nwindow, mask):
        # Create mask for line protection
        nchan_without_edge = nchan - sum(edge)
        if type(masklist) == list or type(masklist) == numpy.ndarray:
            for [m0, m1] in masklist:
                mask[m0:m1] = 0
        else:
            LOG.critical('Invalid masklist')
        num_mask = int(nchan_without_edge - numpy.sum(mask[edge[0]:nchan-edge[1]] * 1.0))
        masklist_all = self._mask_to_masklist(mask)

        LOG.trace('nchan_without_edge, num_mask, diff=%s, %s'%(nchan_without_edge, num_mask))

        outdata = self._get_param(row_idx, polyorder, nchan, mask, edge, nchan_without_edge, num_mask, fragment, nwindow, win_polyorder, masklist_all)

        return outdata
    
    def _mask_to_masklist(self, mask):
        """
        Converts mask array to masklist
        
        Argument
            mask : an array of channel mask in values 0 (rejected) or 1 (adopted)
        """
        nchan = len(mask)
        istart = []
        iend = []
        if mask[0] == 1:
            istart = [0]
        for ichan in range(1, nchan):
            switch = mask[ichan] - mask[ichan-1]
            if switch == 0:
                continue
            elif switch == 1:
                # start of mask channels (0 -> 1)
                istart.append(ichan)
            elif switch == -1:
                # end of mask channels (1 -> 0)
                iend.append(ichan)
        if mask[nchan-1] == 1:
            iend.append(nchan)
        if len(istart) != len(iend):
            raise RuntimeError, "Failed to get mask ranges. The lenght of start channels and end channels do not match."
        masklist = []
        for irange in range(len(istart)):
            if istart[irange] > iend[irange]:
                raise RuntimeError, "Failed to get mask ranges. A start channel index is larger than end channel."
            masklist.append([istart[irange], iend[irange]])
        return masklist

    def plot_spectra(self, source, ant, spwid, pols, grid_table, infile, outdir, outprefix, channelmap_range):
        st = self.inputs.context.observing_run[ant]
        line_range = [[r[0] - 0.5 * r[1], r[0] + 0.5 * r[1]] for r in channelmap_range if r[2] is True]
        if len(line_range) == 0:
            line_range = None
        for pol in pols:
            outfile = os.path.join(outdir, outprefix+'_pol%s.png'%(pol))
            status = plotter.plot_profile_map(self.inputs.context, ant, spwid, pol, grid_table, infile, outfile, line_range)
            if status and os.path.exists(outfile):
                if outprefix.find('spectral_plot_before_subtraction') == -1:
                    plottype = 'sd_sparse_map_after_subtraction'
                else:
                    plottype = 'sd_sparse_map_before_subtraction'
                parameters = {'intent': 'TARGET',
                              'spw': spwid,
                              'pol': sd_polmap[pol],
                              'ant': st.antenna.name,
                              'vis': st.ms.basename,
                              'type': plottype,
                              'file': infile}
                plot = logger.Plot(outfile,
                                   x_axis='Frequency',
                                   y_axis='Intensity',
                                   field=source,
                                   parameters=parameters)
                yield plot
                
    def plot_spectra_with_fit(self, source, ant, spwid, pols, grid_table, prefit_data, postfit_data, outdir, channelmap_range):
        st = self.inputs.context.observing_run[ant]
        line_range = [[r[0] - 0.5 * r[1], r[0] + 0.5 * r[1]] for r in channelmap_range if r[2] is True]
        if len(line_range) == 0:
            line_range = None
        for pol in pols:
            outfile_template = lambda x: 'spectral_plot_%s_subtraction_%s_%s_ant%s_spw%s_pol%s.png'%(x,'.'.join(st.basename.split('.')[:-1]),source,ant,spwid,pol)
            prefit_outfile = os.path.join(outdir, outfile_template('before'))
            postfit_outfile = os.path.join(outdir, outfile_template('after'))
            status = plotter.plot_profile_map_with_fit(self.inputs.context, ant, spwid, pol, grid_table, prefit_data, postfit_data, prefit_outfile, postfit_outfile, line_range)
            if os.path.exists(prefit_outfile):
                parameters = {'intent': 'TARGET',
                              'spw': spwid,
                              'pol': sd_polmap[pol],
                              'ant': st.antenna.name,
                              'vis': st.ms.basename,
                              'type': 'sd_sparse_map_before_subtraction',
                              'file': prefit_data}
                plot = logger.Plot(prefit_outfile,
                                   x_axis='Frequency',
                                   y_axis='Intensity',
                                   field=source,
                                   parameters=parameters)
                yield plot
            if os.path.exists(postfit_outfile):
                parameters = {'intent': 'TARGET',
                              'spw': spwid,
                              'pol': sd_polmap[pol],
                              'ant': st.antenna.name,
                              'vis': st.ms.basename,
                              'type': 'sd_sparse_map_after_subtraction',
                              'file': postfit_data}
                plot = logger.Plot(postfit_outfile,
                                   x_axis='Frequency',
                                   y_axis='Intensity',
                                   field=source,
                                   parameters=parameters)
                yield plot
            

class CubicSplineFitting(FittingBase):
    def _get_param(self, idx, polyorder, nchan, mask, edge, nchan_without_edge, nchan_masked, fragment, nwindow, win_polyorder, masklist):
        num_nomask = nchan_without_edge - nchan_masked
        num_pieces = max(int(min(polyorder * num_nomask / float(nchan_without_edge) + 0.5, 0.1 * num_nomask)), 1)
        LOG.trace('Cubic Spline Fit: Number of Sections = %d' % num_pieces)
        return {'row': idx, 'masklist': masklist, 'npiece': num_pieces, 'blfunc': 'cspline', 'clipthresh': 5.0, 'clipniter': self.ClipCycle}


class FittingSummary(object):
    @staticmethod
    def summary(tablename, stname, spw, iteration, edge, datatable, index_list, nchan):
        header = 'Summary of cspline_baseline for %s (spw%s, iter%s)'%(stname, spw, iteration)
        separator = '=' * len(header)
        LOG.info(separator)
        LOG.info(header)
        LOG.info(separator)

        # edge channels dropped
        LOG.info('1) Number of edge channels dropped')
        LOG.info('')
        LOG.info('\t left edge: %s channels'%(edge[0]))
        LOG.info('\tright edge: %s channels'%(edge[1]))
        LOG.info('')

        # line masks
        LOG.info('2) Masked fraction on each channel')
        LOG.info('')
        histogram = numpy.zeros(nchan, dtype=float)
        nrow = len(index_list)
        for idx in index_list:
            masklist = datatable.getcell('MASKLIST', idx)
            for mask in masklist:
                start = mask[0]
                end = mask[1] + 1
                for ichan in xrange(start, end):
                    histogram[ichan] += 1.0
        nonzero_channels = histogram.nonzero()[0]
        if len(nonzero_channels) > 0:
            dnz = nonzero_channels[1:] - nonzero_channels[:-1]
            mask_edges = numpy.where(dnz > 1)[0]
            start_chan = nonzero_channels.take([0]+(mask_edges+1).tolist())
            end_chan = nonzero_channels.take(mask_edges.tolist()+[-1])
            merged_start_chan = [start_chan[0]]
            merged_end_chan = []
            for i in xrange(1, len(start_chan)):
                if start_chan[i] - end_chan[i-1] > 4:
                    merged_start_chan.append(start_chan[i])
                    merged_end_chan.append(end_chan[i-1])
            merged_end_chan.append(end_chan[-1])
            LOG.info('channel|fraction')
            LOG.info('-------|---------')
            if merged_start_chan[0] > 0:
                LOG.info('%7d|%9.1f%%'%(0, 0))
                LOG.info('       ~')
                LOG.info('       ~')
            for i in xrange(len(merged_start_chan)):
                for j in xrange(max(0,merged_start_chan[i]-1), min(nchan,merged_end_chan[i]+2)):
                    LOG.info('%7d|%9.1f%%'%(j, histogram[j]/nrow*100.0))
                if merged_end_chan[i] < nchan-2:
                    LOG.info('       ~')
                    LOG.info('       ~')
            if merged_end_chan[-1] < nchan-2:
                LOG.info('%7d|%9.1f%%'%(nchan-1, 0))
        else:
            LOG.info('\tNo line mask')
        LOG.info('')
            
        FittingSummary.cspline_summary(tablename)

        footer = separator
        LOG.info(footer)

    @staticmethod
    def cspline_summary(tablename):
        # number of segments for cspline_baseline
        with casatools.TableReader(tablename) as tb:
            nrow = tb.nrows()
            num_segments = [( len(tb.getcell('FUNC_PARAM', irow)) - 1 ) \
                            for irow in xrange(nrow)]
        unique_values = numpy.unique(num_segments)
        max_segments = max(unique_values) + 2
        LOG.info('3) Frequency distribution for number of segments')
        LOG.info('')
        LOG.info('# of segments|frequency')
        LOG.info('-------------|---------')
        for val in xrange(1, max_segments):
            count = num_segments.count(val)
            LOG.info('%13d|%9d'%(val, count))
        LOG.info('')
