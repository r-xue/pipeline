from __future__ import absolute_import

import os
import time
import abc
import numpy
import math
import pylab as pl

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.casatools as casatools

from .utils import utc_locator

LOG = infrastructure.get_logger(__name__)

#ShowPlot = True
ShowPlot = False

DPISummary = 90
#DPIDetail = 120
DPIDetail = 130
LightSpeed = 29972.458 # km/s

class SingleDishDisplayInputs(object):
    def __init__(self, context, result):
        self.context = context
        self.result = result

class SpectralImage(object):
    def __init__(self, imagename):
        # read data to storage
        with utils.open_image(imagename) as ia:
            self.image_shape = ia.shape()
            self.coordsys = ia.coordsys()
            coord_types = self.coordsys.axiscoordinatetypes()
            self.units = self.coordsys.units()
            self.id_direction = coord_types.index('Direction')
            self.id_direction = [self.id_direction, self.id_direction+1]
            self.id_spectral = coord_types.index('Spectral')
            self.id_stokes = coord_types.index('Stokes')
            LOG.debug('id_direction=%s'%(self.id_direction))
            LOG.debug('id_spectral=%s'%(self.id_spectral))
            LOG.debug('id_stokes=%s'%(self.id_stokes))
            self.data = ia.getchunk()
            self.mask = ia.getchunk(getmask=True)
            bottom = ia.toworld(numpy.zeros(len(self.image_shape),dtype=int), 'q')['quantity']
            top = ia.toworld(self.image_shape-1, 'q')['quantity']
            key = lambda x: '*%s'%(x+1)
            ra_min = bottom[key(self.id_direction[0])]
            ra_max = top[key(self.id_direction[0])]
            if ra_min > ra_max:
                ra_min,ra_max = ra_max,ra_min
            self.ra_min = ra_min
            self.ra_max = ra_max
            self.dec_min = bottom[key(self.id_direction[1])]
            self.dec_max = top[key(self.id_direction[1])]
        
    @property
    def nx(self):
        return self.image_shape[self.id_direction[0]]

    @property
    def ny(self):
        return self.image_shape[self.id_direction[1]]

    @property
    def nchan(self):
        return self.image_shape[self.id_spectral]

    @property
    def npol(self):
        return self.image_shape[self.id_stokes]

    def to_velocity(self, frequency, freq_unit='GHz'):
        qa = casatools.quanta
        rest_frequency = self.coordsys.restfrequency()
        if rest_frequency['unit'] != freq_unit:
            vrf = qa.convert(rest_frequency, freq_unit)['value']
        else:
            vrf = rest_frequency['value']
        return (1.0 - (frequency / vrf)) * LightSpeed

    def spectral_axis(self, unit='GHz'):
        return self.__axis(self.id_spectral, unit=unit)

    def direction_axis(self, idx, unit='deg'):
        return self.__axis(self.id_direction[idx], unit=unit)
        
    def __axis(self, idx, unit):
        qa = casatools.quanta
        refpix = self.coordsys.referencepixel()['numeric'][idx]
        refval = self.coordsys.referencevalue()['numeric'][idx]
        increment = self.coordsys.increment()['numeric'][idx]
        _unit = self.units[idx]
        if _unit != unit:
            refval = qa.convert(qa.quantity(refval,_unit),unit)['value']
            increment = qa.convert(qa.quantity(increment,_unit),unit)['value']
        #return numpy.array([refval+increment*(i-refpix) for i in xrange(self.nchan)])
        return (refpix, refval, increment)
        

class SDImageDisplayInputs(SingleDishDisplayInputs):
    def __init__(self, context, result):
        super(SDImageDisplayInputs,self).__init__(context, result)
        self.image = SpectralImage(self.imagename)
        
    @property
    def imagename(self):
        return self.result.outcome['image'].imagename

    @property
    def spw(self):
        return self.result.outcome['image'].spwlist

    @property
    def antenna(self):
        return self.result.outcome['image'].antenna

    @property
    def stage_number(self):
        return self.result.stage_number

    @property
    def stage_dir(self):
        return os.path.join(self.context.report_dir,
                            'stage%d'%(self.stage_number))

    @property
    def source(self):
        return self.result.outcome['image'].sourcename

    @property
    def nx(self):
        return self.image.nx

    @property
    def ny(self):
        return self.image.ny

    @property
    def nchan(self):
        return self.image.nchan

    @property
    def npol(self):
        return self.image.npol

    @property
    def num_valid_spectrum(self):
        return self.__reshape2d(self.result.outcome['validsp'])

    @property
    def rms(self):
        return self.__reshape2d(self.result.outcome['rms'])

    @property
    def edge(self):
        return self.result.outcome['edge']

    def __reshape2d(self, array2d):
        array3d = array2d.reshape((self.npol,self.ny,self.nx)).transpose()
        return numpy.flipud(array3d)

class SDInspectionDisplay(object):
    __metaclass__ = abc.ABCMeta
    Inputs = SingleDishDisplayInputs
    MATPLOTLIB_FIGURE_ID = -1
    AxesManager = None
    
    def __init__(self, inputs):
        self.inputs = inputs
        self.context = self.inputs.context
        self.result = self.inputs.result
        self.datatable = self.result.outcome['instance']

    def plot(self):
        inputs = self.inputs
        result = inputs.result
        
        if result.outcome is None or result.success is None or result.success is False:
            # result object seems to be empty, return empty list
            return []

        if ShowPlot: pl.ion()
        else: pl.ioff()
        pl.figure(self.MATPLOTLIB_FIGURE_ID)
        if ShowPlot: pl.ioff()
        pl.clf()

        self.axes_manager = self.AxesManager()
        
        plots = []
        report_dir = self.context.report_dir
        stage_dir = os.path.join(report_dir, 'stage%d'%(result.stage_number))
        LOG.debug('report_dir=%s'%(report_dir))
        filenames = self.datatable.getkeyword('FILENAMES')
        LOG.debug('filenames=%s'%(filenames))
        for idx in xrange(len(filenames)):
            t0 = time.time()
            plot = self.doplot(idx, stage_dir)
            t1 = time.time()
            LOG.trace('PROFILE: %s.doplot elapsed time %s sec'%(self.__class__.__name__,t1-t0))
            if plot is not None:
                plots.append(plot)
        return [plots]

    @abc.abstractmethod
    def doplot(self, idx, stage_dir):
        raise NotImplementedError()


class SDCalibrationDisplay(object):
    __metaclass__ = abc.ABCMeta
    Inputs = SingleDishDisplayInputs

    def __init__(self, inputs):
        self.inputs = inputs

    def plot(self):
        results = self.inputs.result
        report_dir = self.inputs.context.report_dir
        stage_dir = os.path.join(report_dir, 'stage%d'%(results.stage_number))
        plots = []
        for result in results:
            #table = result.outcome.applytable
            if result is None or result.outcome is None:
                plot = None
            else:
                plot = self.doplot(result, stage_dir)
                
            if plot is not None:
                plots.append(plot)
        return plots

    @abc.abstractmethod
    def doplot(self, result, stage_dir):
        raise NotImplementedError()

        
class SDImageDisplay(object):
    __metaclass__ = abc.ABCMeta
    Inputs = SDImageDisplayInputs

    def __init__(self, inputs):
        self.inputs = inputs

        self.context = self.inputs.context
        self.stage_dir = self.inputs.stage_dir
        self.image = self.inputs.image
        self.spw = self.inputs.spw
        self.antenna = self.inputs.antenna

    def init(self):
        qa = casatools.quanta
        self.nchan = self.image.nchan
        self.data = self.image.data
        self.mask = self.image.mask
        self.nx = self.image.nx
        self.ny = self.image.ny
        self.npol = self.image.npol
        (refpix, refval, increment) = self.image.spectral_axis(unit='GHz')
        self.frequency = numpy.array([refval+increment*(i-refpix) for i in xrange(self.nchan)])
        self.velocity = self.image.to_velocity(self.frequency, freq_unit='GHz')

        self.x_max = self.nx - 1
        self.x_min = 0
        self.y_max = self.ny - 1
        self.y_min = 0
        self.ra_min = qa.convert(self.image.ra_min, 'deg')['value']
        self.ra_max = qa.convert(self.image.ra_max, 'deg')['value']
        self.dec_min = qa.convert(self.image.dec_min, 'deg')['value']
        self.dec_max = qa.convert(self.image.dec_max, 'deg')['value']

        LOG.debug('(ra_min,ra_max)=(%s,%s)'%(self.ra_min,self.ra_max))
        LOG.debug('(dec_min,dec_max)=(%s,%s)'%(self.dec_min,self.dec_max))

        # beam size in deg
        ant_index = -1
        for i in xrange(len(self.context.observing_run)):
            if self.context.observing_run[i].antenna.name == self.antenna:
                ant_index = i
                break
        self.beam_size = qa.convert(self.context.observing_run[ant_index].beam_size[self.spw], 'deg')['value']

        self.beam_radius = self.beam_size / 2.0
        self.grid_size = self.beam_size / 3.0
        LOG.debug('beam_radius=%s'%(self.beam_radius))
        LOG.debug('grid_size=%s'%(self.grid_size))

        # 2008/9/20 Dec Effect has been taken into account
        self.aspect = 1.0 / math.cos(0.5 * (self.dec_min + self.dec_max) / 180.0 * 3.141592653)

    @property
    def id_spectral(self):
        return self.image.id_spectral
    
    @property
    def id_stokes(self):
        return self.image.id_stokes




def get_base_frequency(table, freqid, nchan):
    freq_table = os.path.join(table, 'FREQUENCIES')
    with utils.open_table(freq_table) as tb:
        refpix = tb.getcell('REFPIX', freqid)
        refval = tb.getcell('REFVAL', freqid)
        increment = tb.getcell('INCREMENT', freqid)
        chan_freq = numpy.array([refval + (i - refpix) * increment 
                                 for i in xrange(nchan)])
    return chan_freq

def get_base_frame(table):
    freq_table = os.path.join(table, 'FREQUENCIES')
    with utils.open_table(freq_table) as tb:
        base_frame = tb.getkeyword('BASEFRAME')
    return base_frame

def drop_edge(array):
    # array should be two-dimensional (nchan,nrow)
    nchan = array.shape[0]
    a = None
    if nchan > 2:
        echan = max(1,int(nchan * 0.05))
        a = array[echan:-echan,::]
    return a

def draw_beam(axes, r, aspect, x_base, y_base, offset=1.0):
    xy = numpy.array([[r * (math.sin(t * 0.13) + offset) * aspect + x_base,
                       r * (math.cos(t * 0.13) + offset) + y_base]
                      for t in xrange(50)])
    pl.gcf().sca(axes)
    line = pl.plot(xy[:,0], xy[:,1], 'r-')
    return line[0]


class TimeAxesManager(object):
    def __init__(self):
        self.locator = utc_locator()
        
    def init(self, start_time=None, end_time=None):
        self.locator = utc_locator(start_time, end_time)
