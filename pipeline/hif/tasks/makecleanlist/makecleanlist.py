from __future__ import absolute_import
import types

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from .resultobjects import MakeCleanListResult
from pipeline.hif.heuristics import makecleanlist 
from pipeline.hif.heuristics import clean

LOG = infrastructure.get_logger(__name__)



class MakeCleanListInputs(basetask.StandardInputs):

    def __init__(self, context, output_dir=None, vis=None, 
      imagename=None, intent=None, field=None, spw=None, 
      uvrange=None, mode=None, outframe=None,
      imsize=None, cell=None, phasecenter=None, nchan=None, start=None,
      width=None):

        self._init_properties(vars())

    # context, output_dir and vis setters/getters are handled by 
    # the base class 

    @property
    def spw(self):
        if self._spw is None:
            return ''
        return self._spw

    @spw.setter
    def spw(self, value):
        self._spw = value

    @property
    def intent(self):
        if self._intent is None:
            return 'TARGET'
        return self._intent

    @intent.setter
    def intent(self, value):
        self._intent = value

    @property
    def field(self):
        if self._field is None:
            return ''
        return self._field

    @field.setter
    def field(self, value):
        self._field = value

    @property
    def imagename(self):
        if self._imagename is None:
            return ''
        return self._imagename

    @imagename.setter
    def imagename(self, value):
         self._imagename = value

    @property
    def mode(self):
        if self._mode is None:
            if 'TARGET' in self.intent:
                return 'frequency'
            else:
                return 'mfs'
        return self._mode

    @mode.setter
    def mode(self, value):
        self._mode = value

    @property
    def outframe(self):
        if self._outframe is None:
            return 'LSRK'
        return self._outframe

    @outframe.setter
    def outframe(self, value):
         self._outframe = value

    @property
    def imsize(self):
        if self._imsize is None:
            return []
        return self._imsize

    @imsize.setter
    def imsize(self, value):
         self._imsize = value

    @property
    def cell(self):
        if self._cell is None:
            return []
        return self._cell

    @cell.setter
    def cell(self, value):
        self._cell = value

    @property
    def phasecenter(self):
        if self._phasecenter is None:
            return ''
        return self._phasecenter

    @phasecenter.setter
    def phasecenter(self, value):
        self._phasecenter = value

    # optional added parameters start here

    @property
    def nchan(self):
        if self._nchan is None:
            return 'default'
        return self._nchan

    @nchan.setter
    def nchan(self, value):
        self._nchan = value

    @property
    def start(self):
        if self._start is None:
            return 'default'
        return self._start

    @start.setter
    def start(self, value):
        self._start = value

    @property
    def width(self):
        if self._width is None:
            return 'default'
        return self._width

    @width.setter
    def width(self, value):
        self._width = value

    @property
    def uvrange(self):
        if self._uvrange is None:
            return 'default'
        return self._uvrange

    @uvrange.setter
    def uvrange(self, value):
        self._uvrange = value


class MakeCleanList(basetask.StandardTaskTemplate):
    Inputs = MakeCleanListInputs

    def is_multi_vis_task(self):
        return True

    def prepare(self):
        # this python class will produce a list of images to be calculated.
        inputs = self.inputs

        # make sure inputs.vis is a list, even it is one that contains a
        # single measurement set
        if type(inputs.vis) is not types.ListType:
            inputs.vis = [inputs.vis]

        # read the spw, if none then set default depending on mode
        spw = inputs.spw
   
        #print 'before', spw
        if spw == '':
            ms = inputs.context.observing_run.get_ms(name=inputs.vis[0])
            spws = ms.get_spectral_windows(science_windows_only=True)
            spwids = [spw.id for spw in spws]
            spw = ','.join("'%s'" % (spwid) for spwid in spwids)
            spw = '[%s]' % spw
        #print 'after', spw, inputs.spw

        spwlist = spw.replace('[','').replace(']','')
        spwlist = spwlist[1:-1].split("','")
        #print 'main', spwlist

        # instantiate the heuristics classes needed, some sorting out needed
        # here to remove duplicated code
        self.heuristics = makecleanlist.MakeCleanListHeuristics(
          context=inputs.context, vislist=inputs.vis, spw=spw)

        # get list of field_ids/intents to be cleaned
        field_intent_list = self.heuristics.field_intent_list(
          intent=inputs.intent, field=inputs.field)

        # cell is a list of form [cellx, celly]. If the list has form [cell]
        # then that means the cell is the same size in x and y. If cell is
        # empty then fill it with a heuristic result
        cell = inputs.cell
        cells = {}
        beams = {}
        if cell == []:
            for spwspec in spwlist:
                # the heuristic cell is always the same for x and y as
                # the value derives from a single value returned by
                # imager.advise
                cells[spwspec], beams[spwspec] = self.heuristics.cell(
                  field_intent_list=field_intent_list, spwspec=spwspec)
        else:
            for spwspec in spwlist:
                cells[spwspec] = cell
        #print 'cells', cells

        # if phase center not set then use heuristic code to calculate the
        # centers for each field
        phasecenter = inputs.phasecenter
        if phasecenter == '':
            phasecenters = {}
            for field_intent in field_intent_list:
                try:
                    field_ids = self.heuristics.field(
                      field_intent[1], field_intent[0])
                    phasecenters[field_intent[0]] = \
                      self.heuristics.phasecenter(field_ids)
                except Exception, e:
                    # problem defining center
                    LOG.warn(e)
                    pass
        #print 'phasecenters', phasecenters

        # if imsize not set then use heuristic code to calculate the
        # centers for each field/spwspec
        imsize = inputs.imsize
        imsizes = {}
        if imsize == []:
            for field_intent in field_intent_list:
                for spwspec in spwlist:
                    try:
                        field_ids = self.heuristics.field(
                          field_intent[1], field_intent[0])
                        imsizes[(field_intent[0],spwspec)] = \
                          self.heuristics.imsize(fields=field_ids,
                          cell=cells[spwspec], beam=beams[spwspec])
                    except Exception, e:
                        # problem defining imsize
                        LOG.warn(e)
                        pass
 
        else:
            for field_intent in field_intent_list:
                for spwspec in spwlist:
                    imsizes[(field_intent[0],spwspec)] = imsize
        #print 'imsize', imsizes

        # construct imagename
        imagename = inputs.imagename
        imagenames = {}
        for field_intent in field_intent_list:
            for spwspec in spwlist:
                if inputs.imagename == '':
                    imagenames[(field_intent,spwspec)] = \
                      self.heuristics.imagename(
                      output_dir=inputs.output_dir, intent=field_intent[1],
                      field=field_intent[0], spwspec=spwspec)
                else:
                    imagenames[(field_intent,spwspec)] = inputs.imagename
        #print 'imagenames', imagenames

        # now construct the list of imaging command parameter lists that must
        # be run to obtain the required images
        result = MakeCleanListResult()
        for field_intent in field_intent_list:
            for spwspec in spwlist:
                if imsizes.has_key((field_intent[0],spwspec)):
                    LOG.debug (
                      'field:%s intent:%s spw:%s cell:%s imsize:%s phasecenter:%s'
                      % (field_intent[0], field_intent[1], spwspec,
                      cells[spwspec], imsizes[(field_intent[0],spwspec)],
                      phasecenters[field_intent[0]]))

                    target = {'field':field_intent[0],
                              'intent':field_intent[1],
                              'spw': spwspec,
                              'cell':cells[spwspec],
                              'imsize':imsizes[(field_intent[0],spwspec)],
                              'phasecenter':phasecenters[field_intent[0]],
                              'mode':inputs.mode,
                              'imagename':imagenames[(field_intent,spwspec)]}

                    # optional parameters that will override the defaults of
                    # the clean task if set
                    if inputs.start != 'default':
                        target['start'] = inputs.start
                    if inputs.width != 'default':
                        target['width'] = inputs.width
                    if inputs.nchan != 'default':
                        target['nchan'] = inputs.nchan
                    if inputs.uvrange != 'default':
                        target['uvrange'] = inputs.uvrange

                    #print target
                    result.add_target(target)

                else:
                    LOG.warn (
                      'failed to define image for field:%s intent:%s spw:%s'
                      % (field_intent[0], field_intent[1], spwspec))

        return result

    def analyse(self, result):
        return result

