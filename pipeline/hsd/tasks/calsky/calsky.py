from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.basetask as basetask
#import pipeline.infrastructure.logging as logging
from pipeline.infrastructure import casa_tasks
from .. import common

LOG = infrastructure.get_logger(__name__)
#logging.set_logging_level('trace')

class SDCalSkyInputs(common.SingleDishInputs):
    """
    Inputs for single dish calibraton
    """
    def __init__(self, context, output_dir=None,
                 infiles=None, outfile=None, calmode=None, iflist=None,
                 scanlist=None, pollist=None):
        self._init_properties(vars())            

    def to_casa_args(self):
        args = super(SDCalSkyInputs,self).to_casa_args()

        # take iflist from observing_run (shouldbe ScantableList object)
        if len(args['iflist']) == 0:
            # filter out WVR
            args['iflist'] = self.context.observing_run.get_spw_without_wvr(args['infile'])
        else:
            spw_list = set(self.context.observing_run.get_spw_without_wvr(args['infile']))
            args['iflist'] = list(spw_list.intersection(args['iflist']))
            

        # take calmode
        if args['calmode'] is None or args['calmode'].lower() == 'auto':
            args['calmode'] = self.context.observing_run.get_calmode(args['infile'])
        
        # always overwrite existing data
        args['overwrite'] = True

        # output file
        if args['outfile'] is None or len(args['outfile']) == 0:
            suffix = '_sky'
            args['outfile'] = args['infile'].rstrip('/') + suffix

        return args


class SDCalSkyResults(common.SingleDishResults):
    def __init__(self, task=None, success=None, outcome=None):
        super(SDCalSkyResults,self).__init__(task, success, outcome)

    def merge_with_context(self, context):
        super(SDCalSkyResults,self).merge_with_context(context)
        calapp = self.outcome
        if calapp is not None:
            context.callibrary.add(calapp.calto, calapp.calfrom)
        
    def _outcome_name(self):
        # usually, outcome is a name of the file
        return self.outcome.__str__()
    
class SDCalSky(common.SingleDishTaskTemplate):
    Inputs = SDCalSkyInputs

    def prepare(self):
        # inputs
        inputs = self.inputs

        # if infiles is a list, call prepare for each element
        if isinstance(inputs.infiles, list):
            result = basetask.ResultsList()
            infiles = inputs.infiles[:]
            for infile in infiles:
                inputs.infiles = infile
                result.append(self.prepare())
            # do I need to restore self.inputs.infiles?
            inputs.infiles = infiles[:]
            return result

        # In the following, inputs.infiles should be a string,
        # not a list of string
        args = inputs.to_casa_args()

        if args['calmode'] == 'none':
            # Return empty Results object if calmode='none'
            LOG.info('Calibration is already done for scantable %s'%(args['infile'])) 
            result = SDCalSkyResults(task=self.__class__,
                                     success=True,
                                     outcome=None)
        else:                
            # input file
            args['infile'] = os.path.join(inputs.output_dir, args['infile'])

            # output file
            args['outfile'] = os.path.join(inputs.output_dir, args['outfile'])

            # print calmode
            LOG.info('calibration type is \'%s\' (type=%s)'%(args['calmode'],type(args['calmode'])))

            # create job
            job = casa_tasks.sdcal2(**args)

            # execute job
            self._executor.execute(job)

            # create CalTo object
            # CalTo object is created using associating MS name
            basename = os.path.basename(args['infile'].rstrip('/'))
            scantable = inputs.context.observing_run.get_scantable(basename)
            spw = callibrary.SDCalApplication.iflist_to_spw(args['iflist'])
            calto = callibrary.CalTo(vis=scantable.ms_name,
                                     spw=spw,
                                     antenna=scantable.antenna.name)

            # create SDCalFrom object
            calfrom = callibrary.SDCalFrom(gaintable=args['outfile'],
                                           interp='',
                                           caltype='sky')

            # create SDCalApplication object
            calapp = callibrary.SDCalApplication(calto, calfrom)

            # create result object
            result = SDCalSkyResults(task=self.__class__,
                                     success=True,
                                     outcome=calapp)
        result.task = self.__class__

        if inputs.context.subtask_counter is 0: 
            result.stage_number = inputs.context.task_counter - 1
        else:
            result.stage_number = inputs.context.task_counter               

        return result

    def analyse(self, result):
        return result

