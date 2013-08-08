from __future__ import absolute_import

import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
from pipeline.domain.datatable import DataTableImpl as DataTable
from . import utils

LOG = infrastructure.get_logger(__name__)

def absolute_path(name):
    return os.path.abspath(os.path.expanduser(os.path.expandvars(name)))

class SingleDishInputs(basetask.StandardInputs):
    def __init__(self, context, output_dir=None,
                 infiles=None):
        self._init_properties(vars())

    def to_casa_args(self):
        args = self._get_task_args(ignore=('infiles','vis','output_dir'))
        if isinstance(self.infiles, list):
            args['infile'] = self.infiles[0]
        else:
            args['infile'] = self.infiles
        keys = ('iflist','pollist','scanlist')
        for (k,v) in args.items():
            if k in keys and v is None:
                args[k] = []
            
        return args

    @property
    def infiles(self):
        if self._infiles is not None:
            return self._infiles

        st_names = self.context.observing_run.st_names
        return st_names[0] if len(st_names) == 1 else st_names

    @infiles.setter
    def infiles(self, value):
        if isinstance(value, list):
            for v in value:
                self.context.observing_run.st_names.index(v)
        elif isinstance(value, str):
            if value.startswith('['):
                # may be PPR input (string list as string)
                _value = utils.to_list(value)
                for v in _value:
                    self.context.observing_run.st_names.index(v)
            else:
                self.context.observing_run.st_names.index(value)
            
        LOG.debug('Setting Input._infiles to %s'%(value))
        self._infiles = value

    @property
    def output_dir(self):
        if self._output_dir is None:
            return self.context.output_dir
        else:
            return self._output_dir

    @output_dir.setter
    def output_dir(self, value):
        self._output_dir = value

    # This is dummy
    @property
    def vis(self):
        return None

    def _to_list(self, attributes):
        if isinstance(attributes, str):
            new_value = utils.to_list(getattr(self, attributes))
            setattr(self, attributes, new_value)
        else:
            for attr in attributes:
                new_value = utils.to_list(getattr(self, attr))
                setattr(self, attr, new_value)

    def _to_bool(self, attributes):
        if isinstance(attributes, str):
            new_value = utils.to_bool(getattr(self, attributes))
            setattr(self, attributes, new_value)
        else:
            for attr in attributes:
                new_value = utils.to_bool(getattr(self, attr))
                setattr(self, attr, new_value)

    def _to_numeric(self, attributes):
        if isinstance(attributes, str):
            new_value = utils.to_numeric(getattr(self, attributes))
            setattr(self, attributes, new_value)
        else:
            for attr in attributes:
                new_value = utils.to_numeric(getattr(self, attr))
                setattr(self, attr, new_value)
        

class SingleDishResults(basetask.Results):
    def __init__(self, task=None, success=None, outcome=None):
        super(SingleDishResults, self).__init__()
        self.task = task
        self.success = success
        self.outcome = outcome
        self.error = set()
        
    def merge_with_context(self, context):
        self.error.clear()

    def _outcome_name(self):
        # usually, outcome is a name of the file
        return absolute_path(self.outcome)

    def __repr__(self):
        #taskname = self.task if hasattr(self,'task') else 'none'
        s = '%s:\n\toutcome is %s'%(self.__class__.__name__,self._outcome_name())
        return s

class SingleDishTaskTemplate(basetask.StandardTaskTemplate):
    Inputs = SingleDishInputs

    def __init__(self, inputs):
        super(SingleDishTaskTemplate,self).__init__(inputs)
        self._setup_datatable()
        self._inspect_casa_version()

    def _setup_datatable(self):
        context = self.inputs.context
        observing_run = context.observing_run
        #data_table = observing_run.datatable_instance
        #if data_table is None:
        #    name = observing_run.datatable_name
        #    if name is not None and os.path.exists(name):
        #        LOG.debug('Import DataTable from disk')
        #        data_table = DataTable(name)
        #self.DataTable = data_table
        #observing_run.datatable_instance = self.DataTable
        #observing_run.datatable_instance = DataTable(observing_run.datatable_name)
        
    def _inspect_casa_version(self):
        import inspect
        import sys
        a = inspect.stack()
        stacklevel = 0
        for i in range(len(a)):
            if a[i][1].find( 'ipython console' ) != -1:
                stacklevel = i-1
                break
        casadict = sys._getframe(stacklevel+1).f_globals['casa']
        try:
            self.casa_revision = int( casadict['build']['number'] )
        except:
            self.casa_revision = -1
        self.casa_version_string = casadict['build']['version']
        self.casa_version = int( self.casa_version_string.replace('.','') )
