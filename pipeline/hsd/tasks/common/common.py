"""
This module defines common single dish Results and task container Classes.

Classes and Methods:
    absolute_path: A method to convert a path to an absolute path.
    SingleDishResults: Common single dish Results class.
    ParameterContainerJob: A container class to store task with parameters. 
"""
import os

from typing import Any, Optional

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask

LOG = infrastructure.get_logger(__name__)


def absolute_path(name: str) -> str:
    """Return an abcolute path of a given file."""
    return os.path.abspath(os.path.expanduser(os.path.expandvars(name)))


class SingleDishResults(basetask.Results):
    """
    Single Dish Results class.
    
    This class inherits a common Results class,
    pipeline.infrastructre.basetask.Results. See the documentation of the
    super class for details. The class is usually used as a superclass of
    task specific Results class.
    
    Attributes:
        task: A task class associated with this class.
        success: A boolean to indicate if the task execution was successful
            (True) or not (False).
        outcome: Outcome of the task.
        error: A set of strings to store error messages.
    """
    
    def __init__(self, task: Optional[basetask.StandardTaskTemplate]=None,
                 success: Optional[bool]=None, outcome: Optional[dict]=None):
        """
        Initialize class attributes and super class.
        
        Args:
            task: A taks class object.
            success: True if the task completes successfully. Otherwise, False.
            outcome: Outcome of the task.
        """
        super(SingleDishResults, self).__init__()
        self.task = task
        self.success = success
        self.outcome = outcome
        self.error = set()

    def merge_with_context(self, context: infrastructure.launcher.Context):
        """
        Merge these results with the given context.
        
        See the documenetation of super class for more details.
        """
        self.error.clear()

    def _outcome_name(self) -> str:
        """Return an absolute path of outcome stored as a file."""
        # usually, outcome is a name of the file
        return absolute_path(self.outcome)

    def _get_outcome(self, key: str) -> Any:
        """
        Return a value in outcome dictionary.
        
        Args:
            key: A key of outcome dictionary to get a value.
         
        Retruns:
            The value in outcome dictionary obtained by a given key.
            This method returns None, if the outcome is not a dictionary.
        """
        if isinstance(self.outcome, dict):
            return self.outcome.get(key, None)
        else:
            return None

    def __repr__(self) -> str:
        """Return a printable representation of the class."""
        # taskname = self.task if hasattr(self,'task') else 'none'
        s = '%s:\n\toutcome is %s' % (self.__class__.__name__, self._outcome_name())
        return s


### THIS CLASS WILL BE REMOVED SHORTLY.
### IGNORE IN CODE REVIEW.
class ParameterContainerJob(object):
    """
    A container class to store task class object with parameter values.
    
    Attributes:
        task: A task class object to execute.
        parameters: A disctionary of task parameters (key: parameter name,
            value: parameter value) to invoke task.
    """
    
    def __init__(self, task: basetask.StandardTaskTemplate, **parameters):
        """
        Initialize class attributes.
        
        Args:
            task: Task to be executed.
            **parameters: Task parameters and their values to invoke task.
        """
        self.task = task
        self.parameters = parameters

    def execute(self, dry_run: bool=True) -> basetask.Results:
        """
        Invoke task using given parameters.
        
        Args:
            dry_run: Run task if False, or only display commands if True. 
        
        Retruns:
            A Result object of the task.
        """
        result = self.task.execute(dry_run, **self.parameters)
        return result
