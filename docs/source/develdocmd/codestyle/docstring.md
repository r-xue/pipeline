# code / fonts presentation in docstring:


* In google-style numpy docstring, how do you refer / format a function name?

```python
def my_function(data):
    """Process data using advanced algorithms.
    
    This function calls `numpy.mean` and `scipy.optimize.minimize`
    internally. It's similar to `pandas.DataFrame.apply` but 
    optimized for numerical arrays.
    
    Args:
        data (array_like): Input data array.
        
    Returns:
        float: Processed result.
        
    See Also:
        `calculate_stats`: Related function for statistical analysis.
        `preprocess_data`: Use this to prepare data before calling
            this function.
    """
```

* For a variable's value, you should use inline code (formatted with double backticks ).

This is the standard convention because it visually represents the value as a literal, just as it would appear in code.

inline code (Recommended) : 
```python

``xyz=a``

```


Use this for literal values, variable names, and filenames. It clearly distinguishes code from prose.
 
    """Set the ``hm_phaseup`` parameter to ``'snr'`` or ``'manual'``."""

* italics: *xyz*

Use italics for emphasis or when referring to a variable name in a narrative way. It's not typically used for the value itself.

for file paths.
    """The *hm_phaseup* parameter controls the phase-up heuristics."""

* bold: **xyz**

Use bold for strong emphasis. It's generally too strong for just a variable value and should be reserved for important warnings or notes.

    """Changing this parameter is **not recommended** for most users."""

* For a list of items, use a bulleted list with hyphens or asterisks.

    """python
    Args:
        param1 (int): Description of param1.
        
        chantol: The tolerance in channels for mapping atmospheric calibration windows (TDM) to science windows (FDM or TDM).

            Example: ``chantol=5``

        parallel: Process multiple MeasurementSets in parallel using the casampi parallelization framework.
                
            Options: ``'automatic'``, ``'true'``, ``'false'``, ``True``, ``False``
                
            Default: ``None`` (equivalent to ``False``)
        
        param2 (str): Description of param2.
        param3 (list): Description of param3.

    Examples: 
        is a special directives (don't use anywhere, and should be on the same levels of args and returns)
    ```


def my_function(name: str, greeting: str = "Hello"):
    """Greets a person.

    Args:
        name (str): The name of the person to greet.
        greeting (str, optional): The greeting to use. Defaults to "Hello".

    Returns:
        str: The complete greeting.
    """
    return f"{greeting}, {name}!"


def my_function(name: str, greeting: str = "Hello"):
    """Greets a person.

    Args:
        name (str): The name of the person to greet.
        
        greeting (str, optional): The greeting to use. Defaults to "Hello".

        Example: youcan still use this but just plain text
            test
        
        Default:
        
            yest

    Returns:
        str: The complete greeting.
    """
    return f"{greeting}, {name}!"

def add(a: int, b: int) -> int:
    """Adds two integers together.

    Args:
        a (int): The first integer.
        b (int): The second integer.

    Returns:
        int: The sum of the two integers.

    Examples:
        An example of using the add function::

            >>> add(2, 3)
            5
            >>> add(-1, 1)
            0
    """
    return a + b


    Empty line does mean a new line.


    Note `xyz` are usually used as a ineline blocking

    :math:`A = \pi r^2`



    def check_sn(rms, peak, factor):
    """
    Check whether the S/N condition is satisfied.

    The condition is:

    :math:`\frac{\text{Peak S/N}}{\text{dividing\_factor}} \times \mathrm{RMS} < 5.0`
    """
    ...


    def check_sn(rms, peak, factor):
    """
    Check whether the S/N condition is satisfied.

    The condition is:

    .. math::

        \frac{\text{Peak S/N}}{\text{dividing\_factor}} \times \mathrm{RMS} < 5.0
    """
    ...



    Previous calibrations that have been stored in the pipeline context are
    applied on the fly. Users can interact with these calibrations via the
    h_export_calstate and h_import_calstate tasks.    