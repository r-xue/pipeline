"""This module is imported early on for a Pipeline process and first casatask
impost happens here so we will have a chance to fiddled the casaconfig before
casatasks configuration get intialized."""

import copy
import yaml
import os
from typing import Optional

_default_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'pipeconfig.yaml'))


def get_default_config():
    with open(_default_config_path) as f:
        return yaml.safe_load(f)


def casatasks_startup(casa_config: dict[str, Optional[str]], loglevel: Optional[str] = None) -> tuple[str, str]:
    """Initializes a CASA session with custom configurations and log settings.

    This function updates the casaconfig attributes, sets the CASA log file, and adjusts
    the log filtering level for casalogsink.

    Args:
        casa_config: A dictionary containing CASA configuration attributes. Keys are
                     attribute names (e.g., 'logfile', 'nworkers'), and values are the
                     desired settings. Values can be None, in which case the attribute
                     is not modified.
        loglevel: Optional pipeline log level string:
                        critical, error, warning, attention, info, debug, todo, trace.
                  If provided, the CASA log filter level is adjusted accordingly. If None,
                  casa loglevel defaults to 'INFO1'.

    Returns:
        A tuple containing:
            - The path to the CASA log file (str).
            - The CASA log filter level (str).
    """
    # load the default coniguration fom the package (pipeconfig.yaml)
    _default_config = get_default_config()
    config = copy.deepcopy(_default_config)

    # load any configuration inheriated from the dask client
    try:
        import dask
        config['casaconfig'].update(dask.config.config.get('casaconfig', {}))
    except ImportError:
        pass

    # Import casaconfig module
    import casaconfig.config

    # Update casaconfig attributes
    for key, value in config['casaconfig'].items():
        if hasattr(casaconfig.config, key) and value is not None:
            setattr(casaconfig.config, key, value)
    # Initial import of casatasks with modified casaconfig setup
    import casatasks

    # Get the current casalogfile
    casalogfile = casatasks.casalog.logfile()

    # Adjust log filtering level
    casaloglevel = 'INFO1'
    casatasks.casalog.filter(casaloglevel)
    # print('current:', casalogfile, casaloglevel)
    # Another approach is using environment variable / Workplugin for initialization
    # But they will likely arrive after the import
    # Here, we get configuration directl during Piipeline importing process.

    # need the latest/actual casalogfile path to forward to worker
    config['casaconfig']['logfile'] = casalogfile

    return config


config = casatasks_startup({})




