# CASA-Python Dependency List

Note: this is a work in progress, started from a copy of `pipeline_imports.txt` posted on PIPE-938 and has been updated to reflect the current state of the code.

## Python built-in

```python
from __future__ import print_function  # prevents adding old-style print statements
import abc
import array
import bisect
import collections
import copy
import datetime
import decimal
import functools
import gc
import inspect
import itertools
import logging
import math
import operator
import os
import random
import re
import string
import struct
import typing
import unittest.mock
import xml
import _testcapi
import abc
import argparse
import ast
import atexit
import base64
import bisect
import bz2
import contextlib
import copy
import copyreg
import csv
import ctypes
import datetime
import decimal
import distutils
import enum
import errno
import fnmatch
import functools
import getopt
import glob
import hashlib
import html
import http
import urllib.parse
import inspect
import io
import itertools
import json
import logging
import math
import multiprocessing
import numbers
import operator
import os
import pathlib
import pickle
import pkg_resources
import platform
import pprint
import pydoc
import pytest
import queue
import random
import re
import resource
import setuptools
import shutil
import ssl
import string
import subprocess
import sys
import tarfile
import tempfile
import textwrap
import threading
import time
import traceback
import types
import typing
import unittest
import urllib
import uuid
import warnings
import weakref
import webbrowser
from importlib import import_module
```

## casatasks/casatools

```python
from casampi.MPICommandClient import MPICommandClient
from casampi.MPIEnvironment import MPIEnvironment

from casatasks import casalog
from casatasks import imcollapse
from casatasks import imhead
from casatasks import immath
from casatasks import immoments
from casatasks import imregrid
from casatasks import imsmooth
from casatasks import imstat
from casatasks import imsubimage
from casatasks import makemask

from casatasks.private import flaghelper
from casatasks.private import simutil
from casatasks.private import solar_system_setjy as ss_setjy
from casatasks.private.callibrary import applycaltocallib
from casatasks.private.imagerhelpers.imager_base import PySynthesisImager
from casatasks.private.imagerhelpers.imager_parallel_continuum import PyParallelContSynthesisImager
from casatasks.private.imagerhelpers.input_parameters import ImagerParameters
import casatasks.private.sdbeamutil as sdbeamutil
from casatasks.private import tec_maps
from casashell.private.stack_manip import find_frame

from casaplotms import plotms
import casaplotms.private.plotmstool as plotmstool

from almatask import wvrgcal

from casatools import atmosphere as attool
from casatools import calibrater, ms, table
from casatools import image as iatool
from casatools import measures as metool
from casatools import ms as mstool
from casatools import msmetadata as msmdtool
from casatools import quanta as qatool
from casatools import synthesismaskhandler
from casatools import table as tbtool

```

## 3rd-party Packages from CASA (site-packages/)

```python
import matplotlib
import numpy
import scipy
import pyfits
import pyparsing
```

## 3rd-party Packages not included in the monolithic CASA

```python
import cachetools
import mako
import pypubsub
import intervaltree
import logutils
import ps_mem
import astropy
import bdsf
import csscompressor # (setup.py)
```

## Shell Environment Variables

```console
FLUX_SERVICE_URL
FLUX_SERVICE_URL_BACKUP
SCIPIPE_HEURISTICS
SCIPIPE_ROOTDIR
WEBLOG_RERENDER_STAGES
PIPE356_QA_MODE

ENABLE_TIER0_PLOTMS # infrastructure.mpihelpers.ENABLE_TIER0_PLOTMS
DISABLE_CASA_CALLIBRARY # mpihelpers.ENABLE_TIER0_PLOTMS 
DISABLE_CASA_CALLIBRARY # diable the usage of CASA callibrary and revert to non-callibrary applycal call.
WEBLOG_RERENDER_STAGES # the weblog stages to be rerendered.
```
