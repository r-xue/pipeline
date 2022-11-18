# PIPE-938

note: this is a work in progress, just a copy of pipeline_imports.txt posted in the ticket for now.
```
# python built-in
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

# setup.py
import csscompressor

# /pipeline/extern/
import mako
import cachetools
import intervaltree
import sortedcontainers
import XmlObjectifier
	import interactive
import md5  # ps_mem.py

# /pipeline/extern/logutils/
import redis
import Queue
import cPickle as pickle

# 3rd party CASA /site-packages/
import matplotlib
import numpy
import scipy
import pyfits
import pyparsing

# findcontinuum
import taskinit

# CASA
from casampi.MPICommandClient import MPICommandClient
from casampi.MPIEnvironment import MPIEnvironment
from casarecipes import tec_maps
from casashell.private.stack_manip import find_frame
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
from casatools import atmosphere as attool
from casatools import calibrater, ms, table
from casatools import image as iatool
from casatools import measures as metool
from casatools import ms as mstool
from casatools import msmetadata as msmdtool
from casatools import quanta as qatool
from casatools import synthesismaskhandler
from casatools import table as tbtool
import casa as mycasa
import casadef
from imcollapse_cli import imcollapse_cli as imcollapse
from imhead_cli import imhead_cli as imhead
from immath_cli import immath_cli as immath # only used if pbcube is not passed and no emission is found
from immoments_cli import immoments_cli as immoments
from importlib import import_module
from imregrid_cli import imregrid_cli as imregrid
from imsmooth_cli import imsmooth_cli as imsmooth
from imstat_cli import imstat_cli as imstat  # used by computeMadSpectrum
from imsubimage_cli import imsubimage_cli as imsubimage
import almatasks
import makemask_cli
```