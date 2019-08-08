"""
This module contains a wrapper function for every CASA task. The signature of
each methods exactly matches that of the CASA task it mirrors. However, rather
than executing the task directly when these methods are called,
CASATaskJobGenerator returns a JobRequest for every invocation; these jobs
then be examined and executed at a later date.

The CASA task implementations are located at run-time and proxies for each
task attached to this class at runtime. The name and signature of each
method will match those of the tasks in the CASA environment when this
module was imported.
"""
from __future__ import absolute_import

import shutil

import applycal_cli
import bandpass_cli
import calstat_cli
import clean_cli
import clearcal_cli
import delmod_cli
import exportfits_cli
import flagcmd_cli
import flagdata_cli
import flagmanager_cli
import fluxscale_cli
import gaincal_cli
import gencal_cli
import hanningsmooth_cli
import imdev_cli
import imhead_cli
import immath_cli
import immoments_cli
import impbcor_cli
import importasdm_cli
import imregrid_cli
import imstat_cli
import imval_cli
import imsubimage_cli
import initweights_cli
import listobs_cli
import mstransform_cli
import partition_cli
import plotants_cli
import plotbandpass_cli
import plotms_cli
import plotweather_cli
import polcal_cli
import sdbaseline_cli
import sdcal_cli
import sdimaging_cli
import tsdimaging_cli
import setjy_cli
import split_cli
import statwt_cli
import tclean_cli
import visstat_cli
import wvrgcal_cli

from . import logging
from .jobrequest import JobRequest

LOG = logging.get_logger(__name__)


def applycal(*v, **k):
    return _get_job(applycal_cli.applycal_cli, *v, **k)


def bandpass(*v, **k):
    return _get_job(bandpass_cli.bandpass_cli, *v, **k)


def calstat(*v, **k):
    return _get_job(calstat_cli.calstat_cli, *v, **k)


def clean(*v, **k):
    return _get_job(clean_cli.clean_cli, *v, **k)


def clearcal(*v, **k):
    return _get_job(clearcal_cli.clearcal_cli, *v, **k)


def delmod(*v, **k):
    return _get_job(delmod_cli.delmod_cli, *v, **k)


def exportfits(*v, **k):
    return _get_job(exportfits_cli.exportfits_cli, *v, **k)


def gaincal(*v, **k):
    return _get_job(gaincal_cli.gaincal_cli, *v, **k)


def flagcmd(*v, **k):
    return _get_job(flagcmd_cli.flagcmd_cli, *v, **k)


def flagdata(*v, **k):
    return _get_job(flagdata_cli.flagdata_cli, *v, **k)


def flagmanager(*v, **k):
    return _get_job(flagmanager_cli.flagmanager_cli, *v, **k)


def fluxscale(*v, **k):
    return _get_job(fluxscale_cli.fluxscale_cli, *v, **k)


def gencal(*v, **k):
    return _get_job(gencal_cli.gencal_cli, *v, **k)


def hanningsmooth(*v, **k):
    return _get_job(hanningsmooth_cli.hanningsmooth_cli, *v, **k)


def imdev(*v, **k):
    return _get_job(imdev_cli.imdev_cli, *v, **k)


def imhead(*v, **k):
    return _get_job(imhead_cli.imhead_cli, *v, **k)


def immath(*v, **k):
    return _get_job(immath_cli.immath_cli, *v, **k)


def immoments(*v, **k):
    return _get_job(immoments_cli.immoments_cli, *v, **k)


def imregrid(*v, **k):
    return _get_job(imregrid_cli.imregrid_cli, *v, **k)


def impbcor(*v, **k):
    return _get_job(impbcor_cli.impbcor_cli, *v, **k)


def importasdm(*v, **k):
    return _get_job(importasdm_cli.importasdm_cli, *v, **k)


def imstat(*v, **k):
    return _get_job(imstat_cli.imstat_cli, *v, **k)


def imval(*v, **k):
    return _get_job(imval_cli.imval_cli, *v, **k)


def imsubimage(*v, **k):
    return _get_job(imsubimage_cli.imsubimage_cli, *v, **k)


def initweights(*v, **k):
    return _get_job(initweights_cli.initweights_cli, *v, **k)


def listobs(*v, **k):
    return _get_job(listobs_cli.listobs_cli, *v, **k)


def mstransform(*v, **k):
    return _get_job(mstransform_cli.mstransform_cli, *v, **k)


def partition(*v, **k):
    return _get_job(partition_cli.partition_cli, *v, **k)


def plotants(*v, **k):
    return _get_job(plotants_cli.plotants_cli, *v, **k)


def plotbandpass(*v, **k):
    return _get_job(plotbandpass_cli.plotbandpass_cli, *v, **k)


def plotms(*v, **k):
    return _get_job(plotms_cli.plotms_cli, *v, **k)


def plotweather(*v, **k):
    return _get_job(plotweather_cli.plotweather_cli, *v, **k)


def polcal(*v, **k):
    return _get_job(polcal_cli.polcal_cli, *v, **k)


def setjy(*v, **k):
    return _get_job(setjy_cli.setjy_cli, *v, **k)


def split(*v, **k):
    return _get_job(split_cli.split_cli, *v, **k)


def statwt(*v, **k):
    return _get_job(statwt_cli.statwt_cli, *v, **k)


def tclean(*v, **k):
    return _get_job(tclean_cli.tclean_cli, *v, **k)


def wvrgcal(*v, **k):
    return _get_job(wvrgcal_cli.wvrgcal_cli, *v, **k)


def visstat(*v, **k):
    return _get_job(visstat_cli.visstat_cli, *v, **k)


def uvcontfit(*v, **k):
    # Note this is pipeline CASA style task not a CASA task
    import pipeline.hif.cli.task_uvcontfit as task_uvcontfit
    return _get_job(task_uvcontfit.uvcontfit, *v, **k)


def sdimaging(*v, **k):
    return _get_job(sdimaging_cli.sdimaging_cli, *v, **k)


def tsdimaging(*v, **k):
    return _get_job(tsdimaging_cli.tsdimaging_cli, *v, **k)


def sdcal(*v, **k):
    return _get_job(sdcal_cli.sdcal_cli, *v, **k)


def sdbaseline(*v, **k):
    return _get_job(sdbaseline_cli.sdbaseline_cli, *v, **k)


def copyfile(*v, **k):
    return _get_job(shutil.copyfile, *v, **k)


def copytree(*v, **k):
    return _get_job(shutil.copytree, *v, **k)


def rmtree(*v, **k):
    return _get_job(shutil.rmtree, *v, **k)


def move(*v, **k):
    return _get_job(shutil.move, *v, **k)


def _get_job(task, *v, **k):
    return JobRequest(task, *v, **k)
